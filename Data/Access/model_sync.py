# model_sync.py: Upload/download RL model files to/from Supabase Storage.
# Part of LeoBook Data — Access Layer
#
# Classes: ModelSync
# Usage:
#   ModelSync.push()  → uploads Data/Store/models/ → Supabase "models" bucket
#   ModelSync.pull()  → downloads Supabase "models" bucket → Data/Store/models/

import os
import sys
import time
import logging
import threading
from pathlib import Path
from typing import Optional, List, Dict, Any

from tqdm import tqdm

from Data.Access.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)

# Files to sync (relative to models dir)
MODEL_FILES = [
    "leobook_base.pth",
    "adapter_registry.json",
    "training_config.json",
    "phase1_latest.pth",
    "phase2_latest.pth",
    "phase3_latest.pth",
]

BUCKET_NAME = "models"
PROJECT_ROOT = Path(__file__).parent.parent.parent
MODELS_DIR = PROJECT_ROOT / "Data" / "Store" / "models"

# Files above this size (MB) get a warning and progress indicator
LARGE_FILE_THRESHOLD_MB = 50

# Files above this size will be chunked to bypass Supabase limits (Free tier default is 50MB)
MAX_SINGLE_FILE_SIZE_MB = 40 
CHUNK_SIZE_BYTES = MAX_SINGLE_FILE_SIZE_MB * 1024 * 1024


def _fmt_size(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.0f} KB"
    return f"{size_bytes / (1024 * 1024):.1f} MB"


def _fmt_elapsed(seconds: float) -> str:
    """Format elapsed time."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    return f"{seconds / 60:.1f}m"


class ModelSync:
    """Upload and download RL model files to/from Supabase Storage."""

    def __init__(self, skip_large: bool = False, all_checkpoints: bool = False):
        """
        Args:
            skip_large: If True, skip files > LARGE_FILE_THRESHOLD_MB during push.
            all_checkpoints: If True, sync everything in checkpoints/ folder.
        """
        self.supabase = get_supabase_client()
        self.skip_large = skip_large
        self.all_checkpoints = all_checkpoints
        if not self.supabase:
            raise RuntimeError("Supabase client not available. Check SUPABASE_URL and SUPABASE_KEY in .env")
        self._ensure_bucket()

    def _ensure_bucket(self):
        """Create the models bucket if it doesn't exist."""
        try:
            buckets = self.supabase.storage.list_buckets()
            exists = any(b.name == BUCKET_NAME for b in buckets)
            if not exists:
                print(f"  [ModelSync] Creating storage bucket: '{BUCKET_NAME}'")
                self.supabase.storage.create_bucket(BUCKET_NAME, options={"public": False})
                print(f"  [ModelSync] ✓ Bucket created")
        except Exception as e:
            logger.warning(f"  [ModelSync] Bucket check/create warning: {e}")

    def _list_local_files(self) -> List[Path]:
        """Find all model files that exist locally, skipping redundant checkpoints."""
        files = []

        # 1. Grab major aliases (latest models)
        for name in MODEL_FILES:
            p = MODELS_DIR / name
            if p.exists():
                files.append(p)

        # 2. Add checkpoint files ONLY if specifically requested
        if self.all_checkpoints:
            ckpt_dir = MODELS_DIR / "checkpoints"
            if ckpt_dir.exists():
                for p in sorted(ckpt_dir.glob("*.pth"), reverse=True):
                    files.append(p)

        return files

    def push(self):
        """Upload all local model files to Supabase Storage."""
        files = self._list_local_files()
        if not files:
            print("  [ModelSync] No model files found in Data/Store/models/. Nothing to push.")
            return

        total_size = sum(f.stat().st_size for f in files)
        print(f"\n  [ModelSync] PUSH: {len(files)} file(s), {_fmt_size(total_size)} total → Supabase Storage")

        # Show manifest
        for f in files:
            sz = f.stat().st_size
            flag = " ⚠ LARGE" if sz > LARGE_FILE_THRESHOLD_MB * 1024 * 1024 else ""
            print(f"    • {f.name} ({_fmt_size(sz)}){flag}")
        print()

        uploaded = 0
        skipped = 0

        for i, local_path in enumerate(files, 1):
            remote_path = str(local_path.relative_to(MODELS_DIR)).replace("\\", "/")
            size_bytes = local_path.stat().st_size
            size_mb = size_bytes / (1024 * 1024)
            is_large = size_mb > LARGE_FILE_THRESHOLD_MB

            # Skip large files if requested
            if self.skip_large and is_large:
                print(f"    [{i}/{len(files)}] ⊘ {remote_path} ({_fmt_size(size_bytes)}) — SKIPPED (--skip-large)")
                skipped += 1
                continue

            # Progress indicator with background polling thread
            t0 = time.time()
            try:
                file_size = local_path.stat().st_size
                
                # CHUNKING LOGIC: If file > limit, split and upload parts
                if file_size > CHUNK_SIZE_BYTES:
                    num_chunks = (file_size + CHUNK_SIZE_BYTES - 1) // CHUNK_SIZE_BYTES
                    
                    with tqdm(
                        total=file_size,
                        unit='B',
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=f"    [{i}/{len(files)}] 🧩 {remote_path} (Slicing {num_chunks} parts)",
                        leave=False
                    ) as pbar:
                        with open(local_path, "rb") as f:
                            for part_idx in range(num_chunks):
                                part_name = f"{remote_path}.part{part_idx}"
                                chunk_data = f.read(CHUNK_SIZE_BYTES)
                                
                                # Upload part
                                self.supabase.storage.from_(BUCKET_NAME).upload(
                                    path=part_name,
                                    file=chunk_data,
                                    file_options={"x-upsert": "true", "content-type": "application/octet-stream"},
                                )
                                pbar.update(len(chunk_data))
                else:
                    # Normal Single File Upload (small enough)
                    with open(local_path, "rb") as f:
                        stop_event = threading.Event()
                        with tqdm(
                            total=file_size,
                            unit='B',
                            unit_scale=True,
                            unit_divisor=1024,
                            desc=f"    [{i}/{len(files)}] {remote_path}",
                            leave=False
                        ) as pbar:
                            def poll_progress():
                                while not stop_event.is_set():
                                    try:
                                        curr = f.tell()
                                        pbar.n = curr
                                        pbar.refresh()
                                    except: pass
                                    time.sleep(0.2)
                            
                            monitor_thread = threading.Thread(target=poll_progress, daemon=True)
                            monitor_thread.start()
                            try:
                                self.supabase.storage.from_(BUCKET_NAME).upload(
                                    path=remote_path,
                                    file=f,
                                    file_options={"x-upsert": "true", "content-type": "application/octet-stream"},
                                )
                            finally:
                                stop_event.set()
                                monitor_thread.join(timeout=1.0)
                                pbar.n = file_size
                                pbar.refresh()

                elapsed = time.time() - t0
                uploaded += 1
                speed = size_mb / elapsed if elapsed > 0 else 0
                print(f"    [{i}/{len(files)}] ✓ {remote_path} ({_fmt_elapsed(elapsed)}, {speed:.1f} MB/s)")

            except Exception as e:
                print(f"    [{i}/{len(files)}] ✗ {remote_path} FAILED: {e}")

        print(f"\n  [ModelSync] Push complete: {uploaded} uploaded, {skipped} skipped, {len(files) - uploaded - skipped} failed.")

    def pull(self):
        """Download and reassemble model files from Supabase Storage."""
        print(f"\n  [ModelSync] PULL: Supabase Storage (bucket: '{BUCKET_NAME}') → Data/Store/models/")

        remote_paths = self._list_remote_files()
        if not remote_paths:
            print("  [ModelSync] No files found in remote bucket. Nothing to pull.")
            return

        # Group parts. Part-based files will have entries for each chunk.
        # We'll identify the "base" filename for reassembly.
        file_map = {} # base_path -> list of part_paths or single path
        for rp in remote_paths:
            if ".part" in rp:
                base = rp.split(".part")[0]
                if base not in file_map: file_map[base] = []
                file_map[base].append(rp)
            else:
                file_map[rp] = [rp]

        os.makedirs(MODELS_DIR, exist_ok=True)
        os.makedirs(MODELS_DIR / "checkpoints", exist_ok=True)
        downloaded_count = 0

        for i, (base_remote, parts) in enumerate(file_map.items(), 1):
            local_path = MODELS_DIR / base_remote.replace("/", os.sep)
            os.makedirs(local_path.parent, exist_ok=True)

            print(f"    [{i}/{len(file_map)}] ↓ {base_remote}", end="", flush=True)

            try:
                t0 = time.time()
                parts_list = list(parts)
                if len(parts_list) > 1 or (len(parts_list) == 1 and ".part" in parts_list[0]):
                    # Reassembly logic
                    parts_list.sort(key=lambda x: int(x.split(".part")[-1]))
                    with open(local_path, "wb") as final_f:
                        for p in parts_list:
                            res = self.supabase.storage.from_(BUCKET_NAME).download(p)
                            final_f.write(res)
                else:
                    # Standard single file download
                    res = self.supabase.storage.from_(BUCKET_NAME).download(base_remote)
                    with open(local_path, "wb") as f:
                        f.write(res)

                elapsed = time.time() - t0
                size_mb = local_path.stat().st_size / (1024 * 1024)
                speed = size_mb / elapsed if elapsed > 0 else 0
                downloaded_count += 1
                print(f" ✓ ({_fmt_size(local_path.stat().st_size)}, {_fmt_elapsed(elapsed)}, {speed:.1f} MB/s)")
            except Exception as e:
                print(f" ✗ FAILED: {e}")

        print(f"\n  [ModelSync] Pull complete: {downloaded_count}/{len(file_map)} items resolved at {MODELS_DIR}")

    def _list_remote_files(self, prefix: str = "") -> List[str]:
        """Recursively list all files in the remote bucket."""
        files = []
        try:
            items = self.supabase.storage.from_(BUCKET_NAME).list(prefix)
            for item in items:
                name = item.get("name", "")
                if item.get("metadata") is None or item.get("id") is None:
                    sub_prefix = f"{prefix}/{name}" if prefix else name
                    files.extend(self._list_remote_files(sub_prefix))
                else:
                    full_path = f"{prefix}/{name}" if prefix else name
                    files.append(full_path)
        except Exception as e:
            logger.error(f"  [ModelSync] Failed to list remote files: {e}")
        return files
