# storage_manager.py: Module for Data — Access Layer.
# Part of LeoBook Data — Access Layer
#
# Classes: StorageManager

import os
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from supabase import Client
from Data.Access.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)

class StorageManager:
    """
    Manages Supabase Storage operations for LeoBook assets.
    """
    def __init__(self, bucket_name: str = "logos"):
        self.supabase: Optional[Client] = get_supabase_client()
        self.bucket_name = bucket_name
        if not self.supabase:
            logger.warning("[!] StorageManager initialized without Supabase connection.")

    def _ensure_bucket_exists(self):
        """
        Check if the bucket exists, and attempt to create it if not.
        Note: This requires appropriate permissions (Service Role Key).
        """
        if not self.supabase:
            return False
            
        try:
            buckets = self.supabase.storage.list_buckets()
            exists = any(b.name == self.bucket_name for b in buckets)
            
            if not exists:
                logger.info(f"[*] Creating storage bucket: {self.bucket_name}")
                self.supabase.storage.create_bucket(self.bucket_name, options={"public": True})
                return True
            return True
        except Exception as e:
            logger.error(f"[x] Failed to check/create bucket '{self.bucket_name}': {e}")
            return False

    def upload_file(self, local_path: Path, remote_path: str, content_type: Optional[str] = None) -> Optional[str]:
        """
        Upload a file to Supabase Storage and return its public URL.
        """
        if not self.supabase or not local_path.exists():
            return None

        # Ensure bucket exists before first upload in a session
        self._ensure_bucket_exists()

        try:
            with open(local_path, 'rb') as f:
                file_content = f.read()
            
            # Use upsert=True to overwrite existing files
            self.supabase.storage.from_(self.bucket_name).upload(
                path=remote_path,
                file=file_content,
                file_options={"x-upsert": "true", "content-type": content_type}
            )
            
            # Generate public URL
            public_url = self.supabase.storage.from_(self.bucket_name).get_public_url(remote_path)
            return public_url
        except Exception as e:
            logger.error(f"[x] Failed to upload {local_path} to {remote_path}: {e}")
            return None

    def upload_batch(self, uploads: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Upload multiple files in sequential order.
        uploads: List of {'local_path': Path, 'remote_path': str}
        Returns: Map of local_path (str) -> public_url
        """
        results = {}
        for item in uploads:
            local = item.get('local_path')
            remote = item.get('remote_path')
            if local and remote:
                url = self.upload_file(local, remote)
                if url:
                    results[str(local)] = url
        return results
