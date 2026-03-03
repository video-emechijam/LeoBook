#!/usr/bin/env bash
# =============================================================
# LeoBook — Codespace / VM Setup Script
# Installs ALL dependencies in one shot. Idempotent.
# =============================================================
set -euo pipefail

echo "=== LeoBook Setup ==="
echo "  Environment: $(uname -s) $(uname -m)"
echo "  Python:      $(python --version 2>&1)"

# ---- 1. Core Python Dependencies ----
echo ""
echo "[1/4] Installing core dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# ---- 2. RL / PyTorch (CPU-only, ~115 MB) ----
echo ""
echo "[2/4] Installing PyTorch CPU + RL dependencies..."
pip install -r requirements-rl.txt

# ---- 3. Playwright Browsers ----
echo ""
echo "[3/4] Installing Playwright browsers..."
playwright install chromium
playwright install-deps chromium 2>/dev/null || true

# ---- 4. Create Data Directories ----
echo ""
echo "[4/4] Creating data directories..."
mkdir -p Data/Store/models
mkdir -p Data/Store/Assets
mkdir -p Modules/Assets/logos
mkdir -p Modules/Assets/crests

# ---- Done ----
echo ""
echo "=== LeoBook Setup Complete ==="
echo "  Run:  python Leo.py --help"
echo "  RL:   python Leo.py --train-rl"
echo ""
