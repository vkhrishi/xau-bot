#!/usr/bin/env bash
# ============================================================
#  One-time MT5-under-Wine setup for a headless Hetzner Linux VPS
#  (Debian / Ubuntu). Installs Wine + Xvfb, the IC Markets MT5
#  terminal, and a Windows Python with the MetaTrader5 package,
#  all inside a dedicated Wine prefix so the bot can run headless.
#
#  USAGE (as the user that will run the bot, NOT necessarily root):
#     chmod +x deploy_mt5_wine.sh
#     ./deploy_mt5_wine.sh
#
#  After it finishes, log into MT5 once (GUI over the virtual
#  display) OR rely on headless login via MT5_LOGIN/PASSWORD/SERVER.
#  Then use run_mt5.sh to invoke the bot (and from cron).
# ============================================================
set -euo pipefail

# ---- Config (override via environment) ----
export WINEPREFIX="${WINEPREFIX:-$HOME/.mt5}"
export WINEARCH="${WINEARCH:-win64}"
WINPY_VER="${WINPY_VER:-3.13.1}"
WINPY_URL="${WINPY_URL:-https://www.python.org/ftp/python/${WINPY_VER}/python-${WINPY_VER}-amd64.exe}"
# Verify this URL in your IC Markets client portal - installer names change.
MT5_URL="${MT5_URL:-https://download.mql5.com/cdn/web/ic.markets.sc.pty/mt5/icmarketssc5setup.exe}"
WORK="${WORK:-$HOME/mt5_setup}"
WINPY="C:/Python313/python.exe"   # matches WINPY_VER major.minor

echo ">>> Wine prefix : $WINEPREFIX"
echo ">>> Work dir    : $WORK"
mkdir -p "$WORK"

# ---- 1. System packages (needs sudo) ----
echo ">>> Installing Wine + Xvfb + helpers (sudo required)..."
sudo dpkg --add-architecture i386
sudo apt-get update
sudo apt-get install -y --no-install-recommends \
    wine wine64 wine32 winbind xvfb xauth cabextract wget ca-certificates

# ---- 2. Initialise the Wine prefix ----
echo ">>> Initialising Wine prefix..."
xvfb-run -a wineboot --init
sleep 5

# ---- 3. Download installers ----
echo ">>> Downloading MT5 terminal and Windows Python..."
wget -q -O "$WORK/mt5setup.exe" "$MT5_URL"
wget -q -O "$WORK/winpython.exe" "$WINPY_URL"

# ---- 4. Install the MT5 terminal (silent) ----
echo ">>> Installing MT5 terminal (silent)..."
xvfb-run -a wine "$WORK/mt5setup.exe" /auto || \
    echo "    (If silent install failed, rerun without /auto to click through the GUI.)"
sleep 5

# ---- 5. Install Windows Python inside the prefix ----
echo ">>> Installing Windows Python $WINPY_VER inside Wine..."
xvfb-run -a wine "$WORK/winpython.exe" \
    /quiet InstallAllUsers=1 PrependPath=1 Include_test=0
sleep 5

# ---- 6. Install the MetaTrader5 package into Wine's Python ----
echo ">>> Installing MetaTrader5 into Wine Python..."
xvfb-run -a wine "$WINPY" -m pip install --upgrade pip
xvfb-run -a wine "$WINPY" -m pip install MetaTrader5

echo ""
echo ">>> DONE. Verify with:"
echo "    WINEPREFIX=$WINEPREFIX xvfb-run -a wine \"$WINPY\" -c \"import MetaTrader5 as m; print(m.__version__)\""
echo ""
echo ">>> Next: copy xau_mt5_bot.py to this box and run ./run_mt5.sh test"
