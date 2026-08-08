#!/usr/bin/env bash
# ============================================================
#  Run the MT5 bot through Wine's Python on a headless VPS.
#  Wraps xvfb-run + wine + the Windows Python so cron can call it.
#
#  USAGE:
#     ./run_mt5.sh test
#     ./run_mt5.sh backtest 90
#     ./run_mt5.sh scan
#     ./run_mt5.sh loop
#
#  CRON (scan every 5 min during the session, Mon-Fri):
#     */5 7-15 * * 1-5  /home/USER/SM/run_mt5.sh scan >> /home/USER/SM/cron.log 2>&1
#  (Times are the SERVER clock - keep the VPS on UTC so the 07:00-15:00
#   session gate lines up, or adjust the hour range to your timezone.)
# ============================================================
set -euo pipefail

export WINEPREFIX="${WINEPREFIX:-$HOME/.mt5}"
export WINEARCH="${WINEARCH:-win64}"
export WINEDEBUG="${WINEDEBUG:--all}"
# Windows-Python path inside the Wine prefix (Python310 shipped pip out-of-box).
WINPY="${WINPY:-C:/Python310/python.exe}"
# MT5 terminal path (Windows-style) - initialize() launches it headless.
export MT5_PATH="${MT5_PATH:-C:/Program Files/MetaTrader 5/terminal64.exe}"

# Headless MT5 login (fill these or export them in the environment /
# a systemd unit). Leave blank to use a terminal you logged into manually.
export MT5_LOGIN="${MT5_LOGIN:-}"
export MT5_PASSWORD="${MT5_PASSWORD:-}"
export MT5_SERVER="${MT5_SERVER:-ICMarketsSC-Demo}"
export XAU_MT5_SYMBOL="${XAU_MT5_SYMBOL:-XAUUSD}"

# Path to the bot next to this script.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOT="${BOT:-$HERE/xau_mt5_bot.py}"

# Wine needs a Windows-style path to the script; map the Linux path via winepath.
WIN_BOT="$(WINEPREFIX="$WINEPREFIX" wine winepath -w "$BOT" 2>/dev/null | tr -d '\r')"

exec xvfb-run -a wine "$WINPY" "$WIN_BOT" "$@"
