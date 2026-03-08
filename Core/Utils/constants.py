import os
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# ── Timezone: Africa/Lagos (WAT = UTC+1) ────────────────────────────────────
# All timestamps across the system MUST use this timezone for consistency
# regardless of platform (local Windows, GitHub Codespaces, Supabase).
TZ_NG = timezone(timedelta(hours=1))  # West Africa Time (WAT)

def now_ng() -> datetime:
    """Return current Nigerian time (Africa/Lagos, WAT = UTC+1)."""
    return datetime.now(TZ_NG)

# Timeout Constants (in milliseconds)
NAVIGATION_TIMEOUT = 180000  # 3 minutes for page navigation
WAIT_FOR_LOAD_STATE_TIMEOUT = 90000  # 1.5 minutes for load state operations
STANDINGS_LOAD_TIMEOUT = 20000  # 20 seconds for standings (supplementary data)

# Financial Settings
DEFAULT_STAKE = float(os.getenv("DEFAULT_STAKE", 1.0))
CURRENCY_SYMBOL = os.getenv("CURRENCY_SYMBOL", "$")

# Concurrency Control
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", 1))

# Browser / Mobile Settings
FB_MOBILE_USER_AGENT = "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
FB_MOBILE_VIEWPORT = {'width': 375, 'height': 612}

