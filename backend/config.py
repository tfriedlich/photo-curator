import os

def get_float(key, default):
    try:
        return float(os.environ.get(key, default))
    except:
        return float(default)

def get_int(key, default):
    try:
        return int(os.environ.get(key, default))
    except:
        return int(default)

def get_bool(key, default):
    return os.environ.get(key, str(default)).lower() == 'true'

# Google OAuth
GOOGLE_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
APP_BASE_URL         = os.environ.get("APP_BASE_URL", "https://photos.toddfriedlich.com")

# Anthropic
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Event detection
BASELINE_MULTIPLIER   = get_float("BASELINE_MULTIPLIER", 3)
MIN_PHOTOS_FOR_EVENT  = get_int("MIN_PHOTOS_FOR_EVENT", 20)
LOOKBACK_DAYS         = get_int("LOOKBACK_DAYS", 60)
MAX_LOOKBACK_DAYS     = get_int("MAX_LOOKBACK_DAYS", 365)

# Curation pipeline
TARGET_KEEP_RATIO        = get_float("TARGET_KEEP_RATIO", 0.20)
MIN_SCORE                = get_float("MIN_SCORE", 5.0)
BURST_WINDOW_SECONDS     = get_int("BURST_WINDOW_SECONDS", 30)
DUPLICATE_HASH_THRESHOLD = get_int("DUPLICATE_HASH_THRESHOLD", 15)
MIN_BLUR_SCORE           = get_float("MIN_BLUR_SCORE", 20)
MIN_EXPOSURE_SCORE       = get_float("MIN_EXPOSURE_SCORE", 0.15)

# Location
AIRPORT_FILTER   = get_bool("AIRPORT_FILTER", True)
HOME_LAT         = get_float("HOME_LAT", 40.8448)
HOME_LNG         = get_float("HOME_LNG", -73.6985)
HOME_RADIUS_KM   = get_float("HOME_RADIUS_KM", 10)
