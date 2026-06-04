"""
config/settings.py
==================
مركز الإعدادات - كل القيم القابلة للتعديل موجودة هنا
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ==========================================
# إعدادات MEXC
# ==========================================
MEXC_API_KEY    = os.getenv("MEXC_API_KEY", "")
MEXC_API_SECRET = os.getenv("MEXC_API_SECRET", "")

# ==========================================
# إعدادات Telegram
# ==========================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

# ==========================================
# إعدادات GitHub
# ==========================================
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

# ==========================================
# شروط الفلترة
# ==========================================
MAX_DISTANCE_FROM_LOD    = float(os.getenv("MAX_DISTANCE_FROM_LOD", "0.10"))
MIN_DAILY_VOLUME_USD     = float(os.getenv("MIN_DAILY_VOLUME_USD", "5000000"))
RSI_OVERSOLD_THRESHOLD   = float(os.getenv("RSI_OVERSOLD_THRESHOLD", "35"))
RSI_PERIOD               = int(os.getenv("RSI_PERIOD", "14"))
LOD_DAYS                 = int(os.getenv("LOD_DAYS", "180"))
GITHUB_MAX_INACTIVE_DAYS = int(os.getenv("GITHUB_MAX_INACTIVE_DAYS", "90"))

# ==========================================
# إعدادات الصفقة
# ==========================================
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.08"))
TP1_PCT       = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT       = float(os.getenv("TP2_PCT", "0.60"))
TP3_PCT       = float(os.getenv("TP3_PCT", "1.00"))

TP1_QTY_PCT   = float(os.getenv("TP1_QTY_PCT", "0.40"))
TP2_QTY_PCT   = float(os.getenv("TP2_QTY_PCT", "0.35"))
TP3_QTY_PCT   = float(os.getenv("TP3_QTY_PCT", "0.25"))

TRADE_AMOUNT_USD = float(os.getenv("TRADE_AMOUNT_USD", "100"))
MAX_OPEN_TRADES  = int(os.getenv("MAX_OPEN_TRADES", "3"))

# ==========================================
# إعدادات التشغيل
# ==========================================
SCAN_INTERVAL_MINUTES = int(os.getenv("SCAN_INTERVAL_MINUTES", "60"))
LOG_FILE = "logs/bot.log"

# ==========================================
# التحقق من الإعدادات
# ==========================================
def validate_config():
    errors = []
    if not MEXC_API_KEY:
        errors.append("❌ MEXC_API_KEY غير موجود")
    if not MEXC_API_SECRET:
        errors.append("❌ MEXC_API_SECRET غير موجود")
    if not TELEGRAM_BOT_TOKEN:
        errors.append("❌ TELEGRAM_BOT_TOKEN غير موجود")
    if not TELEGRAM_CHAT_ID:
        errors.append("❌ TELEGRAM_CHAT_ID غير موجود")

    if errors:
        for e in errors:
            print(e)
        raise ValueError("⚠️ أكمل متغيرات البيئة في Railway قبل التشغيل")

    print("✅ جميع الإعدادات صحيحة")
