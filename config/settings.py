"""
config/settings.py — بدون Claude أو Grok
"""

import os
from dotenv import load_dotenv
load_dotenv()

# ==========================================
# MEXC
# ==========================================
MEXC_API_KEY    = os.getenv("MEXC_API_KEY", "")
MEXC_API_SECRET = os.getenv("MEXC_API_SECRET", "")

# ==========================================
# Telegram
# ==========================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

# ==========================================
# AI APIs — Fallback Chain
# ==========================================
GROQ_API_KEY     = os.getenv("GROQ_API_KEY", "")
TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY", "")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")

# ==========================================
# GitHub (اختياري)
# ==========================================
GITHUB_TOKEN             = os.getenv("GITHUB_TOKEN", "")
GITHUB_MAX_INACTIVE_DAYS = int(os.getenv("GITHUB_MAX_INACTIVE_DAYS", "90"))

# ==========================================
# فلاتر السوق
# ==========================================
MAX_DISTANCE_FROM_LOD  = float(os.getenv("MAX_DISTANCE_FROM_LOD",  "0.10"))
MIN_DAILY_VOLUME_USD   = float(os.getenv("MIN_DAILY_VOLUME_USD",   "1000000"))
RSI_OVERSOLD_THRESHOLD = float(os.getenv("RSI_OVERSOLD_THRESHOLD", "40"))
RSI_PERIOD             = int(os.getenv("RSI_PERIOD",               "14"))
LOD_DAYS               = int(os.getenv("LOD_DAYS",                 "180"))

# ==========================================
# إعدادات الصفقة
# ==========================================
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.08"))
TP1_PCT       = float(os.getenv("TP1_PCT",       "0.30"))
TP2_PCT       = float(os.getenv("TP2_PCT",       "0.60"))
TP3_PCT       = float(os.getenv("TP3_PCT",       "1.00"))
TP1_QTY_PCT   = float(os.getenv("TP1_QTY_PCT",   "0.40"))
TP2_QTY_PCT   = float(os.getenv("TP2_QTY_PCT",   "0.35"))
TP3_QTY_PCT   = float(os.getenv("TP3_QTY_PCT",   "0.25"))
TRADE_AMOUNT_USD     = float(os.getenv("TRADE_AMOUNT_USD",     "30"))

# ── Portfolio Manager ──
PORTFOLIO_MODE        = os.getenv("PORTFOLIO_MODE",        "true").lower() == "true"
PORTFOLIO_BUDGET      = float(os.getenv("PORTFOLIO_BUDGET",      "300"))
TRADE_PCT_OF_BALANCE  = float(os.getenv("TRADE_PCT_OF_BALANCE",  "0.25"))
MAX_OPEN_TRADES       = int(os.getenv("MAX_OPEN_TRADES",         "3"))
MIN_TRADE_AMOUNT      = float(os.getenv("MIN_TRADE_AMOUNT",      "10"))

# ==========================================
# إعدادات التشغيل
# ==========================================
SCAN_INTERVAL_MINUTES = int(os.getenv("SCAN_INTERVAL_MINUTES", "60"))
LOG_FILE = "logs/bot.log"


def validate_config():
    errors = []
    if not MEXC_API_KEY:      errors.append("❌ MEXC_API_KEY")
    if not MEXC_API_SECRET:   errors.append("❌ MEXC_API_SECRET")
    if not TELEGRAM_BOT_TOKEN: errors.append("❌ TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT_ID:  errors.append("❌ TELEGRAM_CHAT_ID")

    # يجب أن يكون هناك على الأقل مزود AI واحد
    if not any([GROQ_API_KEY, TOGETHER_API_KEY, DEEPSEEK_API_KEY]):
        errors.append("❌ يجب توفر مفتاح واحد على الأقل: GROQ أو TOGETHER أو DEEPSEEK")

    if errors:
        for e in errors:
            print(e)
        raise ValueError("⚠️ أكمل المتغيرات في Railway")

    # إظهار المزودين المتاحين
    providers = []
    if GROQ_API_KEY:     providers.append("Groq ✅")
    if TOGETHER_API_KEY: providers.append("Together ✅")
    if DEEPSEEK_API_KEY: providers.append("DeepSeek ✅")
    print(f"✅ مزودو AI: {' | '.join(providers)}")
    print("✅ جميع الإعدادات صحيحة")
