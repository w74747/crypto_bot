"""
test_setup.py — اختبار الإعداد لـ MEXC
"""

import sys, os

print("\n" + "="*55)
print("  🧪 اختبار إعداد البوت — MEXC")
print("="*55 + "\n")

# 1. المكتبات
print("📦 [1/4] التحقق من المكتبات...")
missing = []
for module, pkg in {
    "ccxt": "ccxt", "telegram": "python-telegram-bot",
    "dotenv": "python-dotenv", "pandas": "pandas",
    "pandas_ta": "pandas-ta", "requests": "requests",
}.items():
    try:
        __import__(module)
        print(f"  ✅ {pkg}")
    except ImportError:
        print(f"  ❌ {pkg}")
        missing.append(pkg)

if missing:
    print(f"\n⚠️ شغّل: pip install -r requirements.txt")
    sys.exit(1)

# 2. المتغيرات
print("\n⚙️  [2/4] التحقق من المتغيرات...")
from dotenv import load_dotenv
load_dotenv()

required = {
    "MEXC_API_KEY":      "مفتاح MEXC API",
    "MEXC_API_SECRET":   "سر MEXC API",
    "TELEGRAM_BOT_TOKEN":"توكن Telegram",
    "TELEGRAM_CHAT_ID":  "Chat ID",
}
all_ok = True
for var, name in required.items():
    val = os.getenv(var, "")
    if not val or "here" in val:
        print(f"  ❌ {var} ({name}) — غير محدد")
        all_ok = False
    else:
        print(f"  ✅ {var}: {val[:4]}{'*'*(len(val)-4)}")

if not all_ok:
    sys.exit(1)

# 3. اتصال MEXC
print("\n🔗 [3/4] اختبار الاتصال بـ MEXC...")
try:
    import ccxt
    exchange = ccxt.mexc({
        "apiKey": os.getenv("MEXC_API_KEY"),
        "secret": os.getenv("MEXC_API_SECRET"),
        "options": {"defaultType": "spot"},
        "enableRateLimit": True,
    })
    balance  = exchange.fetch_balance()
    usdt_bal = balance.get("USDT", {}).get("free", 0)
    print(f"  ✅ متصل بـ MEXC Spot")
    print(f"  💵 رصيد USDT المتاح: {float(usdt_bal):.2f}")
except Exception as e:
    print(f"  ❌ فشل الاتصال: {e}")
    sys.exit(1)

# 4. Telegram
print("\n💬 [4/4] اختبار Telegram...")
try:
    import asyncio
    from telegram import Bot
    bot  = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
    async def test():
        info = await bot.get_me()
        await bot.send_message(
            chat_id=int(os.getenv("TELEGRAM_CHAT_ID")),
            text="✅ البوت متصل بـ MEXC وجاهز للعمل!"
        )
        return info.username
    username = asyncio.run(test())
    print(f"  ✅ @{username}")
except Exception as e:
    print(f"  ❌ خطأ Telegram: {e}")
    sys.exit(1)

print("\n" + "="*55)
print("  🎉 كل شيء يعمل! شغّل: python main.py")
print("="*55 + "\n")
