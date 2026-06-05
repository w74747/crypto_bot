"""
main.py
========
نقطة الدخول الرئيسية
"""

import asyncio
from datetime import datetime

from config.settings import validate_config, SCAN_INTERVAL_MINUTES
from core.scanner import MarketScanner
from core.telegram_bot import (
    TelegramNotifier,
    register_opportunity,
    build_application,
)
from utils.logger import logger


async def scanner_loop(notifier: TelegramNotifier):
    scanner          = MarketScanner()
    interval_seconds = SCAN_INTERVAL_MINUTES * 60

    # رسالة بدء تشغيل فقط — بدون تفاصيل مزعجة
    await notifier.send_plain_message(
        f"🤖 البوت يعمل الآن ويفحص السوق كل {SCAN_INTERVAL_MINUTES} دقيقة"
    )

    while True:
        logger.info(f"🔄 دورة فحص: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            opportunities = scanner.scan_market()

            if opportunities:
                # أرسل فقط عند وجود فرص
                for opp in opportunities:
                    register_opportunity(opp)
                    await notifier.send_opportunity(opp)
                    await asyncio.sleep(1)
            else:
                # لا ترسل شيئاً — فقط سجّل في الـ logs
                logger.info("😐 لا توجد فرص في هذه الدورة")

        except Exception as e:
            logger.error(f"❌ خطأ في دورة المسح: {e}", exc_info=True)
            # أرسل للتيليغرام فقط عند وجود خطأ حقيقي
            await notifier.send_plain_message(f"⚠️ خطأ تقني: {str(e)[:100]}")

        logger.info(f"⏳ الانتظار {SCAN_INTERVAL_MINUTES} دقيقة...")
        await asyncio.sleep(interval_seconds)


async def main():
    print("\n" + "="*50)
    print("🤖 Crypto Bottom Fisher Bot")
    print("="*50)
    validate_config()

    notifier = TelegramNotifier()
    app      = build_application()

    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("✅ Telegram Polling يعمل")
        await scanner_loop(notifier)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⛔ تم إيقاف البوت")
        logger.info("البوت أُوقف بواسطة المستخدم")
    except Exception as e:
        logger.critical(f"خطأ حرج: {e}", exc_info=True)
        raise
