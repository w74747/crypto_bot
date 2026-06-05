"""
main.py — نظام المجموعات
يرسل الفرص فور اكتمال كل مجموعة
"""

import asyncio
from datetime import datetime

from config.settings import validate_config, SCAN_INTERVAL_MINUTES
from core.scanner import MarketScanner
from core.telegram_bot import TelegramNotifier, register_opportunity, build_application
from utils.logger import logger


async def scanner_loop(notifier: TelegramNotifier):
    scanner          = MarketScanner()
    interval_seconds = SCAN_INTERVAL_MINUTES * 60

    await notifier.send_plain_message(
        f"🤖 البوت يعمل | فحص كل {SCAN_INTERVAL_MINUTES} دقيقة\n"
        f"📦 النظام يرسل الفرص فور اكتمال كل مجموعة\n"
        f"🧠 GitHub AI: Grok + Gemini"
    )

    while True:
        start_time = datetime.now()
        logger.info(f"🔄 دورة جديدة: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        total_sent = 0

        try:
            # فحص بالمجموعات — يرسل الفرص فور اكتمال كل مجموعة
            for batch_opps, batch_num, total_batches in scanner.scan_market_batched():

                if batch_opps:
                    for opp in batch_opps:
                        msg_id = await notifier.send_opportunity(opp)
                        register_opportunity(opp, msg_id)
                        total_sent += 1
                        await asyncio.sleep(0.5)

                # تأخير بسيط بين المجموعات لتجنب Rate Limiting
                if batch_num < total_batches:
                    await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"❌ خطأ في الفحص: {e}", exc_info=True)
            await notifier.send_plain_message(f"⚠️ خطأ: {str(e)[:100]}")

        elapsed = (datetime.now() - start_time).seconds // 60
        logger.info(
            f"✅ الدورة اكتملت خلال {elapsed} دقيقة "
            f"| أُرسلت {total_sent} فرصة"
        )

        logger.info(f"⏳ الانتظار {SCAN_INTERVAL_MINUTES} دقيقة...")
        await asyncio.sleep(interval_seconds)


async def main():
    validate_config()
    notifier = TelegramNotifier()
    app      = build_application()

    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("✅ البوت يعمل")
        await scanner_loop(notifier)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ البوت أُوقف")
    except Exception as e:
        logger.critical(f"خطأ حرج: {e}", exc_info=True)
        raise
