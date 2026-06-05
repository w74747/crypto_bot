"""
main.py — نظام المجموعات مع نقاش تلقائي
"""

import asyncio
from datetime import datetime

from config.settings import validate_config, SCAN_INTERVAL_MINUTES
from core.scanner import MarketScanner
from core.telegram_bot import TelegramNotifier, register_opportunity, build_application
from utils.logger import logger

# هل يعمل النقاش تلقائياً مع كل فرصة؟
import os
AUTO_DEBATE = os.getenv("AUTO_DEBATE", "true").lower() == "true"


async def scanner_loop(notifier: TelegramNotifier):
    scanner          = MarketScanner()
    interval_seconds = SCAN_INTERVAL_MINUTES * 60

    await notifier.send_plain_message(
        f"🤖 البوت يعمل | فحص كل {SCAN_INTERVAL_MINUTES} دقيقة\n"
        f"📦 يرسل الفرص فور اكتمال كل مجموعة\n"
        f"🧠 نقاش الخبراء {'تلقائي' if AUTO_DEBATE else 'عند الطلب'}"
    )

    while True:
        start_time = datetime.now()
        logger.info(f"🔄 دورة جديدة: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        total_sent = 0

        try:
            for batch_opps, batch_num, total_batches in scanner.scan_market_batched():
                for opp in batch_opps:
                    try:
                        if AUTO_DEBATE:
                            # نقاش تلقائي — يُدمج في الرسالة
                            msg_id = await notifier.send_opportunity_with_debate(opp)
                        else:
                            # رسالة عادية مع زر النقاش
                            msg_id = await notifier.send_opportunity(opp)

                        register_opportunity(opp, msg_id)
                        total_sent += 1
                        # تأخير بين الرسائل لتجنب Flood Control
                        await asyncio.sleep(4)

                    except Exception as e:
                        logger.error(f"[Send] خطأ في إرسال {opp.symbol}: {e}")
                        await asyncio.sleep(10)

                if batch_num < total_batches:
                    await asyncio.sleep(3)

        except Exception as e:
            logger.error(f"❌ خطأ في الفحص: {e}", exc_info=True)
            await notifier.send_plain_message(f"⚠️ خطأ: {str(e)[:100]}")

        elapsed = (datetime.now() - start_time).seconds // 60
        logger.info(f"✅ الدورة اكتملت خلال {elapsed} دقيقة | أُرسلت {total_sent} فرصة")
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
