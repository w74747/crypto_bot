"""
main.py — مع فلتر الإرسال الحازم
لا ترسل إشارة إلا إذا وافق Claude + خبير المخاطر
"""

import asyncio
import os
from datetime import datetime

from config.settings import validate_config, SCAN_INTERVAL_MINUTES
from core.scanner import MarketScanner
from core.telegram_bot import TelegramNotifier, register_opportunity, build_application
from utils.logger import logger

AUTO_DEBATE = os.getenv("AUTO_DEBATE", "true").lower() == "true"


async def scanner_loop(notifier: TelegramNotifier):
    scanner          = MarketScanner()
    interval_seconds = SCAN_INTERVAL_MINUTES * 60

    await notifier.send_plain_message(
        f"🤖 البوت يعمل | فحص كل {SCAN_INTERVAL_MINUTES} دقيقة\n"
        f"🔒 فلتر صارم: لا إشارة بدون موافقة Claude + خبير المخاطر\n"
        f"📊 DeepSeek → Groq → Together (تلقائي)"
    )

    while True:
        start_time = datetime.now()
        logger.info(f"🔄 دورة: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        sent_count    = 0
        filtered_count = 0

        try:
            for batch_opps, batch_num, total_batches in scanner.scan_market_batched():

                for opp in batch_opps:
                    try:
                        if AUTO_DEBATE:
                            # نقاش الخبراء
                            from core.ai_analyst import run_expert_debate
                            debate = await asyncio.get_event_loop().run_in_executor(
                                None, run_expert_debate, opp
                            )
                            rec = debate["recommendation"]

                            # ── الفلتر الحاسم ──
                            if not rec.get("send_signal", False):
                                filtered_count += 1
                                logger.info(
                                    f"[Filter] {opp.symbol} مُرفَّح — "
                                    f"{rec['label']} | {rec['votes']}"
                                )
                                continue  # لا ترسل هذه الإشارة

                            # موافقة أساسية → أرسل
                            msg_id = await notifier.send_opportunity_with_debate_result(
                                opp, debate
                            )
                        else:
                            msg_id = await notifier.send_opportunity(opp)

                        register_opportunity(opp, msg_id)
                        sent_count += 1
                        await asyncio.sleep(4)  # تجنب Flood Control

                    except Exception as e:
                        logger.error(f"[Send] خطأ {opp.symbol}: {e}")
                        await asyncio.sleep(10)

                if batch_num < total_batches:
                    await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"❌ خطأ في الفحص: {e}", exc_info=True)
            await notifier.send_plain_message(f"⚠️ خطأ: {str(e)[:100]}")

        elapsed = (datetime.now() - start_time).seconds // 60
        logger.info(
            f"✅ الدورة اكتملت خلال {elapsed} دقيقة | "
            f"أُرسلت: {sent_count} | مُرشَّحة: {filtered_count}"
        )
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
