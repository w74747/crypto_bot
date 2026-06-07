"""
main.py — مع Trading Brain Database
"""

import asyncio
import os
from datetime import datetime

from config.settings import validate_config, SCAN_INTERVAL_MINUTES
from core.scanner import MarketScanner
from core.telegram_bot import TelegramNotifier, register_opportunity, build_application
from core.database import init_db, start_outcome_tracker, get_performance_summary_text
from utils.logger import logger

AUTO_DEBATE = os.getenv("AUTO_DEBATE", "true").lower() == "true"


async def scanner_loop(notifier: TelegramNotifier, scanner: MarketScanner):
    interval_seconds = SCAN_INTERVAL_MINUTES * 60

    await notifier.send_plain_message(
        f"🤖 البوت يعمل | فحص كل {SCAN_INTERVAL_MINUTES} دقيقة\n"
        f"🔒 فلتر: الفني + المخاطر موافقان\n"
        f"💾 Trading Brain نشط\n"
        f"📊 Groq → DeepSeek (تلقائي)"
    )

    while True:
        start_time     = datetime.now()
        sent_count     = 0
        filtered_count = 0

        logger.info(f"🔄 دورة: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            for batch_opps, batch_num, total_batches in scanner.scan_market_batched():
                for opp in batch_opps:
                    try:
                        if AUTO_DEBATE:
                            from core.ai_analyst import run_expert_debate
                            from core.database import log_signal

                            debate = await asyncio.get_event_loop().run_in_executor(
                                None, run_expert_debate, opp
                            )
                            rec = debate["recommendation"]

                            # حفظ كل إشارة في قاعدة البيانات
                            # سواء أُرسلت أم لا
                            signal_id = await asyncio.get_event_loop().run_in_executor(
                                None,
                                lambda: log_signal(
                                    opp, debate,
                                    btc_price  = 0.0,
                                    btc_trend  = "safe",
                                    galaxy_score = 0.0,
                                )
                            )

                            # الفلتر الحاسم
                            if not rec.get("send_signal", False):
                                filtered_count += 1
                                logger.info(
                                    f"[Filter] {opp.symbol} مُرشَّح — "
                                    f"{rec['label']}"
                                )
                                continue

                            # إرسال الفرصة الموافق عليها
                            msg_id = await notifier.send_opportunity_with_debate_result(
                                opp, debate, signal_id=signal_id
                            )
                        else:
                            msg_id    = await notifier.send_opportunity(opp)
                            signal_id = ""

                        register_opportunity(opp, msg_id, signal_id=signal_id)
                        sent_count += 1
                        await asyncio.sleep(4)

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

    # تهيئة قاعدة البيانات
    init_db()

    scanner  = MarketScanner()
    notifier = TelegramNotifier()
    app      = build_application()

    # تشغيل Background Tracker
    start_outcome_tracker(scanner.exchange)

    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("✅ البوت يعمل مع Trading Brain")
        await scanner_loop(notifier, scanner)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ البوت أُوقف")
    except Exception as e:
        logger.critical(f"خطأ حرج: {e}", exc_info=True)
        raise
