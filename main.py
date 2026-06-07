"""
main.py — Hybrid Mode
الأفضل: تنفيذ تلقائي
الباقي: معلومات فقط بدون أزرار
"""

import asyncio
import os
from datetime import datetime

from config.settings import validate_config, SCAN_INTERVAL_MINUTES
from core.scanner import MarketScanner
from core.telegram_bot import TelegramNotifier, register_opportunity, build_application
from core.database import init_db, start_outcome_tracker, get_performance_summary_text
from utils.logger import logger

AUTO_DEBATE      = os.getenv("AUTO_DEBATE",      "true").lower() == "true"
MAX_AUTO_EXECUTE = int(os.getenv("MAX_AUTO_EXECUTE", "1"))  # عدد الفرص تُنفَّذ تلقائياً


async def scanner_loop(notifier: TelegramNotifier, scanner: MarketScanner):
    interval_seconds = SCAN_INTERVAL_MINUTES * 60

    await notifier.send_plain_message(
        f"🤖 البوت يعمل | فحص كل {SCAN_INTERVAL_MINUTES} دقيقة\n"
        f"🔀 الوضع الهجين: ينفذ أفضل {MAX_AUTO_EXECUTE} فرصة تلقائياً\n"
        f"📋 الباقي: معلومات فقط\n"
        f"💼 Portfolio Manager نشط"
    )

    while True:
        start_time     = datetime.now()
        sent_count     = 0
        filtered_count = 0
        all_approved   = []

        logger.info(f"🔄 دورة: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # ── جمع وتحليل الفرص ──
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

                            signal_id = await asyncio.get_event_loop().run_in_executor(
                                None,
                                lambda: log_signal(opp, debate,
                                    btc_price=0.0, btc_trend="safe",
                                    galaxy_score=0.0)
                            )

                            if not rec.get("send_signal", False):
                                filtered_count += 1
                                logger.info(f"[Filter] {opp.symbol} — {rec['label']}")
                                continue

                            # درجة الجودة للترتيب
                            quality = (
                                rec["votes"].count("✅") * 30 +
                                (100 - opp.rsi_daily) +
                                opp.risk_reward_ratio * 10
                            )
                            all_approved.append({
                                "opp":       opp,
                                "debate":    debate,
                                "signal_id": signal_id,
                                "score":     quality,
                            })

                        else:
                            all_approved.append({
                                "opp":       opp,
                                "debate":    None,
                                "signal_id": "",
                                "score":     50,
                            })

                    except Exception as e:
                        logger.error(f"[Debate] {opp.symbol}: {e}")

                if batch_num < total_batches:
                    await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"❌ خطأ في الفحص: {e}", exc_info=True)
            await notifier.send_plain_message(f"⚠️ خطأ: {str(e)[:100]}")

        # ── ترتيب بالجودة ──
        all_approved.sort(key=lambda x: x["score"], reverse=True)
        logger.info(
            f"[Hybrid] {len(all_approved)} فرصة معتمدة | "
            f"تُنفَّذ: {min(MAX_AUTO_EXECUTE, len(all_approved))} | "
            f"معلومات: {max(0, len(all_approved) - MAX_AUTO_EXECUTE)}"
        )

        # ── إرسال ومعالجة ──
        for i, item in enumerate(all_approved):
            opp       = item["opp"]
            debate    = item["debate"]
            signal_id = item["signal_id"]
            is_auto   = (i < MAX_AUTO_EXECUTE)

            try:
                if is_auto:
                    # ── تنفيذ تلقائي ──
                    logger.info(f"[Hybrid] 🚀 تنفيذ تلقائي: {opp.symbol} (#{i+1})")
                    msg_id = await notifier.send_and_execute(
                        opp, debate, signal_id=signal_id
                    )
                else:
                    # ── معلومات فقط ──
                    logger.info(f"[Hybrid] 📋 معلومات: {opp.symbol} (#{i+1})")
                    msg_id = await notifier.send_info_only(
                        opp, debate, rank=i+1
                    )

                register_opportunity(opp, msg_id, signal_id=signal_id)
                sent_count += 1
                await asyncio.sleep(4)

            except Exception as e:
                logger.error(f"[Send] {opp.symbol}: {e}")
                await asyncio.sleep(10)

        elapsed = (datetime.now() - start_time).seconds // 60
        logger.info(
            f"✅ الدورة: {elapsed}د | "
            f"أُرسلت: {sent_count} | مُرشَّحة: {filtered_count}"
        )
        await asyncio.sleep(interval_seconds)


async def main():
    validate_config()
    init_db()

    scanner  = MarketScanner()
    notifier = TelegramNotifier()
    app      = build_application()

    # تهيئة Portfolio Manager في الـ executor
    from core.telegram_bot import _executor
    if _executor:
        _executor.init_portfolio()

    start_outcome_tracker(scanner.exchange)

    # تشغيل نظام التقارير
    from core.reports import ReportScheduler
    from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    portfolio = _executor.portfolio if _executor else None
    report_scheduler = ReportScheduler(
        bot_token = TELEGRAM_BOT_TOKEN,
        chat_id   = TELEGRAM_CHAT_ID,
        exchange  = scanner.exchange,
        portfolio = portfolio,
    )
    report_scheduler.start()

    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("✅ البوت يعمل — Hybrid Mode")
        await scanner_loop(notifier, scanner)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ البوت أُوقف")
    except Exception as e:
        logger.critical(f"خطأ حرج: {e}", exc_info=True)
        raise
