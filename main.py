"""
main.py — Hybrid Mode مع إصلاح name 'os' is not defined
"""

import asyncio
import os
import core.ai_analyst as _analyst
import core.database   as _db
from datetime import datetime

from config.settings import validate_config, SCAN_INTERVAL_MINUTES
from core.scanner import MarketScanner
from core.telegram_bot import TelegramNotifier, register_opportunity, build_application
from core.database import init_db, start_outcome_tracker
from utils.logger import logger

AUTO_DEBATE      = os.getenv("AUTO_DEBATE",      "true").lower() == "true"
MAX_AUTO_EXECUTE = int(os.getenv("MAX_AUTO_EXECUTE", "1"))


async def scanner_loop(notifier: TelegramNotifier, scanner: MarketScanner):
    interval_seconds = SCAN_INTERVAL_MINUTES * 60

    await notifier.send_plain_message(
        f"🤖 البوت يعمل | فحص كل {SCAN_INTERVAL_MINUTES} دقيقة\n"
        f"🔀 الوضع الهجين: ينفذ أفضل {MAX_AUTO_EXECUTE} فرصة تلقائياً\n"
        f"💼 Portfolio Manager نشط"
    )

    while True:
        start_time   = datetime.now()
        filtered_count = 0
        all_approved   = []

        logger.info(f"🔄 دورة: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # ── جمع الفرص ──
        try:
            for batch_opps, batch_num, total_batches in scanner.scan_market_batched():
                for opp in batch_opps:
                    try:
                        if AUTO_DEBATE:
                            loop = asyncio.get_event_loop()

                            # Parallel Committee — 3 متخصصون بالتوازي
                            debate = await _analyst.run_parallel_committee(opp)
                            rec    = debate["recommendation"]

                            # تحديث أهداف الفرصة من الـ Committee
                            if debate.get('fib_targets'):
                                ft = debate['fib_targets']
                                object.__setattr__(opp, 'tp1', ft['tp1'])
                                object.__setattr__(opp, 'tp2', ft['tp2'])
                                object.__setattr__(opp, 'tp3', ft['tp3'])
                                object.__setattr__(opp, 'tp_method', 'Parallel Fib')
                            if debate.get('calculated_sl'):
                                object.__setattr__(opp, 'stop_loss', debate['calculated_sl'])

                            # حفظ في قاعدة البيانات
                            _opp, _debate = opp, debate
                            signal_id = await loop.run_in_executor(
                                None,
                                lambda o=_opp, d=_debate: _db.log_signal(
                                    o, d, btc_price=0.0,
                                    btc_trend="safe", galaxy_score=0.0
                                )
                            )

                            if not rec.get("send_signal", False):
                                filtered_count += 1
                                logger.info(f"[Filter] {opp.symbol} — {rec['label']}")
                                continue

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
                                "opp": opp, "debate": None,
                                "signal_id": "", "score": 50,
                            })

                    except Exception as e:
                        logger.error(f"[Debate] {opp.symbol}: {e}")

                if batch_num < total_batches:
                    await asyncio.sleep(2)

        except Exception as e:
            logger.error(f"❌ خطأ في الفحص: {e}", exc_info=True)
            await notifier.send_plain_message(f"⚠️ خطأ: {str(e)[:100]}")

        # ── ترتيب وإرسال ──
        all_approved.sort(key=lambda x: x["score"], reverse=True)
        logger.info(
            f"[Hybrid] {len(all_approved)} فرصة | "
            f"تُنفَّذ: {min(MAX_AUTO_EXECUTE, len(all_approved))} | "
            f"معلومات: {max(0, len(all_approved) - MAX_AUTO_EXECUTE)} | "
            f"مُرشَّحة: {filtered_count}"
        )

        sent_count = 0
        for i, item in enumerate(all_approved):
            opp       = item["opp"]
            debate    = item["debate"]
            signal_id = item["signal_id"]
            is_auto   = (i < MAX_AUTO_EXECUTE)

            try:
                if is_auto:
                    logger.info(f"[Hybrid] 🚀 تنفيذ تلقائي: {opp.symbol} (#{i+1})")
                    msg_id = await notifier.send_and_execute(
                        opp, debate, signal_id=signal_id
                    )
                else:
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

    from core.telegram_bot import _executor
    if _executor:
        _executor.init_portfolio()

    start_outcome_tracker(scanner.exchange)

    # تشغيل التقارير
    from core.reports import ReportScheduler
    from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    from core.telegram_bot import _executor as exec_ref
    portfolio = exec_ref.portfolio if exec_ref else None
    ReportScheduler(
        bot_token = TELEGRAM_BOT_TOKEN,
        chat_id   = TELEGRAM_CHAT_ID,
        exchange  = scanner.exchange,
        portfolio = portfolio,
    ).start()

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
