"""
main.py — Unified High-Speed Parallel Machine
Layer 1: RSI + CMC + LunarCrush (async shields)
Layer 2: Parallel AI Committee (3 specialists)
Layer 3: Instant MARKET execution
"""

import asyncio
import os
import aiohttp
import core.ai_analyst as _analyst
import core.database   as _db
from datetime import datetime

from config.settings       import validate_config, SCAN_INTERVAL_MINUTES
from core.scanner          import MarketScanner
from core.telegram_bot     import TelegramNotifier, register_opportunity, build_application
from core.database         import init_db, start_outcome_tracker
from utils.data_pipeline   import layer1_shield
from utils.logger          import logger

AUTO_DEBATE      = os.environ.get("AUTO_DEBATE",      "true").lower() == "true"
MAX_AUTO_EXECUTE = int(os.environ.get("MAX_AUTO_EXECUTE", "1"))


async def scanner_loop(notifier: TelegramNotifier, scanner: MarketScanner):
    interval_seconds = SCAN_INTERVAL_MINUTES * 60

    await notifier.send_plain_message(
        f"🤖 البوت يعمل | فحص كل {SCAN_INTERVAL_MINUTES} دقيقة\n"
        f"🛡️ Layer 1: RSI + CMC + LunarCrush\n"
        f"🧠 Layer 2: AI Committee متوازٍ (< 3s)\n"
        f"⚡ Layer 3: MARKET execution فوري"
    )

    while True:
        start_time   = datetime.now()
        filtered_l1  = 0   # مستبعد في Layer 1
        filtered_l2  = 0   # مستبعد في Layer 2
        all_approved = []

        logger.info(f"🔄 دورة: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # ── فتح session واحدة لكل الـ API calls ──
        async with aiohttp.ClientSession() as http_session:

            try:
                for batch_opps, batch_num, total_batches in scanner.scan_market_batched():

                    # ── Layer 1: CMC + LunarCrush بالتوازي لكل الـ batch ──
                    l1_tasks = [
                        layer1_shield(http_session, opp.symbol, opp.rsi_daily)
                        for opp in batch_opps
                    ]
                    l1_results = await asyncio.gather(*l1_tasks, return_exceptions=True)

                    passed_batch = []
                    for opp, result in zip(batch_opps, l1_results):
                        if isinstance(result, Exception):
                            logger.error(f"[L1] {opp.symbol}: {result}")
                            continue
                        passed, reason = result
                        if not passed:
                            filtered_l1 += 1
                            logger.info(f"[L1 ❌] {opp.symbol}: {reason}")
                        else:
                            logger.info(f"[L1 ✅] {opp.symbol}: {reason}")
                            passed_batch.append(opp)

                    # ── Layer 2: AI Committee بالتوازي لكل الـ batch الناجح ──
                    if passed_batch and AUTO_DEBATE:
                        l2_tasks = [
                            _analyst.run_parallel_committee(opp)
                            for opp in passed_batch
                        ]
                        debates = await asyncio.gather(*l2_tasks, return_exceptions=True)

                        for opp, debate in zip(passed_batch, debates):
                            if isinstance(debate, Exception):
                                logger.error(f"[L2] {opp.symbol}: {debate}")
                                continue

                            rec = debate["recommendation"]

                            # تحديث الفرصة بأهداف الـ Committee
                            if debate.get("fib_targets"):
                                ft = debate["fib_targets"]
                                object.__setattr__(opp, "tp1",       ft["tp1"])
                                object.__setattr__(opp, "tp2",       ft["tp2"])
                                object.__setattr__(opp, "tp3",       ft["tp3"])
                                object.__setattr__(opp, "tp_method", "Parallel Fib")
                            if debate.get("calculated_sl"):
                                object.__setattr__(opp, "stop_loss", debate["calculated_sl"])

                            # حفظ في Supabase
                            _opp, _debate = opp, debate
                            signal_id = await asyncio.get_event_loop().run_in_executor(
                                None,
                                lambda o=_opp, d=_debate: _db.log_signal(
                                    o, d, btc_price=0.0,
                                    btc_trend="safe", galaxy_score=0.0
                                )
                            )

                            if not rec.get("send_signal", False):
                                filtered_l2 += 1
                                logger.info(
                                    f"[L2 ❌] {opp.symbol}: {rec['label']} "
                                    f"({debate.get('elapsed_sec', 0):.1f}s)"
                                )
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
                            logger.info(
                                f"[L2 ✅] {opp.symbol}: {rec['label']} "
                                f"({debate.get('elapsed_sec', 0):.1f}s) "
                                f"score={quality:.0f}"
                            )

                    elif passed_batch and not AUTO_DEBATE:
                        for opp in passed_batch:
                            all_approved.append({
                                "opp": opp, "debate": None,
                                "signal_id": "", "score": 50,
                            })

                    if batch_num < total_batches:
                        await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"❌ خطأ في الفحص: {e}", exc_info=True)
                await notifier.send_plain_message(f"⚠️ خطأ: {str(e)[:100]}")

        # ── Layer 3: ترتيب + تنفيذ + إرسال ──
        all_approved.sort(key=lambda x: x["score"], reverse=True)

        logger.info(
            f"[Funnel] L1 مستبعد: {filtered_l1} | "
            f"L2 مستبعد: {filtered_l2} | "
            f"معتمد: {len(all_approved)}"
        )

        sent_count = 0
        for i, item in enumerate(all_approved):
            opp       = item["opp"]
            debate    = item["debate"]
            signal_id = item["signal_id"]
            is_auto   = (i < MAX_AUTO_EXECUTE)

            try:
                if is_auto:
                    logger.info(f"[L3 🚀] تنفيذ تلقائي: {opp.symbol} (#{i+1})")
                    msg_id = await notifier.send_and_execute(
                        opp, debate, signal_id=signal_id
                    )
                else:
                    logger.info(f"[L3 📋] معلومات: {opp.symbol} (#{i+1})")
                    msg_id = await notifier.send_info_only(
                        opp, debate, rank=i+1
                    )

                register_opportunity(opp, msg_id, signal_id=signal_id)
                sent_count += 1
                await asyncio.sleep(3)

            except Exception as e:
                logger.error(f"[L3] {opp.symbol}: {e}")

        elapsed = (datetime.now() - start_time).seconds // 60
        logger.info(
            f"✅ الدورة: {elapsed}د | "
            f"تم إرسال: {sent_count} | "
            f"L1❌: {filtered_l1} | L2❌: {filtered_l2}"
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

    from core.reports    import ReportScheduler
    from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    from core.telegram_bot import _executor as exec_ref
    ReportScheduler(
        bot_token = TELEGRAM_BOT_TOKEN,
        chat_id   = TELEGRAM_CHAT_ID,
        exchange  = scanner.exchange,
        portfolio = exec_ref.portfolio if exec_ref else None,
    ).start()

    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        logger.info("✅ البوت يعمل — 3-Layer Parallel Machine")
        await scanner_loop(notifier, scanner)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ البوت أُوقف")
    except Exception as e:
        logger.critical(f"خطأ حرج: {e}", exc_info=True)
        raise
