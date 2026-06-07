"""
core/telegram_bot.py — نظيف ومرتب
"""

import asyncio
from typing import Optional
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from core.scanner import TradeOpportunity
from core.executor import TradeExecutor
from utils.logger import logger


# ==========================================
# دوال المساعدة
# ==========================================
def _trim(text: str, n: int = 110) -> str:
    text = text.strip()
    if len(text) <= n:
        return text
    cut = text[:n]
    for sep in ['،', '.', '!', '؟']:
        pos = cut.rfind(sep)
        if pos > n * 0.6:
            return cut[:pos+1]
    return cut + "..."


def format_opportunity(opp: TradeOpportunity) -> str:
    vol_m  = opp.volume_24h_usd / 1_000_000
    sl_pct = ((opp.entry_price - opp.stop_loss) / opp.entry_price) * 100
    return (
        f"🎯 <b>فرصة تداول — {opp.symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"<b>التحليل الفني</b>\n"
        f"RSI: <code>{opp.rsi_daily:.1f}</code>  |  "
        f"انهيار: <code>{opp.crash_pct_60d:.0%}</code>  |  "
        f"حجم: <code>${vol_m:.1f}M</code>\n"
        f"إشارة: <code>{opp.signal_type}</code>\n\n"
        f"<b>الصفقة</b>\n"
        f"دخول:  <code>{opp.entry_price:.8g}</code>\n"
        f"وقف:   <code>{opp.stop_loss:.8g}</code>  <i>(-{sl_pct:.1f}%)</i>\n\n"
        f"<b>الأهداف — {opp.tp_method}</b>\n"
        f"TP1: <code>{opp.tp1:.8g}</code>  <i>(+{opp.tp1_pct}%)</i>  → 40%\n"
        f"TP2: <code>{opp.tp2:.8g}</code>  <i>(+{opp.tp2_pct}%)</i>  → 35%\n"
        f"TP3: <code>{opp.tp3:.8g}</code>  <i>(+{opp.tp3_pct}%)</i>  → 25%\n\n"
        f"<b>مرجع Fibonacci</b>\n"
        f"قمة: <code>{opp.fib_high:.8g}</code>  |  "
        f"قاع: <code>{opp.fib_low:.8g}</code>\n\n"
        f"نسبة R/R: <code>1:{opp.risk_reward_ratio}</code>"
    )


def format_with_debate(opp: TradeOpportunity, debate: dict) -> str:
    base = format_opportunity(opp)
    rec  = debate["recommendation"]
    log  = debate.get("debate_log", [])

    tech_model   = debate.get("tech_model",   "فني")
    risk_model   = debate.get("risk_model",   "مخاطر")
    market_model = debate.get("market_model", "سوق")

    finals = {e["speaker"]: e["text"] for e in log if e["round"] == 3}
    reddit = next((e["text"] for e in log if e["round"] == 0), "—")

    t_sum  = _trim(finals.get(f"فني/{tech_model}",   "—"))
    r_sum  = _trim(finals.get(f"مخاطر/{risk_model}", "—"))
    m_sum  = _trim(finals.get(f"سوق/{market_model}", "—"))
    rd_sum = _trim(reddit, 90)

    return (
        base +
        f"\n\n━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 <b>رأي لجنة الخبراء</b>\n\n"
        f"<b>📈 الفني</b> ({tech_model})\n<i>{t_sum}</i>\n\n"
        f"<b>🛡️ المخاطر</b> ({risk_model})\n<i>{r_sum}</i>\n\n"
        f"<b>🌐 السوق</b> ({market_model})\n<i>{m_sum}</i>\n\n"
        f"<b>📊 Reddit</b>\n<i>{rd_sum}</i>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>التصويت:</b>\n{rec['votes']}\n\n"
        f"<b>القرار:</b>  {rec['emoji']} <b>{rec['label']}</b>\n"
        f"<b>الثقة:</b>   {rec['confidence']}"
    )


def create_keyboard(symbol: str) -> InlineKeyboardMarkup:
    clean = symbol.replace("/", "_")
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ موافقة وتنفيذ", callback_data=f"execute:{clean}"),
        InlineKeyboardButton("❌ تجاهل",         callback_data=f"ignore:{clean}"),
    ]])


def create_retry_keyboard(symbol: str) -> InlineKeyboardMarkup:
    clean = symbol.replace("/", "_")
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 إعادة المحاولة", callback_data=f"execute:{clean}"),
        InlineKeyboardButton("❌ تجاهل",          callback_data=f"ignore:{clean}"),
    ]])


# ==========================================
# TelegramNotifier
# ==========================================
class TelegramNotifier:

    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)

    async def send_opportunity(self, opp: TradeOpportunity) -> int:
        text = format_opportunity(opp) + "\n\n⚡ هل تريد تنفيذ هذه الصفقة؟"
        sent = await self.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text=text,
            parse_mode="HTML", reply_markup=create_keyboard(opp.symbol),
        )
        return sent.message_id

    async def send_opportunity_with_debate_result(
        self, opp: TradeOpportunity, debate: dict, signal_id: str = ""
    ) -> int:
        try:
            text = format_with_debate(opp, debate) + "\n\n⚡ هل تريد تنفيذ هذه الصفقة؟"
        except Exception as e:
            logger.error(f"[Format] {e}")
            text = format_opportunity(opp) + "\n\n⚡ هل تريد تنفيذ هذه الصفقة؟"
        sent = await self.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text=text,
            parse_mode="HTML", reply_markup=create_keyboard(opp.symbol),
        )
        logger.info(f"[Telegram] {opp.symbol} | ID: {sent.message_id}")
        return sent.message_id

    async def send_and_execute(
        self, opp: TradeOpportunity, debate: dict | None, signal_id: str = ""
    ) -> int:
        """تنفيذ تلقائي + رسالة تأكيد"""
        if _executor is None:
            logger.error("[Auto Execute] Executor غير جاهز")
            return 0

        result = _executor.execute_full_trade(opp)

        if result.get("success"):
            if signal_id:
                from core.database import log_user_decision
                log_user_decision(signal_id, "executed", opp=opp,
                    filled_price=result.get("filled_price", 0),
                    filled_qty=result.get("filled_qty", 0))

            amount = result.get("amount_usd", 30)
            text = (
                f"🚀 <b>تم التنفيذ التلقائي</b>\n\n"
                f"العملة:     <code>{opp.symbol}</code>\n"
                f"سعر الشراء: <code>{result['filled_price']:.8g}</code>\n"
                f"الكمية:     <code>{result['filled_qty']:.4f}</code>\n"
                f"رأس المال:  <code>${amount:.2f}</code>\n\n"
                f"<b>أوامر البيع (أهداف التوصية)</b>\n"
                f"TP1: <code>{result.get('tp1',0):.8g}</code>  +{opp.tp1_pct}%  → 40%\n"
                f"TP2: <code>{result.get('tp2',0):.8g}</code>  +{opp.tp2_pct}%  → 35%\n"
                f"TP3: <code>{result.get('tp3',0):.8g}</code>  +{opp.tp3_pct}%  → 25%\n"
                f"SL:  <code>{result.get('sl',0):.8g}</code>\n\n"
                f"🛡️ وقف الخسارة نشط | 💾 Trading Brain"
            )
            if debate:
                rec   = debate["recommendation"]
                text += f"\n\n<b>قرار الخبراء:</b> {rec['emoji']} {rec['label']}"

            sent = await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML"
            )
            logger.info(f"[Auto Execute] ✅ {opp.symbol}")
            return sent.message_id

        else:
            error = result.get("error", "خطأ غير معروف")
            text  = (
                f"⚠️ <b>فشل التنفيذ التلقائي</b>\n\n"
                f"العملة: <code>{opp.symbol}</code>\n"
                f"السبب: {error}\n\n"
                f"اضغط 🔄 لإعادة المحاولة"
            )
            clean = opp.symbol.replace("/", "_")
            _pending[clean]    = opp
            _signal_ids[clean] = signal_id
            sent = await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID, text=text,
                parse_mode="HTML", reply_markup=create_retry_keyboard(opp.symbol),
            )
            return sent.message_id

    async def send_info_only(
        self, opp: TradeOpportunity, debate: dict | None, rank: int = 0
    ) -> int:
        """معلومات فقط — بدون أزرار تنفيذ"""
        text = f"📋 <b>فرصة #{rank} — للمعلومية</b>\n\n" + format_opportunity(opp)

        if debate:
            rec = debate["recommendation"]
            log = debate.get("debate_log", [])
            finals = {e["speaker"]: e["text"] for e in log if e["round"] == 3}
            tech_model = debate.get("tech_model", "فني")
            risk_model = debate.get("risk_model", "مخاطر")
            t_sum = _trim(finals.get(f"فني/{tech_model}",   "—"))
            r_sum = _trim(finals.get(f"مخاطر/{risk_model}", "—"))
            text += (
                f"\n\n━━━━━━━━━━━━━━━━━━━━\n"
                f"🧠 <b>رأي الخبراء</b>\n\n"
                f"<b>الفني:</b> <i>{t_sum}</i>\n\n"
                f"<b>المخاطر:</b> <i>{r_sum}</i>\n\n"
                f"<b>القرار:</b> {rec['emoji']} {rec['label']}\n"
                f"<b>التصويت:</b> {rec['votes']}"
            )

        sent = await self.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML"
        )
        return sent.message_id

    async def send_execution_result(
        self, result: dict, opp: Optional[TradeOpportunity] = None
    ):
        from config.settings import TRADE_AMOUNT_USD
        symbol = result.get("symbol", "؟")

        if result.get("success"):
            amount = result.get("amount_usd", TRADE_AMOUNT_USD)
            text = (
                f"✅ <b>تم تنفيذ الصفقة</b>\n\n"
                f"العملة:     <code>{symbol}</code>\n"
                f"سعر الشراء: <code>{result['filled_price']:.8g}</code>\n"
                f"الكمية:     <code>{result['filled_qty']:.4f}</code>\n"
                f"رأس المال:  <code>${amount:.2f}</code>\n\n"
                f"<b>أوامر البيع</b>\n"
                f"TP1: <code>{result.get('tp1',0):.8g}</code>  → 40%\n"
                f"TP2: <code>{result.get('tp2',0):.8g}</code>  → 35%\n"
                f"TP3: <code>{result.get('tp3',0):.8g}</code>  → 25%\n"
                f"SL:  <code>{result.get('sl',0):.8g}</code>\n\n"
                f"🛡️ وقف الخسارة نشط | 💾 Trading Brain"
            )
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML"
            )
        else:
            error = result.get("error", "خطأ غير معروف")
            text = (
                f"❌ <b>فشل التنفيذ</b>\n\n"
                f"العملة: <code>{symbol}</code>\n"
                f"السبب: {error}\n\n"
                f"تحقق من رصيد USDT في Spot Wallet في MEXC"
            )
            if opp:
                await self.bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID, text=text,
                    parse_mode="HTML", reply_markup=create_retry_keyboard(symbol),
                )
            else:
                await self.bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML"
                )

    async def send_plain_message(self, text: str):
        await self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text)


# ==========================================
# مخزن الفرص
# ==========================================
_pending:    dict[str, TradeOpportunity] = {}
_msg_ids:    dict[str, int]              = {}
_signal_ids: dict[str, str]              = {}
_executor:   Optional[TradeExecutor]     = None


def register_opportunity(
    opp: TradeOpportunity, msg_id: int = 0, signal_id: str = ""
):
    clean = opp.symbol.replace("/", "_")
    _pending[clean]    = opp
    _msg_ids[clean]    = msg_id
    _signal_ids[clean] = signal_id


# ==========================================
# معالج الأزرار
# ==========================================
async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _executor
    query          = update.callback_query
    action, symbol = query.data.split(":", 1)
    await query.answer()
    opp       = _pending.get(symbol)
    signal_id = _signal_ids.get(symbol, "")

    if action == "ignore":
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(f"⏭️ تم تجاهل {symbol.replace('_','/')}")
        if signal_id:
            from core.database import log_user_decision
            log_user_decision(signal_id, "ignored", opp=opp)
        _pending.pop(symbol, None)
        _signal_ids.pop(symbol, None)
        return

    if action == "execute":
        if not opp:
            await query.message.reply_text("⚠️ انتهت صلاحية التوصية")
            return
        await query.edit_message_reply_markup(reply_markup=None)
        from config.settings import TRADE_AMOUNT_USD
        await query.message.reply_text(
            f"⏳ جاري تنفيذ {opp.symbol}\n💰 المبلغ: ${TRADE_AMOUNT_USD}"
        )
        if _executor is None:
            _executor = TradeExecutor()
        result   = _executor.execute_full_trade(opp)
        notifier = TelegramNotifier()
        if result.get("success"):
            if signal_id:
                from core.database import log_user_decision
                log_user_decision(signal_id, "executed", opp=opp,
                    filled_price=result.get("filled_price", 0),
                    filled_qty=result.get("filled_qty", 0))
            await notifier.send_execution_result(result, opp=None)
            _pending.pop(symbol, None)
            _signal_ids.pop(symbol, None)
        else:
            await notifier.send_execution_result(result, opp=opp)


# ==========================================
# أوامر المستخدم
# ==========================================
async def cmd_help(update, context):
    text = (
        "🤖 <b>أوامر البوت</b>\n\n"
        "/check XRP    — تحليل فوري لعملة\n"
        "/buy XRP      — تنفيذ توصية موجودة\n"
        "/status       — حالة البوت والمحفظة\n"
        "/report       — تقرير يومي\n"
        "/report weekly  — تقرير أسبوعي\n"
        "/report monthly — تقرير شهري\n"
        "/help         — هذه القائمة"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_status(update, context):
    """حالة البوت والمحفظة"""
    pending_count = len(_pending)
    text = (
        f"🤖 <b>حالة البوت</b>\n\n"
        f"الفرص في الذاكرة: <code>{pending_count}</code>\n"
    )
    if _pending:
        symbols = [s.replace("_", "/") for s in list(_pending.keys())[:5]]
        text += f"متاحة: {', '.join(symbols)}\n"

    if _executor and _executor.portfolio:
        try:
            text += "\n" + _executor.portfolio.get_status_text()
        except Exception as e:
            logger.error(f"[cmd_status] portfolio: {e}")

    try:
        from core.database import get_performance_summary_text
        stats = get_performance_summary_text()
        if stats and "لا توجد" not in stats:
            text += f"\n\n📊 <b>Trading Brain</b>\n{stats}"
    except Exception as e:
        logger.error(f"[cmd_status] db: {e}")

    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_check(update, context):
    if not context.args:
        await update.message.reply_text(
            "⚠️ مثال: <code>/check XRP</code>", parse_mode="HTML"
        )
        return

    symbol = f"{context.args[0].upper().replace('USDT','').strip()}/USDT"
    await update.message.reply_text(f"🔍 جاري تحليل {symbol}...")

    try:
        from core.scanner import calculate_dynamic_targets, calculate_rsi
        import pandas as pd

        if _executor is None:
            await update.message.reply_text("❌ البوت لم يكتمل تهيئته")
            return

        ex    = _executor.exchange
        ohlcv = ex.fetch_ohlcv(symbol, timeframe="1d", limit=100)
        if not ohlcv or len(ohlcv) < 20:
            await update.message.reply_text(f"❌ لا بيانات لـ {symbol}")
            return

        df     = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"]).astype(float)
        closes = df["close"]
        highs  = df["high"]
        lows   = df["low"]

        current = float(closes.iloc[-1])
        rsi     = calculate_rsi(closes)
        support = float(lows.tail(14).min())
        high60  = float(highs.tail(60).max())
        crash   = (high60 - current) / high60 if high60 > 0 else 0

        try:
            ticker = ex.fetch_ticker(symbol)
            vol_m  = float(ticker.get("quoteVolume") or 0) / 1e6
        except Exception:
            vol_m = 0.0

        targets = calculate_dynamic_targets(
            df_daily=df, df_4h=None, df_1h=None,
            entry_price=current, nearest_support=support,
            crash_pct_60d=crash,
        )

        text = (
            f"📊 <b>تحليل — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"السعر: <code>{current:.8g}</code>\n"
            f"RSI:   <code>{rsi:.1f}</code>\n"
            f"انهيار 60 يوم: <code>{crash:.0%}</code>\n"
            f"الحجم: <code>${vol_m:.1f}M</code>\n\n"
            f"<b>الأهداف — {targets['method']}</b>\n"
            f"TP1: <code>{targets['tp1']:.8g}</code>\n"
            f"TP2: <code>{targets['tp2']:.8g}</code>\n"
            f"TP3: <code>{targets['tp3']:.8g}</code>\n\n"
        )

        if rsi < 30 and vol_m >= 5:
            text += "✅ <b>مؤهلة — RSI منخفض وحجم كافٍ</b>"
        elif rsi < 40 and vol_m >= 5:
            text += "⚠️ <b>مؤهلة جزئياً</b>"
        elif vol_m < 5:
            text += f"❌ <b>حجم منخفض</b> (${vol_m:.1f}M < $5M)"
        else:
            text += f"❌ <b>RSI مرتفع</b> ({rsi:.1f})"

        await update.message.reply_text(text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"[cmd_check] {e}")
        await update.message.reply_text(f"❌ خطأ: {str(e)[:100]}")


async def cmd_buy(update, context):
    if not context.args:
        await update.message.reply_text(
            "⚠️ مثال: <code>/buy XRP</code>", parse_mode="HTML"
        )
        return

    symbol = f"{context.args[0].upper().replace('USDT','').strip()}/USDT"
    clean  = symbol.replace("/", "_")
    opp    = _pending.get(clean)

    if not opp:
        await update.message.reply_text(
            f"⚠️ لا توجد توصية حالية لـ <code>{symbol}</code>\n"
            f"انتظر دورة الفحص القادمة أو استخدم /status",
            parse_mode="HTML"
        )
        return

    from config.settings import TRADE_AMOUNT_USD
    await update.message.reply_text(
        f"⏳ جاري تنفيذ توصية {symbol}\n"
        f"💰 المبلغ: ${TRADE_AMOUNT_USD}\n"
        f"🎯 TP1: <code>{opp.tp1:.8g}</code> (+{opp.tp1_pct}%)\n"
        f"🎯 TP2: <code>{opp.tp2:.8g}</code> (+{opp.tp2_pct}%)\n"
        f"🎯 TP3: <code>{opp.tp3:.8g}</code> (+{opp.tp3_pct}%)\n"
        f"🛡️ SL: <code>{opp.stop_loss:.8g}</code>",
        parse_mode="HTML"
    )

    try:
        if _executor is None:
            await update.message.reply_text("❌ البوت لم يكتمل تهيئته")
            return

        result   = _executor.execute_full_trade(opp)
        notifier = TelegramNotifier()

        if result.get("success"):
            sid = _signal_ids.get(clean, "")
            if sid:
                from core.database import log_user_decision
                log_user_decision(sid, "executed", opp=opp,
                    filled_price=result.get("filled_price", 0),
                    filled_qty=result.get("filled_qty", 0))
            _pending.pop(clean, None)
            _signal_ids.pop(clean, None)
            await notifier.send_execution_result(result)
        else:
            await notifier.send_execution_result(result, opp=opp)

    except Exception as e:
        logger.error(f"[cmd_buy] {e}")
        await update.message.reply_text(f"❌ خطأ: {str(e)[:100]}")


async def cmd_report(update, context):
    arg = context.args[0].lower() if context.args else "daily"
    await update.message.reply_text("⏳ جاري إعداد التقرير...")

    try:
        from core.reports import (
            build_daily_report, build_weekly_report, build_monthly_report
        )
        portfolio = _executor.portfolio if _executor else None
        exchange  = _executor.exchange  if _executor else None

        if arg == "weekly":
            text = build_weekly_report(exchange, portfolio)
        elif arg == "monthly":
            text = build_monthly_report(exchange, portfolio)
        else:
            text = build_daily_report(exchange, portfolio)

        await update.message.reply_text(text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"[cmd_report] {e}")
        await update.message.reply_text(f"❌ خطأ: {str(e)[:100]}")


# ==========================================
# بناء التطبيق
# ==========================================
def build_application() -> Application:
    global _executor
    from telegram.ext import CommandHandler

    _executor = TradeExecutor()
    _executor.init_portfolio()

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(CommandHandler("check",   cmd_check))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("buy",     cmd_buy))
    app.add_handler(CommandHandler("report",  cmd_report))
    app.add_handler(CommandHandler("help",    cmd_help))
    return app
