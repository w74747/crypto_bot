"""
core/telegram_bot.py — بدون Claude أو Grok
"""

import asyncio
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from core.scanner import TradeOpportunity
from core.executor import TradeExecutor
from utils.logger import logger


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

    t_sum = _trim(finals.get(f"فني/{tech_model}",   "—"))
    r_sum = _trim(finals.get(f"مخاطر/{risk_model}", "—"))
    m_sum = _trim(finals.get(f"سوق/{market_model}", "—"))
    rd_sum = _trim(reddit, 90)

    return (
        base +
        f"\n\n━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 <b>رأي لجنة الخبراء</b>\n\n"
        f"<b>📈 المحلل الفني</b> ({tech_model})\n"
        f"<i>{t_sum}</i>\n\n"
        f"<b>🛡️ خبير المخاطر</b> ({risk_model})\n"
        f"<i>{r_sum}</i>\n\n"
        f"<b>🌐 محلل السوق</b> ({market_model})\n"
        f"<i>{m_sum}</i>\n\n"
        f"<b>📊 Reddit</b> — مزاج المجتمع\n"
        f"<i>{rd_sum}</i>\n\n"
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
        self, opp: TradeOpportunity, debate: dict
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
        logger.info(f"[Telegram] ✅ {opp.symbol} | ID: {sent.message_id}")
        return sent.message_id

    async def send_execution_result(self, result: dict):
        from config.settings import TRADE_AMOUNT_USD
        symbol = result.get("symbol", "؟")
        if result.get("success"):
            text = (
                f"✅ <b>تم تنفيذ الصفقة</b>\n\n"
                f"العملة:     <code>{symbol}</code>\n"
                f"سعر الشراء: <code>{result['filled_price']:.8g}</code>\n"
                f"الكمية:     <code>{result['filled_qty']}</code>\n"
                f"رأس المال:  <code>${TRADE_AMOUNT_USD}</code>\n\n"
                f"<b>أوامر البيع</b>\n"
                f"TP1: <code>{result.get('tp1',0):.8g}</code>  → 40%\n"
                f"TP2: <code>{result.get('tp2',0):.8g}</code>  → 35%\n"
                f"TP3: <code>{result.get('tp3',0):.8g}</code>  → 25%\n"
                f"SL:  <code>{result.get('sl',0):.8g}</code>\n\n"
                f"🛡️ وقف الخسارة نشط"
            )
        else:
            text = (
                f"❌ <b>فشل التنفيذ</b>\n\n"
                f"العملة: <code>{symbol}</code>\n"
                f"السبب: {result.get('error', 'خطأ غير معروف')}"
            )
        await self.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML"
        )

    async def send_plain_message(self, text: str):
        await self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text)


_pending:  dict[str, TradeOpportunity] = {}
_msg_ids:  dict[str, int]              = {}
_executor = None


def register_opportunity(opp: TradeOpportunity, msg_id: int = 0):
    clean = opp.symbol.replace("/", "_")
    _pending[clean] = opp
    _msg_ids[clean] = msg_id


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _executor
    query          = update.callback_query
    action, symbol = query.data.split(":", 1)
    await query.answer()
    opp = _pending.get(symbol)

    if action == "ignore":
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(f"⏭️ تم تجاهل {symbol.replace('_','/')}")
        _pending.pop(symbol, None)
        return

    if action == "execute":
        if not opp:
            await query.message.reply_text("⚠️ انتهت صلاحية التوصية")
            return
        await query.edit_message_reply_markup(reply_markup=None)
        from config.settings import TRADE_AMOUNT_USD
        await query.message.reply_text(
            f"⏳ جاري تنفيذ صفقة {opp.symbol}\n💰 المبلغ: ${TRADE_AMOUNT_USD}"
        )
        if _executor is None:
            _executor = TradeExecutor()
        result = _executor.execute_full_trade(opp)
        notifier = TelegramNotifier()
        await notifier.send_execution_result(result)
        _pending.pop(symbol, None)


def build_application() -> Application:
    global _executor
    _executor = TradeExecutor()
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CallbackQueryHandler(handle_callback))
    return app
