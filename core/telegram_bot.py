"""
core/telegram_bot.py
=====================
رسالة موحدة: بيانات الفرصة + خلاصة نقاش الخبراء
"""

import asyncio
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from core.scanner import TradeOpportunity
from core.executor import TradeExecutor
from utils.logger import logger


# ==========================================
# رسالة الفرصة الأساسية (بدون نقاش)
# ==========================================
def format_opportunity_message(opp: TradeOpportunity) -> str:
    vol_m  = opp.volume_24h_usd / 1_000_000
    sl_pct = ((opp.entry_price - opp.stop_loss) / opp.entry_price) * 100
    return (
        f"🎯 <b>فرصة تداول جديدة!</b>\n\n"
        f"🪙 <b>العملة:</b> <code>{opp.symbol}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>التحليل الفني:</b>\n"
        f"  • RSI اليومي: <code>{opp.rsi_daily:.1f}</code> ⬇️\n"
        f"  • الانهيار 60 يوم: <code>{opp.crash_pct_60d:.0%}</code> 🔻\n"
        f"  • البعد عن القاع: <code>{opp.distance_from_lod:.1%}</code>\n"
        f"  • نوع الإشارة: <code>{opp.signal_type}</code>\n"
        f"  • حجم التداول: <code>${vol_m:.1f}M</code>\n\n"
        f"💰 <b>تفاصيل الصفقة:</b>\n"
        f"  • سعر الدخول: <code>{opp.entry_price:.8g}</code>\n"
        f"  • وقف الخسارة: <code>{opp.stop_loss:.8g}</code> (-{sl_pct:.1f}%)\n\n"
        f"🎯 <b>الأهداف:</b>\n"
        f"  • TP1 (30%): <code>{opp.tp1:.8g}</code>  → 40%\n"
        f"  • TP2 (60%): <code>{opp.tp2:.8g}</code>  → 35%\n"
        f"  • TP3 (100%): <code>{opp.tp3:.8g}</code> → 25%\n\n"
        f"📐 <b>نسبة R/R:</b> <code>1:{opp.risk_reward_ratio}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )


# ==========================================
# رسالة موحدة: فرصة + خلاصة النقاش
# ==========================================
def format_opportunity_with_debate(opp: TradeOpportunity, debate_result: dict) -> str:
    """رسالة واحدة تجمع بيانات الفرصة وخلاصة نقاش الخبراء"""
    base    = format_opportunity_message(opp)
    rec     = debate_result["recommendation"]

    def trim(text: str, n: int = 120) -> str:
        return text[:n] + "..." if len(text) > n else text

    # خلاصة مختصرة من كل خبير
    log    = debate_result.get("debate_log", [])
    finals = {e["speaker"]: e["text"] for e in log if e["round"] == 3}

    claude_sum = trim(finals.get("Claude", "—"))
    gemini_sum = trim(finals.get("Gemini", "—"))
    grok_sum   = trim(finals.get("Grok",   "—"))

    debate_section = (
        f"\n\n🧠 <b>خلاصة نقاش الخبراء:</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 <b>Claude:</b> <i>{claude_sum}</i>\n\n"
        f"♊ <b>Gemini:</b> <i>{gemini_sum}</i>\n\n"
        f"𝕏 <b>Grok:</b> <i>{grok_sum}</i>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>تصويت الخبراء:</b> {rec['votes']}\n"
        f"<b>النتيجة:</b> {rec['emoji']} <b>{rec['label']}</b>\n"
        f"<b>الثقة:</b> {rec['confidence']}\n\n"
        f"⚡ هل تريد تنفيذ هذه الصفقة؟"
    )
    return base + debate_section


# ==========================================
# الأزرار
# ==========================================
def create_keyboard(symbol: str) -> InlineKeyboardMarkup:
    clean = symbol.replace("/", "_")
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ موافقة وتنفيذ", callback_data=f"execute:{clean}"),
        InlineKeyboardButton("❌ تجاهل",         callback_data=f"ignore:{clean}"),
    ]])


# ==========================================
# TelegramNotifier
# ==========================================
class TelegramNotifier:

    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)

    async def send_opportunity(self, opp: TradeOpportunity) -> int:
        """رسالة عادية بدون نقاش"""
        sent = await self.bot.send_message(
            chat_id      = TELEGRAM_CHAT_ID,
            text         = format_opportunity_message(opp) + "\n\n⚡ هل تريد تنفيذ هذه الصفقة؟",
            parse_mode   = "HTML",
            reply_markup = create_keyboard(opp.symbol),
        )
        logger.info(f"[Telegram] توصية {opp.symbol} | ID: {sent.message_id}")
        return sent.message_id

    async def send_opportunity_with_debate(self, opp: TradeOpportunity) -> int:
        """
        يشغّل نقاش الخبراء تلقائياً ويرسل رسالة موحدة
        """
        logger.info(f"[Auto Debate] بدء نقاش {opp.symbol}...")

        try:
            from core.ai_analyst import run_expert_debate
            debate_result = await asyncio.get_event_loop().run_in_executor(
                None, run_expert_debate, opp
            )
            text = format_opportunity_with_debate(opp, debate_result)
        except Exception as e:
            logger.error(f"[Auto Debate] خطأ لـ {opp.symbol}: {e}")
            # في حالة فشل النقاش، أرسل الرسالة العادية
            text = format_opportunity_message(opp) + "\n\n⚡ هل تريد تنفيذ هذه الصفقة؟"

        sent = await self.bot.send_message(
            chat_id      = TELEGRAM_CHAT_ID,
            text         = text,
            parse_mode   = "HTML",
            reply_markup = create_keyboard(opp.symbol),
        )
        logger.info(f"[Telegram] فرصة+نقاش {opp.symbol} | ID: {sent.message_id}")
        return sent.message_id

    async def send_execution_result(self, result: dict):
        symbol = result.get("symbol", "؟")
        if result.get("success"):
            text = (
                f"✅ <b>تم تنفيذ الصفقة بنجاح!</b>\n\n"
                f"🪙 العملة: <code>{symbol}</code>\n"
                f"💵 سعر الشراء: <code>{result['filled_price']:.8g}</code>\n"
                f"📦 الكمية: <code>{result['filled_qty']}</code>\n\n"
                f"🎯 <b>أوامر البيع:</b>\n"
                f"  TP1: <code>{result.get('tp1',0):.8g}</code>\n"
                f"  TP2: <code>{result.get('tp2',0):.8g}</code>\n"
                f"  TP3: <code>{result.get('tp3',0):.8g}</code>\n"
                f"  SL:  <code>{result.get('sl',0):.8g}</code>\n\n"
                f"🛡️ وقف الخسارة نشط\n"
                f"💰 رأس المال المستخدم: ${result.get('trade_amount', '—')}"
            )
        else:
            text = (
                f"❌ <b>فشل التنفيذ!</b>\n\n"
                f"🪙 {symbol}\n"
                f"⚠️ {result.get('error','خطأ غير معروف')}"
            )
        await self.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML"
        )

    async def send_plain_message(self, text: str):
        await self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text)
        logger.info(f"[Telegram] رسالة: {text[:50]}...")


# ==========================================
# مخزن الفرص
# ==========================================
_pending: dict[str, TradeOpportunity] = {}
_msg_ids: dict[str, int]              = {}
_executor = None


def register_opportunity(opp: TradeOpportunity, msg_id: int = 0):
    clean = opp.symbol.replace("/", "_")
    _pending[clean] = opp
    _msg_ids[clean] = msg_id


# ==========================================
# معالج الأزرار
# ==========================================
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
            f"⏳ جاري تنفيذ صفقة {opp.symbol}...\n"
            f"💰 المبلغ: ${TRADE_AMOUNT_USD}"
        )

        if _executor is None:
            _executor = TradeExecutor()

        result = _executor.execute_full_trade(opp)
        result["trade_amount"] = TRADE_AMOUNT_USD

        notifier = TelegramNotifier()
        await notifier.send_execution_result(result)
        _pending.pop(symbol, None)


def build_application() -> Application:
    global _executor
    _executor = TradeExecutor()
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CallbackQueryHandler(handle_callback))
    return app
