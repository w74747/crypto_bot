"""
core/telegram_bot.py
=====================
بوت Telegram مع نقاش الخبراء الثلاثي
Claude + Gemini + Grok
"""

from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from core.scanner import TradeOpportunity
from core.executor import TradeExecutor
from utils.logger import logger


# ==========================================
# تنسيق رسالة الفرصة
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
        f"  • سعر الدخول: <code>{opp.entry_price:.6f}</code>\n"
        f"  • وقف الخسارة: <code>{opp.stop_loss:.6f}</code> (-{sl_pct:.1f}%)\n\n"
        f"🎯 <b>الأهداف:</b>\n"
        f"  • TP1 (30%): <code>{opp.tp1:.6f}</code>  → 40%\n"
        f"  • TP2 (60%): <code>{opp.tp2:.6f}</code>  → 35%\n"
        f"  • TP3 (100%): <code>{opp.tp3:.6f}</code> → 25%\n\n"
        f"📐 <b>نسبة R/R:</b> <code>1:{opp.risk_reward_ratio}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ هل تريد تنفيذ هذه الصفقة؟"
    )


# ==========================================
# تنسيق رسالة النقاش
# ==========================================
def format_debate_message(result: dict) -> str:
    rec    = result["recommendation"]
    log    = result["debate_log"]
    symbol = result["symbol"]

    def trim(text: str, n: int = 220) -> str:
        return text[:n] + "..." if len(text) > n else text

    rounds = {f"{e['speaker']}_{e['round']}": e["text"] for e in log}

    return (
        f"🧠 <b>نقاش الخبراء — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"

        f"<b>🔵 الجولة الأولى — التحليل المستقل:</b>\n\n"
        f"<b>🤖 Claude (فني):</b>\n<i>{trim(rounds.get('Claude_1','—'))}</i>\n\n"
        f"<b>♊ Gemini (مخاطر):</b>\n<i>{trim(rounds.get('Gemini_1','—'))}</i>\n\n"
        f"<b>𝕏 Grok (مجتمع X):</b>\n<i>{trim(rounds.get('Grok_1','—'))}</i>\n\n"
        f"─────────────────\n\n"

        f"<b>🟣 الجولة الثانية — الرد والنقاش:</b>\n\n"
        f"<b>🤖 Claude:</b>\n<i>{trim(rounds.get('Claude_2','—'))}</i>\n\n"
        f"<b>♊ Gemini:</b>\n<i>{trim(rounds.get('Gemini_2','—'))}</i>\n\n"
        f"<b>𝕏 Grok:</b>\n<i>{trim(rounds.get('Grok_2','—'))}</i>\n\n"
        f"─────────────────\n\n"

        f"<b>🟠 الجولة الثالثة — الحكم النهائي:</b>\n\n"
        f"<b>🤖 Claude:</b>\n<i>{trim(rounds.get('Claude_3','—'))}</i>\n\n"
        f"<b>♊ Gemini:</b>\n<i>{trim(rounds.get('Gemini_3','—'))}</i>\n\n"
        f"<b>𝕏 Grok:</b>\n<i>{trim(rounds.get('Grok_3','—'))}</i>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>تصويت الخبراء:</b>\n{rec['votes']}\n\n"
        f"<b>النتيجة:</b> {rec['emoji']} <b>{rec['label']}</b>\n"
        f"<b>درجة الثقة:</b> {rec['confidence']}"
    )


# ==========================================
# الأزرار التفاعلية
# ==========================================
def create_keyboard(symbol: str, debate_done: bool = False) -> InlineKeyboardMarkup:
    clean = symbol.replace("/", "_")
    if not debate_done:
        keyboard = [
            [InlineKeyboardButton(
                "🧠 نقاش الخبراء (Claude + Gemini + Grok)",
                callback_data=f"debate:{clean}"
            )],
            [
                InlineKeyboardButton("✅ موافقة وتنفيذ", callback_data=f"execute:{clean}"),
                InlineKeyboardButton("❌ تجاهل",         callback_data=f"ignore:{clean}"),
            ],
        ]
    else:
        keyboard = [[
            InlineKeyboardButton("✅ موافقة وتنفيذ", callback_data=f"execute:{clean}"),
            InlineKeyboardButton("❌ تجاهل",         callback_data=f"ignore:{clean}"),
        ]]
    return InlineKeyboardMarkup(keyboard)


# ==========================================
# TelegramNotifier
# ==========================================
class TelegramNotifier:

    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)

    async def send_opportunity(self, opp: TradeOpportunity) -> int:
        sent = await self.bot.send_message(
            chat_id      = TELEGRAM_CHAT_ID,
            text         = format_opportunity_message(opp),
            parse_mode   = "HTML",
            reply_markup = create_keyboard(opp.symbol, debate_done=False),
        )
        logger.info(f"[Telegram] توصية {opp.symbol} | ID: {sent.message_id}")
        return sent.message_id

    async def send_debate_result(self, result: dict, reply_to_id: int):
        await self.bot.send_message(
            chat_id             = TELEGRAM_CHAT_ID,
            text                = format_debate_message(result),
            parse_mode          = "HTML",
            reply_to_message_id = reply_to_id,
        )
        # إزالة زر النقاش من الرسالة الأصلية
        try:
            await self.bot.edit_message_reply_markup(
                chat_id      = TELEGRAM_CHAT_ID,
                message_id   = reply_to_id,
                reply_markup = create_keyboard(result["symbol"], debate_done=True),
            )
        except Exception:
            pass

    async def send_execution_result(self, result: dict):
        symbol = result.get("symbol", "؟")
        if result.get("success"):
            text = (
                f"✅ <b>تم تنفيذ الصفقة بنجاح!</b>\n\n"
                f"🪙 العملة: <code>{symbol}</code>\n"
                f"💵 سعر الشراء: <code>{result['filled_price']:.6f}</code>\n"
                f"📦 الكمية: <code>{result['filled_qty']}</code>\n\n"
                f"🎯 <b>أوامر البيع:</b>\n"
                f"  TP1: <code>{result.get('tp1',0):.6f}</code>\n"
                f"  TP2: <code>{result.get('tp2',0):.6f}</code>\n"
                f"  TP3: <code>{result.get('tp3',0):.6f}</code>\n"
                f"  SL:  <code>{result.get('sl',0):.6f}</code>\n\n"
                f"🛡️ وقف الخسارة نشط"
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


# ==========================================
# معالج الأزرار
# ==========================================
_pending: dict[str, TradeOpportunity] = {}
_msg_ids: dict[str, int]              = {}
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

    opp    = _pending.get(symbol)
    msg_id = _msg_ids.get(symbol, query.message.message_id)

    # تجاهل
    if action == "ignore":
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(f"⏭️ تم تجاهل {symbol.replace('_','/')}")
        _pending.pop(symbol, None)
        return

    # نقاش الخبراء
    if action == "debate":
        if not opp:
            await query.message.reply_text("⚠️ انتهت صلاحية التوصية")
            return
        await query.message.reply_text(
            f"🧠 جاري تشغيل نقاش الخبراء الثلاثي...\n"
            f"🤖 Claude + ♊ Gemini + 𝕏 Grok\n"
            f"⏳ يستغرق 45-90 ثانية"
        )
        try:
            from core.ai_analyst import run_expert_debate
            result   = run_expert_debate(opp)
            notifier = TelegramNotifier()
            await notifier.send_debate_result(result, msg_id)
        except Exception as e:
            logger.error(f"[Debate] خطأ: {e}")
            await query.message.reply_text(f"⚠️ خطأ في النقاش: {str(e)[:100]}")
        return

    # تنفيذ
    if action == "execute":
        if not opp:
            await query.message.reply_text("⚠️ انتهت صلاحية التوصية")
            return
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(f"⏳ جاري تنفيذ صفقة {opp.symbol}...")
        if _executor is None:
            _executor = TradeExecutor()
        result   = _executor.execute_full_trade(opp)
        notifier = TelegramNotifier()
        await notifier.send_execution_result(result)
        _pending.pop(symbol, None)


def build_application() -> Application:
    global _executor
    _executor = TradeExecutor()
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CallbackQueryHandler(handle_callback))
    return app
