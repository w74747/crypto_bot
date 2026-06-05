"""
core/telegram_bot.py
=====================
بوت Telegram: يرسل التوصيات ويستقبل أوامر التنفيذ
"""

from telegram import (
    Bot, Update, InlineKeyboardButton,
    InlineKeyboardMarkup
)
from telegram.ext import (
    Application, CallbackQueryHandler, ContextTypes
)

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from core.scanner import TradeOpportunity
from core.executor import TradeExecutor
from utils.logger import logger


def format_opportunity_message(opp: TradeOpportunity) -> str:
    """
    ينسّق رسالة التوصية بتنسيق جميل لـ Telegram
    """

    def esc(text: str) -> str:
        """يهرّب الأحرف الخاصة في MarkdownV2"""
        special = r"_*[]()~`>#+-=|{}.!"
        for ch in special:
            text = text.replace(ch, f"\\{ch}")
        return text

    vol_m = opp.volume_24h_usd / 1_000_000

    msg = (
        f"🎯 *فرصة تداول جديدة\\!*\n\n"
        f"🪙 *العملة:* `{esc(opp.symbol)}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *التحليل الفني:*\n"
        f"  • RSI اليومي: `{esc(f'{opp.rsi_daily:.1f}')}`  ⬇️ تشبع بيعي\n"
        f"  • الانهيار خلال 60 يوم: `{esc(f'{opp.crash_pct_60d:.0%}')}`  🔻\n"
        f"  • البعد عن القاع 180 يوم: `{esc(f'{opp.distance_from_lod:.1%}')}`\n"
        f"  • نوع الإشارة: `{esc(opp.signal_type)}`\n"
        f"  • حجم التداول: `{esc(f'${vol_m:.1f}M')}`\n\n"
        f"💰 *تفاصيل الصفقة:*\n"
        f"  • سعر الدخول: `{esc(f'{opp.entry_price:.6f}')}`\n"
        f"  • وقف الخسارة: `{esc(f'{opp.stop_loss:.6f}')}` "
        f"\\(\\-{esc(f'{((opp.entry_price - opp.stop_loss)/opp.entry_price)*100:.1f}')}%\\)\n\n"
        f"🎯 *الأهداف:*\n"
        f"  • TP1 \\(30%\\): `{esc(f'{opp.tp1:.6f}')}`  →  40% من الكمية\n"
        f"  • TP2 \\(60%\\): `{esc(f'{opp.tp2:.6f}')}`  →  35% من الكمية\n"
        f"  • TP3 \\(100%\\): `{esc(f'{opp.tp3:.6f}')}`  →  25% من الكمية\n\n"
        f"📐 *نسبة المخاطرة/العائد:* `1:{esc(str(opp.risk_reward_ratio))}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ هل تريد تنفيذ هذه الصفقة؟"
    )
    return msg


def create_opportunity_keyboard(symbol: str) -> InlineKeyboardMarkup:
    """
    يُنشئ الأزرار التفاعلية تحت رسالة التوصية
    """
    clean_symbol = symbol.replace("/", "_")

    keyboard = [
        [
            InlineKeyboardButton(
                "✅ موافقة وتنفيذ",
                callback_data=f"execute:{clean_symbol}"
            ),
            InlineKeyboardButton(
                "❌ تجاهل",
                callback_data=f"ignore:{clean_symbol}"
            ),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


class TelegramNotifier:
    """
    يرسل الرسائل والإشعارات لـ Telegram
    """

    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)

    async def send_opportunity(self, opportunity: TradeOpportunity) -> int:
        """
        يرسل رسالة فرصة التداول مع الأزرار التفاعلية
        """
        message  = format_opportunity_message(opportunity)
        keyboard = create_opportunity_keyboard(opportunity.symbol)

        sent = await self.bot.send_message(
            chat_id      = TELEGRAM_CHAT_ID,
            text         = message,
            parse_mode   = "MarkdownV2",
            reply_markup = keyboard,
        )

        logger.info(f"[Telegram] تم إرسال توصية {opportunity.symbol} | ID: {sent.message_id}")
        return sent.message_id

    async def send_execution_result(self, result: dict):
        """يرسل نتيجة تنفيذ الصفقة"""
        symbol = result.get("symbol", "؟")

        if result.get("success"):
            text = (
                f"✅ *تم تنفيذ الصفقة بنجاح\\!*\n\n"
                f"🪙 العملة: `{symbol}`\n"
                f"💵 سعر الشراء: `{result['filled_price']:.6f}`\n"
                f"📦 الكمية: `{result['filled_qty']}`\n\n"
                f"🎯 *أوامر البيع موضوعة:*\n"
                f"  TP1: `{result.get('tp1', 0):.6f}`\n"
                f"  TP2: `{result.get('tp2', 0):.6f}`\n"
                f"  TP3: `{result.get('tp3', 0):.6f}`\n"
                f"  SL:  `{result.get('sl', 0):.6f}`\n\n"
                f"🛡️ الصفقة محمية ووقف الخسارة نشط"
            )
        else:
            error = result.get("error", "خطأ غير معروف")
            text = (
                f"❌ *فشل تنفيذ الصفقة\\!*\n\n"
                f"🪙 العملة: `{symbol}`\n"
                f"⚠️ السبب: {error}"
            )

        # هرّب الأحرف الخاصة
        special = r"_[]()~`>#+-=|{}.!"
        for ch in special:
            text = text.replace(ch, f"\\{ch}")

        await self.bot.send_message(
            chat_id    = TELEGRAM_CHAT_ID,
            text       = text,
            parse_mode = "MarkdownV2",
        )

    async def send_scan_summary(self, opportunities_count: int, symbols_count: int):
        """يرسل ملخص نتيجة الفحص"""
        if opportunities_count == 0:
            text = (
                f"🔍 اكتمل الفحص \\- لا توجد فرص حالياً\n"
                f"📊 فُحص `{symbols_count}` عملة\n"
                f"⏰ الفحص القادم بعد 60 دقيقة"
            )
        else:
            text = (
                f"✅ اكتمل الفحص\n"
                f"💎 وُجد `{opportunities_count}` فرصة من `{symbols_count}` عملة\n"
                f"⬆️ راجع التوصيات أعلاه"
            )

        await self.bot.send_message(
            chat_id    = TELEGRAM_CHAT_ID,
            text       = text,
            parse_mode = "MarkdownV2",
        )

    async def send_plain_message(self, text: str):
        """يرسل رسالة نصية بسيطة"""
        await self.bot.send_message(
            chat_id = TELEGRAM_CHAT_ID,
            text    = text,
        )
        logger.info(f"[Telegram] رسالة: {text[:50]}...")


# ==========================================
# معالج الأزرار التفاعلية
# ==========================================

_pending_opportunities: dict[str, TradeOpportunity] = {}
_executor = None


def register_opportunity(opportunity: TradeOpportunity):
    """يسجل فرصة في انتظار قرار المستخدم"""
    clean = opportunity.symbol.replace("/", "_")
    _pending_opportunities[clean] = opportunity
    logger.info(f"[Telegram] فرصة {opportunity.symbol} في انتظار القرار")


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    يعالج ضغطة الزر من المستخدم
    execute → ينفذ الصفقة
    ignore  → يتجاهل التوصية
    """
    global _executor

    query          = update.callback_query
    data           = query.data
    action, symbol = data.split(":", 1)

    await query.answer()

    if action == "ignore":
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(
            f"⏭️ تم تجاهل توصية {symbol.replace('_', '/')}"
        )
        _pending_opportunities.pop(symbol, None)
        return

    if action == "execute":
        opportunity = _pending_opportunities.get(symbol)

        if not opportunity:
            await query.message.reply_text("⚠️ انتهت صلاحية هذه التوصية")
            return

        # إزالة الأزرار لمنع الضغط المزدوج
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(
            f"⏳ جاري تنفيذ صفقة {opportunity.symbol}..."
        )

        if _executor is None:
            _executor = TradeExecutor()

        result = _executor.execute_full_trade(opportunity)

        notifier = TelegramNotifier()
        await notifier.send_execution_result(result)

        _pending_opportunities.pop(symbol, None)


def build_application() -> Application:
    """يبني تطبيق Telegram ويسجل المعالجات"""
    global _executor
    _executor = TradeExecutor()

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CallbackQueryHandler(handle_callback))

    return app
