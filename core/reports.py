"""
core/reports.py
===============
نظام التقارير الثلاثي:
  - يومي: كل يوم الساعة 8 صباحاً
  - أسبوعي: كل الأحد
  - شهري: أول كل شهر
  - فوري: عند أحداث مهمة
"""

import asyncio
import threading
import time
from datetime import datetime, timezone, timedelta
from utils.logger import logger


# ==========================================
# بناء نص التقارير
# ==========================================

def build_daily_report(exchange, portfolio=None) -> str:
    """تقرير يومي سريع"""
    now = datetime.now(timezone.utc)

    text = (
        f"📅 <b>التقرير اليومي</b>\n"
        f"{now.strftime('%Y-%m-%d')} — الساعة 8:00 صباحاً\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    # رصيد المحفظة
    if portfolio:
        try:
            available = portfolio.get_available_usdt()
            total     = portfolio.get_total_usdt()
            from config.settings import PORTFOLIO_BUDGET
            pnl     = total - PORTFOLIO_BUDGET
            pnl_pct = (pnl / PORTFOLIO_BUDGET * 100) if PORTFOLIO_BUDGET > 0 else 0

            text += (
                f"💼 <b>المحفظة</b>\n"
                f"الرصيد الكلي:  <code>${total:.2f}</code>\n"
                f"الرصيد المتاح: <code>${available:.2f}</code>\n"
                f"الربح/الخسارة: <code>${pnl:+.2f} ({pnl_pct:+.1f}%)</code>\n"
                f"الصفقات المفتوحة: <code>{len(portfolio._open_symbols)}</code>\n\n"
            )
        except Exception as e:
            logger.error(f"[Report] خطأ في الرصيد: {e}")

    # إحصائيات قاعدة البيانات
    try:
        from core.database import calculate_performance_stats
        stats = calculate_performance_stats()
        if stats and stats.get("total_executed", 0) > 0:
            text += (
                f"📊 <b>إحصائيات أمس</b>\n"
                f"إجمالي الصفقات: <code>{stats['total_executed']}</code>\n"
                f"وصل TP1: <code>{stats['tp1_hit_rate']}%</code>\n"
                f"وصل TP2: <code>{stats['tp2_hit_rate']}%</code>\n"
                f"وقف الخسارة: <code>{stats['sl_hit_rate']}%</code>\n"
            )
        else:
            text += "📊 لا توجد صفقات مكتملة بعد\n"
    except Exception as e:
        logger.error(f"[Report] خطأ في الإحصائيات: {e}")

    text += "\n💡 استخدم /status لمزيد من التفاصيل"
    return text


def build_weekly_report(exchange, portfolio=None) -> str:
    """تقرير أسبوعي شامل"""
    now = datetime.now(timezone.utc)

    text = (
        f"📈 <b>التقرير الأسبوعي</b>\n"
        f"أسبوع {now.strftime('%Y-W%U')}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    if portfolio:
        try:
            available = portfolio.get_available_usdt()
            total     = portfolio.get_total_usdt()
            from config.settings import PORTFOLIO_BUDGET
            pnl     = total - PORTFOLIO_BUDGET
            pnl_pct = (pnl / PORTFOLIO_BUDGET * 100) if PORTFOLIO_BUDGET > 0 else 0

            text += (
                f"💼 <b>أداء المحفظة</b>\n"
                f"الميزانية الأصلية: <code>${PORTFOLIO_BUDGET:.2f}</code>\n"
                f"القيمة الحالية:    <code>${total:.2f}</code>\n"
                f"صافي الربح/الخسارة: <code>${pnl:+.2f} ({pnl_pct:+.1f}%)</code>\n"
                f"الرصيد المتاح:     <code>${available:.2f}</code>\n\n"
            )
        except Exception as e:
            logger.error(f"[Report Weekly] {e}")

    try:
        from core.database import calculate_performance_stats
        stats = calculate_performance_stats()
        if stats and stats.get("total_signals", 0) > 0:
            win_rate = stats["tp1_hit_rate"]
            text += (
                f"📊 <b>إحصائيات الأسبوع</b>\n"
                f"إجمالي الإشارات:  <code>{stats['total_signals']}</code>\n"
                f"منها مُنفَّذة:     <code>{stats['total_executed']}</code>\n"
                f"مُتجاهلة:         <code>{stats['total_ignored']}</code>\n\n"
                f"<b>معدل النجاح</b>\n"
                f"وصل TP1: <code>{stats['tp1_hit_rate']}%</code> ✅\n"
                f"وصل TP2: <code>{stats['tp2_hit_rate']}%</code> ✅\n"
                f"وصل TP3: <code>{stats['tp3_hit_rate']}%</code> ✅\n"
                f"وقف الخسارة: <code>{stats['sl_hit_rate']}%</code> ❌\n"
                f"متوسط أعلى ربح: <code>{stats['avg_max_pct']}%</code>\n\n"
            )

            # تقييم الأداء
            if win_rate >= 60:
                assessment = "🟢 ممتاز — الاستراتيجية تعمل بكفاءة عالية"
            elif win_rate >= 40:
                assessment = "🟡 جيد — أداء مقبول مع مجال للتحسين"
            else:
                assessment = "🔴 يحتاج مراجعة — قد تحتاج لتعديل الفلاتر"

            text += f"<b>التقييم العام:</b> {assessment}\n\n"
            text += f"أفضل إشارة: <code>{stats.get('best_signal_type', '—')}</code>\n"
        else:
            text += "📊 لا توجد بيانات كافية بعد — استمر في التشغيل\n"

    except Exception as e:
        logger.error(f"[Report Weekly Stats] {e}")

    return text


def build_monthly_report(exchange, portfolio=None) -> str:
    """تقرير شهري استراتيجي"""
    now = datetime.now(timezone.utc)

    text = (
        f"📆 <b>التقرير الشهري</b>\n"
        f"{now.strftime('%B %Y')}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    if portfolio:
        try:
            total = portfolio.get_total_usdt()
            from config.settings import PORTFOLIO_BUDGET
            pnl     = total - PORTFOLIO_BUDGET
            pnl_pct = (pnl / PORTFOLIO_BUDGET * 100) if PORTFOLIO_BUDGET > 0 else 0

            # تقدير سنوي
            monthly_rate  = pnl_pct
            annual_est    = ((1 + monthly_rate/100) ** 12 - 1) * 100

            text += (
                f"💼 <b>نمو المحفظة</b>\n"
                f"الميزانية الأصلية: <code>${PORTFOLIO_BUDGET:.2f}</code>\n"
                f"القيمة الحالية:    <code>${total:.2f}</code>\n"
                f"العائد الشهري:     <code>{pnl_pct:+.1f}%</code>\n"
                f"العائد السنوي المتوقع: <code>{annual_est:+.1f}%</code>\n\n"
            )
        except Exception as e:
            logger.error(f"[Report Monthly] {e}")

    try:
        from core.database import calculate_performance_stats
        stats = calculate_performance_stats()
        if stats and stats.get("total_signals", 0) > 0:
            text += (
                f"📊 <b>تحليل الأداء الشهري</b>\n"
                f"إجمالي الإشارات:  <code>{stats['total_signals']}</code>\n"
                f"معدل التنفيذ:     <code>{stats['total_executed']}/{stats['total_signals']}</code>\n"
                f"متوسط الربح:      <code>{stats['avg_max_pct']}%</code>\n"
                f"أفضل صفقة:        <code>{stats['best_trade_pct']}%</code>\n\n"
            )

            # توصيات تلقائية
            text += f"🔧 <b>توصيات التحسين</b>\n"
            if stats["sl_hit_rate"] > 40:
                text += "• وقف الخسارة يُفعَّل كثيراً — فكّر في رفع RSI_OVERSOLD_THRESHOLD\n"
            if stats["tp1_hit_rate"] < 40:
                text += "• TP1 لا يُصل كثيراً — فكّر في تقليل الهدف الأول\n"
            if stats["tp3_hit_rate"] > 30:
                text += "• TP3 يُصل بنسبة جيدة — يمكن رفع TRADE_PCT_OF_BALANCE\n"
            if stats["total_executed"] < 5:
                text += "• صفقات قليلة — فكّر في رفع RSI_OVERSOLD_THRESHOLD أو خفض MIN_DAILY_VOLUME_USD\n"

    except Exception as e:
        logger.error(f"[Report Monthly Stats] {e}")

    return text


def build_trade_alert(symbol: str, event: str, price: float,
                      pct: float, opp=None) -> str:
    """تنبيه فوري عند حدث مهم"""
    emoji = {
        "hit_tp1":  "🎯",
        "hit_tp2":  "🎯🎯",
        "hit_tp3":  "🎯🎯🎯",
        "hit_stop": "🛑",
        "low_balance": "⚠️",
    }.get(event, "📢")

    label = {
        "hit_tp1":  "وصل الهدف الأول TP1",
        "hit_tp2":  "وصل الهدف الثاني TP2",
        "hit_tp3":  "وصل الهدف الثالث TP3",
        "hit_stop": "وُقِّف وقف الخسارة",
        "low_balance": "تحذير: الرصيد منخفض",
    }.get(event, event)

    text = (
        f"{emoji} <b>{label}</b>\n\n"
        f"العملة: <code>{symbol}</code>\n"
        f"السعر: <code>{price:.8g}</code>\n"
        f"التغيير: <code>{pct:+.1f}%</code>\n"
    )

    if opp and event in ["hit_tp1", "hit_tp2", "hit_tp3"]:
        text += f"\nالأهداف المتبقية:\n"
        if event == "hit_tp1":
            text += (
                f"TP2: <code>{opp.tp2:.8g}</code> (+{opp.tp2_pct}%)\n"
                f"TP3: <code>{opp.tp3:.8g}</code> (+{opp.tp3_pct}%)\n"
            )
        elif event == "hit_tp2":
            text += f"TP3: <code>{opp.tp3:.8g}</code> (+{opp.tp3_pct}%)\n"

    return text


# ==========================================
# جدولة التقارير — Background Thread
# ==========================================
class ReportScheduler:

    def __init__(self, bot_token: str, chat_id: int,
                 exchange=None, portfolio=None):
        self.bot_token = bot_token
        self.chat_id   = chat_id
        self.exchange  = exchange
        self.portfolio = portfolio
        self._running  = False

    def _send_sync(self, text: str):
        """إرسال رسالة بدون async"""
        import requests
        try:
            requests.post(
                f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                json={
                    "chat_id":    self.chat_id,
                    "text":       text,
                    "parse_mode": "HTML",
                },
                timeout=15,
            )
        except Exception as e:
            logger.error(f"[Report Send] {e}")

    def _scheduler_loop(self):
        """يعمل في الخلفية ويرسل التقارير في الأوقات المحددة"""
        logger.info("[Reports] ✅ Scheduler نشط")

        last_daily   = None
        last_weekly  = None
        last_monthly = None

        while self._running:
            now = datetime.now(timezone.utc)

            # ── يومي: كل يوم الساعة 8:00 UTC ──
            today = now.date()
            if (now.hour == 8 and now.minute == 0 and
                    last_daily != today):
                try:
                    logger.info("[Reports] إرسال التقرير اليومي...")
                    text = build_daily_report(self.exchange, self.portfolio)
                    self._send_sync(text)
                    last_daily = today
                except Exception as e:
                    logger.error(f"[Report Daily] {e}")

            # ── أسبوعي: كل الأحد ──
            week = now.isocalendar()[1]
            if (now.weekday() == 6 and now.hour == 8 and now.minute == 30 and
                    last_weekly != week):
                try:
                    logger.info("[Reports] إرسال التقرير الأسبوعي...")
                    text = build_weekly_report(self.exchange, self.portfolio)
                    self._send_sync(text)
                    last_weekly = week
                except Exception as e:
                    logger.error(f"[Report Weekly] {e}")

            # ── شهري: أول كل شهر ──
            month = (now.year, now.month)
            if (now.day == 1 and now.hour == 9 and now.minute == 0 and
                    last_monthly != month):
                try:
                    logger.info("[Reports] إرسال التقرير الشهري...")
                    text = build_monthly_report(self.exchange, self.portfolio)
                    self._send_sync(text)
                    last_monthly = month
                except Exception as e:
                    logger.error(f"[Report Monthly] {e}")

            # فحص كل دقيقة
            time.sleep(60)

    def start(self):
        self._running = True
        t = threading.Thread(target=self._scheduler_loop, daemon=True)
        t.start()
        logger.info("[Reports] ✅ التقارير المجدولة نشطة")
        return t

    def stop(self):
        self._running = False
