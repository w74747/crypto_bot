"""
core/portfolio.py
=================
Portfolio Manager — إدارة الميزانية تلقائياً
يقرأ الرصيد الفعلي من MEXC ويحسب مبلغ كل صفقة
"""

import os
import threading
from utils.logger import logger

from config.settings import (
    PORTFOLIO_MODE,
    PORTFOLIO_BUDGET,
    TRADE_PCT_OF_BALANCE,
    MAX_OPEN_TRADES,
    MIN_TRADE_AMOUNT,
)


class PortfolioManager:
    """
    يدير الميزانية ويقرر:
    - كم مبلغ الصفقة القادمة
    - هل يمكن فتح صفقة جديدة
    - كم صفقة مفتوحة حالياً
    """

    def __init__(self, exchange):
        self.exchange      = exchange
        self._lock         = threading.Lock()
        self._open_symbols: set[str] = set()   # العملات المفتوحة حالياً

    # ==========================================
    # قراءة الرصيد الفعلي من MEXC
    # ==========================================
    def get_available_usdt(self) -> float:
        """يجلب رصيد USDT المتاح في Spot"""
        try:
            balance = self.exchange.fetch_balance({"type": "spot"})
            usdt    = balance.get("USDT", {})
            free    = float(usdt.get("free", 0))
            logger.info(f"[Portfolio] رصيد USDT المتاح: ${free:.2f}")
            return free
        except Exception as e:
            logger.error(f"[Portfolio] خطأ في جلب الرصيد: {e}")
            return 0.0

    def get_total_usdt(self) -> float:
        """إجمالي USDT (متاح + مستثمر)"""
        try:
            balance = self.exchange.fetch_balance({"type": "spot"})
            usdt    = balance.get("USDT", {})
            return float(usdt.get("total", 0))
        except Exception:
            return 0.0

    # ==========================================
    # حساب مبلغ الصفقة
    # ==========================================
    def calculate_trade_amount(self) -> float:
        """
        يحسب مبلغ الصفقة القادمة بناءً على:
        - الرصيد المتاح فعلاً في MEXC
        - نسبة TRADE_PCT_OF_BALANCE من الرصيد
        - حد أدنى MIN_TRADE_AMOUNT
        """
        if not PORTFOLIO_MODE:
            from config.settings import TRADE_AMOUNT_USD
            return TRADE_AMOUNT_USD

        available = self.get_available_usdt()

        if available < MIN_TRADE_AMOUNT:
            logger.warning(
                f"[Portfolio] الرصيد المتاح ${available:.2f} "
                f"أقل من الحد الأدنى ${MIN_TRADE_AMOUNT}"
            )
            return 0.0

        # احسب نسبة من الرصيد
        trade_amount = available * TRADE_PCT_OF_BALANCE

        # لا تتجاوز 30% من الميزانية الكلية في صفقة واحدة
        max_single   = PORTFOLIO_BUDGET * 0.30
        trade_amount = min(trade_amount, max_single)

        # لا تقل عن الحد الأدنى
        trade_amount = max(trade_amount, MIN_TRADE_AMOUNT)

        # تقريب لعددين عشريين
        trade_amount = round(trade_amount, 2)

        logger.info(
            f"[Portfolio] مبلغ الصفقة القادمة: ${trade_amount:.2f} "
            f"({TRADE_PCT_OF_BALANCE*100:.0f}% من ${available:.2f})"
        )
        return trade_amount

    # ==========================================
    # التحقق من إمكانية فتح صفقة جديدة
    # ==========================================
    def can_open_trade(self, symbol: str) -> tuple[bool, str]:
        """
        يتحقق من شروط فتح صفقة جديدة:
        1. الرصيد كافٍ
        2. لم يتجاوز الحد الأقصى للصفقات
        3. العملة ليست مفتوحة بالفعل
        """
        with self._lock:
            # تحقق من الصفقات المفتوحة
            open_count = len(self._open_symbols)
            if open_count >= MAX_OPEN_TRADES:
                return False, (
                    f"وصل الحد الأقصى للصفقات: "
                    f"{open_count}/{MAX_OPEN_TRADES}"
                )

            # تحقق من تكرار العملة
            base = symbol.replace("/USDT", "")
            if base in self._open_symbols:
                return False, f"{symbol} مفتوحة بالفعل"

            # تحقق من الرصيد
            trade_amount = self.calculate_trade_amount()
            if trade_amount < MIN_TRADE_AMOUNT:
                return False, (
                    f"الرصيد غير كافٍ "
                    f"(متاح: ${self.get_available_usdt():.2f})"
                )

            return True, f"✅ يمكن فتح الصفقة | مبلغ: ${trade_amount:.2f}"

    def register_open_trade(self, symbol: str):
        """يسجّل صفقة جديدة كمفتوحة"""
        base = symbol.replace("/USDT", "")
        with self._lock:
            self._open_symbols.add(base)
        logger.info(
            f"[Portfolio] صفقة مفتوحة: {symbol} | "
            f"إجمالي: {len(self._open_symbols)}/{MAX_OPEN_TRADES}"
        )

    def register_closed_trade(self, symbol: str):
        """يسجّل صفقة كمغلقة"""
        base = symbol.replace("/USDT", "")
        with self._lock:
            self._open_symbols.discard(base)
        logger.info(
            f"[Portfolio] صفقة مغلقة: {symbol} | "
            f"متبقية: {len(self._open_symbols)}/{MAX_OPEN_TRADES}"
        )

    # ==========================================
    # تقرير الحالة
    # ==========================================
    def get_status_text(self) -> str:
        """نص ملخص لحالة المحفظة"""
        available    = self.get_available_usdt()
        total        = self.get_total_usdt()
        open_count   = len(self._open_symbols)
        trade_amount = self.calculate_trade_amount()
        pnl          = total - PORTFOLIO_BUDGET
        pnl_pct      = (pnl / PORTFOLIO_BUDGET * 100) if PORTFOLIO_BUDGET > 0 else 0

        return (
            f"💼 <b>Portfolio Manager</b>\n\n"
            f"الميزانية الأصلية: <code>${PORTFOLIO_BUDGET:.2f}</code>\n"
            f"الرصيد الكلي:      <code>${total:.2f}</code>\n"
            f"الرصيد المتاح:     <code>${available:.2f}</code>\n"
            f"الربح/الخسارة:    <code>${pnl:+.2f} ({pnl_pct:+.1f}%)</code>\n\n"
            f"الصفقات المفتوحة: <code>{open_count}/{MAX_OPEN_TRADES}</code>\n"
            f"مبلغ الصفقة التالية: <code>${trade_amount:.2f}</code>\n"
            f"({TRADE_PCT_OF_BALANCE*100:.0f}% من الرصيد المتاح)"
        )
