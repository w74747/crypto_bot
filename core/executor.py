"""
core/executor.py
================
TRADE_INVESTMENT_AMOUNT من Railway — مبلغ ثابت بالضبط
MEXC Market Buy: create_market_buy_order(symbol, cost) — ccxt يحوله لـ quoteOrderQty
"""

import os
import math
import ccxt
from typing import Optional

from config.settings import (
    MEXC_API_KEY, MEXC_API_SECRET,
    TP1_QTY_PCT, TP2_QTY_PCT, TP3_QTY_PCT,
)
from utils.logger import logger

# ── المبلغ الثابت من Railway — يُقرأ لحظة التنفيذ ──
def _get_capital() -> float:
    return float(os.environ.get("TRADE_INVESTMENT_AMOUNT", "30"))


class TradeExecutor:

    def __init__(self):
        self.exchange  = self._connect()
        self.portfolio = None

    def _connect(self) -> ccxt.mexc:
        exchange = ccxt.mexc({
            "apiKey":  MEXC_API_KEY,
            "secret":  MEXC_API_SECRET,
            "options": {
                "defaultType":                       "spot",
                "fetchMarkets":                      ["spot"],
                "createMarketBuyOrderRequiresPrice": False,
            },
            "enableRateLimit": True,
            "timeout":         60000,
        })
        try:
            exchange.load_markets()
            logger.info(f"✅ Executor متصل بـ MEXC — {len(exchange.markets)} سوق")
        except Exception as e:
            logger.error(f"❌ Executor فشل: {e}")
            raise
        return exchange

    def init_portfolio(self):
        """تهيئة اختيارية — غير مطلوبة لتحديد المبلغ"""
        try:
            from core.portfolio import PortfolioManager
            self.portfolio = PortfolioManager(self.exchange)
            logger.info("✅ Portfolio Manager نشط (للإحصائيات فقط)")
        except Exception as e:
            logger.warning(f"[Portfolio] {e}")

    def _ensure_markets_loaded(self):
        if not self.exchange.markets:
            logger.warning("[Executor] إعادة تحميل الأسواق...")
            self.exchange.load_markets()

    def _get_live_price(self, symbol: str, fallback: float) -> float:
        """السعر الحالي الفعلي لحظة التنفيذ"""
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            price  = float(ticker.get("last") or ticker.get("close") or 0)
            return price if price > 0 else fallback
        except Exception as e:
            logger.warning(f"[Executor] سعر حي {symbol}: {e}")
            return fallback

    def place_market_buy(
        self,
        symbol:      str,
        entry_price: float,
    ) -> Optional[dict]:
        """
        MEXC Spot Market Buy
        ─────────────────────
        المبلغ: TRADE_INVESTMENT_AMOUNT من Railway (ثابت)
        الطريقة: create_market_buy_order(symbol, cost_in_usdt)
        ccxt يترجمها تلقائياً لـ quoteOrderQty عند MEXC

        لا حاجة لحساب qty يدوياً — MEXC يحسب الكمية من المبلغ
        """
        capital    = _get_capital()
        live_price = self._get_live_price(symbol, entry_price)

        logger.info(
            f"[Executor] {symbol} | "
            f"سعر الإشارة: {entry_price:.8g} | "
            f"السعر الحالي: {live_price:.8g} | "
            f"رأس المال: ${capital:.2f}"
        )

        try:
            self._ensure_markets_loaded()

            # ── MEXC: أرسل التكلفة الإجمالية بالـ USDT ──
            # ccxt يضع createMarketBuyOrderRequiresPrice=False
            # ويُرسل quoteOrderQty=capital إلى MEXC تلقائياً
            order = self.exchange.create_market_buy_order(
                symbol = symbol,
                amount = capital,          # ← USDT cost وليس qty
                params = {"quoteOrderQty": capital},
            )

            filled_price = float(
                order.get("average") or
                order.get("price")   or
                live_price
            )
            filled_qty = float(
                order.get("filled") or
                (capital / filled_price if filled_price > 0 else 0)
            )

            logger.info(
                f"✅ تم الشراء: {symbol} | "
                f"{filled_qty:.6f} @ {filled_price:.8g} | "
                f"${capital:.2f} | ID: {order['id']}"
            )
            return {
                "order_id":     order["id"],
                "filled_price": filled_price,
                "filled_qty":   filled_qty,
                "amount_usd":   capital,
            }

        except ccxt.InsufficientFunds as e:
            logger.error(f"[Executor] رصيد غير كافٍ: {e}")
            return None
        except ccxt.NetworkError as e:
            logger.error(f"[Executor] خطأ شبكة: {e}")
            return None
        except Exception as e:
            logger.error(f"[Executor] خطأ في {symbol}: {e}")
            return None

    def place_tp_and_sl_orders(
        self,
        symbol:       str,
        filled_qty:   float,
        filled_price: float,
        tp1:          float,
        tp2:          float,
        tp3:          float,
        stop_loss:    float,
    ) -> dict:
        """يضع أوامر البيع بأسعار التوصية بالضبط"""

        # stepSize للبيع — MEXC يتطلب precision صحيح
        try:
            self._ensure_markets_loaded()
            market    = self.exchange.market(symbol)
            precision = market.get("precision", {}).get("amount", 4)

            def apply_precision(qty: float) -> float:
                if isinstance(precision, int):
                    factor = 10 ** precision
                    return math.floor(qty * factor) / factor
                elif isinstance(precision, float) and precision > 0:
                    return math.floor(qty / precision) * precision
                return round(qty, 6)

        except Exception:
            def apply_precision(qty): return round(qty, 4)

        qty1 = apply_precision(filled_qty * TP1_QTY_PCT)
        qty2 = apply_precision(filled_qty * TP2_QTY_PCT)
        qty3 = apply_precision(filled_qty * TP3_QTY_PCT)

        logger.info(
            f"[Executor] أوامر {symbol}:\n"
            f"  TP1={tp1:.8g} ×{qty1} | "
            f"TP2={tp2:.8g} ×{qty2} | "
            f"TP3={tp3:.8g} ×{qty3} | "
            f"SL={stop_loss:.8g}"
        )

        results = {
            "tp1": tp1, "tp2": tp2, "tp3": tp3,
            "sl":  stop_loss, "entry_price": filled_price,
        }

        for label, qty, price in [
            ("TP1", qty1, tp1),
            ("TP2", qty2, tp2),
            ("TP3", qty3, tp3),
        ]:
            try:
                o = self.exchange.create_limit_sell_order(symbol, qty, price)
                results[f"{label.lower()}_order_id"] = o["id"]
                logger.info(f"✅ {label}: {price:.8g} | ID: {o['id']}")
            except Exception as e:
                logger.error(f"❌ {label} فشل لـ {symbol}: {e}")

        try:
            o = self.exchange.create_order(
                symbol = symbol,
                type   = "STOP_LOSS_LIMIT",
                side   = "sell",
                amount = filled_qty,
                price  = stop_loss * 0.999,
                params = {"stopPrice": stop_loss, "type": "spot"},
            )
            results["sl_order_id"] = o["id"]
            logger.info(f"✅ SL: {stop_loss:.8g} | ID: {o['id']}")
        except Exception as e:
            logger.error(f"❌ SL فشل لـ {symbol}: {e} — راجع يدوياً!")

        return results

    def move_sl_to_breakeven(
        self, symbol: str, entry_price: float, remaining_qty: float
    ):
        """Break-Even: بعد TP1 → نقل SL إلى سعر الدخول"""
        try:
            remaining_qty = round(remaining_qty, 4)
            o = self.exchange.create_order(
                symbol = symbol,
                type   = "STOP_LOSS_LIMIT",
                side   = "sell",
                amount = remaining_qty,
                price  = entry_price * 0.999,
                params = {"stopPrice": entry_price, "type": "spot"},
            )
            logger.info(
                f"✅ Break-Even {symbol}: "
                f"SL → {entry_price:.8g} | ID: {o['id']}"
            )
            return o["id"]
        except Exception as e:
            logger.error(f"❌ Break-Even فشل لـ {symbol}: {e}")
            return None

    def execute_full_trade(self, opportunity) -> dict:
        """
        تنفيذ صفقة كاملة:
        1. MARKET BUY بـ TRADE_INVESTMENT_AMOUNT من Railway
        2. TP1/TP2/TP3 بأسعار التوصية بالضبط
        3. Stop-Loss ديناميكي
        """
        symbol  = opportunity.symbol
        capital = _get_capital()

        # فحص Portfolio إذا كان نشطاً
        try:
            from config.settings import PORTFOLIO_MODE
            if PORTFOLIO_MODE and self.portfolio:
                can_open, reason = self.portfolio.can_open_trade(symbol)
                if not can_open:
                    return {"success": False, "symbol": symbol, "error": reason}
        except Exception:
            pass

        logger.info(
            f"🚀 تنفيذ: {symbol} | ${capital:.2f}\n"
            f"  TP1={opportunity.tp1:.8g} | "
            f"TP2={opportunity.tp2:.8g} | "
            f"TP3={opportunity.tp3:.8g} | "
            f"SL={opportunity.stop_loss:.8g}"
        )

        # ── الشراء ──
        buy = self.place_market_buy(symbol, opportunity.entry_price)
        if not buy:
            return {
                "success": False,
                "symbol":  symbol,
                "error":   "فشل أمر الشراء — تحقق من رصيد USDT في Spot",
            }

        # سجّل في Portfolio
        try:
            from config.settings import PORTFOLIO_MODE
            if PORTFOLIO_MODE and self.portfolio:
                self.portfolio.register_open_trade(symbol)
        except Exception:
            pass

        # ── أوامر البيع بأهداف التوصية ──
        tp_sl = self.place_tp_and_sl_orders(
            symbol       = symbol,
            filled_qty   = buy["filled_qty"],
            filled_price = buy["filled_price"],
            tp1          = opportunity.tp1,
            tp2          = opportunity.tp2,
            tp3          = opportunity.tp3,
            stop_loss    = opportunity.stop_loss,
        )

        return {
            "success":      True,
            "symbol":       symbol,
            "buy_order_id": buy["order_id"],
            "filled_price": buy["filled_price"],
            "filled_qty":   buy["filled_qty"],
            "amount_usd":   capital,
            **tp_sl,
        }
