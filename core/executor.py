"""
core/executor.py
إصلاح: mexc markets not loaded بعد ساعات من التشغيل
إضافة: إعادة المحاولة عند فشل التنفيذ
"""

import ccxt
import math
from typing import Optional

from config.settings import (
    MEXC_API_KEY, MEXC_API_SECRET,
    TRADE_AMOUNT_USD,
    TP1_QTY_PCT, TP2_QTY_PCT, TP3_QTY_PCT,
)
from utils.logger import logger


class TradeExecutor:

    def __init__(self):
        self.exchange = self._connect()

    def _connect(self) -> ccxt.mexc:
        exchange = ccxt.mexc({
            "apiKey": MEXC_API_KEY,
            "secret": MEXC_API_SECRET,
            "options": {
                "defaultType":  "spot",
                "fetchMarkets": ["spot"],
            },
            "enableRateLimit": True,
            "timeout":         30000,
        })
        try:
            exchange.load_markets()
            logger.info(f"✅ Executor متصل بـ MEXC — {len(exchange.markets)} سوق")
        except Exception as e:
            logger.error(f"❌ Executor فشل تحميل الأسواق: {e}")
            raise
        return exchange

    def _ensure_markets_loaded(self):
        """يتحقق أن الأسواق محمّلة ويُعيد التحميل إذا لزم"""
        if not self.exchange.markets:
            logger.warning("[Executor] الأسواق غير محمّلة — إعادة التحميل...")
            self.exchange.load_markets()
            logger.info(f"[Executor] تم إعادة التحميل: {len(self.exchange.markets)} سوق")

    def _calculate_quantity(self, symbol: str, price: float) -> float:
        self._ensure_markets_loaded()

        market  = self.exchange.market(symbol)
        raw_qty = TRADE_AMOUNT_USD / price

        min_qty   = market.get("limits", {}).get("amount", {}).get("min", 0) or 0
        precision = market.get("precision", {}).get("amount", 8)

        if isinstance(precision, int):
            factor = 10 ** precision
            qty    = math.floor(raw_qty * factor) / factor
        elif isinstance(precision, float) and precision > 0:
            qty = math.floor(raw_qty / precision) * precision
        else:
            qty = round(raw_qty, 6)

        if qty <= 0:
            raise ValueError(f"الكمية صفر أو سالبة: {qty}")

        if min_qty > 0 and qty < min_qty:
            raise ValueError(
                f"الكمية {qty:.8f} أقل من الحد الأدنى {min_qty} لـ {symbol}. "
                f"زد TRADE_AMOUNT_USD في Railway"
            )

        logger.info(f"[Executor] الكمية: {qty} {symbol} (من ${TRADE_AMOUNT_USD})")
        return qty

    def place_market_buy(self, symbol: str, entry_price: float) -> Optional[dict]:
        logger.info(f"[Executor] شراء {symbol} @ ~{entry_price:.8g}")
        try:
            qty = self._calculate_quantity(symbol, entry_price)
            order = self.exchange.create_market_buy_order(
                symbol=symbol,
                amount=qty,
                params={"type": "spot"},
            )
            filled_price = float(
                order.get("average") or order.get("price") or entry_price
            )
            filled_qty = float(order.get("filled") or qty)
            logger.info(
                f"✅ تم الشراء: {symbol} | "
                f"{filled_qty} @ {filled_price:.8g} | "
                f"ID: {order['id']}"
            )
            return {
                "order_id":     order["id"],
                "filled_price": filled_price,
                "filled_qty":   filled_qty,
            }
        except ccxt.InsufficientFunds as e:
            logger.error(f"[Executor] رصيد غير كافٍ في Spot: {e}")
            return None
        except ccxt.NetworkError as e:
            logger.error(f"[Executor] خطأ شبكة: {e}")
            return None
        except ValueError as e:
            logger.error(f"[Executor] خطأ كمية: {e}")
            return None
        except Exception as e:
            logger.error(f"[Executor] خطأ غير متوقع لـ {symbol}: {e}")
            return None

    def place_tp_and_sl_orders(
        self,
        symbol:          str,
        filled_qty:      float,
        filled_price:    float,
        nearest_support: float,
    ) -> dict:
        from config.settings import TP1_PCT, TP2_PCT, TP3_PCT, STOP_LOSS_PCT

        tp1 = filled_price * (1 + TP1_PCT)
        tp2 = filled_price * (1 + TP2_PCT)
        tp3 = filled_price * (1 + TP3_PCT)
        sl  = nearest_support * (1 - STOP_LOSS_PCT)

        qty1 = round(filled_qty * TP1_QTY_PCT, 8)
        qty2 = round(filled_qty * TP2_QTY_PCT, 8)
        qty3 = round(filled_qty * TP3_QTY_PCT, 8)

        logger.info(
            f"[Executor] أوامر {symbol}: "
            f"TP1={tp1:.8g} | TP2={tp2:.8g} | TP3={tp3:.8g} | SL={sl:.8g}"
        )

        results = {"tp1": tp1, "tp2": tp2, "tp3": tp3, "sl": sl}

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
                symbol=symbol,
                type="STOP_LOSS_LIMIT",
                side="sell",
                amount=filled_qty,
                price=sl * 0.995,
                params={"stopPrice": sl, "type": "spot"},
            )
            results["sl_order_id"] = o["id"]
            logger.info(f"✅ SL: {sl:.8g} | ID: {o['id']}")
        except Exception as e:
            logger.error(f"❌ SL فشل لـ {symbol}: {e} — راجع يدوياً!")

        return results

    def execute_full_trade(self, opportunity) -> dict:
        symbol = opportunity.symbol
        logger.info(f"🚀 تنفيذ صفقة: {symbol}")

        buy = self.place_market_buy(symbol, opportunity.entry_price)
        if not buy:
            return {
                "success": False,
                "symbol":  symbol,
                "error":   "فشل أمر الشراء — تحقق من رصيد Spot في MEXC",
            }

        tp_sl = self.place_tp_and_sl_orders(
            symbol          = symbol,
            filled_qty      = buy["filled_qty"],
            filled_price    = buy["filled_price"],
            nearest_support = opportunity.nearest_support,
        )

        return {
            "success":      True,
            "symbol":       symbol,
            "buy_order_id": buy["order_id"],
            "filled_price": buy["filled_price"],
            "filled_qty":   buy["filled_qty"],
            **tp_sl,
        }
