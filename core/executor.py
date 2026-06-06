"""
core/executor.py
=================
محرك التنفيذ عبر MEXC
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
        logger.info("✅ Executor متصل بـ MEXC")

    def _connect(self) -> ccxt.mexc:
        return ccxt.mexc({
            "apiKey": MEXC_API_KEY,
            "secret": MEXC_API_SECRET,
            "options": {
                "defaultType":  "spot",
                "fetchMarkets": ["spot"],
            },
            "enableRateLimit": True,
            "timeout":         30000,
        })

    def _calculate_quantity(self, symbol: str, price: float) -> float:
        """يحسب الكمية بناءً على TRADE_AMOUNT_USD"""
        try:
            market  = self.exchange.market(symbol)
            raw_qty = TRADE_AMOUNT_USD / price

            # الحد الأدنى
            min_qty = market.get("limits", {}).get("amount", {}).get("min", 0) or 0

            # دقة الكمية
            precision = market.get("precision", {}).get("amount", 8)
            if isinstance(precision, int):
                factor = 10 ** precision
                qty    = math.floor(raw_qty * factor) / factor
            elif isinstance(precision, float) and precision > 0:
                qty = math.floor(raw_qty / precision) * precision
            else:
                qty = round(raw_qty, 6)

            if qty <= 0:
                raise ValueError(f"الكمية المحسوبة صفر أو سالبة: {qty}")

            if min_qty > 0 and qty < min_qty:
                raise ValueError(
                    f"الكمية {qty:.8f} أقل من الحد الأدنى {min_qty} لـ {symbol}. "
                    f"زد TRADE_AMOUNT_USD في Railway"
                )

            logger.info(f"[Executor] الكمية لـ {symbol}: {qty} (من ${TRADE_AMOUNT_USD})")
            return qty

        except Exception as e:
            logger.error(f"[Executor] خطأ في حساب الكمية لـ {symbol}: {e}")
            raise

    def place_market_buy(self, symbol: str, entry_price: float) -> Optional[dict]:
        """ينفذ أمر شراء فوري"""
        logger.info(f"[Executor] شراء {symbol} @ ~{entry_price:.8g}")
        try:
            qty = self._calculate_quantity(symbol, entry_price)

            order = self.exchange.create_market_buy_order(
                symbol = symbol,
                amount = qty,
                params = {"type": "spot"},
            )

            filled_price = float(
                order.get("average") or
                order.get("price") or
                entry_price
            )
            filled_qty = float(order.get("filled") or qty)

            logger.info(
                f"✅ تم الشراء: {symbol} | "
                f"{filled_qty} @ {filled_price:.8g} | "
                f"ID: {order['id']}"
            )
            return {
                "order_id":    order["id"],
                "filled_price": filled_price,
                "filled_qty":   filled_qty,
            }

        except ccxt.InsufficientFunds as e:
            logger.error(f"[Executor] رصيد غير كافٍ لـ {symbol}: {e}")
            return None
        except ccxt.NetworkError as e:
            logger.error(f"[Executor] خطأ شبكة لـ {symbol}: {e}")
            return None
        except ValueError as e:
            logger.error(f"[Executor] خطأ كمية لـ {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"[Executor] خطأ غير متوقع لـ {symbol}: {e}")
            return None

    def place_tp_and_sl_orders(
        self,
        symbol:       str,
        filled_qty:   float,
        filled_price: float,
        nearest_support: float,
    ) -> dict:
        """يضع أوامر الأهداف ووقف الخسارة"""
        from config.settings import TP1_PCT, TP2_PCT, TP3_PCT, STOP_LOSS_PCT

        tp1 = filled_price * (1 + TP1_PCT)
        tp2 = filled_price * (1 + TP2_PCT)
        tp3 = filled_price * (1 + TP3_PCT)
        sl  = nearest_support * (1 - STOP_LOSS_PCT)

        qty1 = round(filled_qty * TP1_QTY_PCT, 8)
        qty2 = round(filled_qty * TP2_QTY_PCT, 8)
        qty3 = round(filled_qty * TP3_QTY_PCT, 8)

        logger.info(
            f"[Executor] أوامر البيع لـ {symbol}:\n"
            f"  TP1={tp1:.8g} ({qty1}) | TP2={tp2:.8g} ({qty2}) | "
            f"TP3={tp3:.8g} ({qty3}) | SL={sl:.8g}"
        )

        results = {"tp1": tp1, "tp2": tp2, "tp3": tp3, "sl": sl}

        # TP1
        try:
            o = self.exchange.create_limit_sell_order(symbol, qty1, tp1)
            results["tp1_order_id"] = o["id"]
            logger.info(f"✅ TP1: {tp1:.8g} | ID: {o['id']}")
        except Exception as e:
            logger.error(f"❌ TP1 فشل لـ {symbol}: {e}")

        # TP2
        try:
            o = self.exchange.create_limit_sell_order(symbol, qty2, tp2)
            results["tp2_order_id"] = o["id"]
            logger.info(f"✅ TP2: {tp2:.8g} | ID: {o['id']}")
        except Exception as e:
            logger.error(f"❌ TP2 فشل لـ {symbol}: {e}")

        # TP3
        try:
            o = self.exchange.create_limit_sell_order(symbol, qty3, tp3)
            results["tp3_order_id"] = o["id"]
            logger.info(f"✅ TP3: {tp3:.8g} | ID: {o['id']}")
        except Exception as e:
            logger.error(f"❌ TP3 فشل لـ {symbol}: {e}")

        # Stop Loss
        try:
            o = self.exchange.create_order(
                symbol = symbol,
                type   = "STOP_LOSS_LIMIT",
                side   = "sell",
                amount = filled_qty,
                price  = sl * 0.995,
                params = {"stopPrice": sl, "type": "spot"},
            )
            results["sl_order_id"] = o["id"]
            logger.info(f"✅ SL: {sl:.8g} | ID: {o['id']}")
        except Exception as e:
            logger.error(f"❌ SL فشل لـ {symbol}: {e} — راجع الصفقة يدوياً!")

        return results

    def execute_full_trade(self, opportunity) -> dict:
        """
        ينفذ الصفقة كاملة:
        شراء → أوامر أهداف → وقف خسارة
        """
        symbol = opportunity.symbol
        logger.info(f"🚀 تنفيذ صفقة: {symbol}")

        # الشراء
        buy_result = self.place_market_buy(symbol, opportunity.entry_price)
        if not buy_result:
            return {
                "success": False,
                "symbol":  symbol,
                "error":   "فشل أمر الشراء — تحقق من الرصيد والحد الأدنى للكمية",
            }

        # أوامر البيع
        tp_sl = self.place_tp_and_sl_orders(
            symbol          = symbol,
            filled_qty      = buy_result["filled_qty"],
            filled_price    = buy_result["filled_price"],
            nearest_support = opportunity.nearest_support,
        )

        return {
            "success":      True,
            "symbol":       symbol,
            "buy_order_id": buy_result["order_id"],
            "filled_price": buy_result["filled_price"],
            "filled_qty":   buy_result["filled_qty"],
            **tp_sl,
        }
