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
    TRADE_AMOUNT_USD, TP1_QTY_PCT, TP2_QTY_PCT, TP3_QTY_PCT,
)
from core.scanner import TradeOpportunity
from utils.logger import logger


class TradeExecutor:

    def __init__(self):
        self.exchange = self._connect()
        logger.info("✅ Executor متصل بـ MEXC")

    def _connect(self) -> ccxt.mexc:
        return ccxt.mexc({
            "apiKey": MEXC_API_KEY,
            "secret": MEXC_API_SECRET,
            "options": {"defaultType": "spot"},
            "enableRateLimit": True,
        })

    def _calculate_quantity(self, symbol: str, price: float) -> float:
        market  = self.exchange.market(symbol)
        raw_qty = TRADE_AMOUNT_USD / price
        min_qty = market.get("limits", {}).get("amount", {}).get("min", 0)
        precision = market.get("precision", {}).get("amount", 8)

        if isinstance(precision, int):
            factor = 10 ** precision
            qty = math.floor(raw_qty * factor) / factor
        else:
            qty = math.floor(raw_qty / precision) * precision

        if qty < min_qty:
            raise ValueError(
                f"الكمية {qty} أقل من الحد الأدنى {min_qty} لـ {symbol}. "
                f"زد TRADE_AMOUNT_USD"
            )
        return qty

    def place_market_buy(self, opportunity: TradeOpportunity) -> Optional[dict]:
        symbol = opportunity.symbol
        logger.info(f"[Execute] شراء {symbol} بسعر ~{opportunity.entry_price:.6f}")
        try:
            qty = self._calculate_quantity(symbol, opportunity.entry_price)
            order = self.exchange.create_market_buy_order(
                symbol=symbol, amount=qty,
                params={"type": "spot"}
            )
            filled_price = float(order.get("average") or order.get("price") or opportunity.entry_price)
            filled_qty   = float(order.get("filled") or qty)
            logger.info(f"✅ تم الشراء: {symbol} | {filled_qty} @ {filled_price:.6f} | ID: {order['id']}")
            return {"order_id": order["id"], "filled_price": filled_price, "filled_qty": filled_qty}
        except ccxt.InsufficientFunds as e:
            logger.error(f"[Execute] رصيد غير كافٍ: {e}")
            return None
        except ccxt.NetworkError as e:
            logger.error(f"[Execute] خطأ شبكة: {e}")
            return None
        except ValueError as e:
            logger.error(f"[Execute] خطأ كمية: {e}")
            return None
        except Exception as e:
            logger.error(f"[Execute] خطأ غير متوقع: {e}")
            return None

    def place_tp_and_sl_orders(self, opportunity: TradeOpportunity, filled_qty: float, filled_price: float) -> dict:
        symbol  = opportunity.symbol
        results = {}
        from config.settings import TP1_PCT, TP2_PCT, TP3_PCT, STOP_LOSS_PCT

        tp1 = filled_price * (1 + TP1_PCT)
        tp2 = filled_price * (1 + TP2_PCT)
        tp3 = filled_price * (1 + TP3_PCT)
        sl  = opportunity.nearest_support * (1 - STOP_LOSS_PCT)

        qty1 = round(filled_qty * TP1_QTY_PCT, 8)
        qty2 = round(filled_qty * TP2_QTY_PCT, 8)
        qty3 = round(filled_qty * TP3_QTY_PCT, 8)

        logger.info(f"[Execute] أوامر البيع لـ {symbol}: TP1={tp1:.6f} TP2={tp2:.6f} TP3={tp3:.6f} SL={sl:.6f}")

        # TP1
        try:
            o = self.exchange.create_limit_sell_order(symbol, qty1, tp1)
            results["tp1_order_id"] = o["id"]
            logger.info(f"✅ TP1: {tp1:.6f} | ID: {o['id']}")
        except Exception as e:
            logger.error(f"❌ TP1 فشل: {e}")

        # TP2
        try:
            o = self.exchange.create_limit_sell_order(symbol, qty2, tp2)
            results["tp2_order_id"] = o["id"]
            logger.info(f"✅ TP2: {tp2:.6f} | ID: {o['id']}")
        except Exception as e:
            logger.error(f"❌ TP2 فشل: {e}")

        # TP3
        try:
            o = self.exchange.create_limit_sell_order(symbol, qty3, tp3)
            results["tp3_order_id"] = o["id"]
            logger.info(f"✅ TP3: {tp3:.6f} | ID: {o['id']}")
        except Exception as e:
            logger.error(f"❌ TP3 فشل: {e}")

        # Stop Loss — MEXC يدعم stop_limit
        try:
            o = self.exchange.create_order(
                symbol=symbol, type="STOP_LOSS_LIMIT", side="sell",
                amount=filled_qty, price=sl * 0.995,
                params={"stopPrice": sl, "type": "spot"}
            )
            results["sl_order_id"] = o["id"]
            logger.info(f"✅ SL: {sl:.6f} | ID: {o['id']}")
        except Exception as e:
            logger.error(f"❌ SL فشل: {e} — راجع الصفقة يدوياً!")

        results.update({"tp1": tp1, "tp2": tp2, "tp3": tp3, "sl": sl, "symbol": symbol})
        return results

    def execute_full_trade(self, opportunity: TradeOpportunity) -> dict:
        logger.info(f"🚀 تنفيذ صفقة: {opportunity.symbol}")
        buy_result = self.place_market_buy(opportunity)
        if not buy_result:
            return {"success": False, "error": "فشل أمر الشراء"}
        tp_sl = self.place_tp_and_sl_orders(
            opportunity, buy_result["filled_qty"], buy_result["filled_price"]
        )
        return {
            "success": True,
            "symbol": opportunity.symbol,
            "buy_order_id": buy_result["order_id"],
            "filled_price": buy_result["filled_price"],
            "filled_qty": buy_result["filled_qty"],
            **tp_sl,
        }
