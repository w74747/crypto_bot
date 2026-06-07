"""
core/executor.py
================
إصلاح MEXC: يستخدم quoteOrderQty (مبلغ USDT) بدل الكمية
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
                "createMarketBuyOrderRequiresPrice": False,
            },
            "enableRateLimit": True,
            "timeout":         30000,
        })
        try:
            exchange.load_markets()
            logger.info(f"✅ Executor متصل بـ MEXC — {len(exchange.markets)} سوق")
        except Exception as e:
            logger.error(f"❌ Executor فشل: {e}")
            raise
        return exchange

    def _ensure_markets_loaded(self):
        if not self.exchange.markets:
            logger.warning("[Executor] إعادة تحميل الأسواق...")
            self.exchange.load_markets()

    def place_market_buy(self, symbol: str, entry_price: float) -> Optional[dict]:
        """
        شراء فوري على MEXC
        MEXC يحتاج المبلغ بالـ USDT (quoteOrderQty) وليس الكمية
        """
        logger.info(
            f"[Executor] شراء {symbol} | "
            f"سعر: ~{entry_price:.8g} | مبلغ: ${TRADE_AMOUNT_USD}"
        )
        try:
            self._ensure_markets_loaded()

            order = self.exchange.create_market_buy_order(
                symbol = symbol,
                amount = TRADE_AMOUNT_USD,
                params = {
                    "type":          "spot",
                    "quoteOrderQty": TRADE_AMOUNT_USD,
                },
            )

            filled_price = float(
                order.get("average") or
                order.get("price") or
                entry_price
            )
            filled_qty = float(
                order.get("filled") or
                (TRADE_AMOUNT_USD / filled_price if filled_price > 0 else 0)
            )

            logger.info(
                f"✅ تم الشراء: {symbol} | "
                f"{filled_qty:.4f} @ {filled_price:.8g} | "
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
        except Exception as e:
            logger.error(f"[Executor] خطأ في {symbol}: {e}")
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
            f"TP1={tp1:.8g}({qty1}) | "
            f"TP2={tp2:.8g}({qty2}) | "
            f"TP3={tp3:.8g}({qty3}) | "
            f"SL={sl:.8g}"
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
