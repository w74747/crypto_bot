"""
core/executor.py
================
يجلب السعر الحالي الفعلي لحظة التنفيذ
يستخدم أهداف التوصية مباشرة
"""

import ccxt
from typing import Optional

from config.settings import (
    MEXC_API_KEY, MEXC_API_SECRET,
    TRADE_AMOUNT_USD, PORTFOLIO_MODE,
    TP1_QTY_PCT, TP2_QTY_PCT, TP3_QTY_PCT,
)
from utils.logger import logger


class TradeExecutor:

    def __init__(self):
        self.exchange  = self._connect()
        self.portfolio = None

    def _connect(self) -> ccxt.mexc:
        exchange = ccxt.mexc({
            "apiKey": MEXC_API_KEY,
            "secret": MEXC_API_SECRET,
            "options": {
                "defaultType":  "spot",
                "fetchMarkets": ["spot"],
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
        from core.portfolio import PortfolioManager
        self.portfolio = PortfolioManager(self.exchange)
        logger.info("✅ Portfolio Manager نشط")

    def _ensure_markets_loaded(self):
        if not self.exchange.markets:
            logger.warning("[Executor] إعادة تحميل الأسواق...")
            self.exchange.load_markets()

    def _get_live_price(self, symbol: str, fallback: float) -> float:
        """يجلب السعر الحالي الفعلي من MEXC لحظة التنفيذ"""
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            price  = float(ticker.get("last") or ticker.get("close") or 0)
            return price if price > 0 else fallback
        except Exception as e:
            logger.warning(f"[Executor] فشل جلب السعر الحي لـ {symbol}: {e}")
            return fallback

    def _get_trade_amount(self, symbol: str) -> float:
        if PORTFOLIO_MODE and self.portfolio:
            return self.portfolio.calculate_trade_amount()
        return TRADE_AMOUNT_USD

    def place_market_buy(
        self,
        symbol:      str,
        entry_price: float,
        amount_usd:  float,
    ) -> Optional[dict]:
        # ── جلب السعر الحالي الفعلي لحظة التنفيذ ──
        live_price = self._get_live_price(symbol, entry_price)

        logger.info(
            f"[Executor] شراء {symbol} | "
            f"سعر الإشارة: {entry_price:.8g} | "
            f"السعر الحالي: {live_price:.8g} | "
            f"مبلغ: ${amount_usd:.2f}"
        )

        try:
            self._ensure_markets_loaded()

            # stepSize precision — MEXC يرفض الكسور غير المتوافقة
            market    = self.exchange.market(symbol)
            precision = market.get("precision", {}).get("amount", 4)
            min_qty   = market.get("limits", {}).get("amount", {}).get("min", 0) or 0

            raw_qty = amount_usd / live_price
            if isinstance(precision, int):
                import math
                factor = 10 ** precision
                qty = math.floor(raw_qty * factor) / factor
            elif isinstance(precision, float) and precision > 0:
                import math
                qty = math.floor(raw_qty / precision) * precision
            else:
                qty = round(raw_qty, 4)

            if min_qty > 0 and qty < min_qty:
                logger.error(
                    f"[Executor] الكمية {qty:.8f} < الحد الأدنى {min_qty} "
                    f"لـ {symbol} — زد رأس المال"
                )
                return None

            logger.info(f"[Executor] qty={qty} (precision={precision})")

            order = self.exchange.create_order(
                symbol = symbol,
                type   = "market",
                side   = "buy",
                amount = qty,
                price  = live_price,
                params = {
                    "type":          "spot",
                    "quoteOrderQty": amount_usd,
                },
            )

            filled_price = float(
                order.get("average") or
                order.get("price") or
                live_price
            )
            filled_qty = float(
                order.get("filled") or
                (amount_usd / filled_price if filled_price > 0 else 0)
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
                "amount_usd":   amount_usd,
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
        symbol:       str,
        filled_qty:   float,
        filled_price: float,
        tp1:          float,
        tp2:          float,
        tp3:          float,
        stop_loss:    float,
    ) -> dict:
        """يضع أوامر البيع بأسعار التوصية بالضبط"""
        qty1 = round(filled_qty * TP1_QTY_PCT, 8)
        qty2 = round(filled_qty * TP2_QTY_PCT, 8)
        qty3 = round(filled_qty * TP3_QTY_PCT, 8)

        logger.info(
            f"[Executor] أوامر {symbol}:\n"
            f"  TP1={tp1:.8g} x{qty1} | "
            f"TP2={tp2:.8g} x{qty2} | "
            f"TP3={tp3:.8g} x{qty3} | "
            f"SL={stop_loss:.8g}"
        )

        results = {"tp1": tp1, "tp2": tp2, "tp3": tp3, "sl": stop_loss}

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
                symbol=symbol, type="STOP_LOSS_LIMIT", side="sell",
                amount=filled_qty, price=stop_loss * 0.995,
                params={"stopPrice": stop_loss, "type": "spot"},
            )
            results["sl_order_id"] = o["id"]
            logger.info(f"✅ SL: {stop_loss:.8g} | ID: {o['id']}")
        except Exception as e:
            logger.error(f"❌ SL فشل لـ {symbol}: {e} — راجع يدوياً!")

        return results

    def execute_full_trade(self, opportunity) -> dict:
        symbol = opportunity.symbol

        if PORTFOLIO_MODE and self.portfolio:
            can_open, reason = self.portfolio.can_open_trade(symbol)
            if not can_open:
                return {"success": False, "symbol": symbol, "error": reason}

        amount_usd = self._get_trade_amount(symbol)
        if amount_usd <= 0:
            return {"success": False, "symbol": symbol,
                    "error": "الرصيد غير كافٍ للصفقة"}

        logger.info(
            f"🚀 تنفيذ: {symbol} | ${amount_usd:.2f}\n"
            f"  TP1={opportunity.tp1:.8g} | "
            f"TP2={opportunity.tp2:.8g} | "
            f"TP3={opportunity.tp3:.8g} | "
            f"SL={opportunity.stop_loss:.8g}"
        )

        buy = self.place_market_buy(symbol, opportunity.entry_price, amount_usd)
        if not buy:
            return {
                "success": False, "symbol": symbol,
                "error": "فشل أمر الشراء — تحقق من الرصيد، حد السوق الأدنى، أو تحرك السعر أكثر من 0.3%",
            }

        if PORTFOLIO_MODE and self.portfolio:
            self.portfolio.register_open_trade(symbol)

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
            "amount_usd":   amount_usd,
            **tp_sl,
        }
