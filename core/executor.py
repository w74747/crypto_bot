"""
core/executor.py
================
تنفيذ صفقات Spot على MEXC مع Portfolio Manager.
الإصلاح الأساسي:
- استخدام Limit IOC Buy بدل Market Buy لتجنب خطأ MEXC:
  Mandatory parameter 'price' was not sent
- حساب مبلغ الصفقة من الرصيد المتاح عند PORTFOLIO_MODE=true
- تطبيق Anti-Lag قبل التنفيذ
"""

from typing import Optional

import ccxt

from config.settings import (
    MEXC_API_KEY,
    MEXC_API_SECRET,
    TRADE_AMOUNT_USD,
    PORTFOLIO_MODE,
    TP1_QTY_PCT,
    TP2_QTY_PCT,
    TP3_QTY_PCT,
)

from utils.logger import logger


class TradeExecutor:
    def __init__(self):
        self.exchange = self._connect()
        self.portfolio = None

    def _connect(self) -> ccxt.mexc:
        exchange = ccxt.mexc({
            "apiKey": MEXC_API_KEY,
            "secret": MEXC_API_SECRET,
            "options": {
                "defaultType": "spot",
                "fetchMarkets": ["spot"],
            },
            "enableRateLimit": True,
            "timeout": 60000,
        })

        try:
            exchange.load_markets()
            logger.info(f"✅ Executor متصل بـ MEXC — {len(exchange.markets)} سوق")
        except Exception as e:
            logger.error(f"❌ Executor فشل الاتصال بـ MEXC: {e}")
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

    def _get_trade_amount(self, symbol: str) -> float:
        if PORTFOLIO_MODE and self.portfolio:
            return float(self.portfolio.calculate_trade_amount())

        return float(TRADE_AMOUNT_USD)

    def _get_current_price(self, symbol: str, fallback_price: float) -> float:
        ticker = self.exchange.fetch_ticker(symbol)
        current_price = float(
            ticker.get("last")
            or ticker.get("close")
            or ticker.get("bid")
            or ticker.get("ask")
            or fallback_price
        )

        if current_price <= 0:
            return fallback_price

        return current_price

    def _validate_amount_against_market(self, symbol: str, amount_usd: float) -> tuple[bool, str]:
        market = self.exchange.market(symbol)

        min_cost = None
        try:
            min_cost = market.get("limits", {}).get("cost", {}).get("min")
        except Exception:
            min_cost = None

        if min_cost is not None and amount_usd < float(min_cost):
            return False, f"مبلغ الصفقة ${amount_usd:.2f} أقل من حد السوق الأدنى ${float(min_cost):.2f}"

        if amount_usd <= 0:
            return False, "مبلغ الصفقة غير صالح"

        return True, "OK"

    def place_market_buy(
        self,
        symbol: str,
        entry_price: float,
        amount_usd: float,
    ) -> Optional[dict]:
        """
        الاسم بقي place_market_buy للتوافق مع باقي المشروع،
        لكن التنفيذ الفعلي أصبح Limit IOC Buy لأن MEXC يرفض صيغة Market القديمة.
        """
        logger.info(
            f"[Executor] شراء {symbol} | سعر الإشارة: ~{entry_price:.8g} | مبلغ: ${amount_usd:.2f}"
        )

        try:
            self._ensure_markets_loaded()

            valid_amount, amount_reason = self._validate_amount_against_market(symbol, amount_usd)
            if not valid_amount:
                logger.warning(f"[Executor] رفض {symbol}: {amount_reason}")
                return None

            current_price = self._get_current_price(symbol, entry_price)

            pump_pct = (current_price - entry_price) / entry_price
            if pump_pct > 0.003:
                logger.warning(
                    f"[Anti-Lag] {symbol}: السعر ارتفع {pump_pct:.2%} فوق الدخول "
                    f"({current_price:.8g} > {entry_price:.8g}) — إلغاء الإشارة"
                )
                return None

            # احتياطي بسيط للرسوم حتى لا يفشل الأمر عند استخدام كامل الرصيد.
            spend_usd = amount_usd * 0.995

            # سعر IOC قريب من السوق ولا يتجاوز 0.3% فوق سعر الإشارة.
            limit_price = min(current_price * 1.001, entry_price * 1.003)

            raw_qty = spend_usd / limit_price
            qty = float(self.exchange.amount_to_precision(symbol, raw_qty))
            limit_price = float(self.exchange.price_to_precision(symbol, limit_price))

            if qty <= 0:
                logger.warning(f"[Executor] كمية غير صالحة لـ {symbol}: {qty}")
                return None

            order = self.exchange.create_order(
                symbol=symbol,
                type="limit",
                side="buy",
                amount=qty,
                price=limit_price,
                params={
                    "timeInForce": "IOC",
                },
            )

            filled_price = float(order.get("average") or order.get("price") or limit_price)
            filled_qty = float(order.get("filled") or 0)

            if filled_qty <= 0:
                logger.warning(
                    f"[Executor] لم يتم تعبئة أمر الشراء {symbol} عند {limit_price:.8g}"
                )
                return None

            filled_cost = filled_price * filled_qty

            logger.info(
                f"✅ تم الشراء: {symbol} | "
                f"{filled_qty:.8g} @ {filled_price:.8g} | "
                f"cost=${filled_cost:.2f} | ID: {order.get('id')}"
            )

            return {
                "order_id": order.get("id", ""),
                "filled_price": filled_price,
                "filled_qty": filled_qty,
                "amount_usd": filled_cost,
            }

        except ccxt.InsufficientFunds as e:
            logger.error(f"[Executor] رصيد غير كاف في Spot: {e}")
            return None
        except ccxt.NetworkError as e:
            logger.error(f"[Executor] خطأ شبكة: {e}")
            return None
        except Exception as e:
            logger.error(f"[Executor] خطأ في {symbol}: {e}")
            return None

    def place_tp_and_sl_orders(
        self,
        symbol: str,
        filled_qty: float,
        filled_price: float,
        tp1: float,
        tp2: float,
        tp3: float,
        stop_loss: float,
    ) -> dict:
        qty1 = round(filled_qty * TP1_QTY_PCT, 8)
        qty2 = round(filled_qty * TP2_QTY_PCT, 8)
        qty3 = round(filled_qty * TP3_QTY_PCT, 8)

        logger.info(
            f"[Executor] أوامر البيع {symbol}:\n"
            f"TP1={tp1:.8g} x{qty1} | "
            f"TP2={tp2:.8g} x{qty2} | "
            f"TP3={tp3:.8g} x{qty3} | "
            f"SL={stop_loss:.8g}"
        )

        results = {
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "sl": stop_loss,
        }

        for label, qty, price in [
            ("TP1", qty1, tp1),
            ("TP2", qty2, tp2),
            ("TP3", qty3, tp3),
        ]:
            try:
                if qty <= 0:
                    logger.warning(f"⚠️ {label} كمية غير صالحة لـ {symbol}: {qty}")
                    continue

                qty_precise = float(self.exchange.amount_to_precision(symbol, qty))
                price_precise = float(self.exchange.price_to_precision(symbol, price))

                o = self.exchange.create_limit_sell_order(
                    symbol,
                    qty_precise,
                    price_precise,
                )

                results[f"{label.lower()}_order_id"] = o.get("id", "")
                logger.info(f"✅ {label}: {price_precise:.8g} | ID: {o.get('id')}")

            except Exception as e:
                logger.error(f"❌ {label} فشل لـ {symbol}: {e}")

        try:
            sl_price = stop_loss * 0.995
            sl_price = float(self.exchange.price_to_precision(symbol, sl_price))
            stop_price = float(self.exchange.price_to_precision(symbol, stop_loss))
            sl_qty = float(self.exchange.amount_to_precision(symbol, filled_qty))

            o = self.exchange.create_order(
                symbol=symbol,
                type="STOP_LOSS_LIMIT",
                side="sell",
                amount=sl_qty,
                price=sl_price,
                params={
                    "stopPrice": stop_price,
                    "type": "spot",
                },
            )

            results["sl_order_id"] = o.get("id", "")
            logger.info(f"✅ SL: {stop_price:.8g} | ID: {o.get('id')}")

        except Exception as e:
            logger.error(f"❌ SL فشل لـ {symbol}: {e} — راجع يدويًا!")

        return results

    def execute_full_trade(self, opportunity) -> dict:
        symbol = opportunity.symbol

        if PORTFOLIO_MODE and self.portfolio:
            can_open, reason = self.portfolio.can_open_trade(symbol)
            if not can_open:
                logger.info(f"[Portfolio] رُفض {symbol}: {reason}")
                return {
                    "success": False,
                    "symbol": symbol,
                    "error": reason,
                }

        amount_usd = self._get_trade_amount(symbol)
        if amount_usd <= 0:
            return {
                "success": False,
                "symbol": symbol,
                "error": "الرصيد غير كاف للصفقة",
            }

        logger.info(
            f"🚀 تنفيذ صفقة: {symbol} | ${amount_usd:.2f}\n"
            f"TP1={opportunity.tp1:.8g} | "
            f"TP2={opportunity.tp2:.8g} | "
            f"TP3={opportunity.tp3:.8g} | "
            f"SL={opportunity.stop_loss:.8g}"
        )

        buy = self.place_market_buy(
            symbol=symbol,
            entry_price=opportunity.entry_price,
            amount_usd=amount_usd,
        )

        if not buy:
            return {
                "success": False,
                "symbol": symbol,
                "error": "فشل أمر الشراء — تحقق من الرصيد، حد السوق الأدنى، أو تحرك السعر أكثر من 0.3%",
            }

        if PORTFOLIO_MODE and self.portfolio:
            self.portfolio.register_open_trade(symbol)

        tp_sl = self.place_tp_and_sl_orders(
            symbol=symbol,
            filled_qty=buy["filled_qty"],
            filled_price=buy["filled_price"],
            tp1=opportunity.tp1,
            tp2=opportunity.tp2,
            tp3=opportunity.tp3,
            stop_loss=opportunity.stop_loss,
        )

        return {
            "success": True,
            "symbol": symbol,
            "buy_order_id": buy["order_id"],
            "filled_price": buy["filled_price"],
            "filled_qty": buy["filled_qty"],
            "amount_usd": buy["amount_usd"],
            **tp_sl,
        }
