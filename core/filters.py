"""
core/filters.py
===============
فلاتر متقدمة:
1. Dynamic Stop-Loss (15m Swing Low)
2. Anti-Lag Filter (سعر لم يتجاوز 0.3%)
3. Order Book Depth Check ($50K bid depth)
4. Spot Volume Only Filter
"""

import pandas as pd
import numpy as np
from utils.logger import logger


# ==========================================
# 1. Dynamic Stop-Loss من 15m Swing Low
# ==========================================
def calculate_dynamic_sl(exchange, symbol: str, current_price: float) -> float:
    """
    يجد أدنى قاع محلي في آخر 24-48 شمعة 15m
    يضع SL على بُعد 0.2% تحته
    """
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe="15m", limit=48)
        if not ohlcv or len(ohlcv) < 10:
            # Fallback: 8% تحت الدعم
            return current_price * 0.92

        df   = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
        lows = df["low"].astype(float)

        # إيجاد القيعان المحلية (Swing Lows)
        swing_lows = []
        for i in range(2, len(lows) - 2):
            val = lows.iloc[i]
            if (val <= lows.iloc[i-1] and val <= lows.iloc[i-2] and
                    val <= lows.iloc[i+1] and val <= lows.iloc[i+2]):
                swing_lows.append(float(val))

        if swing_lows:
            # أحدث وأقرب قاع محلي
            recent_swing_low = min(swing_lows[-3:]) if len(swing_lows) >= 3 else min(swing_lows)
            sl = recent_swing_low * (1 - 0.002)  # 0.2% تحت القاع
            logger.info(
                f"[Dynamic SL] {symbol}: "
                f"Swing Low={recent_swing_low:.8g} | SL={sl:.8g} (-0.2%)"
            )
        else:
            # لا توجد قيعان واضحة — استخدم أدنى نقطة
            sl = float(lows.min()) * 0.998
            logger.info(f"[Dynamic SL] {symbol}: No swings → Min={sl:.8g}")

        # تأكد أن SL منطقي (لا يتجاوز 20% تحت السعر الحالي)
        sl = max(sl, current_price * 0.80)
        return sl

    except Exception as e:
        logger.warning(f"[Dynamic SL] {symbol}: {e} — Fallback 8%")
        return current_price * 0.92


# ==========================================
# 2. Fibonacci Retracement للـ RSI Oversold
# ==========================================
def calculate_fibonacci_targets_v2(
    local_high:    float,
    local_low:     float,
    current_price: float,
    rsi:           float,
    exchange=None,
    symbol:        str = "",
) -> dict:
    """
    أهداف محافظة مبنية على 15m candles للـ Scalping
    RSI < 31: Fibonacci Rebound (0.382/0.500/0.618)
    الحد الأقصى للهدف: 15% فوق السعر الحالي
    """
    # جلب نطاق 15m للأهداف المحافظة
    if exchange and symbol:
        try:
            import pandas as pd
            ohlcv  = exchange.fetch_ohlcv(symbol, timeframe="15m", limit=48)
            df_15m = pd.DataFrame(
                ohlcv, columns=["ts","open","high","low","close","volume"]
            ).astype(float)
            local_high_15m = float(df_15m["high"].max())
            local_low_15m  = float(df_15m["low"].min())
            # استخدم 15m إذا نطاقه أصغر (أكثر محافظة)
            if local_high_15m < local_high:
                local_high = local_high_15m
                local_low  = max(local_low, local_low_15m)
        except Exception:
            pass

    fib_range = local_high - local_low
    if fib_range <= 0:
        return {
            "tp1": current_price * 1.03,
            "tp2": current_price * 1.06,
            "tp3": current_price * 1.10,
            "method": "Fixed (fallback)",
        }

    if rsi < 31:
        tp1 = local_low + fib_range * 0.382
        tp2 = local_low + fib_range * 0.500
        tp3 = local_low + fib_range * 0.618
        method = f"Fib Rebound RSI={rsi:.1f}"
    else:
        tp1 = local_low + fib_range * 0.236
        tp2 = local_low + fib_range * 0.382
        tp3 = local_low + fib_range * 0.500
        method = "Fib Conservative"

    # أهداف فوق السعر الحالي
    tp1 = max(tp1, current_price * 1.02)
    tp2 = max(tp2, tp1 * 1.03)
    tp3 = max(tp3, tp2 * 1.03)

    # الحد الأقصى 15% للـ Scalping
    MAX_TARGET_PCT = 0.15
    tp1 = min(tp1, current_price * (1 + MAX_TARGET_PCT))
    tp2 = min(tp2, current_price * (1 + MAX_TARGET_PCT))
    tp3 = min(tp3, current_price * (1 + MAX_TARGET_PCT))

    return {
        "tp1":    round(tp1, 10),
        "tp2":    round(tp2, 10),
        "tp3":    round(tp3, 10),
        "method": method,
    }


# ==========================================
# 3. Anti-Lag Filter (0.3% threshold)
# ==========================================
def check_price_lag(
    exchange,
    symbol:           str,
    signal_entry_price: float,
    threshold_pct:    float = 0.003,
) -> tuple[bool, float]:
    """
    يتحقق من أن السعر الحالي لم يتجاوز سعر الدخول بأكثر من 0.3%
    Returns: (valid, current_price)
    True = الإشارة لا تزال صالحة
    False = السعر تجاوز الحد → إلغاء الإشارة
    """
    try:
        ticker        = exchange.fetch_ticker(symbol)
        current_price = float(ticker.get("last") or ticker.get("close") or 0)

        if current_price <= 0:
            return True, signal_entry_price

        pump_pct = (current_price - signal_entry_price) / signal_entry_price

        if pump_pct > threshold_pct:
            logger.warning(
                f"[Anti-Lag] {symbol}: السعر ارتفع {pump_pct:.2%} "
                f"(حد: {threshold_pct:.1%}) — إلغاء الإشارة ⚠️"
            )
            return False, current_price

        logger.debug(f"[Anti-Lag] {symbol}: {pump_pct:+.2%} — صالحة ✅")
        return True, current_price

    except Exception as e:
        logger.warning(f"[Anti-Lag] {symbol}: {e} — تجاوز")
        return True, signal_entry_price


# ==========================================
# 4. Order Book Depth Check ($50K bid depth)
# ==========================================
def check_order_book_depth(
    exchange,
    symbol:        str,
    current_price: float,
    min_depth_usd: float = 50_000,
    depth_pct:     float = 0.02,
) -> tuple[bool, float]:
    """
    يتحقق من وجود $50K على الأقل في أوامر الشراء
    ضمن نطاق 2% تحت السعر الحالي
    Returns: (passed, total_bid_usd)
    """
    try:
        order_book = exchange.fetch_order_book(symbol, limit=20)
        bids       = order_book.get("bids", [])

        price_floor = current_price * (1 - depth_pct)
        total_usd   = 0.0

        for bid_price, bid_qty in bids:
            bid_price = float(bid_price)
            bid_qty   = float(bid_qty)
            if bid_price >= price_floor:
                total_usd += bid_price * bid_qty

        passed = total_usd >= min_depth_usd
        logger.info(
            f"[OB Depth] {symbol}: ${total_usd:,.0f} في 2% "
            f"{'✅' if passed else '❌ (< $50K)'}"
        )
        return passed, total_usd

    except Exception as e:
        logger.warning(f"[OB Depth] {symbol}: {e} — تجاوز")
        return True, 0.0


# ==========================================
# 5. Spot Volume Only
# ==========================================
def get_spot_volume_only(exchange, symbol: str) -> float:
    """
    يجلب حجم التداول الـ Spot فقط
    يتجنب بيانات Futures/Derivatives
    """
    try:
        ticker = exchange.fetch_ticker(symbol, params={"type": "spot"})
        volume = float(ticker.get("quoteVolume") or 0)
        logger.debug(f"[Spot Vol] {symbol}: ${volume:,.0f}")
        return volume
    except Exception:
        try:
            ticker = exchange.fetch_ticker(symbol)
            return float(ticker.get("quoteVolume") or 0)
        except Exception as e:
            logger.warning(f"[Spot Vol] {symbol}: {e}")
            return 0.0


# ==========================================
# الفلتر الشامل — يجمع كل الفلاتر
# ==========================================
def run_advanced_filters(
    exchange,
    symbol:       str,
    entry_price:  float,
    rsi:          float,
    local_high:   float,
    local_low:    float,
) -> dict:
    """
    يشغّل جميع الفلاتر المتقدمة ويُعيد النتيجة
    """
    result = {
        "passed":     True,
        "cancel_reason": None,
        "dynamic_sl": None,
        "tp1": None, "tp2": None, "tp3": None,
        "tp_method": None,
        "spot_volume": 0,
        "bid_depth": 0,
        "current_price": entry_price,
    }

    # 1. Spot Volume
    spot_vol = get_spot_volume_only(exchange, symbol)
    result["spot_volume"] = spot_vol
    if spot_vol < 1_000_000:
        result["passed"]        = False
        result["cancel_reason"] = f"حجم Spot منخفض: ${spot_vol/1e6:.2f}M"
        return result

    # 2. Anti-Lag Filter
    valid, current_price = check_price_lag(exchange, symbol, entry_price)
    result["current_price"] = current_price
    if not valid:
        result["passed"]        = False
        result["cancel_reason"] = "السعر ارتفع أكثر من 0.3% — إشارة منتهية الصلاحية"
        return result

    # 3. Order Book Depth
    ob_passed, bid_depth = check_order_book_depth(exchange, symbol, current_price)
    result["bid_depth"] = bid_depth
    if not ob_passed:
        result["passed"]        = False
        result["cancel_reason"] = f"عمق دفتر الأوامر ضعيف: ${bid_depth:,.0f} < $50K"
        return result

    # 4. Dynamic Stop-Loss
    dynamic_sl = calculate_dynamic_sl(exchange, symbol, current_price)
    result["dynamic_sl"] = dynamic_sl

    # 5. Fibonacci Targets
    fib = calculate_fibonacci_targets_v2(local_high, local_low, current_price, rsi)
    result["tp1"]       = fib["tp1"]
    result["tp2"]       = fib["tp2"]
    result["tp3"]       = fib["tp3"]
    result["tp_method"] = fib["method"]

    return result
