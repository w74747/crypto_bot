"""
core/scanner.py
===============
Bottom Fisher مع Dynamic Targets
- Fibonacci Retracement (Daily للانهيارات الكبيرة، 4H/1H للتذبذب المحلي)
- Swing Highs من البيانات الفعلية
- BTC Trend Filter لحماية رأس المال
"""

import ccxt
import pandas as pd
import numpy as np
import time
from typing import Optional
from dataclasses import dataclass, field

from config.settings import (
    MEXC_API_KEY, MEXC_API_SECRET,
    MIN_DAILY_VOLUME_USD,
    RSI_OVERSOLD_THRESHOLD, RSI_PERIOD, LOD_DAYS,
    MAX_DISTANCE_FROM_LOD,
)
from utils.logger import logger
from utils.github_checker import is_github_active

BATCH_SIZE = int(__import__('os').getenv("BATCH_SIZE", "100"))

EXCLUDED_BASE_COINS = {
    "USDC","BUSD","TUSD","USDP","GUSD","FRAX","LUSD",
    "DAI","USDD","FDUSD","PYUSD","USDE","SUSD","USD1",
    "USDT","AEUR","EURI","EURS","XAUT","PAXG",
    "EUR","GBP","AUD","JPY","TRY","BRL","CAD",
    "WBTC","WETH","WBNB","WMATIC","WSOL",
    "STETH","RETH","CBETH","SFRXETH",
}


# ==========================================
# RSI Calculator
# ==========================================
def calculate_rsi(closes: pd.Series, period: int = 14) -> float:
    delta    = closes.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    rsi      = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


# ==========================================
# Dynamic Targets Engine
# ==========================================
def find_swing_highs(highs: pd.Series, entry_price: float, window: int = 2) -> list[float]:
    """
    يبحث عن القمم المحلية (Swing Highs) فوق سعر الدخول
    window: عدد الشموع يميناً ويساراً للتأكيد
    """
    peaks = []
    arr   = highs.values
    for i in range(window, len(arr) - window):
        val = arr[i]
        if val <= entry_price:
            continue
        # قمة محلية: أعلى من كل الشموع في النافذة
        is_peak = all(val >= arr[i-j] for j in range(1, window+1)) and \
                  all(val >= arr[i+j] for j in range(1, window+1))
        if is_peak:
            peaks.append(float(val))
    return sorted(set(round(p, 10) for p in peaks))


def calculate_fibonacci_targets(
    local_high:  float,
    local_low:   float,
    entry_price: float,
) -> dict:
    """
    يحسب مستويات Fibonacci من القمة المحلية للقاع
    TP1 = 0.236 | TP2 = 0.382 | TP3 = 0.618
    """
    fib_range = local_high - local_low
    if fib_range <= 0:
        # fallback: نسب ثابتة بسيطة إذا كانت البيانات غير منطقية
        return {
            "tp1": entry_price * 1.08,
            "tp2": entry_price * 1.15,
            "tp3": entry_price * 1.25,
            "method": "Fixed (fallback)",
            "fib_high": local_high,
            "fib_low":  local_low,
        }

    tp1 = local_low + fib_range * 0.236
    tp2 = local_low + fib_range * 0.382
    tp3 = local_low + fib_range * 0.618

    return {
        "tp1":      round(tp1, 10),
        "tp2":      round(tp2, 10),
        "tp3":      round(tp3, 10),
        "fib_high": round(local_high, 10),
        "fib_low":  round(local_low,  10),
    }


def calculate_dynamic_targets(
    df_daily:    pd.DataFrame,
    df_4h:       Optional[pd.DataFrame],
    df_1h:       Optional[pd.DataFrame],
    entry_price: float,
    nearest_support: float,
    crash_pct_60d:   float,
) -> dict:
    """
    يختار الطريقة الأنسب تلقائياً بناءً على السياق:

    انهيار كبير (>40%) على Daily → Fibonacci Daily (من القمة الكبيرة للقاع)
    انهيار متوسط (15-40%)        → Fibonacci 4H  (فرصة ارتداد قصير)
    تذبذب محلي (<15%)            → Fibonacci 1H + Swing Highs المحلية

    في جميع الحالات: Swing Highs من Daily تُستخدم إذا وُجدت
    """
    highs_daily = df_daily["high"]
    lows_daily  = df_daily["low"]

    # ── Swing Highs من Daily (آخر 50 شمعة) ──
    swing_daily = find_swing_highs(highs_daily.tail(50), entry_price, window=2)

    # ── تحديد الإطار الزمني للـ Fibonacci ──

    if crash_pct_60d >= 0.40:
        # انهيار كبير — استخدم Daily Fibonacci
        local_high = float(highs_daily.tail(60).max())
        local_low  = nearest_support
        tf_label   = "Daily Fib"
        fib = calculate_fibonacci_targets(local_high, local_low, entry_price)

    elif crash_pct_60d >= 0.15 and df_4h is not None:
        # انهيار متوسط — استخدم 4H Fibonacci
        # القمة: أعلى سعر في آخر 30 شمعة 4H (~5 أيام)
        local_high = float(df_4h["high"].tail(30).max())
        local_low  = float(df_4h["low"].tail(30).min())
        tf_label   = "4H Fib"
        fib = calculate_fibonacci_targets(local_high, local_low, entry_price)

    elif df_1h is not None:
        # تذبذب محلي — استخدم 1H Fibonacci للـ scalping
        # القمة: أعلى سعر في آخر 24 شمعة 1H (~يوم واحد)
        local_high = float(df_1h["high"].tail(24).max())
        local_low  = float(df_1h["low"].tail(24).min())
        tf_label   = "1H Fib (Scalp)"
        fib = calculate_fibonacci_targets(local_high, local_low, entry_price)

    else:
        # fallback: Daily Fibonacci
        local_high = float(highs_daily.tail(60).max())
        local_low  = nearest_support
        tf_label   = "Daily Fib"
        fib = calculate_fibonacci_targets(local_high, local_low, entry_price)

    tp1_fib = fib["tp1"]
    tp2_fib = fib["tp2"]
    tp3_fib = fib["tp3"]

    # ── دمج Swing Highs مع Fibonacci ──
    if len(swing_daily) >= 3:
        # ثلاث قمم أو أكثر — استخدم الـ Swing Highs مع تأكيد Fibonacci
        tp1 = swing_daily[0]
        tp2 = swing_daily[1]
        tp3 = swing_daily[2]
        method = f"Swing Highs ({tf_label} confirm)"

    elif len(swing_daily) == 2:
        # قمتان — امزج Swing الأولى مع Fibonacci
        tp1 = swing_daily[0]
        tp2 = swing_daily[1]
        tp3 = max(tp3_fib, swing_daily[1] * 1.10)
        method = f"Swing+Fib ({tf_label})"

    elif len(swing_daily) == 1:
        # قمة واحدة — الأهداف الأخرى من Fibonacci
        tp1 = swing_daily[0]
        tp2 = max(tp2_fib, tp1 * 1.10)
        tp3 = max(tp3_fib, tp1 * 1.25)
        method = f"Swing+Fib ({tf_label})"

    else:
        # لا توجد قمم — Fibonacci خالص
        tp1 = tp1_fib
        tp2 = tp2_fib
        tp3 = tp3_fib
        method = tf_label

    # ── ضمان المنطق: الأهداف تصاعدية وفوق الدخول ──
    min_tp1 = entry_price * 1.03   # على الأقل 3%
    min_tp2 = entry_price * 1.08   # على الأقل 8%
    min_tp3 = entry_price * 1.15   # على الأقل 15%

    tp1 = max(tp1, min_tp1)
    tp2 = max(tp2, tp1 * 1.05, min_tp2)
    tp3 = max(tp3, tp2 * 1.08, min_tp3)

    return {
        "tp1":      round(tp1, 10),
        "tp2":      round(tp2, 10),
        "tp3":      round(tp3, 10),
        "method":   method,
        "fib_high": fib.get("fib_high", local_high),
        "fib_low":  fib.get("fib_low",  local_low),
    }


# ==========================================
# TradeOpportunity
# ==========================================
@dataclass
class TradeOpportunity:
    symbol:            str
    current_price:     float
    crash_pct_60d:     float
    lod_180:           float
    distance_from_lod: float
    rsi_daily:         float
    volume_24h_usd:    float
    nearest_support:   float
    github_active:     bool
    signal_type:       str
    # Dynamic targets — تُمرر من الخارج
    tp1:         float = 0.0
    tp2:         float = 0.0
    tp3:         float = 0.0
    tp_method:   str   = "—"
    fib_high:    float = 0.0
    fib_low:     float = 0.0
    # يُحسب بعد تحديد الأهداف
    entry_price: float = field(init=False)
    stop_loss:   float = field(init=False)

    def __post_init__(self):
        from config.settings import STOP_LOSS_PCT
        self.entry_price = self.current_price
        self.stop_loss   = self.nearest_support * (1 - STOP_LOSS_PCT)

    @property
    def risk_reward_ratio(self) -> float:
        risk   = self.entry_price - self.stop_loss
        reward = self.tp1 - self.entry_price
        if risk <= 0 or reward <= 0:
            return 0.0
        return round(reward / risk, 2)

    @property
    def tp1_pct(self) -> float:
        if self.entry_price <= 0:
            return 0.0
        return round((self.tp1 - self.entry_price) / self.entry_price * 100, 1)

    @property
    def tp2_pct(self) -> float:
        if self.entry_price <= 0:
            return 0.0
        return round((self.tp2 - self.entry_price) / self.entry_price * 100, 1)

    @property
    def tp3_pct(self) -> float:
        if self.entry_price <= 0:
            return 0.0
        return round((self.tp3 - self.entry_price) / self.entry_price * 100, 1)


# ==========================================
# MarketScanner
# ==========================================
class MarketScanner:

    def __init__(self):
        self.exchange    = self._connect()
        self._btc_trend  = None   # cache لاتجاه BTC
        self._btc_ts     = 0.0    # وقت آخر تحديث

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
            logger.info(f"✅ تم الاتصال بـ MEXC — {len(exchange.markets)} سوق")
        except Exception as e:
            logger.error(f"❌ فشل تحميل الأسواق: {e}")
            raise
        return exchange

    # ------------------------------------------
    # BTC Trend Filter
    # ------------------------------------------
    def is_btc_safe(self) -> bool:
        """
        يتحقق من اتجاه BTC اليومي
        إذا كان BTC في انهيار حاد → يوقف الإشارات
        يُحدَّث كل 4 ساعات لتوفير الطلبات
        """
        import time as time_module
        now = time_module.time()

        # استخدم الـ cache إذا لم يمضِ 4 ساعات
        if self._btc_trend is not None and (now - self._btc_ts) < 14400:
            return self._btc_trend

        try:
            ohlcv = self.exchange.fetch_ohlcv("BTC/USDT", timeframe="1d", limit=10)
            df    = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
            df    = df.astype({"close": float, "open": float})

            # انهيار حاد: BTC نزل أكثر من 8% في آخر 3 أيام
            recent_high = df["close"].tail(4).iloc[0]
            current     = float(df["close"].iloc[-1])
            drop_3d     = (recent_high - current) / recent_high

            if drop_3d > 0.08:
                logger.warning(f"[BTC Filter] BTC في انهيار {drop_3d:.1%} — إيقاف الإشارات ⚠️")
                self._btc_trend = False
            else:
                logger.info(f"[BTC Filter] BTC آمن ({drop_3d:.1%} تغيير) ✅")
                self._btc_trend = True

            self._btc_ts = now
            return self._btc_trend

        except Exception as e:
            logger.warning(f"[BTC Filter] خطأ في جلب BTC: {e} — السماح بالإشارات")
            return True  # في حالة الشك، لا نوقف الإشارات

    # ------------------------------------------
    # OHLCV Fetchers
    # ------------------------------------------
    def fetch_ohlcv_daily(self, symbol: str, limit: int = 210) -> Optional[pd.DataFrame]:
        return self._fetch_ohlcv(symbol, "1d", limit)

    def fetch_ohlcv_4h(self, symbol: str, limit: int = 60) -> Optional[pd.DataFrame]:
        return self._fetch_ohlcv(symbol, "4h", limit)

    def fetch_ohlcv_1h(self, symbol: str, limit: int = 48) -> Optional[pd.DataFrame]:
        return self._fetch_ohlcv(symbol, "1h", limit)

    def _fetch_ohlcv(self, symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if not ohlcv or len(ohlcv) < 10:
                return None
            df = pd.DataFrame(ohlcv, columns=["timestamp","open","high","low","close","volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.set_index("timestamp", inplace=True)
            return df.astype(float)
        except Exception as e:
            logger.debug(f"[OHLCV] {symbol} {timeframe}: {e}")
            return None

    # ------------------------------------------
    # Indicators
    # ------------------------------------------
    def calculate_indicators(self, df: pd.DataFrame) -> dict:
        closes          = df["close"]
        current_price   = float(closes.iloc[-1])
        rsi             = calculate_rsi(closes, period=RSI_PERIOD)
        lod_180         = float(df["low"].tail(LOD_DAYS).min())
        distance        = (current_price - lod_180) / lod_180 if lod_180 > 0 else 1.0
        high_60d        = float(df["close"].tail(60).max())
        crash_60d       = (high_60d - current_price) / high_60d if high_60d > 0 else 0.0
        last_3_lows     = df["low"].tail(3).values
        is_stabilizing  = bool(last_3_lows[-1] >= last_3_lows[-2] * 0.98)
        nearest_support = float(df["low"].tail(14).min())
        return {
            "rsi":             rsi,
            "lod_180":         lod_180,
            "current_price":   current_price,
            "distance":        distance,
            "crash_60d":       crash_60d,
            "is_stabilizing":  is_stabilizing,
            "nearest_support": nearest_support,
        }

    # ------------------------------------------
    # Volume
    # ------------------------------------------
    def get_24h_volume_usd(self, symbol: str) -> float:
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return float(ticker.get("quoteVolume") or 0.0)
        except Exception:
            return 0.0

    # ------------------------------------------
    # Filters
    # ------------------------------------------
    def get_usdt_symbols(self) -> list[str]:
        try:
            symbols  = []
            excluded = 0
            for s, m in self.exchange.markets.items():
                if m.get("quote") != "USDT":
                    continue
                if m.get("type", "") != "spot":
                    continue
                if not m.get("info", {}).get("isSpotTradingAllowed"):
                    continue
                if m.get("base", "") in EXCLUDED_BASE_COINS:
                    excluded += 1
                    continue
                symbols.append(s)
            logger.info(f"📊 أزواج USDT: {len(symbols)} (استُبعد {excluded})")
            return symbols
        except Exception as e:
            logger.error(f"❌ خطأ: {e}")
            return []

    def passes_filters(self, symbol: str, ind: dict, volume_usd: float) -> tuple[bool, str]:
        # Volume Filter — صارم ($5M الهدف النهائي، $1M للاختبار)
        if volume_usd < MIN_DAILY_VOLUME_USD:
            return False, f"حجم منخفض"

        rsi         = ind["rsi"]
        crash_60d   = ind["crash_60d"]
        distance    = ind["distance"]
        stabilizing = ind["is_stabilizing"]

        # ── RSI Hard Ceiling = 32 ──
        import os as _os
        RSI_HARD_CEILING = float(_os.getenv("RSI_HARD_CEILING", "32"))
        if rsi > RSI_HARD_CEILING:
            logger.debug(f"[RSI Ceiling] {symbol}: RSI={rsi:.1f} > {RSI_HARD_CEILING}")
            return False, ""

        # استراتيجية A
        if crash_60d >= 0.40 and stabilizing:
            return True, f"🅰️ انهيار حديث {crash_60d:.0%} + تماسك"

        # استراتيجية B
        if distance <= MAX_DISTANCE_FROM_LOD and rsi < RSI_OVERSOLD_THRESHOLD:
            return True, f"🅱️ قاع تاريخي RSI={rsi:.1f}"

        # استراتيجية C
        if crash_60d >= 0.60:
            return True, f"🅲 انهيار ضخم {crash_60d:.0%} RSI={rsi:.1f}"

        # استراتيجية D
        if crash_60d >= 0.10 and stabilizing:
            return True, f"🅳 تذبذب محلي {crash_60d:.0%} RSI={rsi:.1f}"

        return False, ""

    # ------------------------------------------
    # معالجة عملة واحدة
    # ------------------------------------------
    def _process_symbol(self, symbol: str) -> Optional[TradeOpportunity]:
        # Daily OHLCV
        df_daily = self.fetch_ohlcv_daily(symbol)
        if df_daily is None:
            return None

        try:
            ind = self.calculate_indicators(df_daily)
        except Exception:
            return None

        # Spot Volume فقط
        from core.filters import get_spot_volume_only
        volume_usd = get_spot_volume_only(self.exchange, symbol)
        passed, signal = self.passes_filters(symbol, ind, volume_usd)
        if not passed:
            return None

        # GitHub Filter
        coin = symbol.replace("/USDT", "")
        if not is_github_active(coin):
            logger.info(f"[GitHub] {symbol}: مشروع غير نشط ❌")
            return None

        # الفريمات الإضافية
        df_4h = self.fetch_ohlcv_4h(symbol, limit=60)
        df_1h = self.fetch_ohlcv_1h(symbol, limit=48)

        # الأهداف الأساسية
        targets = calculate_dynamic_targets(
            df_daily        = df_daily,
            df_4h           = df_4h,
            df_1h           = df_1h,
            entry_price     = ind["current_price"],
            nearest_support = ind["nearest_support"],
            crash_pct_60d   = ind["crash_60d"],
        )

        # الفلاتر المتقدمة
        from core.filters import run_advanced_filters
        local_high = float(df_daily["high"].tail(60).max())
        adv = run_advanced_filters(
            exchange    = self.exchange,
            symbol      = symbol,
            entry_price = ind["current_price"],
            rsi         = ind["rsi"],
            local_high  = local_high,
            local_low   = ind["nearest_support"],
        )

        if not adv["passed"]:
            logger.info(f"[AdvFilter] {symbol} مستبعد: {adv['cancel_reason']}")
            return None

        # استخدم Fib v2 إذا RSI < 31
        if ind["rsi"] < 31 and adv["tp1"]:
            tp1, tp2, tp3 = adv["tp1"], adv["tp2"], adv["tp3"]
            tp_method = adv["tp_method"]
        else:
            tp1, tp2, tp3 = targets["tp1"], targets["tp2"], targets["tp3"]
            tp_method = targets["method"]

        # Dynamic SL
        dynamic_sl = adv["dynamic_sl"] or ind["nearest_support"] * 0.92

        # Anti-Lag: استخدم السعر الحالي الفعلي
        entry_price = adv["current_price"]

        opp = TradeOpportunity(
            symbol            = symbol,
            current_price     = entry_price,
            crash_pct_60d     = ind["crash_60d"],
            lod_180           = ind["lod_180"],
            distance_from_lod = ind["distance"],
            rsi_daily         = ind["rsi"],
            volume_24h_usd    = adv["spot_volume"],
            nearest_support   = ind["nearest_support"],
            github_active     = True,
            signal_type       = signal,
            tp1               = tp1,
            tp2               = tp2,
            tp3               = tp3,
            tp_method         = tp_method,
            fib_high          = local_high,
            fib_low           = ind["nearest_support"],
        )
        # تجاوز وقف الخسارة بالقيمة الديناميكية
        object.__setattr__(opp, "stop_loss", dynamic_sl)

        logger.info(
            f"💎 {symbol} | {signal} | "
            f"دخول: {entry_price:.8g} | "
            f"SL: {dynamic_sl:.8g} (Dynamic) | "
            f"TP1: {tp1:.8g} (+{opp.tp1_pct}%) | "
            f"طريقة: {tp_method}"
        )
        return opp

    # ------------------------------------------
    # الفحص بالمجموعات
    # ------------------------------------------
    def scan_market_batched(self, batch_size: int = BATCH_SIZE):
        """Generator يفحص العملات على دفعات"""

        # BTC Filter أولاً
        if not self.is_btc_safe():
            logger.warning("⚠️ BTC في انهيار — لا إشارات اليوم")
            yield [], 1, 1
            return

        symbols       = self.get_usdt_symbols()
        total         = len(symbols)
        total_batches = (total + batch_size - 1) // batch_size
        all_opps      = []

        logger.info(
            f"🔍 بدء الفحص — {total} عملة "
            f"في {total_batches} مجموعة ({batch_size}/مجموعة)"
        )

        for batch_num in range(total_batches):
            start      = batch_num * batch_size
            end        = min(start + batch_size, total)
            batch      = symbols[start:end]
            batch_opps = []

            logger.info(
                f"\n{'─'*35}\n"
                f"📦 المجموعة {batch_num+1}/{total_batches} "
                f"| العملات {start+1}–{end}\n"
                f"{'─'*35}"
            )

            for symbol in batch:
                opp = self._process_symbol(symbol)
                if opp:
                    batch_opps.append(opp)
                    all_opps.append(opp)
                time.sleep(0.25)

            logger.info(
                f"✅ المجموعة {batch_num+1} اكتملت: "
                f"{len(batch_opps)} فرصة | "
                f"إجمالي: {len(all_opps)}"
            )

            yield batch_opps, batch_num + 1, total_batches

        logger.info(
            f"\n{'='*40}\n"
            f"📊 الفحص الكامل:\n"
            f"  إجمالي العملات: {total}\n"
            f"  إجمالي الفرص:   {len(all_opps)}\n"
            f"{'='*40}"
        )

    def scan_market(self) -> list[TradeOpportunity]:
        """للتوافق — يجمع كل النتائج"""
        all_opps = []
        for batch_opps, _, _ in self.scan_market_batched():
            all_opps.extend(batch_opps)
        return all_opps
