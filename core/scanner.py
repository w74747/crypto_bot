"""
core/scanner.py
===============
Bottom Fisher - نسخة محسّنة
تبحث عن انهيارات حديثة مع بداية تماسك
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


def calculate_rsi(closes: pd.Series, period: int = 14) -> float:
    delta    = closes.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    rsi      = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


@dataclass
class TradeOpportunity:
    symbol:            str
    current_price:     float
    crash_pct_60d:     float   # نسبة الانهيار خلال 60 يوم
    lod_180:           float
    distance_from_lod: float
    rsi_daily:         float
    volume_24h_usd:    float
    nearest_support:   float
    github_active:     bool
    signal_type:       str     # نوع الإشارة

    entry_price: float = field(init=False)
    stop_loss:   float = field(init=False)
    tp1:         float = field(init=False)
    tp2:         float = field(init=False)
    tp3:         float = field(init=False)

    def __post_init__(self):
        from config.settings import STOP_LOSS_PCT, TP1_PCT, TP2_PCT, TP3_PCT
        self.entry_price = self.current_price
        self.stop_loss   = self.nearest_support * (1 - STOP_LOSS_PCT)
        self.tp1 = self.entry_price * (1 + TP1_PCT)
        self.tp2 = self.entry_price * (1 + TP2_PCT)
        self.tp3 = self.entry_price * (1 + TP3_PCT)

    @property
    def risk_reward_ratio(self) -> float:
        risk   = self.entry_price - self.stop_loss
        reward = self.tp1 - self.entry_price
        if risk <= 0:
            return 0
        return round(reward / risk, 2)


class MarketScanner:

    def __init__(self):
        self.exchange = self._connect()
        logger.info("✅ تم الاتصال بـ MEXC")

    def _connect(self) -> ccxt.mexc:
        exchange = ccxt.mexc({
            "apiKey": MEXC_API_KEY,
            "secret": MEXC_API_SECRET,
            "options": {"defaultType": "spot"},
            "enableRateLimit": True,
        })
        exchange.load_markets()
        return exchange

    def get_usdt_symbols(self) -> list[str]:
        symbols = [
            s for s, m in self.exchange.markets.items()
            if m.get("quote") == "USDT"
            and m.get("active", False)
            and m.get("spot", False)
        ]
        logger.info(f"📊 إجمالي أزواج USDT: {len(symbols)}")
        return symbols

    def fetch_ohlcv_daily(self, symbol: str, limit: int = 200) -> Optional[pd.DataFrame]:
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe="1d", limit=limit)
            if not ohlcv or len(ohlcv) < 50:
                return None
            df = pd.DataFrame(
                ohlcv,
                columns=["timestamp","open","high","low","close","volume"]
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.set_index("timestamp", inplace=True)
            return df.astype(float)
        except Exception as e:
            logger.warning(f"[Scanner] {symbol}: {e}")
            return None

    def calculate_indicators(self, df: pd.DataFrame) -> dict:
        closes = df["close"]
        current_price = float(closes.iloc[-1])

        # RSI
        rsi = calculate_rsi(closes, period=RSI_PERIOD)

        # أدنى سعر 180 يوم
        lod_180  = float(df["low"].tail(LOD_DAYS).min())
        distance = (current_price - lod_180) / lod_180 if lod_180 > 0 else 1.0

        # أعلى سعر خلال 60 يوم (لحساب الانهيار الحديث)
        high_60d    = float(df["close"].tail(60).max())
        crash_60d   = (high_60d - current_price) / high_60d if high_60d > 0 else 0.0

        # أعلى سعر خلال 30 يوم (للتحقق من استمرار الانهيار)
        high_30d    = float(df["close"].tail(30).max())
        crash_30d   = (high_30d - current_price) / high_30d if high_30d > 0 else 0.0

        # فحص التماسك: هل آخر 3 شموع لا تصنع قيعاناً جديدة؟
        last_3_lows   = df["low"].tail(3).values
        is_stabilizing = bool(last_3_lows[-1] >= last_3_lows[-2] * 0.98)

        # أقرب دعم (أدنى قاع آخر 14 يوم)
        nearest_support = float(df["low"].tail(14).min())

        return {
            "rsi":              rsi,
            "lod_180":          lod_180,
            "current_price":    current_price,
            "distance":         distance,
            "crash_60d":        crash_60d,
            "crash_30d":        crash_30d,
            "is_stabilizing":   is_stabilizing,
            "nearest_support":  nearest_support,
            "high_60d":         high_60d,
        }

    def get_24h_volume_usd(self, symbol: str) -> float:
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return float(ticker.get("quoteVolume") or 0.0)
        except Exception:
            return 0.0

    def passes_filters(
        self, symbol: str, ind: dict, volume_usd: float
    ) -> tuple[bool, str]:
        """
        يطبق الفلاتر ويُعيد (نجح/فشل, سبب الفشل أو نوع الإشارة)

        الاستراتيجيات الثلاث:
        A — انهيار حديث + تماسك (الأفضل)
        B — قريب من قاع 180 يوم + RSI تشبع بيعي (الكلاسيكي)
        C — انهيار ضخم جداً + RSI منخفض جداً (الانتهازي)
        """

        # فلتر الحجم — إلزامي للجميع
        if volume_usd < MIN_DAILY_VOLUME_USD:
            return False, f"حجم منخفض ${volume_usd:,.0f}"

        rsi          = ind["rsi"]
        crash_60d    = ind["crash_60d"]
        crash_30d    = ind["crash_30d"]
        distance     = ind["distance"]
        stabilizing  = ind["is_stabilizing"]

        # --- استراتيجية A: انهيار حديث + تماسك ---
        # انهارت أكثر من 40% خلال 60 يوم
        # RSI أقل من 45
        # بدأت تتماسك (لا قيعان جديدة)
        if crash_60d >= 0.40 and rsi < 45 and stabilizing:
            return True, f"🅰️ انهيار حديث {crash_60d:.0%} + تماسك"

        # --- استراتيجية B: قريبة من القاع التاريخي ---
        # السعر في نطاق 15% من قاع 180 يوم
        # RSI في تشبع بيعي
        if distance <= MAX_DISTANCE_FROM_LOD and rsi < RSI_OVERSOLD_THRESHOLD:
            return True, f"🅱️ قاع تاريخي RSI={rsi:.1f}"

        # --- استراتيجية C: انهيار ضخم جداً ---
        # انهارت أكثر من 60% خلال 60 يوم
        # RSI أقل من 35 (تشبع بيعي حاد)
        if crash_60d >= 0.60 and rsi < 35:
            return True, f"🅲 انهيار ضخم {crash_60d:.0%} RSI={rsi:.1f}"

        return False, (
            f"لم تجتز | crash60={crash_60d:.0%} "
            f"RSI={rsi:.1f} dist={distance:.0%} "
            f"stable={stabilizing}"
        )

    def scan_market(self) -> list[TradeOpportunity]:
        logger.info("🔍 بدء فحص السوق — استراتيجية ثلاثية...")
        opportunities  = []
        symbols        = self.get_usdt_symbols()

        # إحصائيات للتشخيص
        stats = {"volume": 0, "strategy_a": 0, "strategy_b": 0, "strategy_c": 0}

        for i, symbol in enumerate(symbols, 1):
            if i % 50 == 0:
                logger.info(f"[Scan] {i}/{len(symbols)} | فرص: {len(opportunities)}")

            df = self.fetch_ohlcv_daily(symbol)
            if df is None:
                continue

            try:
                ind = self.calculate_indicators(df)
            except Exception as e:
                logger.warning(f"[Scan] فشل {symbol}: {e}")
                continue

            volume_usd = self.get_24h_volume_usd(symbol)
            if volume_usd >= MIN_DAILY_VOLUME_USD:
                stats["volume"] += 1

            passed, signal = self.passes_filters(symbol, ind, volume_usd)

            if not passed:
                logger.debug(f"[Filter] {symbol}: {signal}")
                continue

            # فلتر GitHub
            coin = symbol.replace("/USDT", "")
            if not is_github_active(coin):
                logger.info(f"[GitHub] {symbol}: مشروع غير نشط ❌")
                continue

            # تسجيل نوع الاستراتيجية
            if "🅰️" in signal: stats["strategy_a"] += 1
            elif "🅱️" in signal: stats["strategy_b"] += 1
            elif "🅲" in signal: stats["strategy_c"] += 1

            opp = TradeOpportunity(
                symbol            = symbol,
                current_price     = ind["current_price"],
                crash_pct_60d     = ind["crash_60d"],
                lod_180           = ind["lod_180"],
                distance_from_lod = ind["distance"],
                rsi_daily         = ind["rsi"],
                volume_24h_usd    = volume_usd,
                nearest_support   = ind["nearest_support"],
                github_active     = True,
                signal_type       = signal,
            )
            opportunities.append(opp)
            logger.info(
                f"💎 {symbol} | {signal} "
                f"| دخول: {opp.entry_price:.6f} "
                f"| SL: {opp.stop_loss:.6f} "
                f"| R/R: {opp.risk_reward_ratio}"
            )
            time.sleep(0.3)

        # ملخص التشخيص
        logger.info(
            f"\n{'='*40}\n"
            f"📊 ملخص الفحص:\n"
            f"  إجمالي العملات:      {len(symbols)}\n"
            f"  اجتازت فلتر الحجم:  {stats['volume']}\n"
            f"  استراتيجية A:        {stats['strategy_a']}\n"
            f"  استراتيجية B:        {stats['strategy_b']}\n"
            f"  استراتيجية C:        {stats['strategy_c']}\n"
            f"  إجمالي الفرص:        {len(opportunities)}\n"
            f"{'='*40}"
        )
        return opportunities
