"""
core/scanner.py
===============
Bottom Fisher — نظام المجموعات (Batching)
يفحص العملات على دفعات ويرسل الفرص فور اكتشافها
"""

import ccxt
import pandas as pd
import numpy as np
import time
from typing import Optional, AsyncGenerator
from dataclasses import dataclass, field

from config.settings import (
    MEXC_API_KEY, MEXC_API_SECRET,
    MIN_DAILY_VOLUME_USD,
    RSI_OVERSOLD_THRESHOLD, RSI_PERIOD, LOD_DAYS,
    MAX_DISTANCE_FROM_LOD,
)
from utils.logger import logger
from utils.github_checker import is_github_active

# حجم كل مجموعة
BATCH_SIZE = int(__import__('os').getenv("BATCH_SIZE", "100"))

# عملات مستبعدة
EXCLUDED_BASE_COINS = {
    "USDC","BUSD","TUSD","USDP","GUSD","FRAX","LUSD",
    "DAI","USDD","FDUSD","PYUSD","USDE","SUSD","USD1",
    "USDT","AEUR","EURI","EURS","XAUT","PAXG",
    "EUR","GBP","AUD","JPY","TRY","BRL","CAD",
    "WBTC","WETH","WBNB","WMATIC","WSOL",
    "STETH","RETH","CBETH","SFRXETH",
}


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
    crash_pct_60d:     float
    lod_180:           float
    distance_from_lod: float
    rsi_daily:         float
    volume_24h_usd:    float
    nearest_support:   float
    github_active:     bool
    signal_type:       str

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
            logger.info(
                f"📊 إجمالي أزواج USDT: {len(symbols)} "
                f"(استُبعد {excluded} stablecoin/فيات)"
            )
            return symbols
        except Exception as e:
            logger.error(f"❌ خطأ: {e}")
            return []

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

    def get_24h_volume_usd(self, symbol: str) -> float:
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return float(ticker.get("quoteVolume") or 0.0)
        except Exception:
            return 0.0

    def passes_filters(self, symbol: str, ind: dict, volume_usd: float) -> tuple[bool, str]:
        if volume_usd < MIN_DAILY_VOLUME_USD:
            return False, f"حجم منخفض"

        rsi         = ind["rsi"]
        crash_60d   = ind["crash_60d"]
        distance    = ind["distance"]
        stabilizing = ind["is_stabilizing"]

        if crash_60d >= 0.40 and rsi < 45 and stabilizing:
            return True, f"🅰️ انهيار حديث {crash_60d:.0%} + تماسك"
        if distance <= MAX_DISTANCE_FROM_LOD and rsi < RSI_OVERSOLD_THRESHOLD:
            return True, f"🅱️ قاع تاريخي RSI={rsi:.1f}"
        if crash_60d >= 0.60 and rsi < 35:
            return True, f"🅲 انهيار ضخم {crash_60d:.0%} RSI={rsi:.1f}"

        return False, ""

    def _process_symbol(self, symbol: str) -> Optional["TradeOpportunity"]:
        """يعالج عملة واحدة ويُعيد الفرصة إن وُجدت"""
        df = self.fetch_ohlcv_daily(symbol)
        if df is None:
            return None
        try:
            ind = self.calculate_indicators(df)
        except Exception:
            return None

        volume_usd       = self.get_24h_volume_usd(symbol)
        passed, signal   = self.passes_filters(symbol, ind, volume_usd)
        if not passed:
            return None

        # فلتر GitHub الذكي
        coin = symbol.replace("/USDT", "")
        if not is_github_active(coin):
            logger.info(f"[GitHub] {symbol}: مشروع غير نشط ❌")
            return None

        return TradeOpportunity(
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

    def scan_market_batched(self, batch_size: int = BATCH_SIZE):
        """
        Generator يفحص العملات على مجموعات
        يُعيد (قائمة الفرص, رقم المجموعة, إجمالي المجموعات)
        بعد كل مجموعة مكتملة
        """
        symbols      = self.get_usdt_symbols()
        total        = len(symbols)
        total_batches = (total + batch_size - 1) // batch_size
        all_opps     = []
        total_stats  = {"a": 0, "b": 0, "c": 0}

        logger.info(
            f"🔍 بدء الفحص — {total} عملة "
            f"في {total_batches} مجموعة "
            f"({batch_size} عملة/مجموعة)"
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
                    if "🅰️" in opp.signal_type: total_stats["a"] += 1
                    elif "🅱️" in opp.signal_type: total_stats["b"] += 1
                    elif "🅲" in opp.signal_type: total_stats["c"] += 1
                    logger.info(
                        f"💎 {symbol} | {opp.signal_type} "
                        f"| دخول: {opp.entry_price:.6f} "
                        f"| R/R: {opp.risk_reward_ratio}"
                    )
                time.sleep(0.2)

            # ملخص المجموعة في الـ logs فقط
            logger.info(
                f"✅ المجموعة {batch_num+1} اكتملت: "
                f"{len(batch_opps)} فرصة | "
                f"إجمالي حتى الآن: {len(all_opps)}"
            )

            # أعطِ الفرص للـ main ليرسلها فوراً
            yield batch_opps, batch_num + 1, total_batches

        # ملخص نهائي
        logger.info(
            f"\n{'='*40}\n"
            f"📊 الفحص الكامل اكتمل:\n"
            f"  إجمالي العملات:   {total}\n"
            f"  استراتيجية A:     {total_stats['a']}\n"
            f"  استراتيجية B:     {total_stats['b']}\n"
            f"  استراتيجية C:     {total_stats['c']}\n"
            f"  إجمالي الفرص:     {len(all_opps)}\n"
            f"{'='*40}"
        )
        return all_opps

    def scan_market(self) -> list[TradeOpportunity]:
        """للتوافق مع الكود القديم — يجمع كل النتائج"""
        all_opps = []
        for batch_opps, _, _ in self.scan_market_batched():
            all_opps.extend(batch_opps)
        return all_opps
