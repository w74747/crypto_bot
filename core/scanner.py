"""
core/scanner.py
===============
قلب النظام - يفحص السوق عبر MEXC ويبحث عن فرص Bottom Fisher
"""

import ccxt
import pandas as pd
import pandas_ta as ta
import time
from typing import Optional
from dataclasses import dataclass, field

from config.settings import (
    MEXC_API_KEY, MEXC_API_SECRET,
    MAX_DISTANCE_FROM_LOD, MIN_DAILY_VOLUME_USD,
    RSI_OVERSOLD_THRESHOLD, RSI_PERIOD, LOD_DAYS
)
from utils.logger import logger
from utils.github_checker import is_github_active


@dataclass
class TradeOpportunity:
    """يحتوي على كل معلومات الفرصة المكتشفة"""
    symbol:            str
    current_price:     float
    lod_180:           float
    distance_from_lod: float
    rsi_daily:         float
    volume_24h_usd:    float
    nearest_support:   float
    github_active:     bool

    entry_price:  float = field(init=False)
    stop_loss:    float = field(init=False)
    tp1:          float = field(init=False)
    tp2:          float = field(init=False)
    tp3:          float = field(init=False)

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
    """يتصل بـ MEXC ويفحص السوق"""

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
            df = pd.DataFrame(ohlcv, columns=["timestamp","open","high","low","close","volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            df.set_index("timestamp", inplace=True)
            return df.astype(float)
        except ccxt.NetworkError as e:
            logger.warning(f"[Scanner] خطأ شبكة {symbol}: {e}")
            return None
        except ccxt.ExchangeError as e:
            logger.warning(f"[Scanner] خطأ منصة {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"[Scanner] خطأ {symbol}: {e}")
            return None

    def calculate_indicators(self, df: pd.DataFrame) -> dict:
        df["rsi"] = ta.rsi(df["close"], length=RSI_PERIOD)
        current_rsi   = float(df["rsi"].iloc[-1])
        lod_180       = float(df["low"].tail(LOD_DAYS).min())
        current_price = float(df["close"].iloc[-1])
        distance      = (current_price - lod_180) / lod_180 if lod_180 > 0 else 1.0
        nearest_support = float(df["low"].tail(20).min())
        return {
            "rsi": current_rsi, "lod_180": lod_180,
            "current_price": current_price, "distance": distance,
            "nearest_support": nearest_support,
        }

    def get_24h_volume_usd(self, symbol: str) -> float:
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            return float(ticker.get("quoteVolume") or 0.0)
        except Exception:
            return 0.0

    def passes_filters(self, symbol: str, indicators: dict, volume_usd: float) -> bool:
        if indicators["distance"] > MAX_DISTANCE_FROM_LOD:
            return False
        if volume_usd < MIN_DAILY_VOLUME_USD:
            return False
        rsi = indicators["rsi"]
        if pd.isna(rsi) or rsi >= RSI_OVERSOLD_THRESHOLD:
            return False
        logger.info(
            f"✅ {symbol} | RSI: {rsi:.1f} "
            f"| البعد: {indicators['distance']:.1%} "
            f"| الحجم: ${volume_usd:,.0f}"
        )
        return True

    def scan_market(self) -> list[TradeOpportunity]:
        logger.info("🔍 بدء فحص السوق على MEXC...")
        opportunities = []
        symbols = self.get_usdt_symbols()

        for i, symbol in enumerate(symbols, 1):
            if i % 50 == 0:
                logger.info(f"[Scan] {i}/{len(symbols)}")

            df = self.fetch_ohlcv_daily(symbol)
            if df is None:
                continue

            try:
                indicators = self.calculate_indicators(df)
            except Exception as e:
                logger.warning(f"[Scan] فشل مؤشرات {symbol}: {e}")
                continue

            volume_usd = self.get_24h_volume_usd(symbol)

            if not self.passes_filters(symbol, indicators, volume_usd):
                continue

            coin_name = symbol.replace("/USDT", "")
            if not is_github_active(coin_name):
                logger.info(f"[GitHub] {symbol}: مشروع غير نشط ❌")
                continue

            opp = TradeOpportunity(
                symbol=symbol,
                current_price=indicators["current_price"],
                lod_180=indicators["lod_180"],
                distance_from_lod=indicators["distance"],
                rsi_daily=indicators["rsi"],
                volume_24h_usd=volume_usd,
                nearest_support=indicators["nearest_support"],
                github_active=True,
            )
            opportunities.append(opp)
            logger.info(
                f"💎 فرصة: {symbol} | دخول: {opp.entry_price:.6f} "
                f"| SL: {opp.stop_loss:.6f} | R/R: {opp.risk_reward_ratio}"
            )
            time.sleep(0.3)

        logger.info(f"✅ اكتمل: {len(opportunities)} فرصة من {len(symbols)} عملة")
        return opportunities
