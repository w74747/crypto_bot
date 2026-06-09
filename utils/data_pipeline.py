"""
utils/data_pipeline.py
=======================
Layer 1 Shields — فلاتر قبل الـ AI Committee
كل القيم من Railway env vars — لا hardcoding
"""

import os
import time
import asyncio
import aiohttp
from utils.logger import logger

# ── مفاتيح API من Railway ──
COINMARKETCAP_API_KEY = os.environ.get("COINMARKETCAP_API_KEY", "")
LUNARCRUSH_API_KEY    = os.environ.get("LUNARCRUSH_API_KEY", "")
COINGECKO_API_KEY     = os.environ.get("COINGECKO_API_KEY", "")

# ── حدود الفلترة من Railway ──
MIN_VOLUME_USD        = float(os.environ.get("MIN_DAILY_VOLUME_USD", "1000000"))
MIN_GALAXY_SCORE      = float(os.environ.get("MIN_GALAXY_SCORE",     "0"))
RSI_OVERSOLD_THRESHOLD = int(os.environ.get("RSI_OVERSOLD_THRESHOLD", "31"))

# ── Cache لتقليل API calls ──
_cmc_cache:   dict  = {}
_luna_cache:  dict  = {}
_cache_ttl:   float = 3600.0  # ساعة واحدة


def _cache_get(cache: dict, key: str) -> dict | None:
    entry = cache.get(key)
    if entry and (time.time() - entry["ts"]) < _cache_ttl:
        return entry["data"]
    return None


def _cache_set(cache: dict, key: str, data: dict):
    cache[key] = {"ts": time.time(), "data": data}


# ==========================================
# CoinMarketCap — حجم التداول الفعلي
# ==========================================
async def get_cmc_volume(session: aiohttp.ClientSession, symbol: str) -> float:
    """
    يجلب حجم التداول 24h من CoinMarketCap
    Free Tier: 333 calls/day — نستخدم Cache لتوفير الحصة
    """
    coin = symbol.replace("/USDT", "").upper()

    # تحقق من الـ Cache أولاً
    cached = _cache_get(_cmc_cache, coin)
    if cached is not None:
        logger.debug(f"[CMC Cache] {coin}: ${cached.get('volume_24h', 0)/1e6:.1f}M")
        return float(cached.get("volume_24h", 0))

    if not COINMARKETCAP_API_KEY:
        logger.debug(f"[CMC] لا يوجد مفتاح — تجاوز الفلتر")
        return MIN_VOLUME_USD  # تجاوز إذا لا يوجد مفتاح

    try:
        async with session.get(
            "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
            headers={"X-CMC_PRO_API_KEY": COINMARKETCAP_API_KEY,
                     "Accept": "application/json"},
            params={"symbol": coin, "convert": "USD"},
            timeout=aiohttp.ClientTimeout(total=6),
        ) as resp:
            if resp.status != 200:
                logger.warning(f"[CMC] {coin}: HTTP {resp.status}")
                return MIN_VOLUME_USD  # تجاوز عند خطأ

            data   = await resp.json()
            quote  = data.get("data", {}).get(coin, {}).get("quote", {}).get("USD", {})
            volume = float(quote.get("volume_24h", 0))

            _cache_set(_cmc_cache, coin, {"volume_24h": volume})
            logger.debug(f"[CMC] {coin}: ${volume/1e6:.1f}M")
            return volume

    except asyncio.TimeoutError:
        logger.warning(f"[CMC] {coin}: Timeout")
        return MIN_VOLUME_USD
    except Exception as e:
        logger.warning(f"[CMC] {coin}: {str(e)[:60]}")
        return MIN_VOLUME_USD


# ==========================================
# LunarCrush — Social Sentiment
# ==========================================
async def get_lunar_sentiment(session: aiohttp.ClientSession, symbol: str) -> dict:
    """
    يجلب Galaxy Score و Sentiment من LunarCrush
    Free Tier: 10 requests/min
    """
    coin = symbol.replace("/USDT", "").lower()

    cached = _cache_get(_luna_cache, coin)
    if cached is not None:
        logger.debug(f"[LC Cache] {coin}: Galaxy={cached.get('galaxy_score', 0)}")
        return cached

    if not LUNARCRUSH_API_KEY:
        return {"available": False, "vote": "neutral",
                "galaxy_score": 0, "sentiment": 3}

    try:
        async with session.get(
            f"https://lunarcrush.com/api4/public/coins/{coin}/v1",
            headers={"Authorization": f"Bearer {LUNARCRUSH_API_KEY}"},
            timeout=aiohttp.ClientTimeout(total=6),
        ) as resp:
            if resp.status != 200:
                return {"available": False, "vote": "neutral",
                        "galaxy_score": 0, "sentiment": 3}

            data         = (await resp.json()).get("data", {})
            galaxy_score = float(data.get("galaxy_score", 0))
            sentiment    = float(data.get("sentiment",    3))
            alt_rank     = int(data.get("alt_rank",       9999))

            if galaxy_score >= 60 and sentiment >= 4:
                vote = "approve"
            elif galaxy_score <= 20 or sentiment <= 1.5:
                vote = "reject"
            else:
                vote = "neutral"

            result = {
                "available":    True,
                "galaxy_score": galaxy_score,
                "sentiment":    sentiment,
                "alt_rank":     alt_rank,
                "vote":         vote,
            }
            _cache_set(_luna_cache, coin, result)
            logger.debug(
                f"[LunarCrush] {coin}: "
                f"Galaxy={galaxy_score} Sentiment={sentiment} → {vote}"
            )
            return result

    except asyncio.TimeoutError:
        logger.warning(f"[LunarCrush] {coin}: Timeout")
        return {"available": False, "vote": "neutral",
                "galaxy_score": 0, "sentiment": 3}
    except Exception as e:
        logger.warning(f"[LunarCrush] {coin}: {str(e)[:60]}")
        return {"available": False, "vote": "neutral",
                "galaxy_score": 0, "sentiment": 3}


# ==========================================
# Layer 1 Shield — الفلتر الشامل
# ==========================================
async def layer1_shield(
    session: aiohttp.ClientSession,
    symbol:  str,
    rsi:     float,
) -> tuple[bool, str]:
    """
    يشغّل CMC + LunarCrush بالتوازي
    يُعيد (passed, reason)

    الشروط:
    1. RSI <= RSI_OVERSOLD_THRESHOLD (من Railway)
    2. Volume 24h >= MIN_VOLUME_USD  (من Railway)
    3. LunarCrush: ليس "reject" صريح
    """
    # 1. RSI (فوري — بدون API)
    if rsi > RSI_OVERSOLD_THRESHOLD:
        return False, f"RSI={rsi:.1f} > {RSI_OVERSOLD_THRESHOLD}"

    # 2 + 3. CMC و LunarCrush بالتوازي
    cmc_task   = get_cmc_volume(session, symbol)
    lunar_task = get_lunar_sentiment(session, symbol)

    cmc_vol, lunar = await asyncio.gather(cmc_task, lunar_task)

    # فلتر الحجم
    if cmc_vol < MIN_VOLUME_USD:
        return False, (
            f"CMC Volume ${cmc_vol/1e6:.1f}M < "
            f"${MIN_VOLUME_USD/1e6:.0f}M"
        )

    # فلتر LunarCrush — رفض صريح فقط
    if lunar.get("available") and lunar.get("vote") == "reject":
        gs = lunar.get("galaxy_score", 0)
        return False, f"LunarCrush رفض (Galaxy={gs:.0f})"

    reason = (
        f"RSI={rsi:.1f} ✅ | "
        f"Vol=${cmc_vol/1e6:.1f}M ✅ | "
        f"Galaxy={lunar.get('galaxy_score', 'N/A')}"
    )
    return True, reason
