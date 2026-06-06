"""
utils/data_pipeline.py
=======================
Data Sourcing & Filtering Pipeline
المصادر المجانية:
  - DefiLlama API  (api.llama.fi + coins.llama.fi) — بدون مفتاح
  - CoinGecko API  — مجاني مع مفتاح اختياري
  - CoinMarketCap  — مجاني (tier مجاني)
  - LunarCrush     — Social sentiment مجاني
"""

import os
import time
import requests
from utils.logger import logger

# ==========================================
# مفاتيح API
# ==========================================
COINGECKO_API_KEY    = os.getenv("COINGECKO_API_KEY", "")
COINMARKETCAP_API_KEY = os.getenv("COINMARKETCAP_API_KEY", "")
LUNARCRUSH_API_KEY   = os.getenv("LUNARCRUSH_API_KEY", "")

# حد الحجم اليومي المطلوب
MIN_VOLUME_USD = float(os.getenv("MIN_DAILY_VOLUME_USD", "5000000"))

# Cache للبروتوكولات (يُحدَّث كل 6 ساعات)
_defillama_protocols_cache: dict = {}
_defillama_dexs_cache:      dict = {}
_cache_timestamp: float = 0.0
CACHE_TTL = 6 * 3600


# ==========================================
# Stage 1 — CoinGecko: قائمة العملات الرائجة
# ==========================================
def get_trending_coins(limit: int = 50) -> list[dict]:
    """
    يجلب قائمة العملات الرائجة من CoinGecko
    مجاني بدون مفتاح (rate limit أبطأ)

    Returns:
        قائمة من dicts: [{"symbol": "SOL", "name": "Solana", "id": "solana"}, ...]
    """
    headers = {"accept": "application/json"}
    if COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY

    try:
        # Trending coins
        r = requests.get(
            "https://api.coingecko.com/api/v3/search/trending",
            headers=headers,
            timeout=10,
        )
        r.raise_for_status()
        coins = r.json().get("coins", [])
        result = []
        for c in coins:
            item = c.get("item", {})
            result.append({
                "symbol": item.get("symbol", "").upper(),
                "name":   item.get("name", ""),
                "id":     item.get("id", ""),
                "rank":   item.get("market_cap_rank", 9999),
            })

        logger.info(f"[CoinGecko] {len(result)} عملة رائجة")
        return result

    except Exception as e:
        logger.error(f"[CoinGecko Trending] {e}")
        return []


def get_top_coins_by_volume(limit: int = 100) -> list[dict]:
    """
    يجلب أعلى عملات بحجم تداول من CoinGecko
    """
    headers = {"accept": "application/json"}
    if COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY

    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            headers=headers,
            params={
                "vs_currency":  "usd",
                "order":        "volume_desc",
                "per_page":     limit,
                "page":         1,
                "sparkline":    False,
            },
            timeout=15,
        )
        r.raise_for_status()
        coins = r.json()
        result = []
        for c in coins:
            result.append({
                "symbol":     c.get("symbol", "").upper(),
                "name":       c.get("name", ""),
                "id":         c.get("id", ""),
                "volume_24h": c.get("total_volume", 0),
                "price":      c.get("current_price", 0),
                "change_24h": c.get("price_change_percentage_24h", 0),
            })

        logger.info(f"[CoinGecko] {len(result)} عملة بحجم تداول عالٍ")
        return result

    except Exception as e:
        logger.error(f"[CoinGecko Markets] {e}")
        return []


# ==========================================
# Stage 2 — DefiLlama: فحص الحجم اليومي
# ==========================================
def _load_defillama_cache():
    """يحمّل بيانات DefiLlama في الـ cache"""
    global _defillama_protocols_cache, _defillama_dexs_cache, _cache_timestamp

    now = time.time()
    if now - _cache_timestamp < CACHE_TTL and _defillama_protocols_cache:
        return  # الـ cache لا يزال صالحاً

    logger.info("[DefiLlama] تحميل بيانات البروتوكولات...")

    # /protocols — TVL وبيانات عامة
    try:
        r = requests.get("https://api.llama.fi/protocols", timeout=15)
        r.raise_for_status()
        protocols = r.json()
        # فهرسة بالـ symbol لبحث سريع
        for p in protocols:
            sym = (p.get("symbol") or "").upper().strip()
            if sym:
                if sym not in _defillama_protocols_cache:
                    _defillama_protocols_cache[sym] = []
                _defillama_protocols_cache[sym].append(p)
        logger.info(f"[DefiLlama] {len(protocols)} بروتوكول محمّل")
    except Exception as e:
        logger.error(f"[DefiLlama /protocols] {e}")

    # /overview/dexs — حجم DEX اليومي (total24h)
    try:
        r = requests.get(
            "https://api.llama.fi/overview/dexs",
            params={"excludeTotalDataChart": "true",
                    "excludeTotalDataChartBreakdown": "true"},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        protocols_dex = data.get("protocols", [])
        for p in protocols_dex:
            # اسم البروتوكول كمفتاح
            name = (p.get("name") or "").lower().strip()
            if name:
                _defillama_dexs_cache[name] = p.get("total24h", 0) or 0
        logger.info(f"[DefiLlama] {len(protocols_dex)} DEX محمّل")
    except Exception as e:
        logger.error(f"[DefiLlama /overview/dexs] {e}")

    _cache_timestamp = now


def get_defillama_volume(coin_symbol: str) -> float:
    """
    يجلب حجم التداول اليومي لعملة من DefiLlama
    يبحث في: /protocols (عام) و /overview/dexs (DEX volume)

    Returns:
        الحجم بالدولار أو 0 إذا لم يُعثر عليها
    """
    _load_defillama_cache()
    symbol = coin_symbol.upper().replace("/USDT", "").strip()
    best_volume = 0.0

    # البحث في /protocols
    if symbol in _defillama_protocols_cache:
        for p in _defillama_protocols_cache[symbol]:
            # volume24h إذا وُجد، أو نحاول جلبه من slug
            v = p.get("volume24h") or p.get("total24h") or 0
            best_volume = max(best_volume, float(v))

    # البحث في /overview/dexs بالاسم
    symbol_lower = symbol.lower()
    for name, vol in _defillama_dexs_cache.items():
        if symbol_lower in name or name in symbol_lower:
            best_volume = max(best_volume, float(vol))

    if best_volume > 0:
        logger.debug(f"[DefiLlama] {symbol}: ${best_volume:,.0f}")

    return best_volume


def check_defillama_volume(coin_symbol: str) -> bool:
    """
    الفلتر الرئيسي — يتحقق من حجم التداول عبر DefiLlama

    Returns:
        True  → الحجم ≥ MIN_VOLUME_USD (يمر للمرحلة التالية)
        False → الحجم منخفض (يُستبعد)
    """
    volume = get_defillama_volume(coin_symbol)

    if volume == 0:
        # لم يُعثر على العملة في DefiLlama → نتجاوز الفلتر
        # (DefiLlama يغطي DeFi فقط، ليس كل العملات)
        logger.debug(f"[DefiLlama] {coin_symbol}: غير موجود — يُعتبر مقبولاً")
        return True

    passed = volume >= MIN_VOLUME_USD
    status = "✅" if passed else "❌"
    logger.info(
        f"[DefiLlama Volume] {coin_symbol}: "
        f"${volume/1e6:.2f}M {status} "
        f"(الحد: ${MIN_VOLUME_USD/1e6:.0f}M)"
    )
    return passed


# ==========================================
# Stage 3 — LunarCrush: Social Sentiment
# ==========================================
def get_social_sentiment(coin_symbol: str) -> dict:
    """
    يجلب مؤشرات المجتمع من LunarCrush
    يُعيد: galaxy_score, alt_rank, sentiment, interactions_24h

    LunarCrush Free Tier: 10 requests/min
    """
    if not LUNARCRUSH_API_KEY:
        return {"available": False, "vote": "neutral"}

    symbol = coin_symbol.replace("/USDT", "").lower()

    try:
        r = requests.get(
            f"https://lunarcrush.com/api4/public/coins/{symbol}/v1",
            headers={"Authorization": f"Bearer {LUNARCRUSH_API_KEY}"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json().get("data", {})

        galaxy_score    = data.get("galaxy_score", 0)
        alt_rank        = data.get("alt_rank", 9999)
        sentiment       = data.get("sentiment", 3)      # 1-5
        interactions_24h = data.get("interactions_24h", 0)

        # تقييم المزاج
        if galaxy_score >= 60 and sentiment >= 4:
            vote = "approve"
        elif galaxy_score <= 30 or sentiment <= 2:
            vote = "reject"
        else:
            vote = "neutral"

        result = {
            "available":       True,
            "galaxy_score":    galaxy_score,
            "alt_rank":        alt_rank,
            "sentiment":       sentiment,
            "interactions_24h": interactions_24h,
            "vote":            vote,
        }

        logger.info(
            f"[LunarCrush] {coin_symbol}: "
            f"Galaxy={galaxy_score} | Sentiment={sentiment} | {vote}"
        )
        return result

    except Exception as e:
        logger.warning(f"[LunarCrush] {coin_symbol}: {e}")
        return {"available": False, "vote": "neutral"}


# ==========================================
# Stage 4 — CoinMarketCap: بيانات إضافية
# ==========================================
def get_coinmarketcap_data(coin_symbol: str) -> dict:
    """
    يجلب بيانات إضافية من CoinMarketCap
    Free Tier: 333 calls/day

    Returns:
        dict بـ volume_24h, market_cap, percent_change_24h
    """
    if not COINMARKETCAP_API_KEY:
        return {"available": False}

    symbol = coin_symbol.replace("/USDT", "").upper()

    try:
        r = requests.get(
            "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
            headers={"X-CMC_PRO_API_KEY": COINMARKETCAP_API_KEY},
            params={"symbol": symbol, "convert": "USD"},
            timeout=10,
        )
        r.raise_for_status()
        data   = r.json().get("data", {}).get(symbol, {})
        quote  = data.get("quote", {}).get("USD", {})

        result = {
            "available":       True,
            "volume_24h":      quote.get("volume_24h", 0),
            "market_cap":      quote.get("market_cap", 0),
            "change_24h":      quote.get("percent_change_24h", 0),
            "change_7d":       quote.get("percent_change_7d", 0),
            "circulating_supply": data.get("circulating_supply", 0),
        }

        logger.info(
            f"[CMC] {coin_symbol}: "
            f"Vol=${result['volume_24h']/1e6:.1f}M | "
            f"Cap=${result['market_cap']/1e6:.0f}M"
        )
        return result

    except Exception as e:
        logger.warning(f"[CMC] {coin_symbol}: {e}")
        return {"available": False}


# ==========================================
# الدالة الرئيسية — Pipeline الكامل
# ==========================================
def run_data_pipeline(coin_symbol: str) -> dict:
    """
    يشغّل Pipeline كامل لتقييم عملة:

    Stage 1: CoinGecko    — السعر والحجم الأساسي
    Stage 2: DefiLlama    — حجم DeFi (فلتر $5M)
    Stage 3: LunarCrush   — Social Sentiment
    Stage 4: CoinMarketCap — بيانات تكميلية

    Returns:
        dict شامل مع "passed" (True/False) و "reason"
    """
    symbol = coin_symbol.replace("/USDT", "").upper()
    result = {
        "symbol":      coin_symbol,
        "passed":      False,
        "reason":      "",
        "volume_24h":  0,
        "sentiment":   "neutral",
        "data":        {},
    }

    # ── Stage 2: DefiLlama Volume Filter ──
    defillama_volume = get_defillama_volume(symbol)
    result["data"]["defillama_volume"] = defillama_volume

    if defillama_volume > 0 and defillama_volume < MIN_VOLUME_USD:
        result["reason"] = (
            f"حجم DefiLlama منخفض: ${defillama_volume/1e6:.2f}M "
            f"< ${MIN_VOLUME_USD/1e6:.0f}M"
        )
        logger.info(f"[Pipeline] {symbol} مستبعد — {result['reason']}")
        return result

    # ── Stage 3: LunarCrush Sentiment ──
    lunar = get_social_sentiment(symbol)
    result["data"]["lunarcrush"] = lunar
    if lunar["available"]:
        result["sentiment"] = lunar["vote"]

    # ── Stage 4: CoinMarketCap ──
    cmc = get_coinmarketcap_data(symbol)
    result["data"]["coinmarketcap"] = cmc

    if cmc["available"]:
        cmc_volume = cmc.get("volume_24h", 0)
        result["volume_24h"] = max(defillama_volume, cmc_volume)

        # فلتر CMC إذا كان الحجم أقل من الحد
        if cmc_volume > 0 and cmc_volume < MIN_VOLUME_USD:
            result["reason"] = (
                f"حجم CMC منخفض: ${cmc_volume/1e6:.2f}M"
            )
            return result

    # ── النتيجة النهائية ──
    result["passed"] = True
    result["reason"] = "اجتاز جميع مراحل Pipeline"
    logger.info(f"[Pipeline] {symbol} ✅ — {result['reason']}")
    return result
