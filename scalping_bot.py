"""
scalping_bot.py — MEXC Production Engine v2
============================================
Classes: Config | SlotState | SlotManager | DataPipeline
         ConsensusCommittee | HighSpeedExecutor | TradeMonitor
         ScalpingOrchestrator
"""

from __future__ import annotations
import asyncio
import math
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import ccxt
import pandas as pd
try:
    import psycopg2
    import psycopg2.extras
    _PSYCOPG2_OK = True
except ImportError:
    _PSYCOPG2_OK = False


# ─────────────────────────────────────────────
# 1. CONFIG
# ─────────────────────────────────────────────
@dataclass(frozen=True)
class Config:
    mexc_api_key:       str   = field(default_factory=lambda: os.environ.get("MEXC_API_KEY", ""))
    mexc_api_secret:    str   = field(default_factory=lambda: os.environ.get("MEXC_API_SECRET", ""))
    telegram_token:     str   = field(default_factory=lambda: os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    telegram_chat_id:   str   = field(default_factory=lambda: os.environ.get("TELEGRAM_CHAT_ID", ""))
    deepseek_api_key:   str   = field(default_factory=lambda: os.environ.get("DEEPSEEK_API_KEY", ""))
    together_api_key:   str   = field(default_factory=lambda: os.environ.get("TOGETHER_API_KEY") or os.environ.get("TOGATHER_API_KEY", ""))
    cmc_api_key:        str   = field(default_factory=lambda: os.environ.get("COINMARKETCAP_API_KEY", ""))
    coingecko_api_key:  str   = field(default_factory=lambda: os.environ.get("COINGECKO_API_KEY", ""))
    lunar_api_key:      str   = field(default_factory=lambda: os.environ.get("LUNARCRUSH_API_KEY", ""))
    whale_alert_api_key: str  = field(default_factory=lambda: os.environ.get("WHALE_ALERT_API_KEY", ""))
    database_url:       str   = field(default_factory=lambda: os.environ.get("DATABASE_URL", ""))

    capital:            float = field(default_factory=lambda: float(os.environ.get("TRADE_INVESTMENT_AMOUNT", "30")))
    max_slots:          int   = field(default_factory=lambda: int(os.environ.get("MAX_CONCURRENT_TRADES", "3")))
    scan_interval:      int   = field(default_factory=lambda: int(os.environ.get("SCAN_INTERVAL_MINUTES", "60")))
    rsi_threshold:      int   = field(default_factory=lambda: int(os.environ.get("RSI_OVERSOLD_THRESHOLD", "31")))
    min_volume_usd:     float = field(default_factory=lambda: float(os.environ.get("MIN_DAILY_VOLUME_USD", "1000000")))
    cmc_top_rank:       int   = field(default_factory=lambda: int(os.environ.get("CMC_TOP_RANK", "300")))
    monitor_interval:   int   = 30
    max_ai_tokens:      int   = 150
    deepseek_model:     str   = field(default_factory=lambda: os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"))
    together_model:     str   = "meta-llama/Llama-3.3-70B-Instruct-Turbo"
    max_trade_hours:    float = field(default_factory=lambda: float(os.environ.get("MAX_TRADE_DURATION_HOURS", "10")))
    extension_hours:    float = 3.0
    reconcile_interval: int   = field(default_factory=lambda: int(os.environ.get("RECONCILE_INTERVAL_SECONDS", "180")))
    sl_retry_attempts:        int   = 3
    disable_timeout_liquidation: bool = True  # Positions run until TP or Shadow SL — no time-based liquidation
    blacklisted_assets: set   = field(default_factory=lambda: {
        # ── إقراض بفائدة (Lending/Interest protocols) ──
        "AAVE", "COMP", "MKR", "CRV", "LDO", "UNI", "SUSHI", "BAL",
        "CAKE", "YFI", "SNX", "DYDX", "ANC",
        "ALPHA", "VENUS", "CREAM", "PENDLE", "RADIANT", "EULER", "FLUID",
        # ── قمار وميسر (Gambling) ──
        "FUN", "WIN", "DICE", "BET", "RLB", "POLS",
        "CHIP", "SLOT", "LOTTO", "LUCKY", "DERC",
        # ── محتوى إباحي (Adult content) ──
        "NSFW", "ADULTS", "FANTASY", "STRIP",
        # ── Metaverse/NFT Gaming محل إشكال ──
        "MANA", "SAND", "GALA", "AXS", "SLP",
        # ── خصوصية مطلقة (Privacy coins) ──
        "XMR", "DASH", "ZEC",
        # ── Leveraged tokens (رافعة مالية) ──
        "BULL", "BEAR", "UP", "DOWN",
    })


def _format_duration(start_time: float) -> str:
    """Formats elapsed seconds into Arabic-friendly h/m/s string."""
    elapsed = int(time.time() - start_time)
    h, rem  = divmod(elapsed, 3600)
    m, s    = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    elif m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


# ─────────────────────────────────────────────
# TRADE LOGGER — Supabase integration
# ─────────────────────────────────────────────
class TradeLogger:
    """
    يسجّل كل صفقة في Supabase:
    - يُنشئ سجلاً عند الشراء
    - يُحدّثه عند كل خروج (TP1/TP2/TP3/SL)
    - يوفر إجمالي أرباح الشهر الحالي
    """

    def __init__(self, database_url: str):
        self.db_url   = database_url
        self._enabled = bool(database_url and _PSYCOPG2_OK)
        if self._enabled:
            _log("[DB] ✅ TradeLogger متصل بـ Supabase")
        else:
            _log("[DB] ⚠️ TradeLogger معطّل (لا DATABASE_URL أو psycopg2)")

    def _get_conn(self):
        return psycopg2.connect(self.db_url, sslmode="require")

    def insert_trade(
        self,
        state:             "SlotState",
        capital:           float,
        ds_vote:           str = "—",
        llama_vote:        str = "—",
        rss_sentiment:     str = "—",
        galaxy_score:      float = 0.0,
        committee_summary: str = "",
    ) -> str | None:
        """يُنشئ سجلاً جديداً عند الشراء. يُعيد الـ UUID."""
        if not self._enabled:
            return None
        try:
            conn = self._get_conn()
            cur  = conn.cursor()
            cur.execute(
                """
                INSERT INTO trades
                    (symbol, opened_at, capital, filled_qty, entry_price,
                     tp1, tp2, tp3, stop_loss, ds_vote, llama_vote,
                     rss_sentiment, galaxy_score, committee_summary)
                VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    state.symbol, capital, state.filled_qty, state.entry_price,
                    state.tp1, state.tp2, state.tp3, state.stop_loss,
                    ds_vote, llama_vote, rss_sentiment, galaxy_score,
                    committee_summary,
                ),
            )
            trade_id = str(cur.fetchone()[0])
            conn.commit()
            cur.close()
            conn.close()
            _log(f"[DB] ✅ صفقة مُسجَّلة: {state.symbol} | ID: {trade_id[:8]}...")
            return trade_id
        except Exception as e:
            _log(f"[DB] ❌ insert_trade: {e}")
            return None

    def update_exit(
        self,
        trade_id:   str | None,
        exit_type:  str,
        exit_price: float,
        exit_qty:   float,
        net_pnl:    float,
        net_pnl_pct: float,
        total_fees: float,
        duration_sec: int,
        notes:      str = "",
    ):
        """يُحدِّث السجل عند الخروج (TP أو SL)."""
        if not self._enabled or not trade_id:
            return
        try:
            conn = self._get_conn()
            cur  = conn.cursor()
            cur.execute(
                """
                UPDATE trades SET
                    closed_at     = NOW(),
                    exit_type     = %s,
                    exit_price    = %s,
                    exit_qty      = %s,
                    net_pnl_usd   = %s,
                    net_pnl_pct   = %s,
                    total_fees    = %s,
                    duration_sec  = %s,
                    notes         = %s
                WHERE id = %s
                """,
                (
                    exit_type, exit_price, exit_qty,
                    net_pnl, net_pnl_pct, total_fees,
                    duration_sec, notes, trade_id,
                ),
            )
            conn.commit()
            cur.close()
            conn.close()
            _log(f"[DB] ✅ تحديث خروج: {exit_type} | PnL={net_pnl:+.3f}")
        except Exception as e:
            _log(f"[DB] ❌ update_exit: {e}")

    def get_monthly_pnl(self) -> dict:
        """إجمالي أرباح الشهر الحالي من Supabase."""
        if not self._enabled:
            return {"total_pnl": 0.0, "trades": 0, "wins": 0, "losses": 0}
        try:
            conn = self._get_conn()
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT * FROM current_month_summary LIMIT 1")
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                return {
                    "total_pnl": float(row["total_pnl_usd"] or 0),
                    "trades":    int(row["total_trades"] or 0),
                    "wins":      int(row["winning_trades"] or 0),
                    "losses":    int(row["losing_trades"] or 0),
                }
        except Exception as e:
            _log(f"[DB] ❌ get_monthly_pnl: {e}")
        return {"total_pnl": 0.0, "trades": 0, "wins": 0, "losses": 0}


MEXC_HEADER = "\U0001f7e6 <b>\u0627\u0644\u0645\u0646\u0635\u0629: MEXC</b>\n"


# ─────────────────────────────────────────────
# 2. SLOT MANAGER
# ─────────────────────────────────────────────
@dataclass
class SlotState:
    symbol:               str
    buy_order_id:         str
    tp1_order_id:         str   = ""
    sl_order_id:          str   = ""
    entry_price:          float = 0.0
    filled_qty:           float = 0.0
    tp1:                  float = 0.0
    tp2:                  float = 0.0
    tp3:                  float = 0.0
    stop_loss:            float = 0.0
    entry_fee:            float = 0.0   # Taker 0.1% on market buy
    # Quantity split: 30% TP1 (exchange), 20% TP2 (shadow), 20% TP3 (shadow), 30% SL
    qty_tp1:              float = 0.0
    qty_tp2:              float = 0.0
    qty_tp3:              float = 0.0
    # Lifecycle flags
    tp1_filled:           bool  = False
    tp2_filled:           bool  = False
    tp3_filled:           bool  = False
    entry_time:           float = field(default_factory=time.time)
    opened_at:            float = field(default_factory=time.time)
    break_even_attempted: bool  = False
    extended:             bool  = False
    db_trade_id:          str   = ""  # Supabase UUID


class SlotManager:
    def __init__(self, cfg: Config):
        self.cfg    = cfg
        self._lock  = threading.Lock()
        self._slots: dict[str, SlotState] = {}

    @property
    def used(self) -> int:
        with self._lock:
            return len(self._slots)

    def is_vacant(self, symbol: str) -> bool:
        with self._lock:
            return symbol not in self._slots and len(self._slots) < self.cfg.max_slots

    def occupy(self, state: SlotState):
        with self._lock:
            self._slots[state.symbol] = state
        _log(f"[Slot] OCCUPIED: {state.symbol} | {len(self._slots)}/{self.cfg.max_slots}")

    def release(self, symbol: str):
        with self._lock:
            self._slots.pop(symbol, None)
        _log(f"[Slot] VACANT: {symbol} | {len(self._slots)}/{self.cfg.max_slots}")

    def get_all_states(self) -> list[SlotState]:
        with self._lock:
            return list(self._slots.values())

    def get_state(self, symbol: str) -> Optional[SlotState]:
        with self._lock:
            return self._slots.get(symbol)

    def update_state(self, symbol: str, **kwargs):
        with self._lock:
            state = self._slots.get(symbol)
            if state:
                for k, v in kwargs.items():
                    object.__setattr__(state, k, v)


# ─────────────────────────────────────────────
# 3. DATA PIPELINE — CMC + CoinGecko + LunarCrush + RSS
# ─────────────────────────────────────────────
class DataPipeline:
    _cmc_cache:      dict = {}
    _cmc_bulk_cache: dict = {}   # {"ts": ..., "data": {COIN: {volume_24h, rank}}}
    _lunar_cache:    dict = {}
    _gecko_cache:    dict = {}
    _CACHE_TTL:      float = 3600.0
    _CMC_BULK_TTL:   float = 1800.0  # نصف ساعة — توازن بين دقة البيانات وتوفير الحصة

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._cmc_bulk_lock = asyncio.Lock()

    def _cache_get(self, store: dict, key: str):
        e = store.get(key)
        if e and (time.time() - e["ts"]) < self._CACHE_TTL:
            return e["data"]
        return None

    def _cache_set(self, store: dict, key: str, data):
        store[key] = {"ts": time.time(), "data": data}

    async def _fetch_cmc_bulk(self, session: aiohttp.ClientSession) -> dict:
        """
        استدعاء واحد فقط يجلب أعلى 500 عملة دفعة واحدة، بدل استدعاء
        منفصل لكل عملة. هذا يوفّر حصة CMC الشهرية بشكل كبير.

        إصلاح حرج: بدون قفل (lock)، عندما تُفحص عدة عملات بالتوازي
        (asyncio.gather على batch) والكاش فارغ، كل عملة تستدعي هذه
        الدالة في نفس اللحظة بالضبط — فيصل عشرات الطلبات لـ CMC API
        دفعة واحدة، فيرفضها CMC بـ HTTP 429 (Too Many Requests) قبل
        أن يكتمل أي طلب وتُعبأ نتيجته في الكاش. النتيجة: رفض كل
        العملات بشكل متكرر دون أن ينجح أي استدعاء أبداً.

        الحل: قفل asyncio.Lock يضمن أن استدعاء واحد فقط يصل CMC API
        فعلياً؛ كل الاستدعاءات الأخرى المتزامنة تنتظر حتى يكتمل الأول
        وتُملأ نتيجته في الكاش، ثم تقرأ منه مباشرة بدلاً من تكرار الطلب.
        """
        cached = self._cmc_bulk_cache.get("data")
        if cached is not None and (time.time() - self._cmc_bulk_cache.get("ts", 0)) < self._CMC_BULK_TTL:
            return cached

        if not self.cfg.cmc_api_key:
            return {}

        async with self._cmc_bulk_lock:
            # إعادة الفحص بعد الحصول على القفل — قد يكون طلب آخر
            # (كان ينتظر القفل) قد أكمل التحديث بالفعل أثناء الانتظار
            cached = self._cmc_bulk_cache.get("data")
            if cached is not None and (time.time() - self._cmc_bulk_cache.get("ts", 0)) < self._CMC_BULK_TTL:
                return cached

            for attempt in range(3):
                try:
                    async with session.get(
                        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
                        headers={"X-CMC_PRO_API_KEY": self.cfg.cmc_api_key},
                        params={"start": "1", "limit": "500", "convert": "USD"},
                        timeout=aiohttp.ClientTimeout(total=15),
                    ) as resp:
                        if resp.status == 429:
                            wait = 2 ** (attempt + 1)  # 2s, 4s, 8s
                            _log(f"[CMC Bulk] HTTP 429 — إعادة محاولة بعد {wait}s ({attempt+1}/3)")
                            await asyncio.sleep(wait)
                            continue

                        if resp.status != 200:
                            _log(f"[CMC Bulk] HTTP {resp.status} — استخدام الكاش القديم إن وجد")
                            return self._cmc_bulk_cache.get("data", {})

                        payload = await resp.json()
                        result  = {}
                        for entry in payload.get("data", []):
                            coin  = entry.get("symbol", "").upper()
                            quote = entry.get("quote", {}).get("USD", {})
                            result[coin] = {
                                "volume_24h": float(quote.get("volume_24h", 0)),
                                "rank":       int(entry.get("cmc_rank", 9999)),
                                "valid":      True,
                            }

                        self._cmc_bulk_cache["ts"]   = time.time()
                        self._cmc_bulk_cache["data"] = result
                        _log(f"[CMC Bulk] ✅ تحديث — {len(result)} عملة (استدعاء واحد فقط)")
                        return result

                except Exception as e:
                    _log(f"[CMC Bulk] محاولة {attempt+1} فشلت: {str(e)[:60]}")
                    await asyncio.sleep(1)

            # كل المحاولات الثلاث فشلت (429 متكرر أو خطأ آخر) — استخدام
            # الكاش القديم إن وُجد، أو قائمة فارغة (يرفض كل العملات
            # بأمان عبر fail-safe بدل قبولها بدون تحقق)
            _log("[CMC Bulk] فشلت 3 محاولات — استخدام الكاش القديم إن وجد")
            return self._cmc_bulk_cache.get("data", {})

    async def get_cmc_data(self, session: aiohttp.ClientSession, symbol: str) -> dict:
        """
        Returns {volume_24h, rank, valid}.

        يقرأ من الكاش الجماعي (bulk) المُحدَّث كل 30 دقيقة بدل استدعاء
        API منفصل لكل عملة. هذا يوفّر الحصة الشهرية بنسبة كبيرة جداً.

        FAIL-SAFE: عملة غير موجودة في القائمة الجماعية (خارج Top 500
        فعلياً) أو فشل الجلب بالكامل → valid=False → تُرفض، بدل قبولها
        تلقائياً كما كان يحدث سابقاً.
        """
        coin = symbol.split("/")[0].split("_")[0].upper()

        if not self.cfg.cmc_api_key:
            return {"volume_24h": 0.0, "rank": 9999, "valid": False}

        bulk_data = await self._fetch_cmc_bulk(session)
        if coin in bulk_data:
            return bulk_data[coin]

        # العملة غير موجودة في Top 500 — رفض مباشر وموثوق
        return {"volume_24h": 0.0, "rank": 9999, "valid": False}

    async def get_lunar_score(self, session: aiohttp.ClientSession, symbol: str) -> dict:
        """Returns {galaxy_score, social_volume, vote}."""
        coin   = symbol.split("/")[0].split("_")[0].lower()
        cached = self._cache_get(self._lunar_cache, coin)
        if cached is not None:
            return cached

        if not self.cfg.lunar_api_key:
            return {"galaxy_score": 50, "social_volume": 1000, "vote": "neutral"}

        try:
            async with session.get(
                f"https://lunarcrush.com/api4/public/coins/{coin}/v1",
                headers={"Authorization": f"Bearer {self.cfg.lunar_api_key}"},
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                if resp.status != 200:
                    return {"galaxy_score": 50, "social_volume": 1000, "vote": "neutral"}
                data         = (await resp.json()).get("data", {})
                galaxy_score = float(data.get("galaxy_score",   50))
                social_vol   = int(data.get("interactions_24h", 0))
                sentiment    = float(data.get("sentiment",       3))
                if galaxy_score <= 20 or sentiment <= 1.5:
                    vote = "reject"
                elif galaxy_score >= 60 and sentiment >= 4:
                    vote = "approve"
                else:
                    vote = "neutral"
                result = {"galaxy_score": galaxy_score, "social_volume": social_vol, "vote": vote}
                self._cache_set(self._lunar_cache, coin, result)
                return result
        except Exception:
            return {"galaxy_score": 50, "social_volume": 1000, "vote": "neutral"}

    async def get_rss_sentiment(self, session: aiohttp.ClientSession) -> str:
        """
        نظام أخبار متعدد المصادر — يستبدل الاعتماد على CoinDesk فقط
        بمزيج من 3 مصادر RSS موثوقة معاً، لتقليل التحيز لمصدر واحد
        وزيادة دقة قراءة المزاج العام للسوق.

        Returns 'bullish' | 'bearish' | 'neutral'.
        """
        sources = [
            "https://www.coindesk.com/arc/outboundfeeds/rss/",
            "https://cointelegraph.com/rss",
            "https://decrypt.co/feed",
        ]

        bearish_kw = ["crash", "ban", "hack", "liquidat", "regulation", "lawsuit",
                      "fear", "dump", "plunge", "collapse", "crisis", "recession",
                      "exploit", "rug pull", "investigation", "sec sues", "delist"]
        bullish_kw = ["rally", "surge", "bull", "adoption", "etf", "institutional",
                      "breakout", "all-time high", "accumulate", "upgrade",
                      "partnership", "integration", "approval", "inflow"]

        total_bear = 0
        total_bull = 0
        fetched    = 0

        for url in sources:
            try:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=6)
                ) as resp:
                    if resp.status != 200:
                        continue
                    text = (await resp.text()).lower()
                    total_bear += sum(text.count(w) for w in bearish_kw)
                    total_bull += sum(text.count(w) for w in bullish_kw)
                    fetched += 1
            except Exception:
                continue

        if fetched == 0:
            # فشل كل المصادر — fail-safe: محايد، لا يرفض ولا يوافق بقوة
            _log("[RSS Multi-Source] كل المصادر فشلت — neutral (fail-safe)")
            return "neutral"

        if total_bear > total_bull + 5:
            return "bearish"
        elif total_bull > total_bear + 2:
            return "bullish"
        return "neutral"

    async def get_whale_activity(self, session: aiohttp.ClientSession, symbol: str) -> dict:
        """
        يفحص حركات المحافظ الضخمة (Whale Movements) لعملة محددة عبر
        Whale Alert API. هذا يكشف بيع/شراء ضخم قد يسبق حركة سعرية
        كبيرة — مؤشر مبكر لا تعكسه مؤشرات RSI أو فيبوناتشي بعد.

        Returns {"whale_alert": "sell"|"buy"|"none", "transactions": int}
        يتطلب WHALE_ALERT_API_KEY — في غيابه يُعاد "none" بأمان (fail-safe).
        """
        if not self.cfg.whale_alert_api_key:
            return {"whale_alert": "none", "transactions": 0}

        coin = symbol.split("/")[0].split("_")[0].lower()
        try:
            async with session.get(
                "https://api.whale-alert.io/v1/transactions",
                params={
                    "api_key": self.cfg.whale_alert_api_key,
                    "currency": coin,
                    "min_value": "500000",
                    "limit": "10",
                },
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                if resp.status != 200:
                    return {"whale_alert": "none", "transactions": 0}
                data = await resp.json()
                txs  = data.get("transactions", [])
                if not txs:
                    return {"whale_alert": "none", "transactions": 0}

                # تحويلات لمنصات تداول (exchange) تشير غالباً لنية بيع
                to_exchange   = sum(1 for t in txs if t.get("to", {}).get("owner_type") == "exchange")
                from_exchange = sum(1 for t in txs if t.get("from", {}).get("owner_type") == "exchange")

                if to_exchange > from_exchange:
                    signal = "sell"   # حيتان تنقل لمنصات — احتمال بيع قادم
                elif from_exchange > to_exchange:
                    signal = "buy"    # حيتان تسحب من منصات — احتمال تجميع/شراء
                else:
                    signal = "none"

                return {"whale_alert": signal, "transactions": len(txs)}
        except Exception as e:
            _log(f"[Whale Alert] {coin}: {str(e)[:60]} — none (fail-safe)")
            return {"whale_alert": "none", "transactions": 0}

    async def layer1_pass(
        self,
        session: aiohttp.ClientSession,
        symbol:  str,
        rsi:     float,
    ) -> tuple[bool, str]:
        """RSI + CMC rank/volume + LunarCrush in parallel."""
        if rsi > self.cfg.rsi_threshold:
            return False, f"RSI={rsi:.1f} > {self.cfg.rsi_threshold}"

        cmc_task   = self.get_cmc_data(session, symbol)
        lunar_task = self.get_lunar_score(session, symbol)
        cmc, lunar = await asyncio.gather(cmc_task, lunar_task)

        # ── Fail-Safe: بيانات CMC غير صالحة (مفتاح معطل/فشل شبكة) ──
        # نرفض العملة بدل المرور التلقائي الذي كان يحدث سابقاً عبر
        # قيمة افتراضية وهمية (rank=1) — هذا كان يُفرغ الفلتر من قيمته
        if not cmc.get("valid", False):
            return False, "CMC غير متاح — رفض احتياطي (fail-safe)"

        if cmc["volume_24h"] < self.cfg.min_volume_usd:
            return False, f"CMC Vol ${cmc['volume_24h']/1e6:.1f}M < min"

        if cmc["rank"] > self.cfg.cmc_top_rank:
            return False, f"CMC Rank #{cmc['rank']} > Top {self.cfg.cmc_top_rank}"

        if lunar["vote"] == "reject":
            return False, f"LunarCrush reject (Galaxy={lunar['galaxy_score']:.0f})"

        return True, (
            f"RSI={rsi:.1f} ✅ Vol=${cmc['volume_24h']/1e6:.1f}M "
            f"Rank=#{cmc['rank']} Galaxy={lunar['galaxy_score']:.0f}"
        )


# ─────────────────────────────────────────────
# 4. FIBONACCI ENGINE
# ─────────────────────────────────────────────
def calculate_cascading_targets(fib_high: float, fib_low: float, entry: float) -> dict:
    """
    Cascading Fibonacci targets — تضمن entry < tp1 < tp2 < tp3.

    إصلاح جذري (الإصدار النهائي): المشكلة الأصلية لم تكن فقط في الكاب
    الصارم (15%)، بل في تسلسل الاعتماد بين الأهداف (tp2 = max(tp2_c,
    tp1 × 1.03)) — بما أن tp1 النهائي يُثبَّت غالباً عند +5% (floor
    مطلوب ومتعمد لاحقاً في execute_full_trade)، فإن أي ربط لـ tp2/tp3
    بقيمة tp1 يُسقط تنوعهما الديناميكي معه بالتسلسل، فتظهر كل الأهداف
    شبه ثابتة دائماً (5% / 8.2% / ...) بغض النظر عن حركة العملة الفعلية.

    الحل: كل هدف يُحسب من فيبوناتشي الخاص به فقط، بدون اعتماد على
    الهدف الذي قبله. الترتيب الصحيح (tp1 < tp2 < tp3) يُفرض فقط في
    أضيق الحالات الاستثنائية (تقاطع نادر)، لا كقاعدة عامة تُطبَّق دائماً.
    """
    fib_range = fib_high - fib_low
    if fib_range > 0 and fib_high > entry:
        tp1_c = fib_low + fib_range * 0.382
        tp2_c = fib_low + fib_range * 0.500
        tp3_c = fib_low + fib_range * 0.618
    else:
        tp1_c = entry * 1.05
        tp2_c = entry * 1.10
        tp3_c = entry * 1.18

    hard_cap = entry * 1.45  # سقف مطلق واسع يمنع أهدافاً غير واقعية فقط

    # كل هدف مستقل تماماً عن الآخر — لا تسلسل اعتماد بينها
    tp1 = min(max(tp1_c, entry * 1.001), hard_cap)
    tp2 = min(max(tp2_c, entry * 1.001), hard_cap)
    tp3 = min(max(tp3_c, entry * 1.001), hard_cap)

    # فرض الترتيب الصحيح فقط عند التقاطع الفعلي (نادر إحصائياً)، بفجوة
    # دنيا 1% بين كل هدف والذي يليه، دون كسر القيم الديناميكية السليمة
    if tp2 <= tp1:
        tp2 = tp1 * 1.01
    if tp3 <= tp2:
        tp3 = tp2 * 1.01

    return {"tp1": round(tp1, 10), "tp2": round(tp2, 10), "tp3": round(tp3, 10)}


# ─────────────────────────────────────────────
# 5. DYNAMIC SL — pure 15m swing low, no AI
# ─────────────────────────────────────────────
def detect_horizontal_support(df: pd.DataFrame, current_price: float, tolerance_pct: float = 0.015) -> dict:
    """
    يكشف مستويات الدعم الأفقي الحقيقية — نقاط سعرية لمسها السعر
    عدة مرات وارتد منها صعوداً خلال آخر 90 يوماً.

    الفكرة (Confluence): صفقة يتوافق فيها RSI + فيبوناتشي + دعم أفقي
    تاريخي معاً أقوى من صفقة تعتمد على مؤشر واحد فقط. هذا لا يستبدل
    فيبوناتشي، بل يضيف تأكيداً مستقلاً عليه.

    الطريقة: نجمع كل القيعان المحلية (swing lows) في النافذة، ثم
    نتحقق هل القاع الحالي يقع ضمن "كتلة" من قيعان سابقة متقاربة
    (بتفاوت tolerance_pct) — إن وُجدت ≥ 2 لمسات سابقة، فهذا دعم
    حقيقي مؤكَّد إحصائياً، لا نقطة عشوائية.
    """
    try:
        lows = df["low"].tail(90).values
        if len(lows) < 10:
            return {"has_support": False, "touches": 0, "support_level": 0.0}

        # القيعان المحلية فقط (swing lows) — تجنّب الضجيج
        swing_lows = [
            lows[i] for i in range(2, len(lows) - 2)
            if lows[i] < lows[i-1] and lows[i] < lows[i-2]
            and lows[i] < lows[i+1] and lows[i] < lows[i+2]
        ]
        if not swing_lows:
            return {"has_support": False, "touches": 0, "support_level": 0.0}

        # تجميع القيعان القريبة من السعر الحالي (ضمن tolerance) لمعرفة
        # كم مرة "لمس" السعر هذا المستوى تقريباً
        nearby = [
            low for low in swing_lows
            if abs(low - current_price) / current_price <= tolerance_pct
        ]

        touches = len(nearby)
        avg_level = sum(nearby) / touches if touches > 0 else 0.0

        # دعم "حقيقي" يتطلب لمستين سابقتين على الأقل (وليس مجرد نقطة عابرة)
        return {
            "has_support":   touches >= 2,
            "touches":       touches,
            "support_level": avg_level,
        }
    except Exception:
        return {"has_support": False, "touches": 0, "support_level": 0.0}



    """
    Stop-loss يعتمد على آخر swing low حقيقي على فريم 15 دقيقة،
    بدل قيمة ثابتة 5% للجميع. القيمة الثابتة كانت تُضرب بسرعة في
    عملات متقلبة لأن SL كان قريباً جداً من سعر الدخول، مما يقطع
    الصفقة قبل أن تصل لـ TP1 بفرصة كافية.

    النطاق المسموح الآن: -4% إلى -8% (بدل تثبيت دقيق عند -5%)
    """
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe="15m", limit=48)
        if not ohlcv or len(ohlcv) < 5:
            raise ValueError("insufficient candles")
        df   = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"]).astype(float)
        lows = df["low"].values
        swing_lows = [
            lows[i] for i in range(2, len(lows) - 2)
            if lows[i] < lows[i-1] and lows[i] < lows[i-2]
            and lows[i] < lows[i+1] and lows[i] < lows[i+2]
        ]
        local_low = min(swing_lows[-3:]) if swing_lows else float(lows[-5:].min())
        sl = local_low * 0.998
    except Exception as e:
        _log(f"[SL Calc] {symbol} fallback 6%: {e}")
        sl = entry_price * 0.94

    # نطاق مرن: لا أقرب من -4% (يمنع الضرب السريع)، لا أبعد من -8% (يحدّ الخسارة القصوى)
    sl = max(entry_price * 0.92, min(sl, entry_price * 0.96))
    return sl




def calculate_micro_swing_sl(exchange, symbol: str, entry_price: float) -> float:
    """
    Stop-loss يعتمد على آخر swing low حقيقي على فريم 15 دقيقة،
    بدل قيمة ثابتة للجميع.
    النطاق المسموح: -4% إلى -8% من سعر الدخول.
    """
    try:
        import pandas as pd
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe="15m", limit=48)
        if not ohlcv or len(ohlcv) < 5:
            raise ValueError("insufficient candles")
        df   = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"]).astype(float)
        lows = df["low"].values
        swing_lows = [
            lows[i] for i in range(2, len(lows) - 2)
            if lows[i] < lows[i-1] and lows[i] < lows[i-2]
            and lows[i] < lows[i+1] and lows[i] < lows[i+2]
        ]
        local_low = min(swing_lows[-3:]) if swing_lows else float(lows[-5:].min())
        sl = local_low * 0.998
    except Exception as e:
        _log(f"[SL Calc] {symbol} fallback 6%: {e}")
        sl = entry_price * 0.94

    # نطاق مرن: لا أقرب من -4%، لا أبعد من -8%
    sl = max(entry_price * 0.92, min(sl, entry_price * 0.96))
    return sl

# ─────────────────────────────────────────────
# 6. CONSENSUS COMMITTEE — DeepSeek + Llama-3.3 unanimous
# ─────────────────────────────────────────────
class ConsensusCommittee:
    """
    Two-agent unanimous vote required:
      DeepSeek  — technical chart evaluation (RSI + Bollinger)
      Llama-3.3 — macro news sentiment + LunarCrush social layer
    If either returns SKIP → trade is blocked instantly.
    """

    DS_SYSTEM = (
        "You are a crypto technical analyst. Evaluate the oversold setup strictly. "
        "Primary signal: RSI < 30 (oversold). Secondary confirmation: price near or below lower Bollinger Band is a plus but not required. "
        "If RSI is clearly oversold (below 25), lean BUY unless there is a strong reason not to. "
        "Respond with exactly one word on the last line: BUY or SKIP."
    )
    LLAMA_SYSTEM = (
        "You are a macro sentiment analyst for crypto markets. "
        "Evaluate news sentiment and social engagement data. "
        "Respond with exactly one word on the last line: BUY or SKIP."
    )

    def __init__(self, cfg: Config):
        self.cfg = cfg

    async def _call(
        self,
        session:  aiohttp.ClientSession,
        api_key:  str,
        base_url: str,
        model:    str,
        system:   str,
        user_msg: str,
        label:    str,
    ) -> str:
        # ── FAIL-SAFE: عدم توفر مفتاح أو فشل API يجب أن يمنع الصفقة ──
        # (سابقاً كان يُعيد "BUY" تلقائياً عند أي فشل — هذا كان يسمح
        # بدخول صفقات بدون أي تحليل فعلي من Committee، وهو سبب جذري
        # محتمل لارتفاع نسبة صفقات SL)
        if not api_key:
            _log(f"[{label}] مفتاح API غير مُعرَّف — SKIP (fail-safe)")
            return "SKIP"
        try:
            async with session.post(
                f"{base_url}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}",
                         "Content-Type":  "application/json"},
                json={
                    "model":       model,
                    "max_tokens":  self.cfg.max_ai_tokens,
                    "temperature": 0.1,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user",   "content": user_msg},
                    ],
                },
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status == 429:
                    _log(f"[{label}] 429 Rate Limit — SKIP (fail-safe)")
                    return "SKIP"
                if resp.status != 200:
                    _log(f"[{label}] HTTP {resp.status} — SKIP (fail-safe)")
                    return "SKIP"
                data = await resp.json()
                return data["choices"][0]["message"]["content"] or ""
        except asyncio.TimeoutError:
            _log(f"[{label}] Timeout — SKIP (fail-safe)")
            return "SKIP"
        except Exception as e:
            _log(f"[{label}] {str(e)[:60]} — SKIP (fail-safe)")
            return "SKIP"

    def _verdict(self, text: str) -> str:
        if not text:
            return "SKIP"
        last = text.strip().split("\n")[-1].strip().upper()
        if last in ("BUY", "SKIP", "HOLD"):
            return "BUY" if last == "BUY" else "SKIP"
        return "BUY" if "BUY" in text.upper() else "SKIP"

    async def run(
        self,
        symbol:        str,
        rsi:           float,
        vol_m:         float,
        entry:         float,
        fib_high:      float,
        fib_low:       float,
        rss_sentiment: str,
        lunar_data:    dict,
        support:       dict = None,
        whale_data:    dict = None,
    ) -> dict:
        start = time.time()
        support = support or {"has_support": False, "touches": 0, "support_level": 0.0}
        whale_data = whale_data or {"whale_alert": "none", "transactions": 0}

        support_line = (
            f"Horizontal Support: {support['touches']} historical touches near "
            f"{support['support_level']:.8g} — "
            f"{'CONFIRMED (strong confluence)' if support['has_support'] else 'none detected'}"
        )

        ds_msg = (
            f"Symbol: {symbol} | RSI: {rsi:.1f} | Entry: {entry:.8g}\n"
            f"Local High: {fib_high:.8g} | Local Low: {fib_low:.8g}\n"
            f"Volume 24h: ${vol_m:.1f}M\n"
            f"{support_line}\n"
            f"Evaluate technical oversold setup, weighting confirmed horizontal "
            f"support as a positive confluence factor if present. "
            f"Last line: BUY or SKIP"
        )
        whale_line = (
            f"Whale Activity: {whale_data['transactions']} large transactions detected, "
            f"signal={whale_data['whale_alert']} "
            f"({'large transfers TO exchanges — possible sell pressure' if whale_data['whale_alert']=='sell' else 'large transfers FROM exchanges — possible accumulation' if whale_data['whale_alert']=='buy' else 'no significant whale signal'})"
        )
        llama_msg = (
            f"Symbol: {symbol} | RSI: {rsi:.1f}\n"
            f"Macro RSS Sentiment: {rss_sentiment}\n"
            f"LunarCrush Galaxy Score: {lunar_data.get('galaxy_score', 50):.0f}\n"
            f"Social Volume 24h: {lunar_data.get('social_volume', 0)}\n"
            f"{whale_line}\n"
            f"Is macro environment safe for a scalp entry? Whale 'sell' signal should "
            f"weigh negatively. Last line: BUY or SKIP"
        )

        async with aiohttp.ClientSession() as session:
            ds_coro    = self._call(session, self.cfg.deepseek_api_key,
                                    "https://api.deepseek.com/v1",
                                    self.cfg.deepseek_model,
                                    self.DS_SYSTEM, ds_msg, "DeepSeek")
            llama_coro = self._call(session, self.cfg.together_api_key,
                                    "https://api.together.xyz/v1",
                                    self.cfg.together_model,
                                    self.LLAMA_SYSTEM, llama_msg, "Llama")
            ds_text, llama_text = await asyncio.gather(ds_coro, llama_coro)

        ds_vote    = self._verdict(ds_text)
        llama_vote = self._verdict(llama_text)

        # UNANIMOUS required — any SKIP kills the trade
        approved = (ds_vote == "BUY" and llama_vote == "BUY")
        elapsed  = time.time() - start

        _log(
            f"[Committee] {symbol}: DeepSeek={ds_vote} Llama={llama_vote} "
            f"→ {'✅ APPROVED' if approved else '❌ BLOCKED'} | {elapsed:.1f}s"
        )

        targets = calculate_cascading_targets(fib_high, fib_low, entry)
        return {
            "approved":  approved,
            "ds_vote":   ds_vote,
            "llama_vote": llama_vote,
            "targets":   targets,
            "elapsed":   round(elapsed, 2),
        }


# ─────────────────────────────────────────────
# 7. HIGH SPEED EXECUTOR
# ─────────────────────────────────────────────
# Scaled Exit Split:
# TP1 = 20% exchange limit (partial profit lock)
# TP2 = 40% shadow (50% of remaining 80%)
# TP3 = 40% shadow (remaining 100% of what's left)
TP1_QTY_PCT = 0.20
TP2_QTY_PCT = 0.40
TP3_QTY_PCT = 0.40


class HighSpeedExecutor:

    def __init__(self, cfg: Config):
        self.cfg      = cfg
        self.exchange = self._connect()

    def _connect(self) -> ccxt.mexc:
        ex = ccxt.mexc({
            "apiKey":  self.cfg.mexc_api_key,
            "secret":  self.cfg.mexc_api_secret,
            "options": {
                "defaultType":                       "spot",
                "fetchMarkets":                      ["spot"],
                "createMarketBuyOrderRequiresPrice": False,
            },
            "enableRateLimit": True,
            "timeout":         60_000,
        })
        ex.load_markets()
        _log(f"✅ Executor connected — {len(ex.markets)} markets loaded")
        return ex

    def _ensure_markets(self):
        if not self.exchange.markets:
            self.exchange.load_markets()

    def _live_price(self, symbol: str, fallback: float) -> float:
        try:
            t = self.exchange.fetch_ticker(symbol)
            p = float(t.get("last") or t.get("close") or 0)
            return p if p > 0 else fallback
        except Exception as e:
            _log(f"[Executor] live_price fallback {symbol}: {e}")
            return fallback

    def _apply_step_size(self, symbol: str, qty: float) -> float:
        try:
            mkt       = self.exchange.market(symbol)
            precision = mkt.get("precision", {}).get("amount", 4)
            if isinstance(precision, int):
                factor = 10 ** precision
                return math.floor(qty * factor) / factor
            elif isinstance(precision, float) and precision > 0:
                return math.floor(qty / precision) * precision
        except Exception:
            pass
        return round(qty, 4)

    def market_buy(self, symbol: str, entry_price: float) -> Optional[dict]:
        capital    = self.cfg.capital
        live_price = self._live_price(symbol, entry_price)
        if not live_price or live_price <= 0:
            _log(f"[Executor] {symbol}: live price = 0 — abort")
            return None

        _log(f"[Executor] BUY {symbol} | signal={entry_price:.8g} live={live_price:.8g} ${capital:.2f}")
        try:
            self._ensure_markets()
            # Apply exchange precision to capital amount
            try:
                precise_capital = float(self.exchange.cost_to_precision(symbol, capital))
            except Exception:
                precise_capital = capital
            order = self.exchange.create_market_buy_order(
                symbol, precise_capital, {"quoteOrderQty": precise_capital}
            )
            filled_price = float(order.get("average") or order.get("price") or live_price)
            filled_qty   = float(order.get("filled") or (capital / filled_price))
            _log(f"✅ FILLED {symbol}: {filled_qty:.6f} @ {filled_price:.8g} ID:{order['id']}")
            return {"order_id": order["id"], "filled_price": filled_price, "filled_qty": filled_qty}
        except ccxt.InsufficientFunds as e:
            _log(f"[Executor] InsufficientFunds {symbol}: {e}")
        except ccxt.NetworkError as e:
            _log(f"[Executor] NetworkError {symbol}: {e} — retry next cycle")
        except ccxt.ExchangeError as e:
            _log(f"[Executor] ExchangeError {symbol}: {e}")
        except Exception as e:
            _log(f"[Executor] ERROR {symbol}: {e}")
        return None

    def place_tp_sl(
        self,
        symbol:       str,
        filled_qty:   float,
        filled_price: float,
        tp1:          float,
        stop_loss:    float,
    ) -> dict:
        """
        SHADOW SL ARCHITECTURE — eliminates double-booking (Insufficient Position):
        ──────────────────────────────────────────────────────────────────────────
        • TP1 Limit Sell → placed on exchange (locks full qty at target)
        • SL → NOT sent to exchange — stored in slot.stop_loss (virtual/shadow)
        • Monitor loop watches live price and triggers market sell if price ≤ SL

        Benefit: no concurrent TP+SL double-booking → zero code 30005/30087
        """
        ids: dict = {}

        # Scaled exit: 20% TP1 on exchange, 40% TP2 shadow, 40% TP3 shadow (= 100% total)
        qty_tp1 = self._apply_step_size(symbol, filled_qty * 0.20)
        qty_tp2 = self._apply_step_size(symbol, filled_qty * 0.40)
        qty_tp3 = self._apply_step_size(symbol, filled_qty * 0.40)
        ids["qty_tp1"] = qty_tp1
        ids["qty_tp2"] = qty_tp2
        ids["qty_tp3"] = qty_tp3

        # ── Post-buy settle: give MEXC time to credit tokens ──
        time.sleep(2)

        # ── TP1 Limit Sell — 30% qty only ──
        try:
            tp1_price = float(self.exchange.price_to_precision(symbol, tp1))
            o = self.exchange.create_limit_sell_order(symbol, qty_tp1, tp1_price)
            ids["tp1_order_id"] = o["id"]
            _log(f"✅ TP1 (30%): {tp1_price:.8g} ×{qty_tp1} ID:{o['id']}")
        except ccxt.NetworkError as e:
            _log(f"[TP1] NetworkError {symbol}: {e} — will retry on next reconcile")
        except ccxt.ExchangeError as e:
            err = str(e)
            if "30005" in err or "Oversold" in err or "oversold" in err:
                # TP exceeds MEXC deviation boundary — compress to +2.5%
                _log(f"[TP1 30005] {symbol}: compressing target to +2.5% from fill")
                try:
                    compressed_tp = float(self.exchange.price_to_precision(
                        symbol, filled_price * 1.025
                    ))
                    o = self.exchange.create_limit_sell_order(symbol, qty_tp1, compressed_tp)
                    ids["tp1_order_id"] = o["id"]
                    _log(f"✅ TP1 compressed (+2.5%): {compressed_tp:.8g} ×{full_qty} ID:{o['id']}")
                except ccxt.ExchangeError as e2:
                    err2 = str(e2)
                    if "30005" in err2 or "Oversold" in err2:
                        # Final fallback: +2.0%
                        _log(f"[TP1 30005] {symbol}: 2nd compression to +2.0%")
                        try:
                            final_tp = float(self.exchange.price_to_precision(
                                symbol, filled_price * 1.02
                            ))
                            o = self.exchange.create_limit_sell_order(symbol, qty_tp1, final_tp)
                            ids["tp1_order_id"] = o["id"]
                            _log(f"✅ TP1 final (+2.0%): {final_tp:.8g} ID:{o['id']}")
                        except Exception as e3:
                            _log(f"❌ TP1 all compressions failed {symbol}: {e3}")
                    else:
                        _log(f"❌ TP1 compressed FAILED {symbol}: {e2}")
                except Exception as e2:
                    _log(f"❌ TP1 compressed FAILED {symbol}: {e2}")
            elif "30087" in err:
                _log(f"[TP1 30087] {symbol}: price out of range — {err[:80]}")
            else:
                _log(f"❌ TP1 ExchangeError {symbol}: {err[:100]}")
        except Exception as e:
            _log(f"❌ TP1 FAILED {symbol}: {str(e)[:100]}")

        # SL is virtual — stored in slot, not sent to exchange
        _log(
            f"[Shadow SL] {symbol}: SL={stop_loss:.8g} (برمجائي صامت — "
            f"المراقب سيُنفّذ market sell إذا وصل السعر)"
        )
        return ids

    def re_place_tp(self, symbol: str, state: SlotState) -> dict:
        """
        Retry: re-place TP1 only if missing.
        SL is virtual — no exchange order needed.
        """
        ids: dict = {}
        if not state.tp1_order_id:
            try:
                full_qty  = self._apply_step_size(symbol, state.filled_qty)
                tp1_price = float(self.exchange.price_to_precision(symbol, state.tp1))
                o = self.exchange.create_limit_sell_order(symbol, full_qty, tp1_price)
                ids["tp1_order_id"] = o["id"]
                _log(f"[Retry] ✅ TP1 re-placed {symbol}: {tp1_price:.8g} ID:{o['id']}")
            except Exception as e:
                _log(f"[Retry] TP1 failed {symbol}: {e}")
        return ids

    def emergency_tp1_sell(self, symbol: str, qty: float, tp1: float) -> Optional[str]:
        """Last-resort: single limit sell at TP1 after retry exhaustion."""
        try:
            full_qty  = self._apply_step_size(symbol, qty)
            tp1_price = float(self.exchange.price_to_precision(symbol, tp1))
            o = self.exchange.create_limit_sell_order(symbol, full_qty, tp1_price)
            _log(f"[Fallback] ✅ Limit sell at TP1 {symbol}: {tp1_price:.8g} ID:{o['id']}")
            return o["id"]
        except Exception as e:
            _log(f"[Fallback] TP1 limit sell FAILED {symbol}: {e}")
            return None

    def emergency_market_sell(self, symbol: str, qty: float) -> bool:
        """Fee-adjusted emergency market sell using live free balance."""
        try:
            base_token = symbol.split("/")[0].split("_")[0]
            balance    = self.exchange.fetch_balance({"type": "spot"})
            free_qty   = float(
                balance.get(base_token, {}).get("free", 0) or
                balance.get("free", {}).get(base_token, 0)
            )
            _log(f"[Emergency] {symbol}: cached={qty:.4f} free={free_qty:.4f}")

            if free_qty <= 0:
                _log(f"[Emergency] {symbol}: free=0 — slot released")
                return True

            sell_qty = self._apply_step_size(symbol, min(qty, free_qty))
            if sell_qty <= 0:
                return True

            try:
                precise_qty = float(self.exchange.amount_to_precision(symbol, sell_qty))
            except Exception:
                precise_qty = sell_qty

            o = self.exchange.create_market_sell_order(symbol, precise_qty)
            _log(f"[Emergency] ✅ {symbol}: sold {precise_qty} @ market ID:{o['id']}")
            return True
        except Exception as e:
            err = str(e)
            if "30005" in err or "Oversold" in err:
                _log(f"[Emergency] {symbol}: 30005 — releasing slot")
                return True
            _log(f"[Emergency] ❌ {symbol}: {err[:120]}")
            return False

    def cancel_order(self, symbol: str, order_id: str):
        try:
            self.exchange.cancel_order(order_id, symbol)
        except Exception as e:
            _log(f"[Executor] Cancel {order_id} failed: {e}")

    def fetch_order_status(self, symbol: str, order_id: str) -> str:
        try:
            o = self.exchange.fetch_order(order_id, symbol)
            return o.get("status", "unknown")
        except Exception:
            return "unknown"

    def execute_full_trade(
        self,
        symbol:      str,
        entry_price: float,
        tp1: float, tp2: float, tp3: float,
        stop_loss:   float,
    ) -> Optional[SlotState]:
        buy = self.market_buy(symbol, entry_price)
        if not buy:
            return None

        # ── Enforce minimum +5% floor on TP1 BEFORE placing the order ──
        # (سابقاً كان الأمر الفعلي على المنصة يُنفَّذ بـ tp1 الأصلي
        # قبل رفعه، فيُمكن أن يُنفَّذ بسعر أقل من +5% المقصود فعلياً)
        effective_tp1 = max(tp1, entry_price * 1.05)
        if effective_tp1 != tp1:
            _log(f"[Hybrid] {symbol}: TP1 lifted from {tp1:.8g} → {effective_tp1:.8g} (floor +5%)")

        # إعادة فحص ترتيب tp2/tp3 ضد القيمة النهائية المُصحَّحة لـ tp1
        # (يمنع كسر الترتيب الذي كان يحدث عند رفع tp1 بعد حساب tp2/tp3)
        effective_tp2 = tp2 if tp2 > effective_tp1 else effective_tp1 * 1.02
        effective_tp3 = tp3 if tp3 > effective_tp2 else effective_tp2 * 1.02

        bracket = {}
        try:
            bracket = self.place_tp_sl(
                symbol, buy["filled_qty"], buy["filled_price"], effective_tp1, stop_loss
            )
        except Exception as e:
            _log(f"[Executor] place_tp_sl error {symbol}: {e}")

        # Entry fee: 0.1% taker on market buy
        entry_fee = self.cfg.capital * 0.001

        return SlotState(
            symbol       = symbol,
            buy_order_id = buy["order_id"],
            tp1_order_id = bracket.get("tp1_order_id", ""),
            sl_order_id  = "",
            entry_price  = buy["filled_price"],
            filled_qty   = buy["filled_qty"],
            tp1          = effective_tp1,
            tp2          = effective_tp2,
            tp3          = effective_tp3,
            stop_loss    = stop_loss,
            entry_fee    = entry_fee,
            qty_tp1      = bracket.get("qty_tp1", buy["filled_qty"] * 0.30),
            qty_tp2      = bracket.get("qty_tp2", buy["filled_qty"] * 0.20),
            qty_tp3      = bracket.get("qty_tp3", buy["filled_qty"] * 0.20),
            entry_time   = time.time(),
        )


# ─────────────────────────────────────────────
# 8. TRADE MONITOR — polls bot order IDs only
# ─────────────────────────────────────────────
class TradeMonitor:

    def __init__(self, cfg: Config, executor: HighSpeedExecutor, slot_mgr: SlotManager, db: "TradeLogger | None" = None):
        self.cfg      = cfg
        self.executor = executor
        self.slots    = slot_mgr
        self.db       = db
        self._running = False

    async def start(self):
        self._running        = True
        self._reconcile_tick = 0
        _log("[TradeMonitor] ✅ started")
        while self._running:
            await self._check_all_slots()
            self._reconcile_tick += self.cfg.monitor_interval
            if self._reconcile_tick >= self.cfg.reconcile_interval:
                self._reconcile_tick = 0
                await self._reconcile_portfolio()
            await asyncio.sleep(self.cfg.monitor_interval)

    def stop(self):
        self._running = False

    async def _check_all_slots(self):
        for state in self.slots.get_all_states():
            await self._check_slot(state)

    async def _check_slot(self, state: SlotState):
        symbol = state.symbol
        loop   = asyncio.get_running_loop()

        # ── Fetch live price once — used for SL + shadow TP checks ──
        curr_price = 0.0
        try:
            ticker     = await loop.run_in_executor(
                None, self.executor.exchange.fetch_ticker, symbol
            )
            curr_price = float(ticker.get("last") or ticker.get("close") or 0)
        except Exception as e:
            err = str(e)
            # ── Delisted symbol detection — code -1121 "invalid symbol" ──
            # المنصة حذفت الزوج بالكامل؛ لا يمكن جلب سعر أو بيع. تحرير الـ
            # slot فوراً مع تنبيه واحد فقط يمنع التكرار اللانهائي.
            if "-1121" in err or "invalid symbol" in err.lower():
                _log(
                    f"[Delisted] 🚫 {symbol}: الزوج غير موجود على المنصة "
                    f"(تم شطبه/حذفه) — تحرير الـ slot نهائياً"
                )
                self.slots.release(symbol)
                await self._notify(
                    "🚫 <b>عملة محذوفة من المنصة</b>\n\n"
                    f"• <b>العملة:</b> <code>{symbol}</code>\n"
                    "• <b>السبب:</b> الزوج غير متوفر على MEXC (delisted) — "
                    "لا يمكن جلب السعر أو البيع تلقائياً\n\n"
                    "<i>تحقق يدوياً من حساب MEXC إذا كان هناك رصيد متبقٍ "
                    "من هذه العملة وتصرف معه حسب الحاجة. تم تحرير الـ slot "
                    "ولن يُعاد التنبيه لهذه الصفقة.</i>"
                )
                return
            _log(f"[Monitor] price fetch failed {symbol}: {e}")
            return

        if curr_price <= 0:
            return

        # ── Shadow SL Monitor ──
        if state.stop_loss > 0 and curr_price <= state.stop_loss:
            _log(
                f"[Shadow SL] 🔻 {symbol}: curr={curr_price:.8g} "
                f"≤ SL={state.stop_loss:.8g} — liquidating remaining qty"
            )
            # Cancel any open TP1 to free locked qty
            if state.tp1_order_id and not state.tp1_filled:
                try:
                    await loop.run_in_executor(
                        None, self.executor.exchange.cancel_all_orders, symbol
                    )
                    _log(f"[Shadow SL] {symbol}: TP1 cancelled")
                except Exception as e:
                    _log(f"[Shadow SL] cancel failed {symbol}: {e}")
                await asyncio.sleep(0.5)

            # حساب الكمية المتبقية الفعلية — يستبعد ما تم بيعه من TP
            remaining = 0.0
            if not state.tp1_filled: remaining += state.qty_tp1
            if not state.tp2_filled: remaining += state.qty_tp2
            if not state.tp3_filled: remaining += state.qty_tp3
            if remaining <= 0:
                remaining = state.filled_qty

            _log(
                f"[Shadow SL] {symbol}: "
                f"tp1={state.tp1_filled} tp2={state.tp2_filled} tp3={state.tp3_filled}"
                f" → بيع {remaining:.4f} عملة"
            )

            await loop.run_in_executor(
                None, self.executor.emergency_market_sell, symbol, remaining
            )
            self.slots.release(symbol)
            await self._notify_exit(state, "SL", curr_price, remaining)
            return

        # ── TP1 Physical Fill Check ──
        if state.tp1_order_id and not state.tp1_filled:
            tp1_status = await loop.run_in_executor(
                None, self.executor.fetch_order_status, symbol, state.tp1_order_id
            )
            if tp1_status == "closed":
                duration = _format_duration(state.entry_time)
                _log(f"[Monitor] 🎯 TP1 HIT: {symbol} | ⏳ {duration}")
                self.slots.update_state(symbol, tp1_filled=True, break_even_attempted=True)
                await self._notify_exit(state, "TP1", state.tp1, state.qty_tp1)

        # ── Shadow TP2 Monitor (Fibonacci dynamic) ──
        if state.tp1_filled and not state.tp2_filled and curr_price >= state.tp2:
            _log(f"[Shadow TP2] 🎯 {symbol}: curr={curr_price:.8g} ≥ TP2={state.tp2:.8g}")
            duration = _format_duration(state.entry_time)

            # Cancel remaining TP1 if somehow still open (safety), then sell TP2
            try:
                open_orders = await loop.run_in_executor(
                    None, self.executor.exchange.fetch_open_orders, symbol
                )
                for o in open_orders:
                    if str(o.get("id")) == str(state.tp1_order_id):
                        await loop.run_in_executor(
                            None, self.executor.exchange.cancel_order, o["id"], symbol
                        )
                        await asyncio.sleep(1.5)
            except Exception as e:
                _log(f"[Shadow TP2] cancel check {symbol}: {e}")

            tp2_qty = self.executor._apply_step_size(symbol, state.qty_tp2)
            try:
                tp2_precise_price = float(
                    self.executor.exchange.price_to_precision(symbol, state.tp2)
                )
                o = await loop.run_in_executor(
                    None,
                    lambda: self.executor.exchange.create_limit_sell_order(
                        symbol, tp2_qty, tp2_precise_price
                    )
                )
                _log(f"[Shadow TP2] ✅ {symbol}: {tp2_qty} @ {tp2_precise_price:.8g} ID:{o['id']} ⏳{duration}")
                self.slots.update_state(symbol, tp2_filled=True)
                await self._notify_exit(state, "TP2", curr_price, state.qty_tp2)
            except Exception as e:
                _log(f"[Shadow TP2] sell failed {symbol}: {e}")

        # ── Shadow TP3 Monitor (Fibonacci dynamic) ──
        if state.tp2_filled and not state.tp3_filled and curr_price >= state.tp3:
            _log(f"[Shadow TP3] 🎯 {symbol}: curr={curr_price:.8g} ≥ TP3={state.tp3:.8g}")
            duration = _format_duration(state.entry_time)

            # TP3: جلب الرصيد الحر الفعلي لضمان بيع كل شيء
            try:
                base_asset = symbol.split("/")[0]
                bal_check  = self.executor.exchange.fetch_balance({"type": "spot"})
                free_qty   = float(
                    bal_check.get(base_asset, {}).get("free", 0) or
                    bal_check.get("free", {}).get(base_asset, 0)
                )
                tp3_qty = self.executor._apply_step_size(symbol, free_qty if free_qty > 0 else state.qty_tp3)
            except Exception:
                tp3_qty = self.executor._apply_step_size(symbol, state.qty_tp3)

            try:
                tp3_precise_price = float(
                    self.executor.exchange.price_to_precision(symbol, state.tp3)
                )
                o = await loop.run_in_executor(
                    None,
                    lambda: self.executor.exchange.create_limit_sell_order(
                        symbol, tp3_qty, tp3_precise_price
                    )
                )
                _log(f"[Shadow TP3] ✅ {symbol}: {tp3_qty} @ {tp3_precise_price:.8g} ID:{o['id']} ⏳{duration}")
                self.slots.update_state(symbol, tp3_filled=True)
                await self._notify_exit(state, "TP3", curr_price, tp3_qty)
                # All targets complete — release slot
                self.slots.release(symbol)
            except Exception as e:
                _log(f"[Shadow TP3] sell failed {symbol}: {e}")

    async def _reconcile_portfolio(self):
        """
        Self-Healing: detects orphaned/timeout positions.
        Retry sequence before emergency liquidation.
        """
        states = self.slots.get_all_states()
        if not states:
            return

        _log(f"[Reconcile] 🔍 فحص {len(states)} صفقة...")

        for state in states:
            symbol         = state.symbol
            open_order_ids: set = set()

            # ── Delisted check — skip audit entirely if symbol vanished ──
            if symbol not in self.executor.exchange.markets:
                _log(
                    f"[Reconcile] 🚫 {symbol}: غير موجود في قائمة الأسواق "
                    f"(محتمل حذف) — تحرير الـ slot نهائياً"
                )
                self.slots.release(symbol)
                await self._notify(
                    "🚫 <b>عملة محذوفة من المنصة</b>\n\n"
                    f"• <b>العملة:</b> <code>{symbol}</code>\n"
                    "• <b>السبب:</b> الزوج غير متوفر على MEXC (delisted)\n\n"
                    "<i>تحقق يدوياً من حساب MEXC. تم تحرير الـ slot ولن "
                    "يُعاد التنبيه لهذه الصفقة.</i>"
                )
                continue

            try:
                orders = await asyncio.get_running_loop().run_in_executor(
                    None, self.executor.exchange.fetch_open_orders, symbol
                )
                open_order_ids = {str(o["id"]) for o in orders}
            except Exception as e:
                err = str(e)
                if "-1121" in err or "invalid symbol" in err.lower():
                    _log(f"[Reconcile] 🚫 {symbol}: invalid symbol — تحرير الـ slot")
                    self.slots.release(symbol)
                    await self._notify(
                        "🚫 <b>عملة محذوفة من المنصة</b>\n\n"
                        f"• <b>العملة:</b> <code>{symbol}</code>\n"
                        "• <b>السبب:</b> الزوج غير متوفر على MEXC (delisted)\n\n"
                        "<i>تحقق يدوياً من حساب MEXC. تم تحرير الـ slot ولن "
                        "يُعاد التنبيه لهذه الصفقة.</i>"
                    )
                    continue
                _log(f"[Reconcile] فشل جلب أوامر {symbol}: {e}")

            await self._audit_slot(state, open_order_ids)

    async def _audit_slot(self, state: SlotState, open_order_ids: set):
        symbol  = state.symbol
        now     = time.time()
        age_hrs = (now - state.opened_at) / 3600

        # Shadow SL: SL is virtual — only check TP1 presence on exchange
        # Shadow SL: SL absence is EXPECTED.
        # TP1 hit check: if tp1_filled=True, TP1 is intentionally gone from exchange
        # — we are now in TP2/TP3 shadow monitoring phase, NOT orphaned.
        if state.tp1_filled:
            # TP1 already filled — position is in shadow TP2/TP3 phase
            # _check_slot handles this — reconcile must not interfere
            _log(
                f"[Reconcile] ✅ {symbol}: TP1 مكتمل — "
                f"في مرحلة TP2/TP3 برمجائية age={age_hrs:.1f}h"
            )
            return

        tp_active = bool(state.tp1_order_id and state.tp1_order_id in open_order_ids)
        orphaned_no_exits = not tp_active

        if not orphaned_no_exits:
            _log(f"[Reconcile] ✅ {symbol}: TP=✅ SL=برمجائي صامت age={age_hrs:.1f}h")
            return

        reason_parts = []
        if orphaned_no_exits: reason_parts.append("لا توجد أوامر TP/SL نشطة على المنصة")
        # Timeout liquidation disabled — only TP absence triggers self-healing
        reason_ar = " | ".join(reason_parts)

        _log(f"🚨 [Self-Healing] {symbol}: {reason_ar}")

        # ── Retry sequence: re_place_tp up to 3 times ──
        recovered = False
        for attempt in range(1, self.cfg.sl_retry_attempts + 1):
            _log(f"[Self-Healing] {symbol}: محاولة إعادة وضع TP/SL ({attempt}/{self.cfg.sl_retry_attempts})")
            try:
                new_ids = await asyncio.get_running_loop().run_in_executor(
                    None, self.executor.re_place_tp, symbol, state
                )
                if new_ids.get("tp1_order_id") or new_ids.get("sl_order_id"):
                    if new_ids.get("tp1_order_id"):
                        self.slots.update_state(symbol, tp1_order_id=new_ids["tp1_order_id"])
                    if new_ids.get("sl_order_id"):
                        self.slots.update_state(symbol, sl_order_id=new_ids["sl_order_id"])
                    _log(f"[Self-Healing] ✅ {symbol}: أوامر أُعيدت في المحاولة {attempt}")
                    recovered = True
                    break
            except Exception as e:
                _log(f"[Self-Healing] محاولة {attempt} فشلت {symbol}: {e}")
            await asyncio.sleep(1)

        if recovered:
            return

        # ── Retry exhausted: fallback limit sell at TP1 ──
        _log(f"[Self-Healing] {symbol}: جميع المحاولات فشلت — limit sell عند TP1")
        fallback_id = await asyncio.get_running_loop().run_in_executor(
            None, self.executor.emergency_tp1_sell, symbol, state.filled_qty, state.tp1
        )
        if fallback_id:
            self.slots.update_state(symbol, tp1_order_id=fallback_id)
            await self._notify(
                f"\U0001f4cc <b>Self-Healing: Limit Sell عند TP1</b>\n\n"
                f"• <b>العملة:</b> <code>{symbol}</code>\n"
                f"• <b>السبب:</b> {reason_ar}\n"
                f"• <b>الإجراء:</b> تم وضع Limit Sell عند <code>{state.tp1:.8g}</code>\n\n"
                "<i>تأمين المركز بدون خسارة سيولة.</i>"
            )
            return

        # ── Ultimate fallback: emergency market sell ──
        _log(f"[Self-Healing] {symbol}: Limit Sell فشل — market sell طارئ")
        for oid in [state.tp1_order_id, state.sl_order_id]:
            if oid and oid in open_order_ids:
                try:
                    await asyncio.get_running_loop().run_in_executor(
                        None, self.executor.cancel_order, symbol, oid
                    )
                except Exception:
                    pass

        await asyncio.sleep(0.5)

        liquidated = await asyncio.get_running_loop().run_in_executor(
            None, self.executor.emergency_market_sell, symbol, state.filled_qty
        )

        exit_price = 0.0
        try:
            ticker     = self.executor.exchange.fetch_ticker(symbol)
            exit_price = float(ticker.get("last") or ticker.get("close") or 0)
        except Exception:
            pass

        entry      = state.entry_price
        filled_qty = state.filled_qty
        entry_fee  = state.entry_fee  # 0.1% taker paid at buy

        if exit_price > 0 and entry > 0:
            exit_fee       = (exit_price * filled_qty) * 0.001  # taker fee on market sell
            gross_pnl      = (exit_price - entry) * filled_qty
            total_fees_usd = entry_fee + exit_fee
            net_pnl_usd    = gross_pnl - total_fees_usd
            net_pnl_pct    = (net_pnl_usd / (entry * filled_qty)) * 100 if entry > 0 else 0.0
            sign_p = "+" if net_pnl_usd >= 0 else ""
            sign_c = "+" if net_pnl_pct >= 0 else ""
            emoji  = "✅" if net_pnl_usd >= 0 else "🔻"
            pnl_line = (
                f"• <b>رسوم المنصة الإجمالية:</b> <code>${total_fees_usd:.4f}</code>\n"
                f"• <b>النتيجة الصافية الحقيقية:</b> {emoji} "
                f"<b>${sign_p}{net_pnl_usd:.3f} ({sign_c}{net_pnl_pct:.2f}%)</b>"
            )
        else:
            pnl_line = "• ⚠️ PnL غير متاح — سعر الخروج غير مرئي"

        if liquidated:
            self.slots.release(symbol)
            await self._notify(
                "\U0001f6a8 <b>Self-Healing: تصفية طارئة</b>\n\n"
                f"• <b>العملة:</b> <code>{symbol}</code>\n"
                f"• <b>السبب:</b> {reason_ar}\n"
                f"• <b>سعر الدخول:</b> <code>{entry:.8g}</code>"
                f" | <b>سعر الخروج:</b> <code>{exit_price:.8g}</code>\n"
                f"{pnl_line}\n\n"
                "<i>تم تسييل المراكز المتعثرة لفتح مقاعد صيد جديدة.</i>"
            )
        else:
            await self._notify(
                "❌ <b>Self-Healing فشل كلياً</b>\n\n"
                f"• <b>العملة:</b> <code>{symbol}</code>\n"
                "• فشل البيع الطارئ — تدخل يدوي عاجل مطلوب!"
            )

    async def _check_rsi_momentum(self, symbol: str) -> bool:
        """Returns True if 15m RSI > 35 (upward bounce from oversold)."""
        try:
            ohlcv = await asyncio.get_running_loop().run_in_executor(
                None,
                self.executor.exchange.fetch_ohlcv,
                symbol, "15m", None, 20,
            )
            if not ohlcv or len(ohlcv) < 15:
                return False
            import pandas as pd
            closes = pd.Series([c[4] for c in ohlcv], dtype=float)
            delta  = closes.diff()
            gain   = delta.clip(lower=0).rolling(14).mean()
            loss   = (-delta.clip(upper=0)).rolling(14).mean()
            rs     = gain / loss.replace(0, 1e-9)
            rsi    = float((100 - 100 / (1 + rs)).iloc[-1])
            _log(f"[RSI Momentum] {symbol}: 15m RSI={rsi:.1f}")
            return rsi > 35
        except Exception as e:
            _log(f"[RSI Momentum] {symbol}: {e}")
            return False

    async def _check_extension_eligibility(self, symbol: str, state: SlotState) -> bool:
        """
        Returns True if 3-hour extension is warranted:
        - LunarCrush Galaxy Score rising OR social volume > threshold
        """
        if not self.cfg.lunar_api_key:
            return False
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"https://lunarcrush.com/api4/public/coins/"
                    f"{symbol.split('/')[0].lower()}/v1",
                    headers={"Authorization": f"Bearer {self.cfg.lunar_api_key}"},
                    timeout=aiohttp.ClientTimeout(total=6),
                ) as resp:
                    if resp.status != 200:
                        return False
                    data         = (await resp.json()).get("data", {})
                    galaxy_score = float(data.get("galaxy_score",    0))
                    social_vol   = int(data.get("interactions_24h",  0))
                    if galaxy_score >= 55 or social_vol >= 50_000:
                        _log(f"[Extension] {symbol}: Galaxy={galaxy_score} SocialVol={social_vol} → تمديد")
                        return True
        except Exception as e:
            _log(f"[Extension] {symbol}: {e}")
        return False

    async def _notify_exit(
        self,
        state:      SlotState,
        exit_type:  str,
        exit_price: float,
        split_qty:  float = 0.0,
    ):
        entry      = state.entry_price
        # Use split_qty if provided (partial exit), else full qty
        exit_qty   = split_qty if split_qty > 0 else state.filled_qty
        entry_fee  = state.entry_fee  # 0.1% taker paid at buy (pro-rated to split)
        entry_fee_split = entry_fee * (exit_qty / state.filled_qty) if state.filled_qty > 0 else entry_fee

        # Exit fee: 0% maker for TP1 limit, 0.1% taker for shadow/SL market
        if exit_type == "TP1":
            exit_fee = 0.0
        else:
            exit_fee = (exit_price * exit_qty) * 0.001

        gross_pnl_usd  = (exit_price - entry) * exit_qty
        total_fees_usd = entry_fee_split + exit_fee
        net_pnl_usd    = gross_pnl_usd - total_fees_usd
        net_pnl_pct    = (net_pnl_usd / (entry * exit_qty)) * 100 if entry > 0 and exit_qty > 0 else 0.0

        emoji     = "✅" if net_pnl_usd >= 0 else "🔻"
        sign_pnl  = "+" if net_pnl_usd >= 0 else ""
        sign_pct  = "+" if net_pnl_pct >= 0 else ""
        duration  = _format_duration(state.entry_time)

        labels = {
            "TP1": "🎯 TP1 وصل الهدف (30% — منصة)",
            "TP2": "🎯 TP2 وصل الهدف (20% — برمجائي)",
            "TP3": "🏆 TP3 وصل الهدف (20% — برمجائي)",
            "SL":  "🔻 وقف الخسارة (Shadow SL)",
        }
        label = labels.get(exit_type, f"📌 {exit_type}")

        tp1_pct = (state.tp1 / entry - 1) * 100 if entry > 0 else 0
        tp2_pct = (state.tp2 / entry - 1) * 100 if entry > 0 else 0
        tp3_pct = (state.tp3 / entry - 1) * 100 if entry > 0 else 0
        sl_pct  = (1 - state.stop_loss / entry) * 100 if entry > 0 else 0

        # تحديد نسبة الكمية المباعة
        qty_pct_map = {"TP1": "20%", "TP2": "40%", "TP3": "الكل المتبقي", "SL": "الكل المتبقي"}
        qty_pct_str = qty_pct_map.get(exit_type, "—")

        # ── تسجيل الخروج في Supabase ──
        notes = ""
        if exit_type == "SL":
            notes = f"Shadow SL — السعر وصل {exit_price:.8g} ≤ SL {state.stop_loss:.8g}"
        elif exit_type in ("TP2", "TP3"):
            notes = f"Shadow {exit_type} — بيع برمجائي عند {exit_price:.8g}"

        if self.db and state.db_trade_id:
            await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: self.db.update_exit(
                    trade_id     = state.db_trade_id,
                    exit_type    = exit_type,
                    exit_price   = exit_price,
                    exit_qty     = exit_qty,
                    net_pnl      = net_pnl_usd,
                    net_pnl_pct  = net_pnl_pct,
                    total_fees   = total_fees_usd,
                    duration_sec = int(time.time() - state.entry_time),
                    notes        = notes,
                )
            )

        await self._notify(
            f"{label}\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 <b>العملة:</b> <code>{state.symbol}</code>\n"
            f"🔢 <b>الكمية المباعة:</b> <code>{exit_qty:.4f}</code> ({qty_pct_str})\n"
            f"📈 <b>سعر الدخول:</b> <code>{entry:.8g}</code>\n"
            f"📉 <b>سعر الخروج:</b> <code>{exit_price:.8g}</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ <b>المدة:</b> <code>{duration}</code>\n"
            f"💸 <b>رسوم المنصة:</b> <code>${total_fees_usd:.4f}</code>\n"
            f"📊 <b>الربح الصافي:</b> {emoji} <b>${sign_pnl}{net_pnl_usd:.3f} ({sign_pct}{net_pnl_pct:.2f}%)</b>\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"خطة الخروج: TP1=+{tp1_pct:.1f}% | TP2=+{tp2_pct:.1f}% | TP3=+{tp3_pct:.1f}% | SL=-{sl_pct:.1f}%"
        )

    async def _notify(self, text: str):
        if not self.cfg.telegram_token or not self.cfg.telegram_chat_id:
            return
        header = MEXC_HEADER
        if "\u0627\u0644\u0645\u0646\u0635\u0629: MEXC" not in text:
            text = header + text
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(
                    f"https://api.telegram.org/bot{self.cfg.telegram_token}/sendMessage",
                    json={"chat_id": self.cfg.telegram_chat_id,
                          "text": text, "parse_mode": "HTML"},
                    timeout=aiohttp.ClientTimeout(total=10),
                )
        except Exception as e:
            _log(f"[Monitor] Telegram error: {e}")


# ─────────────────────────────────────────────
# 9. SCALPING ORCHESTRATOR
# ─────────────────────────────────────────────
class ScalpingOrchestrator:

    def __init__(self, cfg: Config):
        self.cfg                    = cfg
        self.slots                  = SlotManager(cfg)
        self.pipeline               = DataPipeline(cfg)
        self.committee              = ConsensusCommittee(cfg)
        self.executor               = HighSpeedExecutor(cfg)
        self.db                     = TradeLogger(cfg.database_url)
        self.monitor                = TradeMonitor(cfg, self.executor, self.slots, self.db)
        self._processing_symbols:   set[str] = set()
        self._processing_lock:      threading.Lock = threading.Lock()

    def _restore_open_positions(self):
        """
        عند إعادة تشغيل البوت: يفحص MEXC ويسترد الصفقات المفتوحة
        في الذاكرة حتى يعمل TradeMonitor على متابعتها.

        يبحث عن أصول في الحساب تحتوي على Limit Sell orders مفتوحة
        ويُعيد بناء SlotState بسيط لكل منها.
        """
        _log("[Restore] 🔄 فحص صفقات مفتوحة من إعادة التشغيل...")
        try:
            bal      = self.executor.exchange.fetch_balance({"type": "spot"})
            balances = bal.get("total", {})

            restored = 0
            for asset, total_qty in balances.items():
                if asset in ("USDT", "USDC") or float(total_qty or 0) <= 0:
                    continue

                # ── تجاهل الأصول المحظورة شرعياً ──
                if asset.upper() in self.cfg.blacklisted_assets:
                    _log(f"[Restore] ⛔ {asset} في قائمة الحظر — تجاهل")
                    continue

                # ── تجاهل Leveraged tokens ──
                if any(asset.upper().endswith(p) for p in ["3L","3S","5L","5S","UP","DOWN"]):
                    _log(f"[Restore] ⛔ {asset} leveraged token — تجاهل")
                    continue

                # ── احترام حد الـ slots ──
                if self.slots.used >= self.cfg.max_slots:
                    _log(
                        f"[Restore] وصل الحد الأقصى {self.cfg.max_slots} slots "
                        f"— إيقاف الاسترداد"
                    )
                    break

                symbol = f"{asset}/USDT"
                if symbol not in self.executor.exchange.markets:
                    continue

                # جلب الأوامر المفتوحة
                try:
                    open_orders = self.executor.exchange.fetch_open_orders(symbol)
                except Exception:
                    continue

                limit_sells = [o for o in open_orders if o.get("side") == "sell"]
                if not limit_sells:
                    continue

                tp1_order  = limit_sells[0]
                tp1_price  = float(tp1_order.get("price", 0))
                filled_qty = float(total_qty)

                # سعر الدخول التقريبي
                approx_entry = tp1_price / 1.05 if tp1_price > 0 else 0
                approx_sl    = approx_entry * 0.95 if approx_entry > 0 else 0

                if approx_entry <= 0 or filled_qty <= 0:
                    continue

                # إعادة بناء الـ qty split (20/40/40)
                qty_tp1 = round(filled_qty * 0.20, 6)
                qty_tp2 = round(filled_qty * 0.40, 6)
                qty_tp3 = round(filled_qty * 0.40, 6)

                state = SlotState(
                    symbol       = symbol,
                    buy_order_id = "restored",
                    tp1_order_id = str(tp1_order.get("id", "")),
                    entry_price  = approx_entry,
                    filled_qty   = filled_qty,
                    tp1          = tp1_price,
                    tp2          = tp1_price * 1.04,   # Fib تقريبي
                    tp3          = tp1_price * 1.08,   # Fib تقريبي
                    stop_loss    = approx_sl,
                    qty_tp1      = qty_tp1,
                    qty_tp2      = qty_tp2,
                    qty_tp3      = qty_tp3,
                    entry_time   = time.time(),
                )
                self.slots.occupy(state)
                restored += 1
                _log(
                    f"[Restore] ✅ {symbol}: "
                    f"qty={filled_qty:.4f} TP1={tp1_price:.8g} "
                    f"SL={approx_sl:.8g} (برمجائي)"
                )

            _log(f"[Restore] اكتمل — {restored} صفقة مُستردة")

        except Exception as e:
            _log(f"[Restore] ⚠️ خطأ: {e}")

    async def _send_telegram(self, text: str):
        if not self.cfg.telegram_token or not self.cfg.telegram_chat_id:
            return
        header = MEXC_HEADER
        if "\u0627\u0644\u0645\u0646\u0635\u0629: MEXC" not in text:
            text = header + text
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(
                    f"https://api.telegram.org/bot{self.cfg.telegram_token}/sendMessage",
                    json={"chat_id": self.cfg.telegram_chat_id,
                          "text": text, "parse_mode": "HTML"},
                    timeout=aiohttp.ClientTimeout(total=10),
                )
        except Exception as e:
            _log(f"[Telegram] {e}")

    def _calc_rsi(self, closes: pd.Series, period: int = 14) -> float:
        delta = closes.diff()
        gain  = delta.clip(lower=0).rolling(period).mean()
        loss  = (-delta.clip(upper=0)).rolling(period).mean()
        rs    = gain / loss.replace(0, 1e-9)
        return float((100 - 100 / (1 + rs)).iloc[-1])

    def _fetch_indicators(self, symbol: str) -> Optional[dict]:
        try:
            try:
                ohlcv = self.executor.exchange.fetch_ohlcv(symbol, timeframe="1d", limit=120)
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                _log(f"[Scan] OHLCV fetch failed {symbol}: {type(e).__name__}")
                return None
            if not ohlcv or len(ohlcv) < 30:
                return None
            df      = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"]).astype(float)
            closes  = df["close"]
            current = float(closes.iloc[-1])
            rsi     = self._calc_rsi(closes)
            fib_high = float(df["high"].tail(60).max())
            fib_low  = float(df["low"].tail(60).min())
            support  = detect_horizontal_support(df, current)
            vol_usd  = 0.0
            try:
                t       = self.executor.exchange.fetch_ticker(symbol)
                vol_usd = float(t.get("quoteVolume") or 0)
            except ccxt.NetworkError:
                pass
            except ccxt.ExchangeError:
                pass
            except Exception:
                pass
            return {"current": current, "rsi": rsi,
                    "fib_high": fib_high, "fib_low": fib_low, "vol_usd": vol_usd,
                    "support": support}
        except Exception as e:
            _log(f"[Scan] ❌ {symbol}: {type(e).__name__}: {str(e)[:80]}")
            return None

    async def _process_candidate(
        self,
        session:         aiohttp.ClientSession,
        symbol:          str,
        initial_balance: float = 0.0,
    ):
        with self._processing_lock:
            if symbol in self._processing_symbols:
                return
            if not self.slots.is_vacant(symbol):
                return
            self._processing_symbols.add(symbol)

        try:
            await self._process_inner(session, symbol, initial_balance)
        finally:
            with self._processing_lock:
                self._processing_symbols.discard(symbol)

    async def _process_inner(
        self,
        session:         aiohttp.ClientSession,
        symbol:          str,
        initial_balance: float = 0.0,
    ):
        ind = await asyncio.get_running_loop().run_in_executor(
            None, self._fetch_indicators, symbol
        )
        if not ind:
            return

        _log(f"[Scan] {symbol}: RSI={ind['rsi']:.1f} Vol=${ind['vol_usd']/1e6:.1f}M")

        # ── Layer 1: RSI + CMC + LunarCrush ──
        passed, reason = await self.pipeline.layer1_pass(session, symbol, ind["rsi"])
        if not passed:
            _log(f"[L1 ❌] {symbol}: {reason}")
            return
        _log(f"[L1 ✅] {symbol}: {reason}")

        # ── Fetch auxiliary data for committee ──
        lunar_data = await self.pipeline.get_lunar_score(session, symbol)
        rss_sentiment = await self.pipeline.get_rss_sentiment(session)
        whale_data = await self.pipeline.get_whale_activity(session, symbol)

        # ── Layer 2: Consensus Committee (DeepSeek + Llama-3.3) ──
        support_data = ind.get("support", {"has_support": False, "touches": 0, "support_level": 0.0})
        result = await self.committee.run(
            symbol        = symbol,
            rsi           = ind["rsi"],
            vol_m         = ind["vol_usd"] / 1e6,
            entry         = ind["current"],
            fib_high      = ind["fib_high"],
            fib_low       = ind["fib_low"],
            rss_sentiment = rss_sentiment,
            lunar_data    = lunar_data,
            support       = support_data,
            whale_data    = whale_data,
        )

        if whale_data.get("whale_alert") != "none":
            _log(
                f"[Whale] {symbol}: signal={whale_data['whale_alert']} "
                f"({whale_data['transactions']} معاملات ضخمة)"
            )

        if support_data.get("has_support"):
            _log(
                f"[Support] {symbol}: دعم أفقي مؤكَّد — "
                f"{support_data['touches']} لمسات سابقة عند {support_data['support_level']:.8g}"
            )

        if not result["approved"]:
            _log(
                f"[L2 ❌] {symbol}: DeepSeek={result['ds_vote']} "
                f"Llama={result['llama_vote']} ({result['elapsed']}s)"
            )
            return

        targets   = result["targets"]
        stop_loss = await asyncio.get_running_loop().run_in_executor(
            None, calculate_micro_swing_sl,
            self.executor.exchange, symbol, ind["current"]
        )

        _log(
            f"[L2 ✅] {symbol} ({result['elapsed']}s) | "
            f"TP1={targets['tp1']:.6g} TP2={targets['tp2']:.6g} "
            f"TP3={targets['tp3']:.6g} SL={stop_loss:.6g}"
        )

        if not self.slots.is_vacant(symbol):
            _log(f"[L3] {symbol}: slot taken — skip")
            return

        # Live price fallback
        entry_price = ind["current"]
        if not entry_price or entry_price <= 0:
            try:
                ticker      = self.executor.exchange.fetch_ticker(symbol)
                entry_price = float(ticker.get("last") or ticker.get("close") or 0)
                _log(f"[L3] {symbol}: live fallback price: {entry_price:.8g}")
            except Exception as e:
                _log(f"[L3] {symbol}: price fallback failed: {e}")
                return

        if entry_price <= 0:
            _log(f"[L3] {symbol}: price=0 — abort")
            return

        # ── Inline Real-Time Balance Guard (anti-spam injection) ──
        try:
            bal_check = self.executor.exchange.fetch_balance({"type": "spot"})
            free_usdt = float(
                bal_check.get("USDT", {}).get("free", 0) or
                bal_check.get("free", {}).get("USDT", 0)
            )
            if free_usdt < self.cfg.capital:
                _log(
                    f"[Local Balance Guard] Insufficient funds (${free_usdt:.2f} < "
                    f"${self.cfg.capital:.2f}). Halting batch loop."
                )
                return  # silent — no Telegram notification

            # ── One-Position-Per-Symbol Guard ──
            # يمنع شراء عملة موجودة بالفعل في المحفظة (سواء في الذاكرة
            # كـ slot نشط، أو كرصيد حقيقي على المنصة من صفقة سابقة لم
            # تُسجَّل بعد في الذاكرة بسبب إعادة تشغيل أو سباق توقيت).
            # بدون هذا الفحص يمكن أن يتراكم رأس المال على عملة واحدة
            # حتى يتجاوز $100 أو $200 رغم أن الحد المقصود لكل عملة هو
            # صفقة واحدة بقيمة $100 فقط.
            base_asset = symbol.split("/")[0]
            existing_qty = float(
                bal_check.get(base_asset, {}).get("total", 0) or
                bal_check.get("total", {}).get(base_asset, 0) or 0
            )
            if existing_qty > 0:
                try:
                    asset_value_usd = existing_qty * entry_price
                except Exception:
                    asset_value_usd = 0.0
                if asset_value_usd > 1.0:  # تجاهل أتربة (dust) أقل من $1
                    _log(
                        f"[One-Position Guard] {symbol}: رصيد موجود بالفعل "
                        f"({existing_qty:.4f} ≈ ${asset_value_usd:.2f}) — منع صفقة مكررة"
                    )
                    return
        except ccxt.NetworkError as e:
            _log(f"[Local Balance Guard] NetworkError: {e}")
        except ccxt.ExchangeError as e:
            _log(f"[Local Balance Guard] ExchangeError: {e}")
        except Exception as e:
            _log(f"[Local Balance Guard] fetch failed: {e}")

        # ── Layer 3: Execute ──
        state = await asyncio.get_running_loop().run_in_executor(
            None, self.executor.execute_full_trade,
            symbol, entry_price,
            targets["tp1"], targets["tp2"], targets["tp3"], stop_loss,
        )

        if not state:
            # Silent failure — no Telegram spam
            _log(f"[L3 ❌] {symbol}: execution failed — silent cooldown")
            return

        if not state.tp1_order_id and not state.sl_order_id:
            _log(f"[L3 ⚠️] {symbol}: تم الشراء لكن TP/SL لم تُوضع")

        self.slots.occupy(state)

        # ── تسجيل الصفقة في Supabase ──
        committee_summary = (
            f"RSI تشبع بيعي | DeepSeek={result['ds_vote']} | "
            f"Llama={result['llama_vote']} | RSS={rss_sentiment} | "
            f"Galaxy={lunar_data.get('galaxy_score', 0):.0f}"
        )
        trade_id = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: self.db.insert_trade(
                state             = state,
                capital           = self.cfg.capital,
                ds_vote           = result["ds_vote"],
                llama_vote        = result["llama_vote"],
                rss_sentiment     = rss_sentiment,
                galaxy_score      = float(lunar_data.get("galaxy_score", 0)),
                committee_summary = committee_summary,
            )
        )
        if trade_id:
            self.slots.update_state(symbol, db_trade_id=trade_id)

        # ── جلب الرصيد الحالي + أرباح الشهر ──
        current_balance = 0.0
        monthly = {"total_pnl": 0.0, "trades": 0, "wins": 0}
        try:
            bal             = self.executor.exchange.fetch_balance({"type": "spot"})
            current_balance = float(
                bal.get("USDT", {}).get("free", 0) or
                bal.get("free", {}).get("USDT", 0)
            )
        except Exception:
            pass
        monthly = await asyncio.get_running_loop().run_in_executor(
            None, self.db.get_monthly_pnl
        )

        tp1_pct = (state.tp1 / state.entry_price - 1) * 100
        tp2_pct = (state.tp2 / state.entry_price - 1) * 100
        tp3_pct = (state.tp3 / state.entry_price - 1) * 100
        sl_pct  = (1 - state.stop_loss / state.entry_price) * 100

        m_pnl   = monthly.get("total_pnl", 0.0)
        m_count = monthly.get("trades", 0)
        m_sign  = "+" if m_pnl >= 0 else ""

        await self._send_telegram(
            "🚀 <b>صفقة جديدة — تم الدخول</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 <b>العملة:</b> <code>{symbol}</code>\n"
            f"💰 <b>رأس المال:</b> <code>${self.cfg.capital:.2f}</code>\n"
            f"📈 <b>سعر الدخول:</b> <code>{state.entry_price:.8g}</code>\n"
            f"📦 <b>الكمية الكلية:</b> <code>{state.filled_qty:.4f}</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🎯 <b>خطة الخروج</b>\n"
            f"TP1 (+{tp1_pct:.1f}%): <code>{state.tp1:.8g}</code> — 20% ({state.qty_tp1:.4f})\n"
            f"TP2 (+{tp2_pct:.1f}%): <code>{state.tp2:.8g}</code> — 40% ({state.qty_tp2:.4f})\n"
            f"TP3 (+{tp3_pct:.1f}%): <code>{state.tp3:.8g}</code>\n"
            f"🛡 SL (-{sl_pct:.1f}%): <code>{state.stop_loss:.8g}</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"💼 الرصيد: <code>${initial_balance:.2f}</code> → <code>${current_balance:.2f}</code>\n"
            f"📊 إجمالي أرباح الشهر: <code>${m_sign}{m_pnl:.2f}</code> ({m_count} صفقة)\n"
            f"🧠 Committee: DS={result['ds_vote']} | Llama={result['llama_vote']} | RSS={rss_sentiment}\n"
            + (
                f"📍 دعم أفقي مؤكَّد: {support_data['touches']} لمسات سابقة\n"
                if support_data.get("has_support") else ""
            )
            + (
                f"🐋 نشاط حيتان: {whale_data['whale_alert']} ({whale_data['transactions']} معاملة)\n"
                if whale_data.get("whale_alert") != "none" else ""
            )
            + f"⏱️ {result['elapsed']}s"
        )




    async def _check_api_health(self):
        """
        يفحص كل APIs الخارجية ويُرسل تنبيه Telegram واحد إذا توقف أي منها
        (انتهاء رصيد، تجاوز الحد، مفتاح خاطئ، timeout).
        يُستدعى مرة واحدة في بداية كل دورة سكان.
        """
        issues = []

        async with aiohttp.ClientSession() as session:

            # ── DeepSeek ──
            if self.cfg.deepseek_api_key:
                try:
                    async with session.post(
                        "https://api.deepseek.com/v1/chat/completions",
                        headers={"Authorization": f"Bearer {self.cfg.deepseek_api_key}",
                                 "Content-Type": "application/json"},
                        json={"model": self.cfg.deepseek_model, "max_tokens": 1,
                              "messages": [{"role": "user", "content": "hi"}]},
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as resp:
                        if resp.status == 402:
                            issues.append("💳 <b>DeepSeek:</b> رصيد منتهٍ (402)")
                        elif resp.status == 429:
                            issues.append("⏱ <b>DeepSeek:</b> تجاوز الحد المسموح (429)")
                        elif resp.status == 401:
                            issues.append("🔑 <b>DeepSeek:</b> مفتاح API خاطئ (401)")
                        elif resp.status not in (200, 400):
                            issues.append(f"⚠️ <b>DeepSeek:</b> HTTP {resp.status}")
                except asyncio.TimeoutError:
                    issues.append("⏱ <b>DeepSeek:</b> لا استجابة (timeout)")
                except Exception as e:
                    issues.append(f"⚠️ <b>DeepSeek:</b> {str(e)[:50]}")
            else:
                issues.append("🔑 <b>DeepSeek:</b> مفتاح API غير مُعرَّف")

            # ── Together AI (Llama) ──
            if self.cfg.together_api_key:
                try:
                    async with session.post(
                        "https://api.together.xyz/v1/chat/completions",
                        headers={"Authorization": f"Bearer {self.cfg.together_api_key}",
                                 "Content-Type": "application/json"},
                        json={"model": self.cfg.together_model, "max_tokens": 1,
                              "messages": [{"role": "user", "content": "hi"}]},
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as resp:
                        if resp.status == 402:
                            issues.append("💳 <b>Together AI (Llama):</b> رصيد منتهٍ (402)")
                        elif resp.status == 429:
                            issues.append("⏱ <b>Together AI (Llama):</b> تجاوز الحد المسموح (429)")
                        elif resp.status == 401:
                            issues.append("🔑 <b>Together AI (Llama):</b> مفتاح API خاطئ (401)")
                        elif resp.status not in (200, 400):
                            issues.append(f"⚠️ <b>Together AI (Llama):</b> HTTP {resp.status}")
                except asyncio.TimeoutError:
                    issues.append("⏱ <b>Together AI (Llama):</b> لا استجابة (timeout)")
                except Exception as e:
                    issues.append(f"⚠️ <b>Together AI (Llama):</b> {str(e)[:50]}")
            else:
                issues.append("🔑 <b>Together AI (Llama):</b> مفتاح API غير مُعرَّف")

            # ── CoinMarketCap ──
            if self.cfg.cmc_api_key:
                try:
                    async with session.get(
                        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest",
                        headers={"X-CMC_PRO_API_KEY": self.cfg.cmc_api_key},
                        params={"limit": "1"},
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as resp:
                        if resp.status == 401:
                            issues.append("🔑 <b>CoinMarketCap:</b> مفتاح API خاطئ (401)")
                        elif resp.status == 402:
                            issues.append("💳 <b>CoinMarketCap:</b> الحد الشهري منتهٍ (402)")
                        elif resp.status == 429:
                            issues.append("⏱ <b>CoinMarketCap:</b> تجاوز الحد المسموح (429)")
                        elif resp.status not in (200,):
                            issues.append(f"⚠️ <b>CoinMarketCap:</b> HTTP {resp.status}")
                except asyncio.TimeoutError:
                    issues.append("⏱ <b>CoinMarketCap:</b> لا استجابة (timeout)")
                except Exception as e:
                    issues.append(f"⚠️ <b>CoinMarketCap:</b> {str(e)[:50]}")
            else:
                issues.append("🔑 <b>CoinMarketCap:</b> مفتاح API غير مُعرَّف")

            # ── LunarCrush ──
            if self.cfg.lunar_api_key:
                try:
                    async with session.get(
                        "https://lunarcrush.com/api4/public/coins/btc/v1",
                        headers={"Authorization": f"Bearer {self.cfg.lunar_api_key}"},
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as resp:
                        if resp.status == 401:
                            issues.append("🔑 <b>LunarCrush:</b> مفتاح API خاطئ (401)")
                        elif resp.status == 402:
                            issues.append("💳 <b>LunarCrush:</b> رصيد منتهٍ (402)")
                        elif resp.status == 429:
                            issues.append("⏱ <b>LunarCrush:</b> تجاوز الحد المسموح (429)")
                        elif resp.status not in (200,):
                            issues.append(f"⚠️ <b>LunarCrush:</b> HTTP {resp.status}")
                except asyncio.TimeoutError:
                    issues.append("⏱ <b>LunarCrush:</b> لا استجابة (timeout)")
                except Exception as e:
                    issues.append(f"⚠️ <b>LunarCrush:</b> {str(e)[:50]}")

            # ── CoinGecko ──
            if self.cfg.coingecko_api_key:
                try:
                    async with session.get(
                        "https://pro-api.coingecko.com/api/v3/ping",
                        headers={"x-cg-pro-api-key": self.cfg.coingecko_api_key},
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as resp:
                        if resp.status == 401:
                            issues.append("🔑 <b>CoinGecko:</b> مفتاح API خاطئ (401)")
                        elif resp.status == 429:
                            issues.append("⏱ <b>CoinGecko:</b> تجاوز الحد المسموح (429)")
                        elif resp.status not in (200,):
                            issues.append(f"⚠️ <b>CoinGecko:</b> HTTP {resp.status}")
                except asyncio.TimeoutError:
                    issues.append("⏱ <b>CoinGecko:</b> لا استجابة (timeout)")
                except Exception as e:
                    issues.append(f"⚠️ <b>CoinGecko:</b> {str(e)[:50]}")

        # ── إرسال التنبيه إن وُجدت مشاكل ──
        if issues:
            msg = (
                "🔧 <b>تنبيه: أدوات متوقفة أو غير متاحة</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                + "\n".join(f"• {i}" for i in issues)
                + "\n\n<i>راجع مفاتيح API في Railway وتأكد من الرصيد المتاح.</i>"
            )
            await self._send_telegram(msg)
            _log(f"[API Health] ⚠️ {len(issues)} مشكلة مكتشفة — تنبيه أُرسل")
        else:
            _log("[API Health] ✅ كل الأدوات تعمل بشكل طبيعي")

    async def scan_loop(self):
        await self._send_telegram(
            "🤖 <b>Scalping Engine نشط</b>\n"
            f"🛡️ L1: RSI ≤ {self.cfg.rsi_threshold} + CMC Top {self.cfg.cmc_top_rank} + LunarCrush\n"
            f"🧠 L2: DeepSeek + Llama-3.3 (إجماع مطلوب)\n"
            f"⚡ L3: MARKET buy + TP1 Limit + SL Stop-Limit\n"
            f"• Slots: {self.cfg.max_slots} | Capital: ${self.cfg.capital}/trade\n"
            f"• Scan: كل {self.cfg.scan_interval} دقيقة"
        )

        while True:
            start = datetime.now()
            _log(f"🔄 Scan: {start.strftime('%Y-%m-%d %H:%M:%S')}")

            # ── فحص صحة APIs في بداية كل دورة ──
            await self._check_api_health()

            # ── Pre-Flight Balance Audit ──
            # Balance Guard يوقف البحث عن فرص جديدة فقط
            # لا يؤثر على TradeMonitor — المراقبة تعمل دائماً مستقلة
            initial_balance = 0.0
            scanner_active  = True
            try:
                bal             = self.executor.exchange.fetch_balance({"type": "spot"})
                initial_balance = float(
                    bal.get("USDT", {}).get("free", 0) or
                    bal.get("free", {}).get("USDT", 0)
                )
                _log(f"[Balance] الرصيد الافتتاحي: ${initial_balance:.2f} | مطلوب: ${self.cfg.capital:.2f}")

                if initial_balance < self.cfg.capital:
                    _log(
                        "[Balance Guard] Scanner halted. Insufficient funds. "
                        "TradeMonitor continues independently."
                    )
                    scanner_active = False
            except Exception as e:
                _log(f"[Balance ⚠️] {e}")
                scanner_active = False

            # ── Market Scanner — runs only when balance is sufficient ──
            if scanner_active:
                try:
                    # تحديث قائمة الأسواق كل دورة — يكشف الرموز المحذوفة
                    # (delisted) قبل محاولة شرائها، ويمنع التداول على
                    # أزواج أُزيلت من المنصة منذ آخر إعادة تشغيل
                    try:
                        self.executor.exchange.load_markets(reload=True)
                    except Exception as e:
                        _log(f"[Scan] load_markets reload failed: {e}")

                    markets = self.executor.exchange.markets
                    symbols = []
                    for s, mkt in markets.items():
                        if not s.endswith("/USDT"):
                            continue
                        if ":" in s or "swap" in s.lower() or "future" in s.lower():
                            continue
                        base = s.split("/")[0].upper()
                        if base in self.cfg.blacklisted_assets:
                            continue
                        if any(s.endswith(p) for p in ["3L/USDT","3S/USDT","5L/USDT","5S/USDT","UP/USDT","DOWN/USDT","BULL/USDT","BEAR/USDT"]):
                            continue
                        symbols.append(s)

                    _log(f"[Scan] {len(symbols)} عملة Spot/USDT جاهزة للفحص")

                    BATCH = 5
                    async with aiohttp.ClientSession() as session:
                        for i in range(0, len(symbols), BATCH):
                            batch   = symbols[i:i+BATCH]
                            tasks   = [
                                self._process_candidate(session, sym, initial_balance)
                                for sym in batch
                            ]
                            await asyncio.gather(*tasks, return_exceptions=True)
                            checked = i + len(batch)
                            if checked % 50 == 0:
                                _log(f"[Scan] {checked}/{len(symbols)} | slots={self.slots.used}/{self.cfg.max_slots}")
                            await asyncio.sleep(2)

                except Exception as e:
                    _log(f"❌ Scan error: {e}")
            else:
                _log("[Scanner] رصيد غير كافٍ — البحث عن فرص موقوف. المراقبة نشطة.")

            elapsed = (datetime.now() - start).seconds // 60
            _log(f"✅ Cycle: {elapsed}m | slots={self.slots.used}/{self.cfg.max_slots}")
            await asyncio.sleep(self.cfg.scan_interval * 60)

    async def run(self):
        # استرداد الصفقات المفتوحة قبل بدء المراقبة
        self._restore_open_positions()
        await asyncio.gather(self.scan_loop(), self.monitor.start())


# ─────────────────────────────────────────────
# LOGGER & MAIN
# ─────────────────────────────────────────────
def _log(msg: str):
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} | {msg}", flush=True)


if __name__ == "__main__":
    cfg = Config()
    if not cfg.mexc_api_key or not cfg.mexc_api_secret:
        raise RuntimeError("MEXC_API_KEY and MEXC_API_SECRET required")
    bot = ScalpingOrchestrator(cfg)
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        _log("⛔ Bot stopped")
