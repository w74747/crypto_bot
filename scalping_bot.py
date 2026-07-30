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
    fallback_db_url:    str   = field(default_factory=lambda: os.environ.get("FALLBACK_DATABASE_URL", ""))

    # رأس المال يُحسب ديناميكياً = رصيد_حر ÷ max_slots
    # TRADE_INVESTMENT_AMOUNT أُهمل — النظام يقسم الرصيد تلقائياً
    _capital_override: float = field(default_factory=lambda: float(os.environ.get("TRADE_INVESTMENT_AMOUNT", "0")))
    max_slots:          int   = field(default_factory=lambda: int(os.environ.get("MAX_CONCURRENT_TRADES", "3")))
    scan_interval:      int   = field(default_factory=lambda: int(os.environ.get("SCAN_INTERVAL_MINUTES", "60")))
    rsi_threshold:      int   = field(default_factory=lambda: int(os.environ.get("RSI_OVERSOLD_THRESHOLD", "31")))
    min_volume_usd:     float = field(default_factory=lambda: float(os.environ.get("MIN_DAILY_VOLUME_USD", "1000000")))
    cmc_top_rank:       int   = field(default_factory=lambda: int(os.environ.get("CMC_TOP_RANK", "500")))
    monitor_interval:   int   = 30
    max_ai_tokens:      int   = 150
    deepseek_model:     str   = field(default_factory=lambda: os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash"))
    together_model:     str   = "meta-llama/Llama-3.3-70B-Instruct-Turbo"
    max_trade_hours:    float = field(default_factory=lambda: float(os.environ.get("MAX_TRADE_DURATION_HOURS", "10")))
    extension_hours:    float = 3.0
    reconcile_interval: int   = field(default_factory=lambda: int(os.environ.get("RECONCILE_INTERVAL_SECONDS", "180")))
    sl_retry_attempts:        int   = 3
    disable_timeout_liquidation: bool = True  # Positions run until TP or Shadow SL — no time-based liquidation
    shariah_filter_enabled: bool = field(default_factory=lambda: os.environ.get("SHARIAH_FILTER_ENABLED", "true").lower() == "true")

    # ── استراتيجية التشبع البيعي (S1) ──
    s1_btc_rsi_min:     float = field(default_factory=lambda: float(os.environ.get("S1_BTC_RSI_MIN", "40")))
    s1_rsi_extreme:     float = field(default_factory=lambda: float(os.environ.get("S1_RSI_EXTREME", "22")))   # يتجاوز شرط القاعدة

    # ── استراتيجية RSI Bounce Confirmation ──
    rsi_bounce_enabled: bool  = field(default_factory=lambda: os.environ.get("RSI_BOUNCE_ENABLED", "true").lower() == "true")
    rsi_bounce_entry:   float = field(default_factory=lambda: float(os.environ.get("RSI_BOUNCE_ENTRY", "35")))   # RSI يتجاوز هذا صعوداً = تأكيد
    rsi_bounce_lookback:int   = field(default_factory=lambda: int(os.environ.get("RSI_BOUNCE_LOOKBACK", "5")))   # عدد الشموع للبحث عن القاع

    # ── استراتيجية EMA Crossover ──
    ema_cross_enabled:  bool  = field(default_factory=lambda: os.environ.get("EMA_CROSS_ENABLED", "true").lower() == "true")
    ema_fast:           int   = field(default_factory=lambda: int(os.environ.get("EMA_FAST", "9")))
    ema_slow:           int   = field(default_factory=lambda: int(os.environ.get("EMA_SLOW", "21")))
    ema_cross_tolerance:float = field(default_factory=lambda: float(os.environ.get("EMA_CROSS_TOLERANCE_PCT", "1.0")))

    # ── Cost Gate ──
    cost_gate_enabled:  bool  = field(default_factory=lambda: os.environ.get("COST_GATE_ENABLED", "true").lower() == "true")
    cost_gate_pct:      float = field(default_factory=lambda: float(os.environ.get("COST_GATE_PCT", "0.6")))     # الحركة المتوقعة الأدنى %

    # ── BTC Correlation Filter ──
    btc_corr_enabled:    bool  = field(default_factory=lambda: os.environ.get("BTC_CORR_ENABLED", "true").lower() == "true")
    btc_corr_threshold:  float = field(default_factory=lambda: float(os.environ.get("BTC_CORR_THRESHOLD", "0.3")))  # أقل من هذا = مستقل

    # ── Trailing Stop Loss ──
    trailing_sl_enabled: bool  = field(default_factory=lambda: os.environ.get("TRAILING_SL_ENABLED", "true").lower() == "true")
    trailing_sl_pct:     float = field(default_factory=lambda: float(os.environ.get("TRAILING_SL_PCT", "2.0")))    # يتبع السعر بـ 2% تحته
    trailing_sl_trigger: float = field(default_factory=lambda: float(os.environ.get("TRAILING_SL_TRIGGER_PCT", "1.5")))  # يبدأ الـ trailing بعد +1.5% ربح
    s1_sl_min:          float = field(default_factory=lambda: float(os.environ.get("S1_SL_MIN_PCT", "2")))     # % أضيق SL
    s1_sl_max:          float = field(default_factory=lambda: float(os.environ.get("S1_SL_MAX_PCT", "3")))     # % أوسع SL
    s1_tp1_floor:       float = field(default_factory=lambda: float(os.environ.get("S1_TP1_FLOOR_PCT", "6")))  # حد أدنى TP1

    # ── استراتيجية الزخم (S2) ──
    s2_enabled:         bool  = field(default_factory=lambda: os.environ.get("S2_MOMENTUM_ENABLED", "true").lower() == "true")
    s2_rsi_min:         float = field(default_factory=lambda: float(os.environ.get("S2_RSI_MIN", "50")))
    s2_rsi_max:         float = field(default_factory=lambda: float(os.environ.get("S2_RSI_MAX", "65")))
    s2_btc_rsi_min:     float = field(default_factory=lambda: float(os.environ.get("S2_BTC_RSI_MIN", "50")))
    s2_vol_ratio_min:   float = field(default_factory=lambda: float(os.environ.get("S2_VOL_RATIO_MIN", "1.2")))
    s2_breakout_margin: float = field(default_factory=lambda: float(os.environ.get("S2_BREAKOUT_MARGIN_PCT", "0.5")))
    s2_sl_pct:          float = field(default_factory=lambda: float(os.environ.get("S2_SL_PCT", "2.5")))
    s2_tp1_pct:         float = field(default_factory=lambda: float(os.environ.get("S2_TP1_PCT", "3")))
    s2_tp2_pct:         float = field(default_factory=lambda: float(os.environ.get("S2_TP2_PCT", "6")))
    s2_tp3_pct:         float = field(default_factory=lambda: float(os.environ.get("S2_TP3_PCT", "9")))
    # ── إعدادات السوق ──
    market_spot:         bool  = field(default_factory=lambda: os.environ.get("MARKET_SPOT", "true").lower() == "true")
    market_futures:      bool  = field(default_factory=lambda: os.environ.get("MARKET_FUTURES", "false").lower() == "true")

    # ── إعدادات Futures (تُستخدم فقط عند MARKET_FUTURES=true) ──
    futures_leverage:    int   = field(default_factory=lambda: int(os.environ.get("FUTURES_LEVERAGE", "2")))        # رافعة آمنة — لا تتجاوز 5x
    futures_margin_mode: str   = field(default_factory=lambda: os.environ.get("FUTURES_MARGIN_MODE", "isolated"))   # isolated أأمن من cross
    futures_sl_pct:      float = field(default_factory=lambda: float(os.environ.get("FUTURES_SL_PCT", "1.0")))      # -1% بالرافعة = -2% فعلي
    futures_tp1_pct:     float = field(default_factory=lambda: float(os.environ.get("FUTURES_TP1_PCT", "2.0")))     # +2% بالرافعة = +4% فعلي
    futures_tp2_pct:     float = field(default_factory=lambda: float(os.environ.get("FUTURES_TP2_PCT", "3.5")))     # +3.5% بالرافعة = +7% فعلي
    futures_liq_buffer:  float = field(default_factory=lambda: float(os.environ.get("FUTURES_LIQ_BUFFER_PCT", "50"))) # هامش أمان 50% من Liquidation

    # ── الرافعة الديناميكية ──
    futures_leverage_min: int  = field(default_factory=lambda: int(os.environ.get("FUTURES_LEVERAGE_MIN", "1")))   # حد أدنى مطلق
    futures_leverage_max: int  = field(default_factory=lambda: int(os.environ.get("FUTURES_LEVERAGE_MAX", "5")))   # حد أقصى مطلق (لا تتجاوز 5 للأمان)
    futures_dynamic_lev:  bool = field(default_factory=lambda: os.environ.get("FUTURES_DYNAMIC_LEVERAGE", "true").lower() == "true")  # تفعيل/إيقاف الديناميكي

    # ── التقرير الذكي ──
    smart_report_enabled:  bool  = field(default_factory=lambda: os.environ.get("SMART_REPORT_ENABLED", "true").lower() == "true")
    smart_report_interval: int   = field(default_factory=lambda: int(os.environ.get("SMART_REPORT_INTERVAL_HOURS", "1")))

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
    يسجّل كل صفقة في قاعدة البيانات مع دعم Fallback تلقائي:
    - PRIMARY: DATABASE_URL (Supabase أو أي PostgreSQL)
    - FALLBACK: FALLBACK_DATABASE_URL (Neon أو Railway PostgreSQL)
    عند فشل Primary يتحول تلقائياً للـ Fallback بدون توقف.
    """

    def __init__(self, database_url: str, fallback_db_url: str = ""):
        self.db_url          = database_url
        self.fallback_db_url = fallback_db_url
        self._primary_ok     = False
        self._fallback_ok    = False

        # فحص Primary
        if database_url and _PSYCOPG2_OK:
            try:
                conn = psycopg2.connect(database_url, sslmode="require", connect_timeout=5)
                conn.close()
                self._primary_ok = True
                _log("[DB] ✅ Primary DB متصل (Supabase/PostgreSQL)")
            except Exception as e:
                _log(f"[DB] ⚠️ Primary DB فشل: {str(e)[:60]}")

        # فحص Fallback
        if fallback_db_url and _PSYCOPG2_OK:
            try:
                conn = psycopg2.connect(fallback_db_url, sslmode="require", connect_timeout=5)
                conn.close()
                self._fallback_ok = True
                _log("[DB] ✅ Fallback DB متصل (Neon/Railway)")
            except Exception as e:
                _log(f"[DB] ⚠️ Fallback DB فشل: {str(e)[:60]}")

        self._enabled = self._primary_ok or self._fallback_ok

        if not self._enabled:
            _log("[DB] ❌ كلا قاعدتي البيانات غير متاحتين — التسجيل معطّل")

    def _get_conn(self):
        """يحاول Primary أولاً ثم Fallback تلقائياً."""
        if self._primary_ok and self.db_url:
            try:
                return psycopg2.connect(self.db_url, sslmode="require", connect_timeout=5)
            except Exception as e:
                _log(f"[DB] Primary فشل، تحويل للـ Fallback: {str(e)[:50]}")
                self._primary_ok = False  # لا تعيد المحاولة في نفس الجلسة

        if self.fallback_db_url:
            try:
                conn = psycopg2.connect(self.fallback_db_url, sslmode="require", connect_timeout=5)
                if not self._fallback_ok:
                    _log("[DB] ✅ Fallback DB نشط")
                    self._fallback_ok = True
                return conn
            except Exception as e:
                raise ConnectionError(f"كلا قاعدتي البيانات غير متاحتين: {e}")

        raise ConnectionError("لا توجد قاعدة بيانات متاحة")

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

    def save_cmc_cache(self, data: dict) -> bool:
        """يحفظ كاش CMC في قاعدة البيانات — يبقى بعد Restart."""
        if not self._enabled:
            return False
        try:
            import json
            conn = self._get_conn()
            cur  = conn.cursor()
            # نستخدم جدول settings بسيط key-value
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_cache (
                    key        VARCHAR(100) PRIMARY KEY,
                    value      TEXT,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                INSERT INTO bot_cache (key, value, updated_at)
                VALUES ('cmc_bulk_cache', %s, NOW())
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = NOW()
            """, (json.dumps({"ts": __import__("time").time(), "data": data}),))
            conn.commit()
            cur.close()
            conn.close()
            return True
        except Exception as e:
            _log(f"[DB Cache] save_cmc_cache: {str(e)[:60]}")
            return False

    def load_cmc_cache(self) -> dict:
        """يستعيد كاش CMC من قاعدة البيانات عند Restart."""
        if not self._enabled:
            return {}
        try:
            import json, time
            conn = self._get_conn()
            cur  = conn.cursor()
            cur.execute("SELECT value FROM bot_cache WHERE key = 'cmc_bulk_cache'")
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                cached = json.loads(row[0])
                age    = time.time() - cached.get("ts", 0)
                ttl    = float(__import__("os").environ.get("CMC_BULK_TTL_SECONDS", "21600"))
                if age < ttl:
                    _log(f"[DB Cache] ✅ كاش CMC محمّل من DB — عمره {age/3600:.1f} ساعة")
                    return cached.get("data", {})
                else:
                    _log(f"[DB Cache] كاش CMC منتهي الصلاحية ({age/3600:.1f}h) — سيُجدَّد")
        except Exception as e:
            _log(f"[DB Cache] load_cmc_cache: {str(e)[:60]}")
        return {}

    def get_open_trades(self) -> list:
        """
        يسترد الصفقات المفتوحة (بدون closed_at) من Supabase.
        يُعيد entry_price وstop_loss وtp1/tp2/tp3 الحقيقية
        لاستخدامها في Restore بدل إعادة الحساب التقريبي.
        """
        if not self._enabled:
            return []
        try:
            conn = self._get_conn()
            cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT id, symbol, entry_price, filled_qty,
                       tp1, tp2, tp3, stop_loss
                FROM trades
                WHERE closed_at IS NULL
                ORDER BY opened_at DESC
                """
            )
            rows = cur.fetchall()
            cur.close()
            conn.close()
            return [dict(r) for r in rows] if rows else []
        except Exception as e:
            _log(f"[DB] ❌ get_open_trades: {e}")
            return []

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
    # ── حقول Futures (فارغة في Spot) ──
    is_futures:           bool  = False
    leverage:             int   = 1
    liquidation_price:    float = 0.0   # سعر التصفية — خط الموت
    margin_used:          float = 0.0   # الهامش المستخدم فعلاً
    funding_rate:         float = 0.0   # آخر معدل تمويل (كل 8 ساعات)
    funding_cost_usd:     float = 0.0   # تكلفة التمويل المتراكمة
    position_side:        str   = "long" # long أو short
    trailing_sl_enabled:  bool  = False   # هل Trailing SL مفعّل
    trailing_sl_pct:      float = 0.0     # نسبة Trailing من أعلى سعر
    highest_price:        float = 0.0     # أعلى سعر وصله منذ الدخول


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
    _CMC_BULK_TTL:   float = float(os.environ.get('CMC_BULK_TTL_SECONDS', '21600'))  # 6 ساعات افتراضياً (قابل للتعديل من Railway)

    def __init__(self, cfg: Config, db=None):
        self.cfg     = cfg
        self._db_ref = db  # مرجع لـ TradeLogger لحفظ/استعادة الكاش
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

        # ── محاولة استعادة الكاش من DB قبل استدعاء API ──
        if hasattr(self, "_db_ref") and self._db_ref:
            db_cache = self._db_ref.load_cmc_cache()
            if db_cache:
                self._cmc_bulk_cache["ts"]   = time.time()
                self._cmc_bulk_cache["data"] = db_cache
                return db_cache

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
                        # حفظ في DB لبقائه بعد Restart
                        if hasattr(self, "_db_ref") and self._db_ref:
                            self._db_ref.save_cmc_cache(result)
                        return result

                except Exception as e:
                    _log(f"[CMC Bulk] محاولة {attempt+1} فشلت: {str(e)[:60]}")
                    await asyncio.sleep(1)

            # كل المحاولات الثلاث فشلت (429 متكرر أو خطأ آخر) — استخدام
            # الكاش القديم إن وُجد، أو قائمة فارغة (يرفض كل العملات
            # بأمان عبر fail-safe بدل قبولها بدون تحقق)
            _log("[CMC Bulk] فشلت 3 محاولات — استخدام الكاش القديم أو CoinGecko")
            old_cache = self._cmc_bulk_cache.get("data", {})
            if old_cache:
                _log(f"[CMC Bulk] الكاش القديم يحتوي {len(old_cache)} عملة — يُستخدم مؤقتاً")
            else:
                _log("[CMC Bulk] لا كاش قديم — CoinGecko سيعمل كـ fallback")
            return old_cache

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

        # ── CMC فشل أو العملة غير موجودة → جرب CoinGecko ──
        if self.cfg.coingecko_api_key and bulk_data == {}:
            _log(f"[CMC→Gecko] {coin}: CMC فارغ — جاري المحاولة مع CoinGecko")
            gecko = await self.get_coingecko_data(session, symbol)
            if gecko.get("valid"):
                return gecko

        # العملة غير موجودة في Top 500 — رفض مباشر وموثوق
        return {"volume_24h": 0.0, "rank": 9999, "valid": False}

    async def get_social_sentiment(self, session: aiohttp.ClientSession, symbol: str) -> dict:
        """
        نظام Sentiment متعدد المصادر — يعمل بدون LunarCrush.
        3 مصادر مجانية: CoinGecko Community + Fear&Greed + RSS Keywords
        galaxy_score محسوب (0-100) للتوافق مع باقي الكود.
        """
        coin   = symbol.split("/")[0].split("_")[0].upper()
        cached = self._cache_get(self._lunar_cache, coin)
        if cached is not None:
            return cached

        score_components = []
        social_volume    = 0

        # ── المصدر 1: CoinGecko Community Metrics (0-45 نقطة) ──
        try:
            coin_id_map = {
                "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
                "BNB": "binancecoin", "XRP": "ripple", "ADA": "cardano",
                "AVAX": "avalanche-2", "DOT": "polkadot", "LINK": "chainlink",
                "MATIC": "matic-network", "UNI": "uniswap", "ATOM": "cosmos",
                "HBAR": "hedera-hashgraph", "INJ": "injective-protocol",
                "RAY": "raydium",
            }
            coin_id = coin_id_map.get(coin, coin.lower())
            is_demo = self.cfg.coingecko_api_key.startswith("CG-") if self.cfg.coingecko_api_key else True
            base_url = "https://api.coingecko.com" if is_demo else "https://pro-api.coingecko.com"
            hkey     = "x-cg-demo-api-key" if is_demo else "x-cg-pro-api-key"
            headers  = {hkey: self.cfg.coingecko_api_key} if self.cfg.coingecko_api_key else {}
            async with session.get(
                f"{base_url}/api/v3/coins/{coin_id}",
                headers=headers,
                params={"localization": "false", "tickers": "false",
                        "market_data": "false", "community_data": "true",
                        "developer_data": "true"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status == 200:
                    data       = await resp.json()
                    community  = data.get("community_data", {})
                    developer  = data.get("developer_data", {})
                    twitter_f  = int(community.get("twitter_followers") or 0)
                    reddit_s   = int(community.get("reddit_subscribers") or 0)
                    gh_commits = int(developer.get("commit_count_4_weeks") or 0)
                    social_volume = twitter_f // 1000 + reddit_s // 100
                    if twitter_f > 1_000_000 or reddit_s > 100_000:
                        cg_score = 40
                    elif twitter_f > 100_000 or reddit_s > 10_000:
                        cg_score = 30
                    elif twitter_f > 10_000 or reddit_s > 1_000:
                        cg_score = 20
                    else:
                        cg_score = 10
                    if gh_commits > 50:
                        cg_score += 5
                    score_components.append(("CoinGecko", min(cg_score, 45), 45))
                    _log(f"[Sentiment] {coin}: CG={cg_score} T={twitter_f} R={reddit_s} GH={gh_commits}")
        except Exception as e:
            score_components.append(("CoinGecko", 22, 45))

        # ── المصدر 2: Fear & Greed (0-30 نقطة) ──
        try:
            fg       = await self.get_fear_greed_index(session)
            fg_val   = fg.get("value", 50)
            fg_score = int(fg_val * 0.30)
            score_components.append(("FearGreed", fg_score, 30))
        except Exception:
            score_components.append(("FearGreed", 15, 30))

        # ── المصدر 3: RSS Keywords (0-25 نقطة) ──
        try:
            rss_raw  = await self.get_rss_sentiment(session)
            rss_map  = {"bullish": 25, "neutral": 12, "bearish": 3}
            rss_score = rss_map.get(rss_raw, 12)
            score_components.append(("RSS", rss_score, 25))
        except Exception:
            score_components.append(("RSS", 12, 25))

        # ── galaxy_score الإجمالي (0-100) ──
        total_weight = sum(w for _, _, w in score_components)
        total_score  = sum(s for _, s, _ in score_components)
        galaxy_score = int((total_score / total_weight) * 100) if total_weight > 0 else 50
        vote = "approve" if galaxy_score >= 60 else ("reject" if galaxy_score <= 25 else "neutral")

        result = {
            "galaxy_score":  galaxy_score,
            "social_volume": social_volume,
            "vote":          vote,
            "sources":       [f"{n}={s}" for n, s, _ in score_components],
        }
        self._cache_set(self._lunar_cache, coin, result)
        _log(f"[Sentiment] {coin}: score={galaxy_score}/100 vote={vote}")
        return result

    async def get_lunar_score(self, session: aiohttp.ClientSession, symbol: str) -> dict:
        """Alias للتوافق — يستخدم get_social_sentiment."""
        return await self.get_social_sentiment(session, symbol)

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

    async def get_coingecko_data(self, session: aiohttp.ClientSession, symbol: str) -> dict:
        """
        يجلب بيانات العملة من CoinGecko كبديل احتياطي لـ CMC.
        يُستخدم فقط عند فشل CMC (429 أو انتهاء الحصة).

        CoinGecko Pro: 10,000 credit/شهر — كافية كـ fallback.
        Returns: {volume_24h, rank, valid}
        """
        if not self.cfg.coingecko_api_key:
            return {"volume_24h": 0.0, "rank": 9999, "valid": False}

        coin = symbol.split("/")[0].split("_")[0].lower()
        # تحويل بعض الرموز الشائعة
        coin_map = {"btc": "bitcoin", "eth": "ethereum", "bnb": "binancecoin",
                    "sol": "solana", "xrp": "ripple", "ada": "cardano"}
        coin_id = coin_map.get(coin, coin)

        cache_key = f"gecko_{coin}"
        cached = self._cache_get(self._gecko_cache, cache_key)
        if cached is not None:
            return cached

        try:
            # Demo key يبدأ بـ CG- → نستخدم api.coingecko.com
            # Pro key يبدأ بـ pro- أو مختلف → نستخدم pro-api.coingecko.com
            is_demo = self.cfg.coingecko_api_key.startswith("CG-")
            base_url = "https://api.coingecko.com" if is_demo else "https://pro-api.coingecko.com"
            header_key = "x-cg-demo-api-key" if is_demo else "x-cg-pro-api-key"
            async with session.get(
                f"{base_url}/api/v3/coins/{coin_id}",
                headers={header_key: self.cfg.coingecko_api_key},
                params={"localization": "false", "tickers": "false",
                        "community_data": "false", "developer_data": "false"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status == 404:
                    # اسم العملة غير موجود — نحاول البحث
                    return {"volume_24h": 0.0, "rank": 9999, "valid": False}
                if resp.status != 200:
                    return {"volume_24h": 0.0, "rank": 9999, "valid": False}

                data       = await resp.json()
                market     = data.get("market_data", {})
                volume_24h = float(market.get("total_volume", {}).get("usd", 0) or 0)
                rank       = int(data.get("market_cap_rank") or 9999)

                result = {"volume_24h": volume_24h, "rank": rank, "valid": True}
                self._cache_set(self._gecko_cache, cache_key, result)
                return result
        except Exception as e:
            _log(f"[CoinGecko] {coin}: {str(e)[:50]}")
            return {"volume_24h": 0.0, "rank": 9999, "valid": False}

    async def get_order_book_signal(self, session: aiohttp.ClientSession, symbol: str) -> dict:
        """
        يحسب نسبة الطلب/العرض من Order Book.
        نسبة > 1.5 = ضغط شراء قوي ← إشارة إيجابية
        نسبة < 0.7 = ضغط بيع قوي  ← إشارة سلبية
        يُستخدم كفلتر إضافي — لا يوقف الصفقة بمفرده.
        """
        cache_key = f"ob_{symbol}"
        cached = self._cache_get(self._gecko_cache, cache_key)
        if cached is not None:
            return cached

        try:
            import ccxt
            # نستخدم ccxt مباشرة لجلب Order Book
            # الكاش TTL = 30 ثانية (بيانات Order Book تتغير بسرعة)
            ob = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: __import__('ccxt').mexc({
                    "options": {"defaultType": "spot"}
                }).fetch_order_book(symbol, limit=20)
            )
            bids_vol = sum(b[1] for b in ob.get("bids", [])[:10])
            asks_vol = sum(a[1] for a in ob.get("asks", [])[:10])
            ratio    = bids_vol / asks_vol if asks_vol > 0 else 1.0

            if ratio >= 1.5:
                signal = "buy_pressure"
            elif ratio <= 0.7:
                signal = "sell_pressure"
            else:
                signal = "neutral"

            result = {"ratio": round(ratio, 2), "signal": signal,
                      "bids": round(bids_vol, 2), "asks": round(asks_vol, 2)}
            # كاش 30 ثانية فقط
            self._gecko_cache[cache_key] = {"ts": time.time(), "data": result}
            return result
        except Exception as e:
            return {"ratio": 1.0, "signal": "neutral", "bids": 0, "asks": 0}

    async def get_funding_rates(self, session: aiohttp.ClientSession, symbols: list) -> dict:
        """
        يجلب Funding Rates لعقود Futures من MEXC مجاناً.
        Rate سالب = السوق يدفع لـ Long holders ← فرصة ممتازة
        Rate موجب عالٍ (>0.1%) = تكلفة كبيرة على Long ← تجنب

        Returns: {symbol: {"rate": float, "signal": "good"/"neutral"/"costly"}}
        """
        if not symbols:
            return {}
        result = {}
        try:
            # MEXC Funding Rate endpoint
            for sym in symbols[:10]:  # نحد بـ 10 لتجنب rate limit
                clean = sym.replace("/USDT:USDT", "").replace("/USDT", "") + "_USDT"
                async with session.get(
                    f"https://contract.mexc.com/api/v1/contract/funding_rate/{clean}",
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rate = float(data.get("data", {}).get("fundingRate", 0) or 0)
                        if rate < -0.0005:
                            signal = "good"      # السوق يدفع لك
                        elif rate > 0.001:
                            signal = "costly"    # تكلفة عالية
                        else:
                            signal = "neutral"
                        result[sym] = {"rate": rate, "signal": signal}
        except Exception as e:
            _log(f"[Funding Rate] {str(e)[:50]}")
        return result

    async def get_fear_greed_index(self, session: aiohttp.ClientSession) -> dict:
        """
        يجلب مؤشر الخوف والطمع (Fear & Greed Index) من alternative.me.
        مجاني تماماً — لا مفتاح API مطلوب.

        القيم:
        0-24  : خوف شديد (Extreme Fear)  → أفضل وقت للشراء تاريخياً
        25-49 : خوف (Fear)               → فرصة جيدة
        50-74 : طمع (Greed)              → حذر
        75-100: طمع شديد (Extreme Greed) → خطر، تجنب الشراء

        يُخزَّن في كاش ساعة كاملة — استدعاء واحد فقط كل ساعة.
        """
        cached = self._cache_get(self._gecko_cache, "fear_greed")
        if cached is not None:
            return cached

        try:
            async with session.get(
                "https://api.alternative.me/fng/?limit=2",
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                if resp.status != 200:
                    return {"value": 50, "label": "Neutral", "status": "unavailable"}
                data      = await resp.json()
                entries   = data.get("data", [])
                if not entries:
                    return {"value": 50, "label": "Neutral", "status": "unavailable"}

                current   = entries[0]
                value     = int(current.get("value", 50))
                label     = current.get("value_classification", "Neutral")

                # تحديد الإشارة
                if value <= 24:
                    signal = "strong_buy"    # خوف شديد = فرصة ممتازة
                elif value <= 49:
                    signal = "buy"           # خوف = فرصة جيدة
                elif value <= 74:
                    signal = "neutral"       # طمع = حذر
                else:
                    signal = "avoid"         # طمع شديد = تجنب

                result = {
                    "value":  value,
                    "label":  label,
                    "signal": signal,
                    "status": "ok",
                }
                # كاش ساعة كاملة
                self._cache_set(self._gecko_cache, "fear_greed", result)
                _log(f"[Fear&Greed] {value}/100 — {label} ({signal})")
                return result
        except Exception as e:
            _log(f"[Fear&Greed] {str(e)[:50]} — neutral fallback")
            return {"value": 50, "label": "Neutral", "signal": "neutral", "status": "error"}

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
        vol_usd: float = 0.0,
    ) -> tuple[bool, str]:
        """RSI + CMC rank/volume + Sentiment in parallel."""
        if rsi > self.cfg.rsi_threshold:
            return False, f"RSI={rsi:.1f} > {self.cfg.rsi_threshold}"

        cmc_task   = self.get_cmc_data(session, symbol)
        lunar_task = self.get_lunar_score(session, symbol)
        cmc, lunar = await asyncio.gather(cmc_task, lunar_task)

        # ── Fail-Safe: CMC غير متاح — نستخدم حجم MEXC كبديل ──
        if not cmc.get("valid", False):
            mexc_vol = vol_usd if vol_usd > 0 else 0
            if mexc_vol >= self.cfg.min_volume_usd:
                _log(f"[L1 ⚠️] {symbol}: CMC غير متاح — Vol MEXC=${mexc_vol/1e6:.2f}M ✅")
                cmc = {"valid": True, "volume_24h": mexc_vol, "rank": 999}
            else:
                # CMC غير متاح وحجم MEXC صغير — نُخفف الشرط للعملات المستقلة
                _log(f"[L1 ⚠️] {symbol}: CMC غير متاح + Vol=${mexc_vol/1e6:.2f}M — مسموح بدون CMC")
                cmc = {"valid": True, "volume_24h": mexc_vol, "rank": 999}

        # فحص الحجم (نستخدم الأعلى بين CMC وMEXC)
        effective_vol = max(cmc["volume_24h"], vol_usd)
        if effective_vol < self.cfg.min_volume_usd:
            return False, f"Vol ${effective_vol/1e6:.2f}M < min ${self.cfg.min_volume_usd/1e6:.1f}M"

        # فلتر Rank — لا يُطبَّق على عملات بدون CMC (Rank=999)
        if cmc["rank"] < 999 and cmc["rank"] > self.cfg.cmc_top_rank:
            return False, f"CMC Rank #{cmc['rank']} > Top {self.cfg.cmc_top_rank}"

        if lunar["vote"] == "reject":
            return False, f"Sentiment reject (score={lunar['galaxy_score']:.0f})"

        return True, (
            f"RSI={rsi:.1f} ✅ Vol=${effective_vol/1e6:.2f}M "
            f"Rank=#{cmc['rank']} Score={lunar['galaxy_score']:.0f}"
        )


# ─────────────────────────────────────────────
# 4. FIBONACCI ENGINE
# ─────────────────────────────────────────────
def calculate_atr(df: "pd.DataFrame", period: int = 14) -> float:
    """
    Average True Range — يقيس متوسط التقلب الحقيقي.
    True Range = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
    كلما ارتفع ATR، كلما تقلبت العملة أكثر، وكلما انخفضت الرافعة الآمنة.
    """
    try:
        high  = df["high"]
        low   = df["low"]
        close = df["close"]
        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low  - prev_close).abs(),
        ], axis=1).max(axis=1)
        return float(tr.rolling(period).mean().iloc[-1])
    except Exception:
        return 0.0


def calculate_dynamic_leverage(
    entry_price:   float,
    atr:           float,
    rsi:           float,
    support_score: int,
    whale_signal:  str,
    galaxy_score:  float,
    sl_pct:        float,
    lev_min:       int = 1,
    lev_max:       int = 5,
) -> tuple[int, str]:
    """
    يحسب الرافعة المثلى ديناميكياً بناءً على قوة الإشارة وتقلب العملة.

    المنطق الأساسي:
    ─────────────────
    رافعة آمنة = SL% / (ATR% × 2)
    كلما كانت ATR أصغر (أقل تقلباً) → رافعة أعلى مسموحة
    كلما كانت الإشارة أقوى → رافعة أعلى ضمن الحد الآمن

    نقاط القوة (Confluence Score):
    ─────────────────────────────
    +3: RSI < 20 (تشبع شديد جداً)
    +2: RSI 20-25
    +1: RSI 25-30
    +3: دعم أفقي قوي (3+ لمسات)
    +2: دعم أفقي متوسط (2 لمسات)
    +2: Whale "buy" signal
    -3: Whale "sell" signal
    +1: Galaxy Score > 60
    -1: Galaxy Score < 30

    النتيجة:
    ─────────
    Score ≥ 7 → رافعة عالية (ضمن الحد الآمن)
    Score 4-6 → رافعة متوسطة
    Score 1-3 → رافعة منخفضة
    Score ≤ 0 → الحد الأدنى (1x)

    Returns: (leverage, explanation)
    """
    if entry_price <= 0 or atr <= 0:
        return lev_min, "ATR غير متاح — الحد الأدنى"

    # ── حساب الحد الآمن من ATR ──
    atr_pct = (atr / entry_price) * 100
    # رافعة آمنة = SL% / (ATR% × 2) — ضرب 2 للهامش
    safe_leverage = sl_pct / (atr_pct * 2) if atr_pct > 0 else lev_max
    safe_leverage = max(lev_min, min(int(safe_leverage), lev_max))

    # ── حساب نقاط القوة ──
    score = 0
    reasons = []

    # RSI
    if rsi < 20:
        score += 3
        reasons.append(f"RSI={rsi:.1f} شديد جداً (+3)")
    elif rsi < 25:
        score += 2
        reasons.append(f"RSI={rsi:.1f} تشبع قوي (+2)")
    elif rsi < 30:
        score += 1
        reasons.append(f"RSI={rsi:.1f} تشبع (+1)")

    # دعم أفقي
    if support_score >= 2:
        score += 3
        reasons.append("دعم قوي 3+ لمسات (+3)")
    elif support_score == 1:
        score += 2
        reasons.append("دعم متوسط (+2)")
    else:
        reasons.append("بدون دعم (0)")

    # Whale
    if whale_signal == "buy":
        score += 2
        reasons.append("Whale buy (+2)")
    elif whale_signal == "sell":
        score -= 3
        reasons.append("Whale sell (-3) ⚠️")

    # Galaxy Score
    if galaxy_score > 60:
        score += 1
        reasons.append(f"Galaxy={galaxy_score:.0f} (+1)")
    elif galaxy_score < 30:
        score -= 1
        reasons.append(f"Galaxy={galaxy_score:.0f} (-1)")

    # ── تحويل النقاط إلى رافعة ──
    if score >= 7:
        lev_from_score = safe_leverage          # أعلى رافعة آمنة
    elif score >= 4:
        lev_from_score = max(lev_min, safe_leverage - 1)
    elif score >= 1:
        lev_from_score = max(lev_min, min(2, safe_leverage))
    else:
        lev_from_score = lev_min

    # الرافعة النهائية: الأصغر من (الحد الآمن من ATR) و (المقترح من النقاط)
    final_lev = max(lev_min, min(lev_from_score, safe_leverage, lev_max))

    explanation = (
        f"ATR={atr_pct:.2f}% → حد آمن={safe_leverage}x | "
        f"Score={score} → {' | '.join(reasons)} | "
        f"رافعة نهائية={final_lev}x"
    )

    return final_lev, explanation


def calculate_liquidation_price(
    entry_price: float,
    leverage: int,
    margin_mode: str = "isolated",
    position_side: str = "long",
    maintenance_margin_rate: float = 0.004,  # MEXC Futures: 0.4% للعقود الصغيرة
) -> float:
    """
    يحسب سعر التصفية (Liquidation Price) بدقة لـ MEXC Futures.

    معادلة Isolated Margin (Long):
    Liq = Entry × (1 - 1/Leverage + Maintenance_Margin_Rate)

    معادلة Isolated Margin (Short):
    Liq = Entry × (1 + 1/Leverage - Maintenance_Margin_Rate)

    المصدر: MEXC Futures Documentation
    """
    if leverage <= 0:
        return 0.0

    if margin_mode == "isolated":
        if position_side == "long":
            liq = entry_price * (1 - 1/leverage + maintenance_margin_rate)
        else:
            liq = entry_price * (1 + 1/leverage - maintenance_margin_rate)
    else:
        # Cross margin — أخطر، نستخدم نفس الحساب كتقدير محافظ
        if position_side == "long":
            liq = entry_price * (1 - 1/leverage + maintenance_margin_rate)
        else:
            liq = entry_price * (1 + 1/leverage - maintenance_margin_rate)

    return round(liq, 10)


def calculate_safe_sl_futures(
    entry_price: float,
    leverage: int,
    sl_pct: float,
    liq_price: float,
    liq_buffer_pct: float = 50.0,
) -> float:
    """
    يحسب SL آمن للـ Futures يضمن:
    1. لا يتجاوز sl_pct% من سعر الدخول
    2. يبقى فوق (أو أسفل للـ short) سعر التصفية بهامش أمان كافٍ

    هامش الأمان الافتراضي: 50% من المسافة بين الدخول والتصفية
    """
    sl_from_pct = entry_price * (1 - sl_pct / 100)

    # هامش الأمان من Liquidation (50% من المسافة بين الدخول والتصفية)
    if liq_price > 0:
        safety_distance = abs(entry_price - liq_price) * (liq_buffer_pct / 100)
        sl_min_safe = liq_price + safety_distance  # Long: SL يجب أن يكون فوق liq

        # نأخذ الأعلى (الأكثر أماناً)
        sl = max(sl_from_pct, sl_min_safe)
    else:
        sl = sl_from_pct

    return round(sl, 10)


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

    # نطاق مُحسَّن: لا أقرب من -3%، لا أبعد من -5%
    # ضيّقنا النطاق لتحسين R:R — SL أضيق = خسارة أصغر عند الفشل
    sl = max(entry_price * 0.97, min(sl, entry_price * 0.98))
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
        "You are a crypto technical analyst evaluating momentum bounce setups. "
        "Entry logic: RSI was oversold (<30) and has now bounced above 35 (confirmation), "
        "AND EMA9 is above EMA21 (short-term uptrend confirmed). "
        "This is NOT a falling knife setup — the bounce has already started. "
        "BUY if: RSI bounce confirmed + EMA alignment + horizontal support adds confluence. "
        "SKIP if: RSI bounce looks like a dead-cat bounce (no volume), or EMA still bearish, "
        "or price rejected at a major resistance level just above entry. "
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

        support_score = support.get("score", 0) if isinstance(support, dict) else 0
        support_label = (
            "STRONG (3+ touches — high confluence)" if support.get("touches", 0) >= 3 and support.get("has_support")
            else "MODERATE (2 touches — some confluence)" if support.get("has_support")
            else "NONE DETECTED — proceed with extra caution, RSI must be very oversold"
        )
        support_line = (
            f"Horizontal Support Score: {support_label} "
            f"| touches={support.get('touches',0)} near {support.get('support_level',0):.8g}"
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
            f"Social Score: {lunar_data.get('galaxy_score', 50):.0f}/100 "
            f"(CoinGecko Community + Fear&Greed + RSS)\n"
            f"Social Volume: {lunar_data.get('social_volume', 0)} | "
            f"Vote: {lunar_data.get('vote', 'neutral')}\n"
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

        # MAJORITY: يكفي موافقة واحدة من اثنين
        # DeepSeek أولوية أعلى — إذا وافق وحده يكفي
        # Llama وحده يكفي أيضاً لكن بتأهل أقل
        approved = (ds_vote == "BUY" or llama_vote == "BUY")
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
# استراتيجية الهدف الواحد السريع:
# TP1 = 80% عند +6% → ربح سريع ومضمون ($0.96 على $20)
# TP2 = 20% عند +9% → مكافأة إذا استمر الصعود
# SL  = -3%          → خسارة محدودة ($0.61 على $20)
# R:R = 1:1.6 — الأفضل حتى الآن
TP1_QTY_PCT = 0.80   # الجزء الأكبر يخرج سريعاً عند +6%
TP2_QTY_PCT = 0.20   # الباقي ينتظر +9%
TP3_QTY_PCT = 0.00   # غير مستخدم


class HighSpeedExecutor:

    def __init__(self, cfg: Config):
        self.cfg      = cfg
        self.exchange = self._connect()

    def _connect(self) -> ccxt.mexc:
        """
        يتصل دائماً بـ MEXC Spot بغض النظر عن MARKET_FUTURES.
        self.executor = Spot دائماً
        self.futures_executor = Futures (يُنشأ منفصلاً في Orchestrator)
        """
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
        _log(f"✅ Executor connected [🟢 Spot] — {len(ex.markets)} markets loaded")
        return ex

    def _connect_spot_reference(self) -> ccxt.mexc:
        """اتصال Spot منفصل لجلب بيانات السوق حتى في وضع Futures."""
        ex = ccxt.mexc({
            "apiKey":  self.cfg.mexc_api_key,
            "secret":  self.cfg.mexc_api_secret,
            "options": {"defaultType": "spot", "fetchMarkets": ["spot"],
                        "createMarketBuyOrderRequiresPrice": False},
            "enableRateLimit": True,
            "timeout":         60_000,
        })
        ex.load_markets()
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
        """
        ينفذ أمر شراء — يدعم Spot وFutures.

        Spot:    market buy بالقيمة الإجمالية (quoteOrderQty)
        Futures: يضبط Leverage وMargin Mode أولاً، ثم يفتح Long position
                 بحجم يُحسب من رأس المال × الرافعة
        """
        # استخدام رأس المال الديناميكي إذا كان محدداً
        # رأس المال الديناميكي من cfg._capital_override (يُحدَّث كل دورة)
        capital = self.cfg._capital_override if self.cfg._capital_override > 0 else 10.0
        live_price = self._live_price(symbol, entry_price)
        if not live_price or live_price <= 0:
            _log(f"[Executor] {symbol}: live price = 0 — abort")
            return None

        _log(f"[Executor] {'FUTURES' if self.cfg.market_futures else 'SPOT'} BUY {symbol} | "
             f"signal={entry_price:.8g} live={live_price:.8g} ${capital:.2f}"
             + (f" | leverage={self.cfg.futures_leverage}x" if self.cfg.market_futures else ""))

        try:
            self._ensure_markets()

            if self.cfg.market_futures:
                return self._futures_open_long(symbol, live_price, capital)
            else:
                return self._spot_market_buy(symbol, live_price, capital)

        except ccxt.InsufficientFunds as e:
            _log(f"[Executor] InsufficientFunds {symbol}: {e}")
        except ccxt.NetworkError as e:
            _log(f"[Executor] NetworkError {symbol}: {e} — retry next cycle")
        except ccxt.ExchangeError as e:
            _log(f"[Executor] ExchangeError {symbol}: {e}")
        except Exception as e:
            _log(f"[Executor] ERROR {symbol}: {e}")
        return None

    def _spot_market_buy(self, symbol: str, live_price: float, capital: float) -> Optional[dict]:
        """تنفيذ شراء Spot عادي."""
        try:
            precise_capital = float(self.exchange.cost_to_precision(symbol, capital))
        except Exception:
            precise_capital = capital
        order = self.exchange.create_market_buy_order(
            symbol, precise_capital, {"quoteOrderQty": precise_capital}
        )
        filled_price = float(order.get("average") or order.get("price") or live_price)
        filled_qty   = float(order.get("filled") or (capital / filled_price))
        _log(f"✅ SPOT FILLED {symbol}: {filled_qty:.6f} @ {filled_price:.8g} ID:{order['id']}")
        return {"order_id": order["id"], "filled_price": filled_price, "filled_qty": filled_qty,
                "is_futures": False}

    def _futures_open_long(self, symbol: str, live_price: float, capital: float) -> Optional[dict]:
        """
        فتح Long position في Futures مع ضبط Leverage وMargin Mode.

        الحجم = (رأس المال × الرافعة) / سعر الدخول
        مثال: ($20 × 2x) / $0.5 = 80 وحدة

        خطوات الأمان:
        1. ضبط Isolated Margin (أأمن من Cross)
        2. ضبط الرافعة
        3. فتح Long market order
        4. استرداد Liquidation Price من الـ position
        """
        lev = self.cfg.futures_leverage

        # ── الخطوة 1: ضبط Leverage مع MEXC Futures params ──
        # MEXC يتطلب openType في params لـ setMarginMode
        # وleverageRatio بدل leverage في بعض الإصدارات
        try:
            # MEXC Futures: نستخدم الـ endpoint المباشر
            clean_sym = symbol.replace("/USDT:USDT", "").replace("/USDT", "") + "_USDT"
            self.exchange.fapiPrivatePostPositionChangeLeverage({
                "symbol": clean_sym,
                "leverage": lev,
            })
            _log(f"[Futures] {symbol}: Leverage={lev}x ✅")
        except Exception as e1:
            # محاولة ثانية بـ ccxt standard
            try:
                self.exchange.set_leverage(lev, symbol, params={"leverage": lev})
                _log(f"[Futures] {symbol}: Leverage={lev}x (ccxt) ✅")
            except Exception as e2:
                # Leverage قد يكون مضبوطاً بالفعل — نكمل
                _log(f"[Futures] Leverage note {symbol}: {str(e2)[:60]}")

        # ── الخطوة 2: Margin Mode — MEXC يدعم Isolated افتراضياً ──
        # لا نغيره لتجنب الأخطاء — Isolated هو الافتراضي في MEXC
        _log(f"[Futures] {symbol}: Margin=Isolated (افتراضي MEXC)")

        # ── الخطوة 3: حساب الحجم ──
        # الحجم بالعملة = (رأس المال × الرافعة) / سعر الدخول
        notional = capital * lev
        qty_raw  = notional / live_price
        qty      = self._apply_step_size(symbol, qty_raw)

        if qty <= 0:
            _log(f"[Futures] {symbol}: qty=0 — abort")
            return None

        # ── الخطوة 4: فتح Long Position بـ MEXC Futures params ──
        # MEXC Futures يستخدم openType=1 (Isolated) أو openType=2 (Cross)
        # وleverageLevel لتحديد الرافعة مع الأمر مباشرة
        try:
            order = self.exchange.create_market_buy_order(
                symbol, qty,
                params={
                    "leverage":    lev,
                    "openType":    1,     # 1=Isolated, 2=Cross
                    "positionType": 1,    # 1=Long
                }
            )
        except Exception as e1:
            _log(f"[Futures] MEXC params failed: {str(e1)[:80]} — retry standard")
            order = self.exchange.create_market_buy_order(symbol, qty)
        filled_price = float(order.get("average") or order.get("price") or live_price)
        filled_qty   = float(order.get("filled") or qty)

        # ── الخطوة 5: جلب Liquidation Price من المنصة ──
        liq_price = 0.0
        try:
            time.sleep(1)  # انتظار لتحديث الـ position
            positions = self.exchange.fetch_positions([symbol])
            for pos in positions:
                if pos.get("symbol") == symbol and float(pos.get("contracts", 0)) > 0:
                    liq_price = float(pos.get("liquidationPrice") or 0)
                    margin_used = float(pos.get("initialMargin") or capital)
                    break
            if liq_price <= 0:
                # حساب يدوي كـ fallback
                liq_price = calculate_liquidation_price(filled_price, lev)
                margin_used = capital
        except Exception as e:
            liq_price   = calculate_liquidation_price(filled_price, lev)
            margin_used = capital
            _log(f"[Futures] Liq price fallback {symbol}: {e}")

        # رسوم Taker Futures = 0.06% (أقل من Spot 0.1%)
        entry_fee = notional * 0.0006

        _log(
            f"✅ FUTURES LONG {symbol}: {filled_qty:.6f} @ {filled_price:.8g} "
            f"| Lev={lev}x | Liq={liq_price:.8g} | Margin=${margin_used:.2f} "
            f"| ID:{order['id']}"
        )
        return {
            "order_id":      order["id"],
            "filled_price":  filled_price,
            "filled_qty":    filled_qty,
            "is_futures":    True,
            "leverage":      lev,
            "liquidation_price": liq_price,
            "margin_used":   margin_used,
            "entry_fee":     entry_fee,
        }

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
        qty_tp1 = self._apply_step_size(symbol, filled_qty * 0.80)
        qty_tp2 = self._apply_step_size(symbol, filled_qty * 0.20)
        qty_tp3 = self._apply_step_size(symbol, filled_qty * 0.00)
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

    def emergency_market_sell(self, symbol: str, qty: float, is_futures: bool = False) -> bool:
        """
        إغلاق طارئ — يدعم Spot وFutures.

        Spot:    market sell للكمية المتاحة
        Futures: إغلاق Long position بالكامل (close position)
                 لا يحتاج كمية — يُغلق كل الـ position دفعة واحدة
        """
        try:
            if is_futures or self.cfg.market_futures:
                return self._futures_close_long(symbol, qty)
            else:
                return self._spot_emergency_sell(symbol, qty)
        except Exception as e:
            _log(f"[Emergency] ❌ {symbol}: {str(e)[:120]}")
            return False

    def _spot_emergency_sell(self, symbol: str, qty: float) -> bool:
        """إغلاق طارئ Spot."""
        try:
            base_token = symbol.split("/")[0].split("_")[0]
            balance    = self.exchange.fetch_balance({"type": "spot"})
            free_qty   = float(
                balance.get(base_token, {}).get("free", 0) or
                balance.get("free", {}).get(base_token, 0)
            )
            _log(f"[Emergency Spot] {symbol}: cached={qty:.4f} free={free_qty:.4f}")

            if free_qty <= 0:
                _log(f"[Emergency Spot] {symbol}: free=0 — slot released")
                return True

            sell_qty = self._apply_step_size(symbol, min(qty, free_qty))
            if sell_qty <= 0:
                return True

            try:
                precise_qty = float(self.exchange.amount_to_precision(symbol, sell_qty))
            except Exception:
                precise_qty = sell_qty

            o = self.exchange.create_market_sell_order(symbol, precise_qty)
            _log(f"[Emergency Spot] ✅ {symbol}: sold {precise_qty} ID:{o['id']}")
            return True
        except Exception as e:
            err = str(e)
            if "30005" in err or "Oversold" in err:
                _log(f"[Emergency Spot] {symbol}: 30005 — releasing slot")
                return True
            _log(f"[Emergency Spot] ❌ {symbol}: {err[:120]}")
            return False

    def _futures_close_long(self, symbol: str, qty: float) -> bool:
        """
        إغلاق Long position في Futures بالكامل.

        يستخدم reduce_only=True لضمان الإغلاق فقط (لا فتح Short)
        ويجلب الكمية الفعلية من الـ position لا من الذاكرة.
        """
        try:
            # جلب الكمية الفعلية من المنصة
            actual_qty = 0.0
            try:
                positions = self.exchange.fetch_positions([symbol])
                for pos in positions:
                    if pos.get("symbol") == symbol and pos.get("side") == "long":
                        actual_qty = float(pos.get("contracts") or pos.get("amount") or 0)
                        break
            except Exception:
                actual_qty = qty

            close_qty = self._apply_step_size(symbol, actual_qty if actual_qty > 0 else qty)
            if close_qty <= 0:
                _log(f"[Emergency Futures] {symbol}: qty=0 — position likely already closed")
                return True

            o = self.exchange.create_market_sell_order(
                symbol, close_qty,
                params={
                    "positionSide": "LONG",
                    "reduceOnly":   True,
                }
            )
            _log(f"[Emergency Futures] ✅ {symbol}: closed {close_qty} Long ID:{o['id']}")
            return True
        except Exception as e:
            err = str(e)
            if "position" in err.lower() and "not exist" in err.lower():
                _log(f"[Emergency Futures] {symbol}: position already closed")
                return True
            _log(f"[Emergency Futures] ❌ {symbol}: {err[:120]}")
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
        effective_tp1 = max(tp1, entry_price * (1 + self.cfg.s1_tp1_floor / 100))
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

        # بيانات Futures إضافية
        is_futures      = buy.get("is_futures", False)
        leverage        = buy.get("leverage", 1)
        liq_price       = buy.get("liquidation_price", 0.0)
        margin_used     = buy.get("margin_used", self.cfg.capital)

        return SlotState(
            symbol            = symbol,
            buy_order_id      = buy["order_id"],
            tp1_order_id      = bracket.get("tp1_order_id", ""),
            sl_order_id       = "",
            entry_price       = buy["filled_price"],
            filled_qty        = buy["filled_qty"],
            tp1               = effective_tp1,
            tp2               = effective_tp2,
            tp3               = effective_tp3,
            stop_loss         = stop_loss,
            entry_fee         = entry_fee,
            qty_tp1           = bracket.get("qty_tp1", buy["filled_qty"] * 0.80),
            qty_tp2           = bracket.get("qty_tp2", buy["filled_qty"] * 0.20),
            qty_tp3           = bracket.get("qty_tp3", buy["filled_qty"] * 0.00),
            entry_time        = time.time(),
            is_futures        = is_futures,
            leverage          = leverage,
            liquidation_price = liq_price,
            margin_used       = margin_used,
            position_side         = "long",
            trailing_sl_enabled   = self.cfg.trailing_sl_enabled,
            trailing_sl_pct       = self.cfg.trailing_sl_pct,
            highest_price         = buy["filled_price"],
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

        # ── Futures: فحص Liquidation Price وFunding Rate ──
        if state.is_futures:
            # تحذير إذا اقترب السعر من سعر التصفية (أقل من 20% مسافة)
            if state.liquidation_price > 0:
                liq_distance_pct = abs(curr_price - state.liquidation_price) / curr_price * 100
                if liq_distance_pct < 20:
                    _log(
                        f"[Futures ⚠️] {symbol}: سعر التصفية قريب! "
                        f"curr={curr_price:.8g} liq={state.liquidation_price:.8g} "
                        f"(مسافة {liq_distance_pct:.1f}%)"
                    )
                    # إرسال تنبيه طارئ فوري
                    await self._notify(
                        f"🚨 <b>تحذير Futures — سعر التصفية قريب!</b>\n\n"
                        f"• <b>العملة:</b> <code>{symbol}</code>\n"
                        f"• <b>السعر الحالي:</b> <code>{curr_price:.8g}</code>\n"
                        f"• <b>سعر التصفية:</b> <code>{state.liquidation_price:.8g}</code>\n"
                        f"• <b>المسافة:</b> <code>{liq_distance_pct:.1f}%</code> فقط!\n\n"
                        f"<i>SL سيُنفَّذ قريباً لحماية الحساب.</i>"
                    )

            # تحديث تكلفة Funding Rate (كل 8 ساعات)
            age_hours = (time.time() - state.entry_time) / 3600
            if age_hours > 0 and state.funding_rate > 0:
                expected_funding_payments = int(age_hours / 8)
                total_funding_cost = state.margin_used * state.funding_rate * expected_funding_payments
                if total_funding_cost != state.funding_cost_usd:
                    self.slots.update_state(symbol, funding_cost_usd=total_funding_cost)
                    _log(
                        f"[Futures] {symbol}: Funding cost=${total_funding_cost:.4f} "
                        f"({expected_funding_payments} payments)"
                    )

        # ── Shadow SL Monitor ──
        if state.stop_loss > 0 and curr_price <= state.stop_loss:
            _log(
                f"[Shadow SL] 🔻 {symbol}: curr={curr_price:.8g} "
                f"≤ SL={state.stop_loss:.8g} — liquidating remaining qty"
            )
            try:
                import gc
                for obj in gc.get_objects():
                    if isinstance(obj, ScalpingOrchestrator):
                        obj._recent_events.append(f"🔻 SL: {symbol} عند {curr_price:.6g}")
                        break
            except Exception:
                pass
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

        # ── Trailing Stop Loss Monitor ──
        if state.trailing_sl_enabled and curr_price > 0 and state.entry_price > 0:
            # تحديث أعلى سعر
            if curr_price > state.highest_price:
                self.slots.update_state(symbol, highest_price=curr_price)
                highest = curr_price
            else:
                highest = state.highest_price

            # هل وصل الربح للحد المطلوب لبدء الـ trailing؟
            profit_pct = (highest - state.entry_price) / state.entry_price * 100
            if profit_pct >= self.cfg.trailing_sl_trigger:
                # حساب SL الجديد: أعلى سعر - trailing_pct
                new_sl = highest * (1 - state.trailing_sl_pct / 100)
                if new_sl > state.stop_loss:
                    old_sl = state.stop_loss
                    self.slots.update_state(symbol, stop_loss=new_sl)
                    _log(
                        f"[Trailing SL] 📈 {symbol}: أعلى={highest:.6g} "
                        f"SL رُفع {old_sl:.6g} → {new_sl:.6g} "
                        f"(+{profit_pct:.1f}% من الدخول)"
                    )

        # ── TP1 Physical Fill Check ──
        if state.tp1_order_id and not state.tp1_filled:
            tp1_status = await loop.run_in_executor(
                None, self.executor.fetch_order_status, symbol, state.tp1_order_id
            )
            if tp1_status == "closed":
                duration = _format_duration(state.entry_time)
                _log(f"[Monitor] 🎯 TP1 HIT: {symbol} | ⏳ {duration}")
                # تسجيل الحدث للتقرير الذكي
                try:
                    orch = None
                    import gc
                    for obj in gc.get_objects():
                        if isinstance(obj, ScalpingOrchestrator):
                            orch = obj
                            break
                    if orch:
                        orch._recent_events.append(f"🎯 TP1: {symbol} بعد {duration}")
                except Exception:
                    pass
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
        Returns True if 3-hour extension is warranted.
        يستخدم نظام Sentiment الجديد (CoinGecko + Fear&Greed + RSS)
        بدلاً من LunarCrush.
        """
        try:
            async with aiohttp.ClientSession() as session:
                sentiment = await self.pipeline.get_social_sentiment(session, symbol)
                galaxy_score = sentiment.get("galaxy_score", 0)
                social_vol   = sentiment.get("social_volume", 0)
                if galaxy_score >= 55 or social_vol >= 1_000:
                    _log(f"[Extension] {symbol}: Score={galaxy_score} Vol={social_vol} → تمديد")
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
        qty_pct_map = {"TP1": "80%", "TP2": "20%", "TP3": "—", "SL": "الكل المتبقي"}
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

        # بيانات إضافية للـ Futures
        futures_line = ""
        if state.is_futures:
            effective_pnl = net_pnl_usd - state.funding_cost_usd
            lev_str = f"{state.leverage}x"
            futures_line = (
                f"⚡ <b>Futures:</b> Leverage={lev_str} | "
                f"Funding cost=${state.funding_cost_usd:.4f}\n"
                f"💰 <b>PnL بعد Funding:</b> {emoji} "
                f"<b>${'+' if effective_pnl>=0 else ''}{effective_pnl:.3f}</b>\n"
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
            f"{futures_line}"
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
        self.pipeline               = DataPipeline(cfg, db=None)  # سيُربط بـ db بعد إنشائه
        self.committee              = ConsensusCommittee(cfg)

        # ── Executor الأساسي (Spot افتراضياً) ──
        self.executor               = HighSpeedExecutor(cfg)

        # ── Futures Executor منفصل (يُنشأ فقط إذا MARKET_FUTURES=true) ──
        self.futures_executor: Optional[HighSpeedExecutor] = None
        if cfg.market_futures:
            try:
                # Futures executor منفصل يتصل بـ swap مباشرة
                self.futures_executor = HighSpeedExecutor.__new__(HighSpeedExecutor)
                self.futures_executor.cfg = cfg
                import ccxt as _ccxt
                _fex = _ccxt.mexc({
                    "apiKey":  cfg.mexc_api_key,
                    "secret":  cfg.mexc_api_secret,
                    "options": {
                        "defaultType":                       "swap",
                        "fetchMarkets":                      ["swap"],
                        "createMarketBuyOrderRequiresPrice": False,
                    },
                    "enableRateLimit": True,
                    "timeout":         60_000,
                })
                _fex.load_markets()
                self.futures_executor.exchange = _fex
                _log(f"✅ Futures Executor متصل — {len(_fex.markets)} Perpetual markets")
            except Exception as e:
                _log(f"⚠️ فشل إنشاء Futures Executor: {e} — سيعمل Spot فقط")

        self.db                     = TradeLogger(cfg.database_url, cfg.fallback_db_url)
        self.pipeline._db_ref       = self.db  # ربط الكاش بقاعدة البيانات
        self.monitor                = TradeMonitor(cfg, self.executor, self.slots, self.db)
        self._processing_symbols:   set[str] = set()
        self._processing_lock:      threading.Lock = threading.Lock()
        self._last_api_health_alert: float = 0.0
        self._last_smart_report:    float = 0.0
        self._recent_events:        list  = []



    def _restore_futures_positions(self):
        """
        يسترد Futures positions المفتوحة من MEXC عند Restart.
        Futures positions تُسترد من fetch_positions() لا من fetch_balance().
        """
        _log("[Futures Restore] 🔄 استرداد Futures positions...")
        try:
            positions = self.executor.exchange.fetch_positions()
            restored  = 0

            for pos in positions:
                contracts = float(pos.get("contracts") or pos.get("amount") or 0)
                if contracts <= 0:
                    continue
                if pos.get("side") != "long":
                    continue  # ندعم Long فقط حالياً

                symbol    = pos.get("symbol", "")
                if not symbol or symbol not in self.executor.exchange.markets:
                    continue

                if self.slots.used >= self.cfg.max_slots:
                    break

                entry_price  = float(pos.get("entryPrice") or pos.get("averagePrice") or 0)
                liq_price    = float(pos.get("liquidationPrice") or 0)
                margin_used  = float(pos.get("initialMargin") or self.cfg.capital)
                leverage     = int(pos.get("leverage") or self.cfg.futures_leverage)
                unrealized   = float(pos.get("unrealizedPnl") or 0)

                if entry_price <= 0:
                    continue

                # SL آمن بعيد عن Liquidation
                stop_loss = calculate_safe_sl_futures(
                    entry_price, leverage,
                    self.cfg.futures_sl_pct,
                    liq_price,
                    self.cfg.futures_liq_buffer,
                )

                # أهداف من إعدادات Railway
                tp1 = entry_price * (1 + self.cfg.futures_tp1_pct / 100)
                tp2 = entry_price * (1 + self.cfg.futures_tp2_pct / 100)
                tp3 = tp2 * 1.02

                state = SlotState(
                    symbol            = symbol,
                    buy_order_id      = "restored_futures",
                    entry_price       = entry_price,
                    filled_qty        = contracts,
                    tp1               = tp1,
                    tp2               = tp2,
                    tp3               = tp3,
                    stop_loss         = stop_loss,
                    qty_tp1           = round(contracts * 0.80, 6),
                    qty_tp2           = round(contracts * 0.20, 6),
                    qty_tp3           = 0.0,
                    entry_time        = time.time(),
                    is_futures        = True,
                    leverage          = leverage,
                    liquidation_price = liq_price,
                    margin_used       = margin_used,
                    position_side         = "long",
            trailing_sl_enabled   = self.cfg.trailing_sl_enabled,
            trailing_sl_pct       = self.cfg.trailing_sl_pct,
            highest_price         = buy["filled_price"],
                )
                self.slots.occupy(state)
                restored += 1

                liq_dist = abs(entry_price - liq_price) / entry_price * 100 if liq_price > 0 else 0
                _log(
                    f"[Futures Restore] ✅ {symbol}: "
                    f"qty={contracts:.4f} entry={entry_price:.8g} "
                    f"Lev={leverage}x Liq={liq_price:.8g} ({liq_dist:.1f}% مسافة) "
                    f"PnL={unrealized:+.4f}"
                )

            _log(f"[Futures Restore] اكتمل — {restored} position مُستردة")

        except Exception as e:
            _log(f"[Futures Restore] ⚠️ خطأ: {e}")

    def _post_restore_health_check(self):
        """
        Post-Restore Health Check — يعمل بعد _restore_open_positions مباشرة.

        يفحص كل صفقة مُستردة ويتحقق من:
        1. العملة لا تزال موجودة على MEXC (لم تُحذف)
        2. SL معقول — ليس أعلى من السعر الحالي (يمنع الإغلاق الفوري الوهمي)
        3. TP1 لا يزال نشطاً على المنصة (وإلا Self-Healing سيعيده)
        4. الكمية الفعلية تتطابق مع المسجّلة (تحقق من dust أو إغلاق جزئي)

        لا يُعدّل الأهداف (TP) آلياً — يُرسل تقرير Telegram فقط لكل ما يحتاج انتباهاً.
        SL الخاطئ (> سعر حالي) هو الاستثناء الوحيد الذي يُصحَّح آلياً.
        """
        states = self.slots.get_all_states()
        if not states:
            return

        _log(f"[Health Check] 🔍 فحص {len(states)} صفقة مُستردة...")

        alerts = []
        auto_fixed = []

        for state in states:
            symbol = state.symbol
            issues = []

            # ── 1: فحص وجود العملة على MEXC ──
            if symbol not in self.executor.exchange.markets:
                alerts.append(f"🚫 <b>{symbol}</b>: محذوفة من MEXC (Delisted) — تحرير الـ slot")
                self.slots.release(symbol)
                continue

            # ── 2: جلب السعر الحالي ──
            try:
                ticker    = self.executor.exchange.fetch_ticker(symbol)
                curr_price = float(ticker.get("last") or ticker.get("close") or 0)
            except Exception as e:
                alerts.append(f"⚠️ <b>{symbol}</b>: فشل جلب السعر — {str(e)[:40]}")
                continue

            if curr_price <= 0:
                continue

            # ── 3: فحص SL — الأخطر ──
            sl = state.stop_loss
            if sl <= 0:
                issues.append("SL = 0 (غير مُعيَّن)")
            elif sl >= curr_price * 0.99:
                # SL أعلى من السعر الحالي → سيُغلق الصفقة فوراً بخسارة وهمية
                old_sl = sl
                new_sl = curr_price * 0.94  # -6% آمن
                self.slots.update_state(symbol, stop_loss=new_sl)
                auto_fixed.append(
                    f"🔧 <b>{symbol}</b>: SL={old_sl:.6g} > سعر={curr_price:.6g} "
                    f"→ صُحِّح تلقائياً إلى {new_sl:.6g} (-6%)"
                )
            elif sl < curr_price * 0.85:
                # SL بعيد جداً (أكثر من -15%) — تنبيه فقط، لا تعديل
                sl_pct = (1 - sl / curr_price) * 100
                issues.append(f"SL بعيد جداً (-{sl_pct:.1f}%) — قد تكون الخسارة كبيرة")

            # ── 4: فحص TP1 على المنصة (إذا لم يكتمل بعد) ──
            if not state.tp1_filled:
                if not state.tp1_order_id:
                    issues.append("TP1 order ID مفقود — Self-Healing سيعيده في الدورة القادمة")
                else:
                    try:
                        open_orders = self.executor.exchange.fetch_open_orders(symbol)
                        open_ids = {str(o["id"]) for o in open_orders}
                        if state.tp1_order_id not in open_ids:
                            issues.append("TP1 غير موجود على المنصة — Self-Healing سيعيده")
                    except Exception:
                        pass  # فشل الفحص لا يعني وجود مشكلة

            # ── 5: فحص الكمية الفعلية ──
            try:
                base_asset = symbol.split("/")[0]
                bal        = self.executor.exchange.fetch_balance({"type": "spot"})
                real_qty   = float(
                    bal.get(base_asset, {}).get("total", 0) or
                    bal.get("total", {}).get(base_asset, 0) or 0
                )
                if real_qty <= 0:
                    issues.append(f"⚠️ لا يوجد رصيد فعلي على MEXC — قد تكون الصفقة مُغلقة")
                elif abs(real_qty - state.filled_qty) / state.filled_qty > 0.15:
                    issues.append(
                        f"فرق كمية: مسجّل={state.filled_qty:.4f} فعلي={real_qty:.4f} "
                        f"({abs(real_qty-state.filled_qty)/state.filled_qty*100:.0f}% فرق)"
                    )
            except Exception:
                pass

            # ── تجميع المشاكل ──
            if issues:
                entry_pct = (curr_price / state.entry_price - 1) * 100 if state.entry_price > 0 else 0
                sign = "+" if entry_pct >= 0 else ""
                issue_lines = "\n".join(f"   • {iss}" for iss in issues)
                alerts.append(
                    f"⚠️ <b>{symbol}</b> ({sign}{entry_pct:.1f}% من الدخول)\n{issue_lines}"
                )
            else:
                _log(f"[Health Check] ✅ {symbol}: سعر={curr_price:.6g} SL={sl:.6g} — سليم")

        # ── إرسال التقرير ──
        if auto_fixed or alerts:
            msg_parts = ["🔍 <b>Post-Restore Health Check</b>\n━━━━━━━━━━━━━━━━━━━━\n"]

            if auto_fixed:
                msg_parts.append("🔧 <b>تصحيحات تلقائية:</b>")
                msg_parts.extend(auto_fixed)
                msg_parts.append("")

            if alerts:
                msg_parts.append("⚠️ <b>تحتاج مراجعة:</b>")
                msg_parts.extend(alerts)
                msg_parts.append("")

            msg_parts.append(f"<i>فُحصت {len(states)} صفقة | {len(auto_fixed)} تصحيح تلقائي | {len(alerts)} تنبيه</i>")

            # إرسال متزامن (نحن في __init__ قبل asyncio loop)
            import urllib.request, json as _json
            try:
                payload = _json.dumps({
                    "chat_id": self.cfg.telegram_chat_id,
                    "text": MEXC_HEADER + "\n".join(msg_parts),
                    "parse_mode": "HTML"
                }).encode()
                req = urllib.request.Request(
                    f"https://api.telegram.org/bot{self.cfg.telegram_token}/sendMessage",
                    data=payload,
                    headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(req, timeout=10)
            except Exception as e:
                _log(f"[Health Check] Telegram error: {e}")
        else:
            _log(f"[Health Check] ✅ كل الصفقات سليمة — لا تنبيهات")

    def _restore_open_positions(self):
        """
        عند إعادة تشغيل البوت: يسترد الصفقات المفتوحة بالبيانات الحقيقية.

        الأولوية:
        1. Supabase — يجلب entry_price/stop_loss/tp1/tp2/tp3 الحقيقية
           التي سُجِّلت لحظة الشراء، بدل إعادة الحساب التقريبي الذي
           كان يُغلق صفقات سليمة بخطأ (مشكلة SEI -16% الوهمية)
        2. Fallback — إذا لم تكن Supabase متاحة يُقدَّر SL ديناميكياً
           من swing lows الفعلية على فريم 15 دقيقة

        الأولوية الكاملة لحماية الصفقة المفتوحة — بغض النظر عن
        القائمة السوداء (عملة محظورة فُتحت قبل التحديث تُحمى حتى تُغلق)
        """
        _log("[Restore] 🔄 فحص صفقات مفتوحة من إعادة التشغيل...")
        try:
            # ── المرحلة 1: جلب البيانات الحقيقية من Supabase ──
            db_trades = {}
            if self.db and self.db._enabled:
                raw = self.db.get_open_trades()
                for row in raw:
                    sym = row.get("symbol", "")
                    if sym:
                        db_trades[sym] = row
                _log(f"[Restore] Supabase: {len(db_trades)} صفقة مفتوحة")
            else:
                _log("[Restore] Supabase غير متاح — سيُستخدم Fallback")

            # ── المرحلة 2: فحص الرصيد الفعلي على MEXC ──
            if self.cfg.market_futures:
                # ── Futures Restore: نجلب من open positions ──
                self._restore_futures_positions()
                return

            # ── Spot Restore ──
            bal      = self.executor.exchange.fetch_balance({"type": "spot"})
            balances = bal.get("total", {})

            restored = 0
            for asset, total_qty in balances.items():
                if asset in ("USDT", "USDC") or float(total_qty or 0) <= 0:
                    continue

                # تجاهل Leveraged tokens
                if any(asset.upper().endswith(p) for p in ["3L","3S","5L","5S"]):
                    continue

                if self.slots.used >= self.cfg.max_slots:
                    _log(f"[Restore] وصل الحد الأقصى {self.cfg.max_slots} slots — إيقاف")
                    break

                symbol = f"{asset}/USDT"
                if symbol not in self.executor.exchange.markets:
                    continue

                # تحقق من القيمة الفعلية — تجاهل الأتربة < $1
                try:
                    ticker     = self.executor.exchange.fetch_ticker(symbol)
                    live_price = float(ticker.get("last") or ticker.get("close") or 0)
                    asset_value = float(total_qty) * live_price
                except Exception:
                    live_price  = 0.0
                    asset_value = 0.0

                if asset_value < 1.0:
                    continue

                filled_qty = float(total_qty)

                # ── جلب الأوامر المفتوحة لمعرفة حالة TP1 ──
                tp1_order_id = ""
                tp1_filled   = False
                try:
                    open_orders = self.executor.exchange.fetch_open_orders(symbol)
                    limit_sells = [o for o in open_orders if o.get("side") == "sell"]
                    if limit_sells:
                        tp1_order_id = str(limit_sells[0].get("id", ""))
                    else:
                        tp1_filled = True
                        _log(f"[Restore] {symbol}: لا limit sell — مرحلة TP2/TP3 shadow")
                except Exception:
                    tp1_filled = True

                # ── المرحلة 3: البيانات الحقيقية من Supabase أو Fallback ──
                db_row = db_trades.get(symbol)

                if db_row:
                    # ✅ بيانات حقيقية من Supabase
                    entry_price = float(db_row["entry_price"])
                    stop_loss   = float(db_row["stop_loss"])
                    tp1_val     = float(db_row["tp1"])
                    tp2_val     = float(db_row["tp2"])
                    tp3_val     = float(db_row["tp3"])
                    db_trade_id = str(db_row["id"])
                    source      = "Supabase ✅"
                else:
                    # ⚠️ Fallback: تقدير من السوق الحالي
                    _log(f"[Restore] {symbol}: لا سجل في Supabase — Fallback")
                    if live_price <= 0:
                        continue

                    # سعر الدخول: من TP1 إن وجد، أو تقدير من السعر الحالي
                    open_orders_prices = []
                    try:
                        oo = self.executor.exchange.fetch_open_orders(symbol)
                        open_orders_prices = [float(o["price"]) for o in oo if o.get("side") == "sell" and o.get("price")]
                    except Exception:
                        pass

                    if open_orders_prices:
                        tp1_val     = open_orders_prices[0]
                        entry_price = tp1_val / 1.05
                    else:
                        entry_price = live_price
                        tp1_val     = entry_price * 1.05

                    # SL ديناميكي من swing lows
                    try:
                        stop_loss = calculate_micro_swing_sl(
                            self.executor.exchange, symbol, entry_price
                        )
                    except Exception:
                        stop_loss = entry_price * 0.94

                    tp2_val     = tp1_val * 1.04
                    tp3_val     = tp1_val * 1.08
                    db_trade_id = ""
                    source      = "Fallback ⚠️"

                # ── تحقق أمان: SL لا يُغلق الصفقة فوراً ──
                # إذا كان السعر الحالي أقل من SL بأكثر من 1% → SL خاطئ، اضبطه
                if live_price > 0 and stop_loss >= live_price * 0.99:
                    old_sl    = stop_loss
                    stop_loss = live_price * 0.94  # fallback آمن -6%
                    _log(
                        f"[Restore] ⚠️ {symbol}: SL={old_sl:.6g} ≥ سعر حالي={live_price:.6g} "
                        f"— تم تعديله لـ {stop_loss:.6g} (-6%) لمنع إغلاق فوري"
                    )

                qty_tp1 = round(filled_qty * 0.20, 6)
                qty_tp2 = round(filled_qty * 0.40, 6)
                qty_tp3 = round(filled_qty * 0.40, 6)

                state = SlotState(
                    symbol       = symbol,
                    buy_order_id = "restored",
                    tp1_order_id = tp1_order_id,
                    entry_price  = entry_price,
                    filled_qty   = filled_qty,
                    tp1          = tp1_val,
                    tp2          = tp2_val,
                    tp3          = tp3_val,
                    stop_loss    = stop_loss,
                    tp1_filled   = tp1_filled,
                    qty_tp1      = qty_tp1,
                    qty_tp2      = qty_tp2,
                    qty_tp3      = qty_tp3,
                    db_trade_id  = db_trade_id,
                    entry_time   = time.time(),
                )
                self.slots.occupy(state)
                restored += 1

                sl_pct  = (1 - stop_loss / entry_price) * 100 if entry_price > 0 else 0
                tp_status = "TP1 مكتمل — shadow" if tp1_filled else f"TP1={tp1_val:.6g}"
                blacklisted_note = " ⚠️ محظور — محمي حتى الإغلاق" if asset.upper() in self.cfg.blacklisted_assets else ""
                _log(
                    f"[Restore] ✅ {symbol} [{source}]: "
                    f"entry={entry_price:.6g} SL={stop_loss:.6g} (-{sl_pct:.1f}%) "
                    f"≈${asset_value:.1f} | {tp_status}{blacklisted_note}"
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

    def _calc_btc_correlation(self, symbol: str) -> float:
        """
        يحسب معامل ارتباط بيرسون بين العملة وBTC على آخر 20 شمعة 4H.
        r > 0.7  = مرتبطة ببيتكوين  → تحتاج BTC RSI filter
        r < 0.3  = مستقلة عن BTC    → تتجاوز BTC filter
        r 0.3-0.7 = ارتباط متوسط   → تخفيف جزئي
        """
        try:
            import pandas as pd
            # جلب بيانات BTC
            btc_ohlcv = self.executor.exchange.fetch_ohlcv("BTC/USDT", "4h", limit=22)
            sym_ohlcv = self.executor.exchange.fetch_ohlcv(symbol, "4h", limit=22)
            if not btc_ohlcv or not sym_ohlcv or len(btc_ohlcv) < 10:
                return 1.0
            btc_closes = pd.Series([c[4] for c in btc_ohlcv]).pct_change().dropna()
            sym_closes = pd.Series([c[4] for c in sym_ohlcv]).pct_change().dropna()
            min_len = min(len(btc_closes), len(sym_closes))
            if min_len < 5:
                return 1.0
            corr = float(btc_closes.iloc[-min_len:].corr(sym_closes.iloc[-min_len:]))
            return corr if not pd.isna(corr) else 1.0
        except Exception:
            return 1.0

    def _get_btc_rsi(self) -> float:
        """
        يجلب RSI البيتكوين على فريم 4H كمؤشر لاتجاه السوق العام.
        إذا كان RSI البيتكوين < 45 (سوق هابط عام) نرفض الصفقة.
        هذا يمنع الشراء في عملات صغيرة بينما البيتكوين ينهار.
        """
        try:
            ohlcv = self.executor.exchange.fetch_ohlcv("BTC/USDT", timeframe="4h", limit=50)
            if not ohlcv or len(ohlcv) < 20:
                return 50.0  # fail-safe: محايد
            df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"]).astype(float)
            return self._calc_rsi(df["close"])
        except Exception:
            return 50.0  # fail-safe: محايد

    def _fetch_indicators(self, symbol: str) -> Optional[dict]:
        """
        يجمع إشارتين: فريم 1D للاتجاه الكبير + فريم 4H للتوقيت الدقيق.
        الشرط: RSI مُتشبَّع بيعياً على كلا الفريمين أو على 4H مع تأكيد 1D.
        هذا يحسن توقيت الدخول ويقلل الدخول في منتصف الانهيار.
        """
        try:
            # ── فريم 1D: الاتجاه الكبير والـ Fibonacci ──
            try:
                ohlcv_1d = self.executor.exchange.fetch_ohlcv(symbol, timeframe="1d", limit=120)
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                _log(f"[Scan] OHLCV 1D fetch failed {symbol}: {type(e).__name__}")
                return None
            if not ohlcv_1d or len(ohlcv_1d) < 30:
                return None

            df_1d    = pd.DataFrame(ohlcv_1d, columns=["ts","open","high","low","close","vol"]).astype(float)
            closes_1d = df_1d["close"]
            current   = float(closes_1d.iloc[-1])
            rsi_1d    = self._calc_rsi(closes_1d)
            fib_high  = float(df_1d["high"].tail(60).max())
            fib_low   = float(df_1d["low"].tail(60).min())
            support   = detect_horizontal_support(df_1d, current)

            # ── فريم 4H: توقيت الدخول الدقيق ──
            rsi_4h = rsi_1d  # fallback
            try:
                ohlcv_4h = self.executor.exchange.fetch_ohlcv(symbol, timeframe="4h", limit=60)
                if ohlcv_4h and len(ohlcv_4h) >= 20:
                    df_4h  = pd.DataFrame(ohlcv_4h, columns=["ts","open","high","low","close","vol"]).astype(float)
                    rsi_4h = self._calc_rsi(df_4h["close"])
            except Exception:
                pass

            # ── RSI المُستخدَم للقرار: الأعلى من الاثنين (أكثر تحفظاً) ──
            # إذا كلاهما مُتشبَّع بيعي → إشارة قوية جداً
            # إذا واحد فقط → إشارة متوسطة، نأخذها لكن نسجلها
            rsi_decision = max(rsi_1d, rsi_4h)
            rsi_note = "dual✅" if (rsi_1d <= 32 and rsi_4h <= 35) else f"1D={rsi_1d:.0f}/4H={rsi_4h:.0f}"

            vol_usd = 0.0
            try:
                t       = self.executor.exchange.fetch_ticker(symbol)
                vol_usd = float(t.get("quoteVolume") or 0)
            except Exception:
                pass

            # ── EMA 9/21 على 4H للـ EMA Crossover ──
            ema9  = float(df_4h["close"].ewm(span=9,  adjust=False).mean().iloc[-1]) if "df_4h" in dir() else current
            ema21 = float(df_4h["close"].ewm(span=21, adjust=False).mean().iloc[-1]) if "df_4h" in dir() else current

            # ── RSI السابق (5 شموع) للـ RSI Bounce Confirmation ──
            rsi_4h_prev = float((100 - 100 / (1 + (
                df_4h["close"].diff().clip(lower=0).rolling(14).mean() /
                (-df_4h["close"].diff().clip(upper=0).rolling(14).mean().replace(0, 1e-9))
            ))).iloc[-6]) if "df_4h" in dir() and len(df_4h) >= 20 else rsi_4h

            # ── ATR للـ Cost Gate ──
            atr_val = calculate_atr(df_4h) if "df_4h" in dir() else 0.0

            return {
                "current":      current,
                "rsi":          rsi_decision,
                "rsi_1d":       rsi_1d,
                "rsi_4h":       rsi_4h,
                "rsi_4h_prev":  rsi_4h_prev,
                "rsi_note":     rsi_note,
                "fib_high":     fib_high,
                "fib_low":      fib_low,
                "vol_usd":      vol_usd,
                "support":      support,
                "ema9":         ema9,
                "ema21":        ema21,
                "atr":          atr_val,
            }
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

        rsi_note    = ind.get("rsi_note", "")
        rsi_4h      = ind.get("rsi_4h", ind["rsi"])
        rsi_4h_prev = ind.get("rsi_4h_prev", rsi_4h)
        ema9        = ind.get("ema9", 0.0)
        ema21       = ind.get("ema21", 0.0)
        atr_val     = ind.get("atr", 0.0)
        current     = ind["current"]

        _log(f"[Scan] {symbol}: RSI_4H={rsi_4h:.1f} (prev={rsi_4h_prev:.1f}) EMA9={ema9:.6g}/EMA21={ema21:.6g} Vol=${ind['vol_usd']/1e6:.1f}M")

        # ════════════════════════════════════════════════════════
        # الاستراتيجية 1: RSI Bounce Confirmation
        # الإصلاح الجذري — بدل الشراء عند RSI<30 مباشرة
        # ننتظر تأكيد الارتداد الفعلي
        # ════════════════════════════════════════════════════════
        if self.cfg.rsi_bounce_enabled:
            # تجاهل العملة إذا RSI=0 (بيانات غير كافية)
            if rsi_4h <= 0 or rsi_4h_prev <= 0:
                _log(f"[RSI Bounce ❌] {symbol}: RSI=0 — بيانات غير كافية")
                return
            rsi_was_oversold = rsi_4h_prev <= self.cfg.rsi_threshold
            rsi_now_bouncing = rsi_4h >= self.cfg.rsi_bounce_entry
            if not (rsi_was_oversold and rsi_now_bouncing):
                # RSI لم يرتد بعد من التشبع البيعي
                _log(
                    f"[RSI Bounce ❌] {symbol}: "
                    f"prev={rsi_4h_prev:.1f} (كان<{self.cfg.rsi_threshold}? {rsi_was_oversold}) "
                    f"now={rsi_4h:.1f} (>={self.cfg.rsi_bounce_entry}? {rsi_now_bouncing})"
                )
                return
            _log(f"[RSI Bounce ✅] {symbol}: ارتد من {rsi_4h_prev:.1f} إلى {rsi_4h:.1f} — تأكيد الارتداد")

        # ════════════════════════════════════════════════════════
        # الاستراتيجية 2: EMA Crossover 9/21
        # الدخول فقط عند تأكيد اتجاه قصير المدى صاعد
        # ════════════════════════════════════════════════════════
        if self.cfg.ema_cross_enabled and ema9 > 0 and ema21 > 0:
            ema_gap_pct = (ema21 - ema9) / ema21 * 100  # موجب = EMA9 تحت EMA21
            tolerance   = self.cfg.ema_cross_tolerance
            if ema_gap_pct > tolerance:
                _log(f"[EMA Cross ❌] {symbol}: EMA9={ema9:.6g} أقل من EMA21={ema21:.6g} بفرق {ema_gap_pct:.2f}% > {tolerance}%")
                return
            elif ema_gap_pct > 0:
                _log(f"[EMA Cross ⚠️] {symbol}: فرق {ema_gap_pct:.2f}% ضمن التسامح {tolerance}% — مسموح")
            else:
                _log(f"[EMA Cross ✅] {symbol}: EMA9={ema9:.6g} > EMA21={ema21:.6g} — اتجاه صاعد")

        # ── فلتر BTC + Correlation + Fear & Greed ──
        btc_rsi = await asyncio.get_running_loop().run_in_executor(
            None, self._get_btc_rsi
        )

        # حساب ارتباط العملة بـ BTC (آخر 20 شمعة 4H)
        btc_correlation = 1.0  # افتراضي: مرتبط كلياً
        try:
            btc_correlation = await asyncio.get_running_loop().run_in_executor(
                None, self._calc_btc_correlation, symbol
            )
            _log(f"[Correlation] {symbol}: r={btc_correlation:.2f} مع BTC")
        except Exception:
            pass

        # Fear & Greed Index — يُكمّل BTC RSI
        fg_data = {"value": 50, "signal": "neutral", "label": "Neutral"}
        try:
            async with aiohttp.ClientSession() as _s:
                fg_data = await self.pipeline.get_fear_greed_index(_s)
        except Exception:
            pass

        fg_value  = fg_data.get("value", 50)
        fg_signal = fg_data.get("signal", "neutral")
        fg_label  = fg_data.get("label", "Neutral")

        # منطق الجمع بين BTC RSI و Fear & Greed:
        # خوف شديد (≤24) يتجاوز BTC RSI Filter — فرصة نادرة لا نفوتها
        # خوف (25-49) يخفف شرط BTC RSI من 40 إلى 35
        # طمع شديد (≥75) يوقف الشراء بغض النظر عن BTC RSI
        if fg_signal == "avoid":
            _log(f"[F&G ❌] {symbol}: طمع شديد {fg_value}/100 ({fg_label}) — تجنب الشراء")
            return

        # ── منطق الفلتر يأخذ الارتباط مع BTC بعين الاعتبار ──
        # عملة مستقلة (r<0.3) لا تتأثر بـ BTC → تتجاوز الفلتر
        # عملة مرتبطة (r>0.7) تتأثر بـ BTC → تحتاج الفلتر كاملاً
        if btc_correlation < 0.3:
            _log(f"[BTC Filter ✅] {symbol}: ارتباط منخفض مع BTC (r={btc_correlation:.2f}) — تجاوز الفلتر")
        else:
            effective_btc_min = self.cfg.s1_btc_rsi_min

            # تخفيف بناءً على F&G
            if fg_signal == "strong_buy":
                effective_btc_min = max(20, self.cfg.s1_btc_rsi_min - 20)
                _log(f"[F&G ✅] {symbol}: خوف شديد {fg_value}/100 — حد BTC={effective_btc_min}")
            elif fg_signal == "buy":
                effective_btc_min = max(25, self.cfg.s1_btc_rsi_min - 15)
                _log(f"[F&G ✅] {symbol}: خوف {fg_value}/100 — حد BTC={effective_btc_min}")

            # تخفيف إضافي بناءً على الارتباط المتوسط
            if 0.3 <= btc_correlation < 0.7:
                effective_btc_min = max(20, effective_btc_min - 10)
                _log(f"[Correlation ⚠️] {symbol}: ارتباط متوسط (r={btc_correlation:.2f}) — حد BTC={effective_btc_min}")

            # BTC نفسه تشبع بيعي شديد → السماح
            if btc_rsi <= 25:
                _log(f"[BTC ⚡] {symbol}: BTC RSI={btc_rsi:.1f} تشبع بيعي — السماح")
            elif btc_rsi < effective_btc_min:
                _log(f"[BTC Filter ❌] {symbol}: RSI={btc_rsi:.1f} < {effective_btc_min} (r={btc_correlation:.2f})")
                return

        _log(f"[BTC Filter ✅] {symbol}: BTC RSI={btc_rsi:.1f} | F&G={fg_value}/100 | r={btc_correlation:.2f}")

        # ── Support Filter: وزن لا رفض ──
        # القاعدة السعرية تُحسن جودة الإشارة لكن لا تمنع الصفقة كلياً
        # RSI < 22 (تشبع بيعي شديد جداً) يتجاوز شرط القاعدة
        support_data = ind.get("support", {"has_support": False, "touches": 0, "support_level": 0.0})
        rsi_1d = ind.get("rsi_1d", ind["rsi"])
        support_score = 0  # 0=بدون دعم، 1=دعم ضعيف، 2=دعم قوي
        if support_data["has_support"]:
            support_score = 2 if support_data["touches"] >= 3 else 1
            _log(f"[Support ✅] {symbol}: {support_data['touches']} لمسات — score={support_score}")
        elif rsi_1d <= self.cfg.s1_rsi_extreme:
            # تشبع بيعي شديد → نمرر حتى بدون قاعدة
            support_score = 1
            _log(f"[Support ⚡] {symbol}: RSI_1D={rsi_1d:.1f} ≤ {self.cfg.s1_rsi_extreme} شديد جداً — تجاوز شرط القاعدة")
        else:
            # بدون قاعدة ورسي معتدل → نمرر لكن نُخبر Committee
            support_score = 0
            _log(f"[Support ⚠️] {symbol}: بدون قاعدة سعرية — Committee سيحكم")

        # ── Layer 1: RSI + CMC + LunarCrush ──
        # نستخدم rsi_4h_prev (قبل الارتداد) لأن RSI بعد الارتداد سيكون > 30
        # RSI Bounce يعني: كان < 30 ثم ارتد — Layer 1 يجب أن يرى القاع لا الارتداد
        rsi_for_l1 = min(rsi_4h_prev, rsi_4h) if rsi_4h_prev > 0 else ind["rsi"]
        passed, reason = await self.pipeline.layer1_pass(session, symbol, rsi_for_l1, vol_usd=ind.get("vol_usd", 0))
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
        # إضافة support_score للـ support_data قبل تمريرها للـ committee
        support_data["score"] = support_score
        # Order Book Signal
        ob_data = {"ratio": 1.0, "signal": "neutral"}
        try:
            async with aiohttp.ClientSession() as _s:
                ob_data = await self.pipeline.get_order_book_signal(_s, symbol)
            if ob_data["signal"] == "sell_pressure":
                _log(f"[OB ⚠️] {symbol}: ضغط بيع ratio={ob_data['ratio']:.2f} — تحذير للـ Committee")
            elif ob_data["signal"] == "buy_pressure":
                _log(f"[OB ✅] {symbol}: ضغط شراء ratio={ob_data['ratio']:.2f}")
        except Exception:
            pass

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

        if self.cfg.market_futures:
            # ── Futures: SL وTP محسوبة من إعدادات Railway مباشرة ──
            entry_est  = ind["current"]
            liq_price  = calculate_liquidation_price(
                entry_est, self.cfg.futures_leverage,
                self.cfg.futures_margin_mode
            )
            stop_loss  = calculate_safe_sl_futures(
                entry_est,
                self.cfg.futures_leverage,
                self.cfg.futures_sl_pct,
                liq_price,
                self.cfg.futures_liq_buffer,
            )
            # أهداف Futures أصغر (بالرافعة تصبح مضخَّمة)
            targets = {
                "tp1": entry_est * (1 + self.cfg.futures_tp1_pct / 100),
                "tp2": entry_est * (1 + self.cfg.futures_tp2_pct / 100),
                "tp3": entry_est * (1 + self.cfg.futures_tp2_pct / 100 * 1.5),
            }
            _log(
                f"[Futures Targets] {symbol}: "
                f"Liq={liq_price:.8g} SL={stop_loss:.8g} "
                f"TP1={targets['tp1']:.8g} TP2={targets['tp2']:.8g}"
            )
        else:
            # ── Spot: SL ديناميكي من swing lows ──
            raw_sl    = await asyncio.get_running_loop().run_in_executor(
                None, calculate_micro_swing_sl,
                self.executor.exchange, symbol, ind["current"]
            )
            sl_min = ind["current"] * (1 - self.cfg.s1_sl_max / 100)
            sl_max = ind["current"] * (1 - self.cfg.s1_sl_min / 100)
            stop_loss = max(sl_min, min(raw_sl, sl_max))

        # ════════════════════════════════════════════════════════
        # الاستراتيجية 3: Cost Gate
        # رفض الصفقة إذا كانت الحركة المتوقعة لا تغطي الرسوم + هامش ربح
        # ════════════════════════════════════════════════════════
        if self.cfg.cost_gate_enabled:
            tp1_distance_pct = (targets["tp1"] - ind["current"]) / ind["current"] * 100
            if tp1_distance_pct < self.cfg.cost_gate_pct:
                _log(
                    f"[Cost Gate ❌] {symbol}: TP1 بعيد {tp1_distance_pct:.2f}% "
                    f"< حد أدنى {self.cfg.cost_gate_pct}% — لا يغطي الرسوم"
                )
                return
            _log(f"[Cost Gate ✅] {symbol}: TP1 = +{tp1_distance_pct:.2f}% > {self.cfg.cost_gate_pct}%")

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
            f"RSI Bounce: {rsi_4h_prev:.1f}→{rsi_4h:.1f} | "
            f"EMA9/21: {'✅' if ema9>ema21 else '❌'} | "
            f"DeepSeek={result['ds_vote']} | Llama={result['llama_vote']} | "
            f"RSS={rss_sentiment} | Galaxy={lunar_data.get('galaxy_score', 0):.0f}"
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
            f"🧠 DS={result['ds_vote']} | Llama={result['llama_vote']} | RSS={rss_sentiment}\n"
            f"😱 F&G: {fg_value}/100 ({fg_label}) | BTC RSI={btc_rsi:.1f}\n"
            f"📈 RSI Bounce: {rsi_4h_prev:.1f}→{rsi_4h:.1f} | EMA9/21: {'✅' if ema9>ema21 else '❌'}\n"
            + (f"📊 Order Book: ratio={ob_data['ratio']:.2f} ({ob_data['signal']})\n" if ob_data['ratio'] != 1.0 else "")
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
            # Sentiment متعدد المصادر: CoinGecko + Fear&Greed + RSS
            # LunarCrush أُستبدل — لا حاجة لمفتاحه
            try:
                async with session.get(
                    "https://api.alternative.me/fng/?limit=1",
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status != 200:
                        issues.append(f"⚠️ <b>Fear&Greed API:</b> HTTP {resp.status}")
            except Exception:
                issues.append("⏱ <b>Fear&Greed API:</b> لا استجابة")

            # ── CoinGecko ──
            if self.cfg.coingecko_api_key:
                try:
                    _is_demo = self.cfg.coingecko_api_key.startswith("CG-")
                    _base = "https://api.coingecko.com" if _is_demo else "https://pro-api.coingecko.com"
                    _hkey = "x-cg-demo-api-key" if _is_demo else "x-cg-pro-api-key"
                    async with session.get(
                        f"{_base}/api/v3/ping",
                        headers={_hkey: self.cfg.coingecko_api_key},
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as resp:
                        if resp.status == 401:
                            issues.append("🔑 <b>CoinGecko:</b> مفتاح API خاطئ (401)")
                        elif resp.status == 429:
                            issues.append("⏱ <b>CoinGecko:</b> تجاوز الحد المسموح (429)")
                        elif resp.status == 400:
                            pass  # 400 طبيعي مع Free tier — CoinGecko غير مستخدمة في القرارات
                        elif resp.status not in (200,):
                            issues.append(f"⚠️ <b>CoinGecko:</b> HTTP {resp.status}")
                except asyncio.TimeoutError:
                    issues.append("⏱ <b>CoinGecko:</b> لا استجابة (timeout)")
                except Exception as e:
                    issues.append(f"⚠️ <b>CoinGecko:</b> {str(e)[:50]}")

        # ── إرسال التنبيه مرة كل 12 ساعة فقط (بدل كل دورة) ──
        now = time.time()
        alert_interval = 12 * 3600  # 12 ساعة

        if issues:
            _log(f"[API Health] ⚠️ {len(issues)} مشكلة: {', '.join(i[:30] for i in issues)}")
            if (now - self._last_api_health_alert) >= alert_interval:
                msg = (
                    "🔧 <b>تنبيه: أدوات متوقفة أو غير متاحة</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━\n\n"
                    + "\n".join(f"   • {i}" for i in issues)
                    + "\n\n<i>راجع مفاتيح API في Railway وتأكد من الرصيد المتاح.</i>"
                )
                await self._send_telegram(msg)
                self._last_api_health_alert = now
                _log("[API Health] تنبيه Telegram أُرسل — التالي بعد 12 ساعة")
            else:
                remaining = int((alert_interval - (now - self._last_api_health_alert)) / 3600)
                _log(f"[API Health] تنبيه مؤجل — التالي بعد ~{remaining} ساعة")
        else:
            _log("[API Health] ✅ كل الأدوات تعمل بشكل طبيعي")


    # ─────────────────────────────────────────────────────────
    # استراتيجية الزخم — RSI 50-65 مع كسر مستوى مقاومة
    # تعمل بالتوازي مع استراتيجية التشبع البيعي في نفس الـ slots
    # ─────────────────────────────────────────────────────────

    def _fetch_indicators_momentum(self, symbol: str) -> Optional[dict]:
        """
        يفحص إشارات الزخم الصاعد على فريم 4H:
        - RSI بين 50-65: في منطقة صعود لكن لم يتشبع شراءً بعد
        - السعر فوق MA20 (الزخم الإيجابي)
        - حجم تداول متزايد (تأكيد الحركة)
        - كسر مستوى مقاومة أفقي سابق
        """
        try:
            ohlcv = self.executor.exchange.fetch_ohlcv(symbol, timeframe="4h", limit=80)
            if not ohlcv or len(ohlcv) < 30:
                return None

            df     = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"]).astype(float)
            closes = df["close"]
            highs  = df["high"]
            vols   = df["vol"]
            current = float(closes.iloc[-1])

            rsi = self._calc_rsi(closes, period=14)

            # شرط RSI: بين 50-65 (زخم صاعد، ليس مفرط الشراء)
            if not (self.cfg.s2_rsi_min <= rsi <= self.cfg.s2_rsi_max):
                return None

            # MA20: السعر فوقها → زخم إيجابي
            ma20 = float(closes.tail(20).mean())
            if current < ma20 * 0.99:  # هامش 1%
                return None

            # حجم متزايد: آخر شمعة فوق متوسط الـ 20
            avg_vol = float(vols.tail(20).mean())
            vol_ratio = float(vols.iloc[-1]) / avg_vol if avg_vol > 0 else 1.0
            if vol_ratio < self.cfg.s2_vol_ratio_min:
                return None

            # كسر مقاومة: السعر الحالي أعلى من أعلى نقطة في آخر 20 شمعة (عدا آخر 3)
            recent_high = float(highs.iloc[-23:-3].max())
            breakout = current > recent_high * (1 + self.cfg.s2_breakout_margin / 100)

            if not breakout:
                return None

            # أهداف Fibonacci للزخم (أصغر من التشبع البيعي)
            fib_high = float(highs.tail(40).max())
            fib_low  = float(df["low"].tail(40).min())

            # حجم تداول
            vol_usd = 0.0
            try:
                t = self.executor.exchange.fetch_ticker(symbol)
                vol_usd = float(t.get("quoteVolume") or 0)
            except Exception:
                pass

            return {
                "current":   current,
                "rsi":       rsi,
                "rsi_1d":    rsi,
                "rsi_4h":    rsi,
                "rsi_note":  f"momentum⚡ RSI={rsi:.0f} [{self.cfg.s2_rsi_min:.0f}-{self.cfg.s2_rsi_max:.0f}] vol×{vol_ratio:.1f}",
                "fib_high":  fib_high,
                "fib_low":   fib_low,
                "vol_usd":   vol_usd,
                "support":   {"has_support": True, "touches": 2, "support_level": recent_high, "score": 2},
                "strategy":  "momentum",
                "breakout_level": recent_high,
                "vol_ratio": vol_ratio,
                "ma20":      ma20,
            }
        except Exception as e:
            _log(f"[Momentum] ❌ {symbol}: {str(e)[:60]}")
            return None

    async def _process_momentum_candidate(
        self,
        session:         aiohttp.ClientSession,
        symbol:          str,
        initial_balance: float = 0.0,
    ):
        """
        يعالج مرشحات استراتيجية الزخم — مسار مستقل عن التشبع البيعي.
        أهداف أصغر لكن نسبة نجاح أعلى.
        كل المعاملات قابلة للتعديل من Railway.
        """
        # فحص تفعيل الاستراتيجية من Railway
        if not self.cfg.s2_enabled:
            return

        with self._processing_lock:
            if symbol in self._processing_symbols:
                return
            if not self.slots.is_vacant(symbol):
                return
            self._processing_symbols.add(symbol)

        try:
            ind = await asyncio.get_running_loop().run_in_executor(
                None, self._fetch_indicators_momentum, symbol
            )
            if not ind:
                return

            _log(f"[Momentum ⚡] {symbol}: {ind['rsi_note']} breakout={ind['breakout_level']:.6g}")

            # فلتر BTC: في استراتيجية الزخم نشترط BTC RSI > 50 (أقوى)
            btc_rsi = await asyncio.get_running_loop().run_in_executor(
                None, self._get_btc_rsi
            )
            if btc_rsi < self.cfg.s2_btc_rsi_min:
                _log(f"[Momentum BTC ❌] {symbol}: BTC RSI={btc_rsi:.1f} < {self.cfg.s2_btc_rsi_min}")
                return

            # فلتر CMC
            async with aiohttp.ClientSession() as s:
                cmc = await self.pipeline.get_cmc_data(s, symbol)
            if not cmc.get("valid"):
                return
            if cmc["volume_24h"] < self.cfg.min_volume_usd:
                return
            if cmc["rank"] > self.cfg.cmc_top_rank:
                return

            # Shariah filter
            base = symbol.split("/")[0].upper()
            if self.cfg.shariah_filter_enabled and base in self.cfg.blacklisted_assets:
                return

            # Committee بـ prompt مخصص للزخم
            lunar_data    = {"galaxy_score": 50, "social_volume": 0, "vote": "neutral"}
            rss_sentiment = "neutral"
            whale_data    = {"whale_alert": "none", "transactions": 0}

            try:
                async with aiohttp.ClientSession() as s:
                    lunar_data    = await self.pipeline.get_lunar_score(s, symbol)
                    rss_sentiment = await self.pipeline.get_rss_sentiment(s)
                    whale_data    = await self.pipeline.get_whale_activity(s, symbol)
            except Exception:
                pass

            result = await self.committee.run(
                symbol        = symbol,
                rsi           = ind["rsi"],
                vol_m         = ind["vol_usd"] / 1e6,
                entry         = ind["current"],
                fib_high      = ind["fib_high"],
                fib_low       = ind["fib_low"],
                rss_sentiment = rss_sentiment,
                lunar_data    = lunar_data,
                support       = ind["support"],
                whale_data    = whale_data,
            )

            if not result["approved"]:
                _log(f"[Momentum L2 ❌] {symbol}: DS={result['ds_vote']} Llama={result['llama_vote']}")
                return

            # SL وTP من متغيرات Railway
            entry_price = ind["current"]
            try:
                raw_sl    = calculate_micro_swing_sl(self.executor.exchange, symbol, entry_price)
                sl_target = entry_price * (1 - self.cfg.s2_sl_pct / 100)
                stop_loss = max(sl_target * 0.995, min(raw_sl, sl_target * 1.005))
            except Exception:
                stop_loss = entry_price * (1 - self.cfg.s2_sl_pct / 100)

            targets = {
                "tp1": entry_price * (1 + self.cfg.s2_tp1_pct / 100),
                "tp2": entry_price * (1 + self.cfg.s2_tp2_pct / 100),
                "tp3": entry_price * (1 + self.cfg.s2_tp3_pct / 100),
            }

            if not self.slots.is_vacant(symbol):
                return

            # Balance Guard
            try:
                bal = self.executor.exchange.fetch_balance({"type": "spot"})
                free_usdt = float(bal.get("USDT", {}).get("free", 0) or bal.get("free", {}).get("USDT", 0))
                if free_usdt < self.cfg.capital:
                    return
                existing_qty = float(bal.get(base, {}).get("total", 0) or bal.get("total", {}).get(base, 0) or 0)
                if existing_qty * entry_price > 1.0:
                    return
            except Exception:
                return

            state = await asyncio.get_running_loop().run_in_executor(
                None, self.executor.execute_full_trade,
                symbol, entry_price,
                targets["tp1"], targets["tp2"], targets["tp3"], stop_loss,
            )

            if not state:
                return

            self.slots.occupy(state)

            # تسجيل في Supabase
            trade_id = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: self.db.insert_trade(
                    state             = state,
                    capital           = self.cfg.capital,
                    ds_vote           = result["ds_vote"],
                    llama_vote        = result["llama_vote"],
                    rss_sentiment     = rss_sentiment,
                    galaxy_score      = float(lunar_data.get("galaxy_score", 0)),
                    committee_summary = f"Momentum⚡ RSI={ind['rsi']:.0f} breakout={ind['breakout_level']:.6g} vol×{ind['vol_ratio']:.1f}",
                )
            )
            if trade_id:
                self.slots.update_state(symbol, db_trade_id=trade_id)

            tp1_pct = (state.tp1 / state.entry_price - 1) * 100
            tp2_pct = (state.tp2 / state.entry_price - 1) * 100
            tp3_pct = (state.tp3 / state.entry_price - 1) * 100
            sl_pct  = (1 - state.stop_loss / state.entry_price) * 100

            await self._send_telegram(
                "⚡ <b>صفقة زخم جديدة — Momentum</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📌 <b>العملة:</b> <code>{symbol}</code>\n"
                f"💰 <b>رأس المال:</b> <code>${self.cfg.capital:.2f}</code>\n"
                f"📈 <b>سعر الدخول:</b> <code>{state.entry_price:.8g}</code>\n"
                f"📊 <b>RSI 4H:</b> <code>{ind['rsi']:.1f}</code> | حجم ×{ind['vol_ratio']:.1f}\n\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "🎯 <b>خطة الخروج (أهداف مضغوطة)</b>\n"
                f"TP1 (+{tp1_pct:.1f}%): <code>{state.tp1:.8g}</code>\n"
                f"TP2 (+{tp2_pct:.1f}%): <code>{state.tp2:.8g}</code>\n"
                f"TP3 (+{tp3_pct:.1f}%): <code>{state.tp3:.8g}</code>\n"
                f"🛡 SL (-{sl_pct:.1f}%): <code>{state.stop_loss:.8g}</code>\n"
            )

        finally:
            with self._processing_lock:
                self._processing_symbols.discard(symbol)


    def _reassess_restored_positions(self):
        """
        إعادة تقييم الصفقات المُستردة بـ Fallback — يعمل مرة واحدة عند Restart.

        المنطق:
        - إذا كان السعر الحالي قريباً من TP1 (أقل من 15% بعيد) → استمر كما هو
        - إذا كان TP1 بعيداً جداً (> 15%) → أعد حساب أقرب هدف واقعي:
            * إذا كان RSI بدأ يرتد (> 35) → TP جديد عند +5% من الحالي
            * إذا كان RSI لا يزال منخفضاً (≤ 35) → انتظر قليلاً (ربما ارتداد قادم)
            * إذا كانت الخسارة > 30% من سعر الدخول المقدَّر → أغلق فوراً بـ market sell

        لا يُغلق الصفقة بخسارة إلا إذا كانت الخسارة كبيرة جداً وبلا أمل تقني.
        """
        states = self.slots.get_all_states()
        if not states:
            return

        # نعمل فقط على الصفقات التي استُردت بـ Fallback (لا بيانات دقيقة)
        fallback_states = [s for s in states if not s.db_trade_id]
        if not fallback_states:
            _log("[Reassess] ✅ كل الصفقات لها بيانات دقيقة — لا حاجة لإعادة تقييم")
            return

        _log(f"[Reassess] 🔍 إعادة تقييم {len(fallback_states)} صفقة Fallback...")
        actions = []

        for state in fallback_states:
            symbol = state.symbol

            # جلب السعر الحالي
            try:
                ticker     = self.executor.exchange.fetch_ticker(symbol)
                curr_price = float(ticker.get("last") or ticker.get("close") or 0)
            except Exception:
                continue
            if curr_price <= 0:
                continue

            # جلب RSI على 4H للتقييم
            rsi_4h = 50.0
            try:
                ohlcv = self.executor.exchange.fetch_ohlcv(symbol, timeframe="4h", limit=20)
                if ohlcv and len(ohlcv) >= 15:
                    import pandas as pd
                    df    = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"]).astype(float)
                    delta = df["close"].diff()
                    gain  = delta.clip(lower=0).rolling(14).mean()
                    loss  = (-delta.clip(upper=0)).rolling(14).mean()
                    rs    = gain / loss.replace(0, 1e-9)
                    rsi_4h = float((100 - 100 / (1 + rs)).iloc[-1])
            except Exception:
                pass

            tp1 = state.tp1
            if tp1 <= 0:
                continue

            # نسبة بُعد السعر الحالي عن TP1
            dist_to_tp1_pct = (tp1 - curr_price) / curr_price * 100

            # نسبة الخسارة من سعر الدخول المقدَّر
            entry = state.entry_price
            loss_pct = (entry - curr_price) / entry * 100 if entry > 0 else 0

            _log(
                f"[Reassess] {symbol}: curr={curr_price:.6g} "
                f"TP1={tp1:.6g} (بُعد {dist_to_tp1_pct:.1f}%) "
                f"RSI_4H={rsi_4h:.1f} خسارة={loss_pct:.1f}%"
            )

            # ── القرار ──

            # الحالة 1: قريب من TP1 — استمر
            if dist_to_tp1_pct <= 15:
                _log(f"[Reassess] ✅ {symbol}: قريب من TP1 — استمر")
                actions.append(f"✅ <b>{symbol}</b>: قريب من TP1 ({dist_to_tp1_pct:.1f}%) — استمر")
                continue

            # الحالة 2: خسارة > 30% — أغلق فوراً
            if loss_pct > 30:
                _log(f"[Reassess] 🔻 {symbol}: خسارة {loss_pct:.1f}% > 30% — إغلاق فوري")
                try:
                    self.executor.emergency_market_sell(symbol, state.filled_qty)
                    self.slots.release(symbol)
                    actions.append(
                        f"🔻 <b>{symbol}</b>: خسارة {loss_pct:.1f}% تجاوزت 30% — "
                        f"أُغلقت بـ market sell عند {curr_price:.6g}"
                    )
                except Exception as e:
                    _log(f"[Reassess] ❌ فشل الإغلاق {symbol}: {e}")
                continue

            # الحالة 3: RSI ≤ 35 (ارتداد محتمل) — انتظر
            if rsi_4h <= 35:
                _log(f"[Reassess] ⏳ {symbol}: RSI={rsi_4h:.1f} ≤ 35 — ارتداد محتمل، انتظر")
                actions.append(
                    f"⏳ <b>{symbol}</b>: TP1 بعيد ({dist_to_tp1_pct:.1f}%) "
                    f"لكن RSI={rsi_4h:.1f} يشير لارتداد محتمل — انتظر"
                )
                continue

            # الحالة 4: RSI > 35 وTP1 بعيد جداً — أعد حساب هدف واقعي
            # الهدف الجديد: +5% من السعر الحالي (هدف قابل للتحقيق قريباً)
            new_tp = curr_price * 1.05
            new_sl = curr_price * 0.96  # -4% من الحالي كحماية

            # إلغاء TP1 القديم وإعادة وضع limit sell عند الهدف الجديد
            try:
                # إلغاء الأوامر المفتوحة
                if state.tp1_order_id and not state.tp1_filled:
                    try:
                        self.executor.exchange.cancel_all_orders(symbol)
                        _log(f"[Reassess] {symbol}: TP1 القديم أُلغي")
                    except Exception:
                        pass
                    import time as _time
                    _time.sleep(0.5)

                # وضع limit sell جديد عند الهدف الواقعي
                qty_tp1  = self.executor._apply_step_size(symbol, state.filled_qty * 0.40)
                new_tp_p = float(self.executor.exchange.price_to_precision(symbol, new_tp))
                o = self.executor.exchange.create_limit_sell_order(symbol, qty_tp1, new_tp_p)

                # تحديث الـ slot
                self.slots.update_state(
                    symbol,
                    tp1         = new_tp,
                    tp2         = new_tp * 1.04,
                    tp3         = new_tp * 1.08,
                    stop_loss   = new_sl,
                    tp1_order_id = o["id"],
                    tp1_filled  = False,
                )

                tp1_pct = (new_tp / curr_price - 1) * 100
                _log(
                    f"[Reassess] ✅ {symbol}: هدف جديد "
                    f"TP1={new_tp:.6g} (+{tp1_pct:.1f}%) "
                    f"SL={new_sl:.6g} (-4%) ID:{o['id']}"
                )
                actions.append(
                    f"🔄 <b>{symbol}</b>: أُعيد ضبط الأهداف\n"
                    f"   TP1 القديم: {tp1:.6g} (بعيد {dist_to_tp1_pct:.1f}%)\n"
                    f"   TP1 الجديد: {new_tp:.6g} (+{tp1_pct:.1f}% من الحالي)\n"
                    f"   SL الجديد: {new_sl:.6g} (-4%)"
                )
            except Exception as e:
                _log(f"[Reassess] ❌ {symbol}: فشل إعادة الضبط: {e}")
                actions.append(f"❌ <b>{symbol}</b>: فشل إعادة ضبط الأهداف — {str(e)[:50]}")

        # ── إرسال تقرير Telegram ──
        if actions:
            import urllib.request, json as _json
            try:
                msg = (
                    "🔄 <b>إعادة تقييم الصفقات عند Restart</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━\n\n"
                    + "\n\n".join(actions)
                )
                payload = _json.dumps({
                    "chat_id":    self.cfg.telegram_chat_id,
                    "text":       MEXC_HEADER + msg,
                    "parse_mode": "HTML"
                }).encode()
                req = urllib.request.Request(
                    f"https://api.telegram.org/bot{self.cfg.telegram_token}/sendMessage",
                    data=payload, headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(req, timeout=10)
            except Exception as e:
                _log(f"[Reassess] Telegram error: {e}")

        _log(f"[Reassess] اكتمل — {len(actions)} إجراء")


    async def _process_futures_candidate(
        self,
        session:         aiohttp.ClientSession,
        symbol:          str,
        initial_balance: float = 0.0,
    ):
        """
        مسار معالجة Futures — مستقل عن Spot.
        يستخدم futures_executor المتصل بـ defaultType="swap".

        الفلاتر:
        - RSI ≤ 30 على 4H (أسرع من 1D لأن Futures يحتاج توقيتاً أدق)
        - BTC RSI > s2_btc_rsi_min (أعلى من Spot لأن الرافعة تضخم المخاطر)
        - CMC Top + Volume
        - Committee موافقة

        الأهداف: من FUTURES_TP1_PCT وFUTURES_TP2_PCT (أصغر من Spot)
        SL: محسوب بأمان من Liquidation Price الفعلي
        """
        if not self.futures_executor:
            return

        with self._processing_lock:
            if symbol in self._processing_symbols:
                return
            if not self.slots.is_vacant(symbol):
                return
            self._processing_symbols.add(symbol)

        try:
            # ── جلب مؤشرات Futures (4H أساساً) ──
            ind = await asyncio.get_running_loop().run_in_executor(
                None, self._fetch_indicators_futures, symbol
            )
            if not ind:
                return

            _log(f"[Futures Scan] {symbol}: RSI_4H={ind['rsi']:.1f} Vol=${ind['vol_usd']/1e6:.1f}M")

            # ── فلتر BTC (أصعب من Spot) ──
            btc_rsi = await asyncio.get_running_loop().run_in_executor(
                None, self._get_btc_rsi
            )
            if btc_rsi < self.cfg.s2_btc_rsi_min:
                _log(f"[Futures BTC ❌] {symbol}: BTC RSI={btc_rsi:.1f} < {self.cfg.s2_btc_rsi_min}")
                return

            # ── فلتر CMC ──
            async with aiohttp.ClientSession() as s:
                spot_symbol = symbol.replace(":USDT", "")
                cmc = await self.pipeline.get_cmc_data(s, spot_symbol)
            if not cmc.get("valid"):
                return
            if cmc["volume_24h"] < self.cfg.min_volume_usd * 2:  # حجم أعلى للـ Futures
                return
            if cmc["rank"] > self.cfg.cmc_top_rank:
                return

            # ── Shariah Filter ──
            base = symbol.split("/")[0].upper()
            if self.cfg.shariah_filter_enabled and base in self.cfg.blacklisted_assets:
                return

            # ── Committee ──
            lunar_data    = {"galaxy_score": 50, "social_volume": 0, "vote": "neutral"}
            rss_sentiment = "neutral"
            whale_data    = {"whale_alert": "none", "transactions": 0}
            try:
                async with aiohttp.ClientSession() as s:
                    lunar_data    = await self.pipeline.get_lunar_score(s, spot_symbol)
                    rss_sentiment = await self.pipeline.get_rss_sentiment(s)
                    whale_data    = await self.pipeline.get_whale_activity(s, spot_symbol)
            except Exception:
                pass

            support_data = ind.get("support", {"has_support": False, "touches": 0, "support_level": 0.0, "score": 0})
            result = await self.committee.run(
                symbol        = spot_symbol,
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

            if not result["approved"]:
                _log(f"[Futures L2 ❌] {symbol}: DS={result['ds_vote']} Llama={result['llama_vote']}")
                return

            if not self.slots.is_vacant(symbol):
                return

            # ── Funding Rate Check ──
            try:
                async with aiohttp.ClientSession() as _s:
                    fr_data = await self.pipeline.get_funding_rates(_s, [symbol])
                    fr = fr_data.get(symbol, {"rate": 0, "signal": "neutral"})
                    if fr["signal"] == "costly":
                        _log(f"[Funding Rate ❌] {symbol}: rate={fr['rate']:.4%} — تكلفة عالية جداً")
                        return
                    _log(f"[Funding Rate ✅] {symbol}: rate={fr['rate']:.4%} ({fr['signal']})")
            except Exception:
                pass  # Funding Rate اختياري — لا يوقف الصفقة عند الفشل

            # ── Balance Guard — Futures Wallet ──
            try:
                bal = self.futures_executor.exchange.fetch_balance({"type": "swap"})
                free_usdt = float(
                    bal.get("USDT", {}).get("free", 0) or
                    bal.get("free", {}).get("USDT", 0)
                )
                if free_usdt < self.cfg.capital:
                    _log(f"[Futures Balance] رصيد Futures غير كافٍ: ${free_usdt:.2f}")
                    return
            except Exception as e:
                _log(f"[Futures Balance] {e}")
                return

            entry_price = ind["current"]

            # ── ATR من المؤشرات (محسوب مسبقاً) أو إعادة الحساب ──
            atr = ind.get("atr", 0.0)
            if atr <= 0:
                try:
                    atr = calculate_atr(
                        pd.DataFrame(
                            self.futures_executor.exchange.fetch_ohlcv(symbol, "4h", limit=20),
                            columns=["ts","open","high","low","close","vol"]
                        ).astype(float)
                    )
                except Exception:
                    pass

            if self.cfg.futures_dynamic_lev and atr > 0:
                dynamic_lev, lev_reason = calculate_dynamic_leverage(
                    entry_price   = entry_price,
                    atr           = atr,
                    rsi           = ind["rsi"],
                    support_score = support_data.get("score", 0),
                    whale_signal  = whale_data.get("whale_alert", "none"),
                    galaxy_score  = lunar_data.get("galaxy_score", 50),
                    sl_pct        = self.cfg.futures_sl_pct,
                    lev_min       = self.cfg.futures_leverage_min,
                    lev_max       = self.cfg.futures_leverage_max,
                )
                _log(f"[Dynamic Lev] {symbol}: {lev_reason}")
            else:
                dynamic_lev = self.cfg.futures_leverage
                lev_reason  = f"ثابت={dynamic_lev}x (FUTURES_DYNAMIC_LEVERAGE=false)"

            # ── حساب Liquidation وSL بالرافعة الديناميكية ──
            liq_price = calculate_liquidation_price(
                entry_price, dynamic_lev, self.cfg.futures_margin_mode
            )
            stop_loss = calculate_safe_sl_futures(
                entry_price, dynamic_lev,
                self.cfg.futures_sl_pct, liq_price, self.cfg.futures_liq_buffer
            )
            tp1 = entry_price * (1 + self.cfg.futures_tp1_pct / 100)
            tp2 = entry_price * (1 + self.cfg.futures_tp2_pct / 100)
            tp3 = tp2 * 1.02

            # ── تحديث الرافعة في Config مؤقتاً للـ executor ──
            object.__setattr__(self.futures_executor.cfg, "futures_leverage", dynamic_lev)

            # ── تنفيذ الصفقة عبر futures_executor ──
            state = await asyncio.get_running_loop().run_in_executor(
                None, self.futures_executor.execute_full_trade,
                symbol, entry_price, tp1, tp2, tp3, stop_loss,
            )

            if not state:
                _log(f"[Futures L3 ❌] {symbol}: تنفيذ فشل")
                return

            self.slots.occupy(state)

            # تسجيل في DB
            trade_id = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: self.db.insert_trade(
                    state             = state,
                    capital           = self.cfg.capital,
                    ds_vote           = result["ds_vote"],
                    llama_vote        = result["llama_vote"],
                    rss_sentiment     = rss_sentiment,
                    galaxy_score      = float(lunar_data.get("galaxy_score", 0)),
                    committee_summary = (
                        f"Futures⚡ RSI_4H={ind['rsi']:.0f} "
                        f"Lev={self.cfg.futures_leverage}x "
                        f"Liq={liq_price:.6g}"
                    ),
                )
            )
            if trade_id:
                self.slots.update_state(symbol, db_trade_id=trade_id)

            lev     = dynamic_lev
            tp1_eff = (tp1 / entry_price - 1) * 100 * lev
            sl_eff  = (1 - stop_loss / entry_price) * 100 * lev
            atr_pct = (atr / entry_price * 100) if entry_price > 0 else 0

            await self._send_telegram(
                "⚡ <b>صفقة Futures جديدة — Long</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📌 <b>العملة:</b> <code>{symbol}</code>\n"
                f"💰 <b>رأس المال:</b> <code>${self.cfg.capital:.2f}</code> × {lev}x "
                f"({'ديناميكي 🧠' if self.cfg.futures_dynamic_lev else 'ثابت'})\n"
                f"📈 <b>سعر الدخول:</b> <code>{entry_price:.8g}</code>\n"
                f"📦 <b>الكمية:</b> <code>{state.filled_qty:.4f}</code>\n"
                f"📊 <b>ATR 4H:</b> <code>{atr_pct:.2f}%</code> (تقلب يومي)\n\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 TP1: <code>{tp1:.8g}</code> (+{self.cfg.futures_tp1_pct:.1f}% | فعلي +{tp1_eff:.1f}%)\n"
                f"🎯 TP2: <code>{tp2:.8g}</code> (+{self.cfg.futures_tp2_pct:.1f}% | فعلي +{self.cfg.futures_tp2_pct*lev:.1f}%)\n"
                f"🛡 SL:  <code>{stop_loss:.8g}</code> (-{self.cfg.futures_sl_pct:.1f}% | فعلي -{sl_eff:.1f}%)\n"
                f"☠️ Liq: <code>{liq_price:.8g}</code>\n\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"🧠 DS={result['ds_vote']} | Llama={result['llama_vote']} | BTC RSI={btc_rsi:.1f}\n"
                f"📐 {lev_reason[:80]}"
            )

        finally:
            with self._processing_lock:
                self._processing_symbols.discard(symbol)

    def _fetch_indicators_futures(self, symbol: str) -> Optional[dict]:
        """
        مؤشرات مُحسَّنة للـ Futures — تعتمد على 4H كفريم رئيسي.
        Futures يحتاج توقيتاً أدق لأن الرافعة تضخم الحركات.
        """
        try:
            # الرمز الأساسي بدون :USDT للبحث في OHLCV
            spot_symbol = symbol.replace(":USDT", "")
            # نحاول أولاً على نفس الـ futures symbol، ثم spot
            try:
                ohlcv_4h = self.futures_executor.exchange.fetch_ohlcv(
                    symbol, timeframe="4h", limit=60
                )
            except Exception:
                ohlcv_4h = self.executor.exchange.fetch_ohlcv(
                    spot_symbol, timeframe="4h", limit=60
                )

            if not ohlcv_4h or len(ohlcv_4h) < 20:
                return None

            df_4h   = pd.DataFrame(ohlcv_4h, columns=["ts","open","high","low","close","vol"]).astype(float)
            closes  = df_4h["close"]
            current = float(closes.iloc[-1])
            rsi_4h  = self._calc_rsi(closes)

            # RSI فلتر أصعب للـ Futures
            if rsi_4h > self.cfg.rsi_threshold:
                return None

            fib_high = float(df_4h["high"].tail(40).max())
            fib_low  = float(df_4h["low"].tail(40).min())
            support  = detect_horizontal_support(df_4h, current)
            support["score"] = 2 if support.get("touches", 0) >= 3 else (1 if support.get("has_support") else 0)

            vol_usd = 0.0
            try:
                t = self.futures_executor.exchange.fetch_ticker(symbol)
                vol_usd = float(t.get("quoteVolume") or 0)
            except Exception:
                try:
                    t = self.executor.exchange.fetch_ticker(spot_symbol)
                    vol_usd = float(t.get("quoteVolume") or 0)
                except Exception:
                    pass

            # حساب ATR هنا ليُستخدم لاحقاً في الرافعة الديناميكية
            atr = calculate_atr(df_4h)
            atr_pct = (atr / current * 100) if current > 0 else 0

            return {
                "current":  current,
                "rsi":      rsi_4h,
                "rsi_1d":   rsi_4h,
                "rsi_4h":   rsi_4h,
                "rsi_note": f"futures⚡ RSI_4H={rsi_4h:.1f} ATR={atr_pct:.2f}%",
                "fib_high": fib_high,
                "fib_low":  fib_low,
                "vol_usd":  vol_usd,
                "support":  support,
                "atr":      atr,
            }
        except Exception as e:
            _log(f"[Futures Scan] ❌ {symbol}: {str(e)[:60]}")
            return None


    async def _send_smart_report(self):
        """
        تقرير ذكي كل ساعة يُرسَل لـ Telegram — يحلله DeepSeek ويكتبه بالعربية.
        يشمل: حالة الصفقات، الأرباح، المشاكل التقنية، توصيات.
        """
        if not self.cfg.smart_report_enabled:
            return
        if not self.cfg.deepseek_api_key:
            return

        now = time.time()
        if (now - self._last_smart_report) < self.cfg.smart_report_interval * 3600:
            return

        _log("[Smart Report] 📊 جاري إعداد التقرير الذكي...")

        # ── جمع البيانات ──
        slots_data = self.slots.get_all_states()
        monthly    = {"total_pnl": 0.0, "trades": 0, "wins": 0, "losses": 0}
        try:
            monthly = await asyncio.get_running_loop().run_in_executor(
                None, self.db.get_monthly_pnl
            )
        except Exception:
            pass

        # رصيد Spot
        spot_balance    = 0.0
        futures_balance = 0.0
        try:
            bal = self.executor.exchange.fetch_balance({"type": "spot"})
            spot_balance = float(bal.get("USDT", {}).get("free", 0) or bal.get("free", {}).get("USDT", 0))
        except Exception:
            pass
        try:
            if self.futures_executor:
                fbal = self.futures_executor.exchange.fetch_balance({"type": "swap"})
                futures_balance = float(fbal.get("USDT", {}).get("free", 0) or fbal.get("free", {}).get("USDT", 0))
        except Exception:
            pass

        # تفاصيل الصفقات المفتوحة
        open_trades_summary = []
        for s in slots_data:
            curr_price = 0.0
            try:
                t = self.executor.exchange.fetch_ticker(s.symbol)
                curr_price = float(t.get("last") or 0)
            except Exception:
                pass
            pnl_pct = (curr_price / s.entry_price - 1) * 100 if s.entry_price > 0 and curr_price > 0 else 0
            age_h   = (time.time() - s.entry_time) / 3600
            open_trades_summary.append(
                f"{s.symbol}: دخول={s.entry_price:.6g} حالي={curr_price:.6g} "
                f"PnL={pnl_pct:+.1f}% عمر={age_h:.1f}h "
                f"TP1={s.tp1:.6g} SL={s.stop_loss:.6g} "
                f"{'Futures' if s.is_futures else 'Spot'}"
            )

        # الأحداث الأخيرة
        recent = self._recent_events[-10:] if self._recent_events else ["لا أحداث مسجّلة"]

        # ── إرسال لـ DeepSeek للتحليل ──
        # Fear & Greed للتقرير
        fg_report = {"value": 50, "label": "Neutral", "signal": "neutral"}
        try:
            async with aiohttp.ClientSession() as _s:
                fg_report = await self.pipeline.get_fear_greed_index(_s)
        except Exception:
            pass

        data_summary = f"""
حالة البوت الآن:
- الرصيد الحر (Spot): ${spot_balance:.2f} | الرصيد الحر (Futures): ${futures_balance:.2f}
- Fear & Greed Index: {fg_report.get('value', 50)}/100 — {fg_report.get('label', 'N/A')}
- الصفقات المفتوحة: {len(slots_data)}/{self.cfg.max_slots}
- أرباح الشهر: ${monthly.get('total_pnl', 0):+.2f} ({monthly.get('trades', 0)} صفقة، {monthly.get('wins', 0)} رابحة، {monthly.get('losses', 0)} خاسرة)

الصفقات المفتوحة:
{chr(10).join(open_trades_summary) if open_trades_summary else 'لا توجد صفقات مفتوحة'}

أحداث الساعة الأخيرة:
{chr(10).join(recent)}

الاستراتيجية المفعّلة:
- RSI Bounce: نعم (ارتداد RSI من <30 إلى >35)
- EMA Cross: نعم (EMA9 > EMA21)
- Cost Gate: نعم (حركة متوقعة > 0.6%)
- BTC Filter S1: RSI BTC > 40
- السوق: Spot + Futures (2x Isolated)
"""

        # تنظيف data_summary من أي أحرف خاصة
        safe_summary = data_summary.replace('"', "'").replace('\n', ' | ')[:800]
        prompt = (
            "أنت محلل بوت تداول. اكتب تقريراً مختصراً بالعربية (4-6 أسطر) عن هذا الوضع:\n\n"
            + safe_summary
            + "\n\nاذكر: الحالة العامة، أبرز الصفقات، أي مخاوف، وتوصية واحدة."
        )

        try:
            import json as _json
            payload = _json.dumps({
                "model":       "deepseek-v4-flash",
                "max_tokens":  400,
                "temperature": 0.3,
                "messages":    [{"role": "user", "content": prompt}],
            }, ensure_ascii=False)

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.deepseek.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {self.cfg.deepseek_api_key}",
                             "Content-Type": "application/json; charset=utf-8"},
                    data=payload.encode("utf-8"),
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        err_text = await resp.text()
                        _log(f"[Smart Report] DeepSeek HTTP {resp.status}: {err_text[:100]}")
                    if resp.status == 200:
                        data    = await resp.json()
                        report  = data["choices"][0]["message"]["content"].strip()
                        win_rate = (monthly.get('wins', 0) / monthly.get('trades', 1) * 100) if monthly.get('trades', 0) > 0 else 0
                        pnl_sign = "+" if monthly.get('total_pnl', 0) >= 0 else ""

                        time_str = datetime.now().strftime("%H:%M")
                        pnl_sign = "+" if monthly.get("total_pnl", 0) >= 0 else ""
                        win_rate = (monthly.get("wins", 0) / monthly.get("trades", 1) * 100) if monthly.get("trades", 0) > 0 else 0
                        msg = (
                            f"📊 <b>التقرير الذكي</b> — {time_str}\n"
                            "━━━━━━━━━━━━━━━━━━━━\n\n"
                            f"{report}\n\n"
                            "━━━━━━━━━━━━━━━━━━━━\n"
                            f"💼 Spot: <code>${spot_balance:.2f}</code> | Futures: <code>${futures_balance:.2f}</code> | "
                            f"Slots: <code>{len(slots_data)}/{self.cfg.max_slots}</code>\n"
                            f"📈 الشهر: <code>{pnl_sign}{monthly.get('total_pnl', 0):.2f}$</code> | "
                            f"Win Rate: <code>{win_rate:.0f}%</code> ({monthly.get('trades', 0)} صفقة)"
                        )
                        await self._send_telegram(msg)
                        self._last_smart_report = now
                        _log("[Smart Report] ✅ تقرير أُرسل")
                        self._recent_events = []
                    else:
                        _log(f"[Smart Report] DeepSeek HTTP {resp.status}")
        except Exception as e:
            _log(f"[Smart Report] ❌ {str(e)[:60]}")


    async def _auto_heal_positions(self):
        """
        نظام Auto-Heal الشامل — يعمل عند كل Restart تلقائياً.
        يُصحّح وضع الصفقات بدون تدخل يدوي.

        الحالات المعالجة:
        1. صفقة في DB لكن لا رصيد فعلي → تُحذف من DB (أُغلقت يدوياً)
        2. رصيد فعلي بدون صفقة في DB → تُضاف وتُحمى
        3. أهداف بعيدة جداً عن السعر الحالي → تُعاد
        4. خسارة > 25% → إغلاق تلقائي

        النتيجة: DB دائماً متزامن مع الواقع
        """
        _log("[Auto-Heal] 🔧 بدء فحص وتصحيح الصفقات...")
        actions = []

        try:
            # ── جلب حالة DB وMEXC ──
            db_trades = {}
            if self.db and self.db._enabled:
                for row in self.db.get_open_trades():
                    sym = row.get("symbol", "")
                    if sym:
                        db_trades[sym] = row

            bal       = self.executor.exchange.fetch_balance({"type": "spot"})
            balances  = {k: float(v or 0) for k, v in bal.get("total", {}).items()
                         if float(v or 0) > 0 and k not in ("USDT","USDC")}

            _log(f"[Auto-Heal] DB: {len(db_trades)} صفقة | MEXC: {len(balances)} عملة")

            # ══════════════════════════════════════════════
            # الحالة 1: صفقة في DB بدون رصيد فعلي
            # ══════════════════════════════════════════════
            for sym, row in db_trades.items():
                asset = sym.split("/")[0]
                actual_qty = balances.get(asset, 0)
                entry_price = float(row.get("entry_price") or 0)

                # جلب السعر الحالي
                curr_price = 0.0
                try:
                    t = self.executor.exchange.fetch_ticker(sym)
                    curr_price = float(t.get("last") or 0)
                except Exception:
                    pass

                actual_value = actual_qty * curr_price if curr_price > 0 else 0

                # حد الأتربة = 20% من رأس المال (مثلاً $2 إذا capital=$10)
                dust_threshold = self.cfg.capital * 0.20
                if actual_value < dust_threshold:
                    # الرصيد الفعلي أتربة = الصفقة أُغلقت خارج البوت
                    _log(f"[Auto-Heal] {sym}: رصيد فعلي=${actual_value:.2f} < ${dust_threshold:.2f} → تنظيف DB")
                    try:
                        await asyncio.get_running_loop().run_in_executor(
                            None, lambda r=row: self.db.update_exit(
                                trade_id   = r.get("id"),
                                exit_type  = "manual_healed",
                                exit_price = curr_price or entry_price,
                                exit_qty   = float(r.get("filled_qty") or 0),
                                net_pnl_usd= (curr_price - entry_price) * float(r.get("filled_qty") or 0) if curr_price > 0 else 0,
                                net_pnl_pct= ((curr_price / entry_price) - 1) * 100 if entry_price > 0 and curr_price > 0 else 0,
                                total_fees = 0,
                            )
                        )
                        # إزالة من الـ slots إذا كانت موجودة
                        if self.slots.is_occupied(sym):
                            self.slots.release(sym)
                        actions.append(f"🧹 <b>{sym}</b>: أُزيلت من DB (أُغلقت خارج البوت)")
                    except Exception as e:
                        _log(f"[Auto-Heal] {sym}: فشل تنظيف DB: {e}")
                    continue

                # ══════════════════════════════════════════
                # الحالة 3: فحص الخسارة والأهداف
                # ══════════════════════════════════════════
                if entry_price > 0 and curr_price > 0:
                    loss_pct = (entry_price - curr_price) / entry_price * 100

                    # خسارة > 25% → إغلاق تلقائي
                    if loss_pct > 25:
                        _log(f"[Auto-Heal] {sym}: خسارة {loss_pct:.1f}% > 25% → إغلاق تلقائي")
                        try:
                            sell_qty = self.executor._apply_step_size(sym, actual_qty * 0.99)
                            o = self.executor.exchange.create_market_sell_order(sym, sell_qty)
                            pnl = (curr_price - entry_price) * actual_qty
                            await asyncio.get_running_loop().run_in_executor(
                                None, lambda r=row: self.db.update_exit(
                                    trade_id=r.get("id"), exit_type="auto_healed_sl",
                                    exit_price=curr_price, exit_qty=actual_qty,
                                    net_pnl_usd=pnl, net_pnl_pct=-loss_pct, total_fees=0,
                                )
                            )
                            if self.slots.is_occupied(sym):
                                self.slots.release(sym)
                            actions.append(
                                f"🔻 <b>{sym}</b>: خسارة {loss_pct:.1f}% — أُغلقت تلقائياً | PnL: ${pnl:+.3f}"
                            )
                        except Exception as e:
                            actions.append(f"❌ <b>{sym}</b>: فشل الإغلاق التلقائي — {str(e)[:50]}")
                        continue

                    # أهداف بعيدة > 30% → إعادة الحساب
                    tp1 = float(row.get("tp1") or 0)
                    if tp1 > 0:
                        dist_pct = (tp1 - curr_price) / curr_price * 100
                        if dist_pct > 30 or dist_pct < -5:
                            new_tp1 = curr_price * 1.06
                            new_sl  = curr_price * 0.97
                            _log(f"[Auto-Heal] {sym}: TP1 بعيد {dist_pct:.1f}% → إعادة ضبط")
                            try:
                                # إلغاء الأوامر القديمة
                                self.executor.exchange.cancel_all_orders(sym)
                                import time as _t; _t.sleep(0.5)
                                # وضع TP1 جديد
                                qty_tp1 = self.executor._apply_step_size(sym, actual_qty * 0.80)
                                tp1_p   = float(self.executor.exchange.price_to_precision(sym, new_tp1))
                                o = self.executor.exchange.create_limit_sell_order(sym, qty_tp1, tp1_p)
                                # تحديث الـ slot
                                if self.slots.is_occupied(sym):
                                    self.slots.update_state(sym, tp1=new_tp1, stop_loss=new_sl,
                                                           tp1_order_id=o["id"], tp1_filled=False)
                                actions.append(
                                    f"🔄 <b>{sym}</b>: أُعيد ضبط الأهداف | TP1: {new_tp1:.6g} (+6%) | SL: {new_sl:.6g} (-3%)"
                                )
                            except Exception as e:
                                actions.append(f"⚠️ <b>{sym}</b>: فشل إعادة الضبط — {str(e)[:50]}")

            # ══════════════════════════════════════════
            # الحالة 2: رصيد فعلي بدون صفقة في DB
            # ══════════════════════════════════════════
            for asset, qty in balances.items():
                if any(asset.endswith(p) for p in ["3L","3S","5L","5S"]):
                    continue
                sym = f"{asset}/USDT"
                if sym in db_trades or sym not in self.executor.exchange.markets:
                    continue
                try:
                    t = self.executor.exchange.fetch_ticker(sym)
                    price = float(t.get("last") or 0)
                    value = qty * price
                    dust_limit = self.cfg.capital * 0.20
                    if value < dust_limit:
                        # أتربة — تجاهل
                        _log(f"[Auto-Heal] {sym}: أتربة ${value:.2f} < ${dust_limit:.2f} → تجاهل")
                        continue
                    _log(f"[Auto-Heal] {sym}: رصيد ${value:.2f} بدون DB → تسجيل ومراقبة")
                    actions.append(
                        f"📋 <b>{sym}</b>: رصيد ${value:.2f} غير مسجّل — "
                        f"TradeMonitor سيراقبه بـ SL=-3%"
                    )
                    # إضافة للـ slots للمراقبة بدون إعادة شراء
                    if not self.slots.is_occupied(sym) and self.slots.used < self.cfg.max_slots:
                        from dataclasses import replace
                        state = SlotState(
                            symbol      = sym,
                            entry_price = price,
                            filled_qty  = qty,
                            tp1         = price * 1.06,
                            tp2         = price * 1.09,
                            tp3         = price * 1.12,
                            stop_loss   = price * 0.97,
                            qty_tp1     = qty * 0.80,
                            qty_tp2     = qty * 0.20,
                        )
                        self.slots.occupy(state)
                except Exception:
                    pass

        except Exception as e:
            _log(f"[Auto-Heal] ❌ خطأ: {e}")
            return

        # ── إرسال تقرير Auto-Heal ──
        if actions:
            try:
                msg = (
                    "🔧 <b>Auto-Heal — تصحيح تلقائي</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━\n\n"
                    + "\n\n".join(actions)
                )
                await self._send_telegram(msg)
            except Exception:
                pass
        else:
            _log("[Auto-Heal] ✅ كل شيء متزامن — لا إجراءات مطلوبة")

        _log(f"[Auto-Heal] اكتمل — {len(actions)} إجراء")

    async def scan_loop(self):
        # ── تحذير Futures عند الإطلاق ──
        if self.cfg.market_futures and not self.cfg.market_spot:
            await self._send_telegram(
                "⚡ <b>تحذير: وضع Futures مفعَّل</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"• <b>الرافعة:</b> {'ديناميكية 🧠' if self.cfg.futures_dynamic_lev else 'ثابتة'} "
                f"({self.cfg.futures_leverage_min}x - {self.cfg.futures_leverage_max}x)\n"
                f"• <b>Margin Mode:</b> <code>{self.cfg.futures_margin_mode}</code>\n"
                f"• <b>SL:</b> <code>-{self.cfg.futures_sl_pct}%</code> "
                f"(فعلي: -{self.cfg.futures_sl_pct * self.cfg.futures_leverage:.1f}% بالرافعة)\n"
                f"• <b>TP1:</b> <code>+{self.cfg.futures_tp1_pct}%</code> "
                f"(فعلي: +{self.cfg.futures_tp1_pct * self.cfg.futures_leverage:.1f}% بالرافعة)\n\n"
                "⚠️ <b>تأكد من وجود رصيد في Futures Wallet على MEXC</b>\n"
                "<i>Spot Wallet لا يُستخدم في Futures.</i>"
            )
        elif self.cfg.market_futures and self.cfg.market_spot:
            await self._send_telegram(
                "⚡ <b>وضع مزدوج: Spot + Futures</b>\n"
                f"Spot: S1 (RSI≤{self.cfg.rsi_threshold}) + S2 (Momentum)\n"
                f"Futures: Long {self.cfg.futures_leverage}x | SL=-{self.cfg.futures_sl_pct}% | "
                f"TP1=+{self.cfg.futures_tp1_pct}%"
            )

        await self._send_telegram(
            f"🤖 <b>Scalping Engine v3 نشط</b> | فلتر الشريعة: {'✅ مفعّل' if self.cfg.shariah_filter_enabled else '⏸ موقوف مؤقتاً'}\n"
            f"🛡️ S1 (تشبع): RSI≤{self.cfg.rsi_threshold} | SL -{self.cfg.s1_sl_min}%~-{self.cfg.s1_sl_max}% | BTC≥{self.cfg.s1_btc_rsi_min:.0f}\n"
            f"⚡ S2 (زخم): {'✅' if self.cfg.s2_enabled else '⏸'} RSI {self.cfg.s2_rsi_min:.0f}-{self.cfg.s2_rsi_max:.0f} | TP +{self.cfg.s2_tp1_pct:.0f}%/+{self.cfg.s2_tp2_pct:.0f}%/+{self.cfg.s2_tp3_pct:.0f}% | BTC≥{self.cfg.s2_btc_rsi_min:.0f}\n"
            f"📍 L1+: فلتر القاعدة السعرية (≥2 لمسات تاريخية مطلوبة)\n"
            f"🧠 L2: DeepSeek + MiniMax M3 (أغلبية — واحد يكفي)\n"
            f"⚡ L3: MARKET buy | TP1=80%(+6%) TP2=20%(+9%) | SL=-2% to -3% | BTC≥{self.cfg.s1_btc_rsi_min:.0f}\n"
            f"• Slots: {self.cfg.max_slots} | Capital: ${self.cfg.capital}/trade\n"
            f"• Scan: كل {self.cfg.scan_interval} دقيقة"
        )

        while True:
            start = datetime.now()
            _log(f"🔄 Scan: {start.strftime('%Y-%m-%d %H:%M:%S')}")
            if not self.cfg.shariah_filter_enabled:
                _log("[Shariah Filter] ⏸ موقوف مؤقتاً — SHARIAH_FILTER_ENABLED=false")

            # ── فحص صحة APIs في بداية كل دورة ──
            await self._check_api_health()

            # ── التقرير الذكي كل ساعة ──
            await self._send_smart_report()

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
                # ── رأس المال الديناميكي ──
                # إذا وُجد override يدوي في Railway → استخدمه
                # وإلا → قسّم الرصيد الحر على عدد الـ slots المتاحة
                free_slots = max(1, self.cfg.max_slots - self.slots.used)
                if self.cfg._capital_override > 0:
                    dynamic_capital = self.cfg._capital_override
                else:
                    # الرصيد ÷ عدد الـ slots = حصة كل صفقة
                    dynamic_capital = round(initial_balance / self.cfg.max_slots, 2)
                    dynamic_capital = max(dynamic_capital, 1.0)  # حد أدنى $1

                # تحديث رأس المال ديناميكياً
                object.__setattr__(self.cfg, "_capital_override", 0)  # لا override
                self._dynamic_capital = dynamic_capital
                # تحديث cfg حتى يصل لـ executor
                object.__setattr__(self.cfg, "_capital_override", dynamic_capital)

                min_required = dynamic_capital * self.cfg.min_capital_pct
                _log(
                    f"[Balance] الرصيد: ${initial_balance:.2f} | "
                    f"حصة/صفقة: ${dynamic_capital:.2f} | "
                    f"slots حرة: {free_slots}/{self.cfg.max_slots}"
                )

                if initial_balance < min_required and self.slots.used == 0:
                    _log(
                        f"[Balance Guard] رصيد ${initial_balance:.2f} < ${min_required:.2f} "
                        "— Scanner halted. TradeMonitor continues independently."
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

                    # ── جمع الأزواج من Spot وFutures معاً ──
                    spot_symbols    = []
                    futures_symbols = []

                    # Spot symbols
                    if self.cfg.market_spot:
                        for s, mkt in self.executor.exchange.markets.items():
                            if not s.endswith("/USDT"):
                                continue
                            if ":" in s or "swap" in s.lower() or "future" in s.lower():
                                continue
                            base = s.split("/")[0].upper()
                            if self.cfg.shariah_filter_enabled and base in self.cfg.blacklisted_assets:
                                continue
                            if any(base.endswith(p) for p in ["3L","3S","5L","5S"]):
                                continue
                            spot_symbols.append(("spot", s))

                    # Futures symbols
                    if self.cfg.market_futures and self.futures_executor:
                        try:
                            self.futures_executor.exchange.load_markets(reload=True)
                        except Exception:
                            pass
                        for s, mkt in self.futures_executor.exchange.markets.items():
                            if "/USDT:USDT" not in s:
                                continue
                            base = s.split("/")[0].upper()
                            if self.cfg.shariah_filter_enabled and base in self.cfg.blacklisted_assets:
                                continue
                            futures_symbols.append(("futures", s))

                    all_symbols = spot_symbols + futures_symbols
                    _log(
                        f"[Scan] {len(spot_symbols)} Spot + {len(futures_symbols)} Futures "
                        f"= {len(all_symbols)} زوج جاهز للفحص"
                    )

                    BATCH = 5
                    async with aiohttp.ClientSession() as session:
                        for i in range(0, len(all_symbols), BATCH):
                            batch = all_symbols[i:i+BATCH]
                            tasks = []
                            for market_type, sym in batch:
                                if market_type == "spot":
                                    # استراتيجيتا Spot
                                    tasks.append(self._process_candidate(session, sym, initial_balance))
                                    tasks.append(self._process_momentum_candidate(session, sym, initial_balance))
                                else:
                                    # استراتيجية Futures (تشبع بيعي فقط، بأهداف مختلفة)
                                    tasks.append(self._process_futures_candidate(session, sym, initial_balance))
                            await asyncio.gather(*tasks, return_exceptions=True)
                            checked = i + len(batch)
                            if checked % 50 == 0:
                                _log(f"[Scan] {checked}/{len(all_symbols)} | slots={self.slots.used}/{self.cfg.max_slots}")
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
        # فحص صحة الصفقات المُستردة وإرسال تقرير Telegram
        self._post_restore_health_check()
        # إعادة تقييم الصفقات Fallback وضبط أهداف واقعية
        self._reassess_restored_positions()
        # Auto-Heal: مزامنة DB مع MEXC الفعلي
        await self._auto_heal_positions()
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
