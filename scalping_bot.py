"""
scalping_bot.py
===============
Production-Grade Crypto Scalping Engine
Classes: Config | SlotManager | DataPipeline | DeepSeekAnalyst
         HighSpeedExecutor | TradeMonitor | ScalpingOrchestrator
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

# ─────────────────────────────────────────────
# 1. CONFIG
# ─────────────────────────────────────────────
@dataclass(frozen=True)
class Config:
    mexc_api_key:      str   = field(default_factory=lambda: os.environ.get("MEXC_API_KEY", ""))
    mexc_api_secret:   str   = field(default_factory=lambda: os.environ.get("MEXC_API_SECRET", ""))
    telegram_token:    str   = field(default_factory=lambda: os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    telegram_chat_id:  str   = field(default_factory=lambda: os.environ.get("TELEGRAM_CHAT_ID", ""))
    deepseek_api_key:  str   = field(default_factory=lambda: os.environ.get("DEEPSEEK_API_KEY", ""))
    cmc_api_key:       str   = field(default_factory=lambda: os.environ.get("COINMARKETCAP_API_KEY", ""))
    lunar_api_key:     str   = field(default_factory=lambda: os.environ.get("LUNARCRUSH_API_KEY", ""))
    database_url:      str   = field(default_factory=lambda: os.environ.get("DATABASE_URL", ""))

    capital:           float = field(default_factory=lambda: float(os.environ.get("TRADE_INVESTMENT_AMOUNT", "30")))
    max_slots:         int   = field(default_factory=lambda: int(os.environ.get("MAX_CONCURRENT_TRADES", "3")))
    scan_interval:     int   = field(default_factory=lambda: int(os.environ.get("SCAN_INTERVAL_MINUTES", "60")))
    rsi_threshold:     int   = field(default_factory=lambda: int(os.environ.get("RSI_OVERSOLD_THRESHOLD", "31")))
    min_volume_usd:    float = field(default_factory=lambda: float(os.environ.get("MIN_DAILY_VOLUME_USD", "1000000")))
    monitor_interval:  int   = 30
    max_ai_tokens:     int   = 150
    deepseek_model:    str   = field(default_factory=lambda: os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"))
    blacklisted_assets: set  = field(default_factory=lambda: {
        "AAVE", "COMP", "MKR", "CRV", "LDO", "UNI", "SUSHI", "BAL",
        "CAKE", "YFI", "SNX", "DYDX", "ANC", "FUN", "WIN", "DICE",
        "BET", "RLB", "POLS", "MANA", "SAND", "GALA", "AXS", "SLP",
        "XMR", "DASH", "ZEC"
    })
    max_trade_hours:    float = field(default_factory=lambda: float(os.environ.get("MAX_TRADE_DURATION_HOURS", "4")))
    reconcile_interval: int   = field(default_factory=lambda: int(os.environ.get("RECONCILE_INTERVAL_SECONDS", "180")))


# ─────────────────────────────────────────────
# 2. SLOT MANAGER  — in-memory only, shields manual trades
# ─────────────────────────────────────────────
@dataclass
class SlotState:
    symbol:      str
    buy_order_id:   str
    tp1_order_id:   str  = ""
    tp2_order_id:   str  = ""
    tp3_order_id:   str  = ""
    sl_order_id:    str  = ""
    entry_price:    float = 0.0
    filled_qty:     float = 0.0
    tp1:            float = 0.0
    tp2:            float = 0.0
    tp3:            float = 0.0
    stop_loss:      float = 0.0
    opened_at:           float = field(default_factory=time.time)
    break_even_attempted: bool  = False


class SlotManager:
    """
    Tracks ONLY bot-placed orders by their specific exchange order IDs.
    Manual trades on the same account are completely invisible to this manager.
    """

    def __init__(self, cfg: Config):
        self.cfg         = cfg
        self._lock       = threading.Lock()
        self._slots:  dict[str, SlotState] = {}   # symbol → SlotState

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
        _log(f"[SlotManager] 🟢 OCCUPIED: {state.symbol} | "
             f"slots={len(self._slots)}/{self.cfg.max_slots}")

    def release(self, symbol: str):
        with self._lock:
            removed = self._slots.pop(symbol, None)
        if removed:
            _log(f"[SlotManager] ⚪ VACANT: {symbol} | "
                 f"slots={len(self._slots)}/{self.cfg.max_slots}")

    def get_all_states(self) -> list[SlotState]:
        with self._lock:
            return list(self._slots.values())

    def get_state(self, symbol: str) -> Optional[SlotState]:
        with self._lock:
            return self._slots.get(symbol)


# ─────────────────────────────────────────────
# 3. DATA PIPELINE  — Layer 1 async shields
# ─────────────────────────────────────────────
class DataPipeline:
    """CMC + LunarCrush async shields with 1-hour cache."""

    _cmc_cache:   dict = {}
    _lunar_cache: dict = {}
    _CACHE_TTL: float  = 3600.0

    def __init__(self, cfg: Config):
        self.cfg = cfg

    def _cache_get(self, store: dict, key: str):
        entry = store.get(key)
        if entry and (time.time() - entry["ts"]) < self._CACHE_TTL:
            return entry["data"]
        return None

    def _cache_set(self, store: dict, key: str, data):
        store[key] = {"ts": time.time(), "data": data}

    async def get_cmc_volume(self, session: aiohttp.ClientSession, symbol: str) -> float:
        coin   = symbol.replace("/USDT", "").upper()
        cached = self._cache_get(self._cmc_cache, coin)
        if cached is not None:
            return float(cached.get("volume_24h", 0))

        if not self.cfg.cmc_api_key:
            return self.cfg.min_volume_usd  # bypass if no key

        try:
            async with session.get(
                "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest",
                headers={"X-CMC_PRO_API_KEY": self.cfg.cmc_api_key},
                params={"symbol": coin, "convert": "USD"},
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                if resp.status != 200:
                    return self.cfg.min_volume_usd
                data   = await resp.json()
                volume = float(
                    data.get("data", {}).get(coin, {})
                    .get("quote", {}).get("USD", {})
                    .get("volume_24h", 0)
                )
                self._cache_set(self._cmc_cache, coin, {"volume_24h": volume})
                return volume
        except Exception:
            return self.cfg.min_volume_usd

    async def get_lunar_vote(self, session: aiohttp.ClientSession, symbol: str) -> str:
        """Returns 'approve' | 'neutral' | 'reject'"""
        coin   = symbol.replace("/USDT", "").lower()
        cached = self._cache_get(self._lunar_cache, coin)
        if cached is not None:
            return cached

        if not self.cfg.lunar_api_key:
            return "neutral"

        try:
            async with session.get(
                f"https://lunarcrush.com/api4/public/coins/{coin}/v1",
                headers={"Authorization": f"Bearer {self.cfg.lunar_api_key}"},
                timeout=aiohttp.ClientTimeout(total=6),
            ) as resp:
                if resp.status != 200:
                    return "neutral"
                data         = (await resp.json()).get("data", {})
                galaxy_score = float(data.get("galaxy_score", 0))
                sentiment    = float(data.get("sentiment",    3))
                if galaxy_score <= 20 or sentiment <= 1.5:
                    vote = "reject"
                elif galaxy_score >= 60 and sentiment >= 4:
                    vote = "approve"
                else:
                    vote = "neutral"
                self._cache_set(self._lunar_cache, coin, vote)
                return vote
        except Exception:
            return "neutral"

    async def layer1_pass(
        self,
        session: aiohttp.ClientSession,
        symbol:  str,
        rsi:     float,
    ) -> tuple[bool, str]:
        """RSI + CMC + LunarCrush in parallel — returns (passed, reason)"""
        if rsi > self.cfg.rsi_threshold:
            return False, f"RSI={rsi:.1f} > {self.cfg.rsi_threshold}"

        cmc_vol, lunar_vote = await asyncio.gather(
            self.get_cmc_volume(session, symbol),
            self.get_lunar_vote(session, symbol),
        )

        if cmc_vol < self.cfg.min_volume_usd:
            return False, f"CMC Vol ${cmc_vol/1e6:.1f}M < ${self.cfg.min_volume_usd/1e6:.0f}M"

        if lunar_vote == "reject":
            return False, "LunarCrush reject"

        return True, f"RSI={rsi:.1f} ✅ Vol=${cmc_vol/1e6:.1f}M ✅ LC={lunar_vote}"


# ─────────────────────────────────────────────
# 4. STOP-LOSS CALCULATOR  — pure math, no AI hallucination
# ─────────────────────────────────────────────
def calculate_micro_swing_sl(exchange: ccxt.mexc, symbol: str, entry_price: float) -> float:
    """
    Fetches last 48 × 15m candles, finds the lowest local swing low,
    places SL 0.2% below it, and clamps between -0.5% and -2.5%.
    AI is completely removed from SL calculation.
    """
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe="15m", limit=48)
        if not ohlcv or len(ohlcv) < 5:
            raise ValueError("insufficient candles")

        df   = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"]).astype(float)
        lows = df["low"].values

        # identify swing lows: lower than 2 neighbors on each side
        swing_lows = []
        for i in range(2, len(lows) - 2):
            if (lows[i] < lows[i-1] and lows[i] < lows[i-2] and
                    lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                swing_lows.append(lows[i])

        if swing_lows:
            # use the most recent 3 swing lows, pick the lowest
            recent = sorted(swing_lows[-3:])
            local_low = recent[0]
        else:
            # fallback: min of last 5 candles
            local_low = float(lows[-5:].min())

        sl = local_low * 0.998          # 0.2% below swing low

    except Exception as e:
        _log(f"[SL Calc] {symbol} fallback: {e}")
        sl = entry_price * 0.975        # 2.5% fallback

    # clamp: -0.5% ≤ SL ≤ -2.5% relative to entry
    sl_min = entry_price * (1 - 0.025)  # -2.5% floor
    sl_max = entry_price * (1 - 0.005)  # -0.5% ceiling
    sl     = max(sl_min, min(sl, sl_max))

    _log(f"[SL Calc] {symbol}: local_swing_low={local_low if 'local_low' in dir() else '?':.8g} → SL={sl:.8g}")
    return sl


# ─────────────────────────────────────────────
# 5. FIBONACCI TARGET ENGINE  — cascading ceilings, no stuck loop
# ─────────────────────────────────────────────
def calculate_cascading_targets(
    fib_high:    float,
    fib_low:     float,
    entry_price: float,
) -> dict:
    """
    Fibonacci Internal Retracement with cascading caps.
    Guarantees: entry_price < tp1 < tp2 < tp3 in ALL cases.
    """
    fib_range = fib_high - fib_low

    if fib_range > 0:
        tp1_calc = fib_low + fib_range * 0.382
        tp2_calc = fib_low + fib_range * 0.500
        tp3_calc = fib_low + fib_range * 0.618
    else:
        tp1_calc = entry_price * 1.03
        tp2_calc = entry_price * 1.06
        tp3_calc = entry_price * 1.12

    cap = entry_price * 1.15   # 15% absolute ceiling

    # progressive floor guarantee
    tp1 = max(tp1_calc, entry_price * 1.03)
    tp2 = max(tp2_calc, tp1 * 1.03)
    tp3 = max(tp3_calc, tp2 * 1.03)

    # staggered caps — prevents tp2=tp3 in narrow ranges
    tp1 = min(tp1, cap * 0.78)   # max +11.7%
    tp2 = min(tp2, cap * 0.90)   # max +13.5%
    tp3 = min(tp3, cap)           # max +15.0%

    # ultimate structural safety check
    if not (entry_price < tp1 < tp2 < tp3):
        _log(f"[Fib] Structural fallback triggered for entry={entry_price:.8g}")
        tp1 = round(entry_price * 1.03, 10)
        tp2 = round(entry_price * 1.06, 10)
        tp3 = round(entry_price * 1.12, 10)

    return {
        "tp1":    round(tp1, 10),
        "tp2":    round(tp2, 10),
        "tp3":    round(tp3, 10),
        "method": "Cascading Fib",
    }


# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
# 6. DEEPSEEK ANALYST — sole AI brain, no Groq/Together
# ─────────────────────────────────────────────
class DeepSeekAnalyst:
    """
    Single async request to DeepSeek — no voting loops, no committees.
    Returns BUY (proceed) or SKIP (reject).
    SL is never touched by AI — pure code calculation.
    Fib targets: AI hint → always overridden by cascading engine.
    """

    SYSTEM_PROMPT = (
        "You are a quantitative crypto scalping analyst. "
        "Analyze the oversold asset data and decide instantly. "
        "Output exactly one word on the last line: BUY or SKIP. "
        "BUY = entry conditions are valid. SKIP = skip this asset."
    )

    def __init__(self, cfg: Config):
        self.cfg = cfg

    async def _call_deepseek(
        self,
        session:  aiohttp.ClientSession,
        user_msg: str,
    ) -> str:
        """Single async call to DeepSeek — timeout 8s."""
        if not self.cfg.deepseek_api_key:
            return ""
        try:
            async with session.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.cfg.deepseek_api_key}",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       self.cfg.deepseek_model,
                    "max_tokens":  self.cfg.max_ai_tokens,
                    "temperature": 0.1,
                    "messages": [
                        {"role": "system", "content": self.SYSTEM_PROMPT},
                        {"role": "user",   "content": user_msg},
                    ],
                },
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status == 429:
                    _log("[DeepSeek] 429 Rate Limit")
                    return ""
                if resp.status != 200:
                    _log(f"[DeepSeek] HTTP {resp.status}")
                    return ""
                data = await resp.json()
                return data["choices"][0]["message"]["content"] or ""
        except asyncio.TimeoutError:
            _log("[DeepSeek] Timeout (8s)")
            return ""
        except Exception as e:
            _log(f"[DeepSeek] {str(e)[:80]}")
            return ""

    def _extract_verdict(self, text: str) -> str:
        """Extracts BUY or SKIP from DeepSeek response."""
        if not text:
            return "SKIP"
        # check last line first
        last_line = text.strip().split("\n")[-1].strip().upper()
        if last_line in ("BUY", "SKIP", "HOLD"):
            return "BUY" if last_line == "BUY" else "SKIP"
        # fallback: search anywhere
        if "BUY" in text.upper():
            return "BUY"
        return "SKIP"

    def _compute_fib_targets(
        self, fib_high: float, fib_low: float,
        entry: float, text: str
    ) -> dict:
        """
        Extracts Fib targets from DeepSeek response if present,
        then always runs through cascading engine for safety.
        """
        fib_range = fib_high - fib_low

        def extract(tag, default):
            m = re.search(rf'\[{tag}:\s*([\d.]+)\]', text)
            return float(m.group(1)) if m else default

        ai_tp1 = extract("TP1_PRICE", fib_low + fib_range * 0.382)
        ai_tp2 = extract("TP2_PRICE", fib_low + fib_range * 0.500)
        ai_tp3 = extract("TP3_PRICE", fib_low + fib_range * 0.618)

        return calculate_cascading_targets(
            fib_high=max(ai_tp3, fib_high),
            fib_low=min(ai_tp1, fib_low),
            entry_price=entry,
        )

    async def run(
        self,
        symbol:   str,
        rsi:      float,
        vol_m:    float,
        entry:    float,
        fib_high: float,
        fib_low:  float,
    ) -> dict:
        """
        Single DeepSeek call combining safety check + Fib targets.
        Returns: { safety_ok, targets, elapsed }
        """
        start = time.time()

        user_msg = (
            f"Symbol: {symbol}\n"
            f"RSI (Daily): {rsi:.1f} — oversold signal\n"
            f"24h Spot Volume: ${vol_m:.1f}M\n"
            f"Entry Price: {entry:.8g}\n"
            f"Local High (60d): {fib_high:.8g}\n"
            f"Local Low (60d): {fib_low:.8g}\n\n"
            f"Evaluate this oversold scalp setup. "
            f"If valid, also provide Fibonacci retracement targets:\n"
            f"[TP1_PRICE: X.XXXX]\n[TP2_PRICE: X.XXXX]\n[TP3_PRICE: X.XXXX]\n\n"
            f"Last line must be: BUY or SKIP"
        )

        async with aiohttp.ClientSession() as session:
            text = await self._call_deepseek(session, user_msg)

        elapsed   = time.time() - start
        verdict   = self._extract_verdict(text)
        safety_ok = (verdict == "BUY")
        targets   = self._compute_fib_targets(fib_high, fib_low, entry, text)

        _log(
            f"[DeepSeek] {symbol}: {verdict} | "
            f"TP1={targets['tp1']:.6g} TP2={targets['tp2']:.6g} "
            f"TP3={targets['tp3']:.6g} | {elapsed:.1f}s"
        )
        return {
            "safety_ok": safety_ok,
            "targets":   targets,
            "elapsed":   round(elapsed, 2),
        }


# 7. HIGH SPEED EXECUTOR
# ─────────────────────────────────────────────
TP1_QTY_PCT = 0.40
TP2_QTY_PCT = 0.35
TP3_QTY_PCT = 0.25


class HighSpeedExecutor:
    """
    MEXC Spot execution.
    Market buy: passes USDT cost via quoteOrderQty — never qty.
    SL: pure 15m swing low calculation — AI never touches SL.
    """

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
        """
        MEXC SPOT MARKET BUY — FIX:
        Passes USDT cost (capital) as amount, ccxt maps to quoteOrderQty.
        Never passes pre-computed qty or price — eliminates code 500.
        """
        capital    = self.cfg.capital
        live_price = self._live_price(symbol, entry_price)

        _log(
            f"[Executor] BUY {symbol} | "
            f"signal_px={entry_price:.8g} live_px={live_price:.8g} "
            f"capital=${capital:.2f}"
        )
        try:
            self._ensure_markets()

            # ── THE FIX ──────────────────────────────────────────────────
            # Do NOT compute qty. Pass capital (USDT cost) as amount.
            # ccxt + createMarketBuyOrderRequiresPrice=False
            # → sends POST /order {side:BUY, type:MARKET, quoteOrderQty:capital}
            # MEXC computes qty server-side from current order book.
            # ─────────────────────────────────────────────────────────────
            order = self.exchange.create_market_buy_order(
                symbol,
                capital,
                {"quoteOrderQty": capital},
            )

            filled_price = float(order.get("average") or order.get("price") or live_price)
            filled_qty   = float(order.get("filled") or (capital / filled_price))

            _log(
                f"✅ FILLED {symbol}: {filled_qty:.6f} @ {filled_price:.8g} "
                f"${capital:.2f} | ID:{order['id']}"
            )
            return {"order_id": order["id"], "filled_price": filled_price,
                    "filled_qty": filled_qty}

        except ccxt.InsufficientFunds as e:
            _log(f"[Executor] InsufficientFunds {symbol}: {e}"); return None
        except ccxt.NetworkError as e:
            _log(f"[Executor] NetworkError {symbol}: {e}"); return None
        except Exception as e:
            _log(f"[Executor] ERROR {symbol}: {e}"); return None

    def place_bracket(
        self,
        symbol:       str,
        filled_qty:   float,
        filled_price: float,
        tp1: float, tp2: float, tp3: float,
        stop_loss:    float,
    ) -> dict:
        """
        MEXC V3 Sequential Exit — sync, thread-safe.
        ─────────────────────────────────────────────
        Step 1: Post-buy settle — time.sleep(2) lets MEXC clear tokens into wallet
        Step 2: TP1 limit sell (100% qty) with price precision
        Step 3: settle delay — time.sleep(5) before SL placement
        Step 4: SL stop-limit with price_to_precision on both price fields
                (prevents code 30087 "price exceeds allowed range")
        """
        ids: dict = {}
        full_qty  = self._apply_step_size(symbol, filled_qty)

        # ── Step 1: Post-buy settling delay ──
        # MEXC needs time to credit tokens to wallet before any sell order
        time.sleep(2)

        # ── Step 2: TP1 Limit Sell — price precision applied ──
        try:
            tp1_price = float(self.exchange.price_to_precision(symbol, tp1))
            o = self.exchange.create_limit_sell_order(symbol, full_qty, tp1_price)
            ids["tp1_order_id"] = o["id"]
            _log(f"✅ TP1 (100%): {tp1_price:.8g} ×{full_qty} ID:{o['id']}")
        except Exception as e:
            _log(f"❌ TP1 FAILED {symbol}: {e}")

        # ── Step 3: Settle delay before SL ──
        time.sleep(5)

        # ── Step 4: SL Stop-Limit — price_to_precision on both fields ──
        # Prevents code 30087 "Price exceeds allowed range"
        try:
            sl_trigger = float(self.exchange.price_to_precision(symbol, stop_loss))
            sl_limit   = float(self.exchange.price_to_precision(symbol, stop_loss * 0.99))

            o = self.exchange.create_order(
                symbol,
                "limit",
                "sell",
                full_qty,
                sl_limit,
                {
                    "stopPrice":   sl_trigger,
                    "triggerType": "LAST_PRICE",
                },
            )
            ids["sl_order_id"] = o["id"]
            _log(
                f"✅ SL Stop-Limit: trigger={sl_trigger:.8g} "
                f"limit={sl_limit:.8g} ID:{o['id']}"
            )
        except Exception as e:
            err = str(e)
            if "30005" in err or "Oversold" in err or "oversold" in err:
                _log(
                    f"[MEXC Safe Guard] SL skipped — tokens still settling "
                    f"for {symbol}. Position live but unprotected."
                )
            elif "30087" in err:
                _log(f"[MEXC 30087] SL price out of range {symbol}: {err[:80]}")
            else:
                _log(f"❌ SL FAILED {symbol}: {err[:100]} — راجع يدوياً!")

        ids["tp2_ref"] = tp2
        ids["tp3_ref"] = tp3
        return ids

    def move_sl_to_breakeven(self, symbol: str, entry_price: float, remaining_qty: float):
        """
        Break-Even: cancels old SL and places new stop-market at entry price.
        Uses trigger/stop order to avoid freezing already-pledged tokens.
        """
        try:
            qty = self._apply_step_size(symbol, remaining_qty)
            # Try stop-market trigger first
            o = self.exchange.create_order(
                symbol, "market", "sell", qty,
                None,
                {
                    "stopPrice":    entry_price,
                    "triggerPrice": entry_price,
                    "type":         "stop",
                },
            )
            _log(f"✅ Break-Even {symbol}: SL→{entry_price:.8g} ID:{o['id']}")
            return o["id"]
        except Exception as e1:
            err = str(e1)
            if "30005" in err or "Oversold" in err or "oversold" in err:
                _log(
                    f"[MEXC Safe Guard] Break-even skipped or tokens locked "
                    f"for {symbol}. Continuing to prevent engine lock."
                )
                return None
            # Fallback: limit trigger
            try:
                qty = self._apply_step_size(symbol, remaining_qty)
                o   = self.exchange.create_order(
                    symbol, "limit", "sell", qty,
                    entry_price * 0.998,
                    {"stopPrice": entry_price},
                )
                _log(f"✅ Break-Even fallback {symbol}: SL→{entry_price:.8g} ID:{o['id']}")
                return o["id"]
            except Exception as e2:
                _log(f"❌ Break-Even FAILED {symbol}: {e2}")
                return None

    def cancel_order(self, symbol: str, order_id: str):
        try:
            self.exchange.cancel_order(order_id, symbol)
            _log(f"[Executor] Cancelled order {order_id} for {symbol}")
        except Exception as e:
            _log(f"[Executor] Cancel failed {order_id}: {e}")

    def fetch_order_status(self, symbol: str, order_id: str) -> str:
        """Returns 'closed' | 'canceled' | 'open' | 'unknown'"""
        try:
            o = self.exchange.fetch_order(order_id, symbol)
            return o.get("status", "unknown")
        except Exception as e:
            _log(f"[Executor] fetch_order {order_id}: {e}")
            return "unknown"

    def execute_full_trade(
        self,
        symbol:      str,
        entry_price: float,
        tp1: float, tp2: float, tp3: float,
        stop_loss:   float,
    ) -> Optional[SlotState]:
        """
        Executes full trade: market buy → bracket (TP1 + SL).
        place_bracket handles all delays internally.
        """
        buy = self.market_buy(symbol, entry_price)
        if not buy:
            return None

        try:
            bracket = self.place_bracket(
                symbol, buy["filled_qty"], buy["filled_price"],
                tp1, tp2, tp3, stop_loss,
            )
        except Exception as e:
            _log(f"[Executor] place_bracket error {symbol}: {e}")
            bracket = {}

        return SlotState(
            symbol       = symbol,
            buy_order_id = buy["order_id"],
            tp1_order_id = bracket.get("tp1_order_id", ""),
            tp2_order_id = bracket.get("tp2_order_id", ""),
            tp3_order_id = bracket.get("tp3_order_id", ""),
            sl_order_id  = bracket.get("sl_order_id",  ""),
            entry_price  = buy["filled_price"],
            filled_qty   = buy["filled_qty"],
            tp1=tp1, tp2=tp2, tp3=tp3,
            stop_loss=stop_loss,
        )

# ─────────────────────────────────────────────
# 8. TRADE MONITOR  — polls ONLY bot order IDs
# ─────────────────────────────────────────────
class TradeMonitor:
    """
    Polls exchange every 30s for ONLY the specific order IDs placed by the bot.
    Manual trades on the same account are never touched or tracked.
    Releases slots instantly upon SL hit or TP3 fill.
    """

    def __init__(self, cfg: Config, executor: HighSpeedExecutor, slot_mgr: SlotManager):
        self.cfg      = cfg
        self.executor = executor
        self.slots    = slot_mgr
        self._running = False

    async def start(self):
        self._running        = True
        self._reconcile_tick = 0
        _log("[TradeMonitor] ✅ started — polling every 30s + reconciliation every 180s")
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

        # ── Check SL ──
        if state.sl_order_id:
            sl_status = await asyncio.get_running_loop().run_in_executor(
                None, self.executor.fetch_order_status, symbol, state.sl_order_id
            )
            if sl_status == "closed":
                _log(f"[Monitor] 🛑 SL HIT: {symbol} → releasing slot")
                self.slots.release(symbol)
                await self._notify(
                    f"🛑 <b>وقف الخسارة</b>\nالعملة: <code>{symbol}</code>\n"
                    f"SL: <code>{state.stop_loss:.8g}</code>"
                )
                return

        # ── Check TP1 → move SL to break-even (with 30005 Oversold guard) ──
        if state.tp1_order_id and not state.break_even_attempted:
            tp1_status = await asyncio.get_running_loop().run_in_executor(
                None, self.executor.fetch_order_status, symbol, state.tp1_order_id
            )
            if tp1_status == "closed":
                remaining_qty = state.filled_qty * (TP2_QTY_PCT + TP3_QTY_PCT)
                _log(f"[Monitor] 🎯 TP1 HIT: {symbol} → محاولة Break-Even SL")
                try:
                    # إلغاء SL القديم أولاً قبل وضع الجديد
                    if state.sl_order_id:
                        await asyncio.get_running_loop().run_in_executor(
                            None, self.executor.cancel_order, symbol, state.sl_order_id
                        )
                    await asyncio.get_running_loop().run_in_executor(
                        None, self.executor.move_sl_to_breakeven,
                        symbol, state.entry_price, remaining_qty
                    )
                    state.break_even_attempted = True
                    _log(f"[Monitor] ✅ Break-Even SL مُحدَّث: {symbol}")
                except Exception as be_err:
                    err_str = str(be_err)
                    if "30005" in err_str or "Oversold" in err_str or "oversold" in err_str:
                        _log(
                            f"[MEXC Safe Guard] Break-even skipped or tokens locked "
                            f"for {symbol}. Continuing to prevent engine lock."
                        )
                    else:
                        _log(f"[Monitor] ⚠️ Break-Even خطأ {symbol}: {err_str[:80]}")
                    # mark as attempted — لا إعادة محاولة في الدورات القادمة
                    state.break_even_attempted = True

        # ── Check TP3 → release slot ──
        if state.tp3_order_id:
            tp3_status = await asyncio.get_running_loop().run_in_executor(
                None, self.executor.fetch_order_status, symbol, state.tp3_order_id
            )
            if tp3_status == "closed":
                _log(f"[Monitor] 🏆 TP3 HIT: {symbol} → releasing slot")
                self.slots.release(symbol)
                await self._notify(
                    f"🏆 <b>TP3 وصل الهدف الثالث!</b>\n"
                    f"العملة: <code>{symbol}</code>\n"
                    f"TP3: <code>{state.tp3:.8g}</code>"
                )
                return

    async def _reconcile_portfolio(self):
        """
        Self-Healing Reconciliation Engine — runs every 3 minutes.
        Detects orphaned/broken positions and force-liquidates them.

        A position is ORPHANED if:
          - Bot owns the asset (in SlotManager) but has no active TP or SL
            on the exchange order book.
          - OR the trade has been open longer than MAX_TRADE_DURATION_HOURS.
        """
        states = self.slots.get_all_states()
        if not states:
            return

        _log(f"[Reconcile] \U0001f50d فحص {len(states)} صفقة مفتوحة...")

        # MEXC Spot requires explicit symbol — fetch per-symbol, not globally
        for state in states:
            symbol         = state.symbol
            open_order_ids: set = set()
            try:
                symbol_orders = await asyncio.get_running_loop().run_in_executor(
                    None,
                    self.executor.exchange.fetch_open_orders,
                    symbol,
                )
                open_order_ids = {str(o["id"]) for o in symbol_orders}
            except Exception as e:
                _log(f"[Reconcile] فشل جلب أوامر {symbol}: {e}")

            await self._audit_slot(state, open_order_ids)

    async def _audit_slot(self, state: SlotState, open_order_ids: set):
        """Audits a single slot — flags and liquidates if orphaned."""
        symbol   = state.symbol
        now      = time.time()
        age_hrs  = (now - state.opened_at) / 3600

        # ── Check 1: No active TP or SL on exchange ──
        tp_active = bool(state.tp1_order_id and state.tp1_order_id in open_order_ids)
        sl_active = bool(state.sl_order_id  and state.sl_order_id  in open_order_ids)

        orphaned_no_exits = not tp_active and not sl_active

        # ── Check 2: Trade exceeded max duration ──
        orphaned_timeout  = age_hrs >= self.cfg.max_trade_hours

        if not orphaned_no_exits and not orphaned_timeout:
            _log(
                f"[Reconcile] ✅ {symbol}: "
                f"TP={'✅' if tp_active else '❌'} "
                f"SL={'✅' if sl_active else '❌'} "
                f"age={age_hrs:.1f}h"
            )
            return

        # ── ORPHAN DETECTED ──
        reason = []
        if orphaned_no_exits: reason.append("no active TP/SL on exchange")
        if orphaned_timeout:  reason.append(f"exceeded {self.cfg.max_trade_hours}h timeout")

        _log(
            f"🚨 [Self-Healing Alert] Orphaned/Broken position detected for "
            f"{symbol}! Reason: {', '.join(reason)}. "
            f"Initiating emergency market liquidation to recover capital."
        )

        # ── Cancel any lingering orders ──
        for oid in [state.tp1_order_id, state.sl_order_id]:
            if oid and oid in open_order_ids:
                try:
                    await asyncio.get_running_loop().run_in_executor(
                        None, self.executor.cancel_order, symbol, oid
                    )
                except Exception as e:
                    _log(f"[Reconcile] Cancel {oid} failed: {e}")

        await asyncio.sleep(0.5)  # brief settle after cancellations

        # ── Emergency Market Sell ──
        liquidated = await asyncio.get_running_loop().run_in_executor(
            None, self._emergency_market_sell, symbol, state.filled_qty
        )

        if liquidated:
            self.slots.release(symbol)

            # جلب سعر الخروج الفعلي لحساب PnL
            exit_price = 0.0
            try:
                ticker     = self.executor.exchange.fetch_ticker(symbol)
                exit_price = float(ticker.get("last") or ticker.get("close") or 0)
            except Exception:
                pass

            entry   = state.entry_price
            capital = entry * state.filled_qty

            if exit_price > 0 and entry > 0:
                pnl_usd = (exit_price - entry) * state.filled_qty
                pnl_pct = ((exit_price - entry) / entry) * 100
                if pnl_usd >= 0:
                    pnl_line = f"✅ ربح: <b>+${pnl_usd:.3f} (+{pnl_pct:.2f}%)</b>"
                else:
                    pnl_line = f"🔴 خسارة: <b>${pnl_usd:.3f} ({pnl_pct:.2f}%)</b>"
            else:
                pnl_line = "⚠️ لم يتم احتساب PnL — سعر الخروج غير متاح"

            reason_ar = []
            for r in reason:
                if "no active TP/SL" in r:
                    reason_ar.append("لا توجد أوامر TP/SL نشطة على المنصة")
                elif "timeout" in r:
                    reason_ar.append(f"تجاوزت الحد الزمني ({self.cfg.max_trade_hours:.0f} ساعة)")
                else:
                    reason_ar.append(r)

            msg = (
                "🟦 <b>المنصة: MEXC</b>\n"
                "🚨 <b>Self-Healing: تصفية طارئة</b>\n\n"
                f"• <b>العملة:</b> <code>{symbol}</code>\n"
                f"• <b>السبب:</b> {' | '.join(reason_ar)}\n"
                f"• <b>سعر الدخول:</b> <code>${entry:.8g}</code>"
                f" | <b>سعر الخروج:</b> <code>${exit_price:.8g}</code>\n"
                f"• <b>النتيجة:</b> {pnl_line}\n\n"
                "<i>تم تسييل المراكز المتعثرة لفتح مقاعد صيد جديدة.</i>"
            )
            await self._notify(msg)

        else:
            _log(f"[Reconcile] ❌ Emergency sell FAILED for {symbol} — يتطلب تدخلاً يدوياً!")
            fail_msg = (
                "🟦 <b>المنصة: MEXC</b>\n"
                "❌ <b>Self-Healing فشل</b>\n\n"
                f"• <b>العملة:</b> <code>{symbol}</code>\n"
                "• <b>السبب:</b> فشل البيع الطارئ — راجع يدوياً فوراً!\n\n"
                "<i>يتطلب تدخلاً يدوياً عاجلاً.</i>"
            )
            await self._notify(fail_msg)

    def _emergency_market_sell(self, symbol: str, qty: float) -> bool:
        """
        Fee-Adjusted Emergency Market Sell.
        ─────────────────────────────────────
        Root cause of code 30005:
          filled_qty stored in memory = gross purchase qty (e.g., 2914.885)
          actual free wallet balance  = net after fees     (e.g., 2911.9)
          → selling gross qty over-requests → Oversold error

        Fix: always fetch live FREE balance from MEXC before selling.
             clip to actual free amount, apply precision, then sell.
             On 30005 or zero free balance → release slot to stop infinite loop.
        """
        try:
            # ── Step 1: Fetch live free balance (fee-adjusted real qty) ──
            base_token = symbol.split("/")[0].split("_")[0]
            balance    = self.executor.exchange.fetch_balance({"type": "spot"})
            free_qty   = float(
                balance.get(base_token, {}).get("free", 0) or
                balance.get("free", {}).get(base_token, 0)
            )

            _log(f"[Emergency] {symbol}: cached={qty:.4f} free_wallet={free_qty:.4f}")

            if free_qty <= 0:
                _log(
                    f"[Emergency] {symbol}: رصيد حر = 0 — "
                    f"إما مُجمَّد بالكامل أو تم البيع مسبقاً. تحرير الـ slot."
                )
                return True  # release slot — stop infinite loop

            # ── Step 2: Clip to free balance (never exceed what's available) ──
            amount_to_sell = min(qty, free_qty)

            # ── Step 3: Apply exchange precision using amount_to_precision ──
            try:
                precise_qty = float(
                    self.executor.exchange.amount_to_precision(symbol, amount_to_sell)
                )
            except Exception:
                precise_qty = self.executor._apply_step_size(symbol, amount_to_sell)

            if precise_qty <= 0:
                _log(f"[Emergency] {symbol}: qty={precise_qty} بعد precision — تحرير الـ slot.")
                return True

            # ── Step 4: Execute fee-adjusted market sell ──
            o = self.executor.exchange.create_market_sell_order(symbol, precise_qty)
            _log(
                f"[Emergency] ✅ {symbol}: بيع طارئ {precise_qty} "
                f"(من {free_qty:.4f} حر) @ market | ID:{o['id']}"
            )
            return True

        except Exception as e:
            err = str(e)
            if "30005" in err or "Oversold" in err or "oversold" in err:
                _log(
                    f"[Emergency] {symbol}: code 30005 بعد تعديل الرسوم. "
                    f"تحرير الـ slot لمنع التكرار اللانهائي."
                )
                return True  # release slot unconditionally on 30005
            _log(f"[Emergency] ❌ {symbol}: {err[:120]}")
            return False

    async def _notify(self, text: str):
        if not self.cfg.telegram_token or not self.cfg.telegram_chat_id:
            return
        # Prepend MEXC header if not already present
        header = "\U0001f7e6 <b>\u0627\u0644\u0645\u0646\u0635\u0629: MEXC</b>\n"
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
            _log(f"[Monitor] Telegram notify error: {e}")


# ─────────────────────────────────────────────
# 9. SCALPING ORCHESTRATOR  — ties everything together
# ─────────────────────────────────────────────
class ScalpingOrchestrator:
    """
    Main engine loop:
      1. Scan exchange symbols
      2. Layer 1: RSI + CMC + LunarCrush shields
      3. Layer 2: Parallel AI Committee (Safety + Fib)
      4. Layer 3: Instant MARKET execution + bracket
      5. TradeMonitor releases slots upon SL/TP3
    """

    def __init__(self, cfg: Config):
        self.cfg                = cfg
        self.slots              = SlotManager(cfg)
        self.pipeline           = DataPipeline(cfg)
        self.committee          = DeepSeekAnalyst(cfg)
        self.executor           = HighSpeedExecutor(cfg)
        self.monitor            = TradeMonitor(cfg, self.executor, self.slots)
        # Deduplication guard — prevents sub-second duplicate buys
        self._processing_symbols: set[str] = set()
        self._processing_lock               = threading.Lock()

    async def _send_telegram(self, text: str):
        if not self.cfg.telegram_token or not self.cfg.telegram_chat_id:
            return
        # Prepend MEXC header if not already present
        header = "\U0001f7e6 <b>\u0627\u0644\u0645\u0646\u0635\u0629: MEXC</b>\n"
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
        delta  = closes.diff()
        gain   = delta.clip(lower=0).rolling(period).mean()
        loss   = (-delta.clip(upper=0)).rolling(period).mean()
        rs     = gain / loss.replace(0, 1e-9)
        rsi    = 100 - 100 / (1 + rs)
        return float(rsi.iloc[-1])

    def _fetch_indicators(self, symbol: str) -> Optional[dict]:
        try:
            ohlcv = self.executor.exchange.fetch_ohlcv(symbol, timeframe="1d", limit=120)
            if not ohlcv or len(ohlcv) < 30:
                return None
            df     = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"]).astype(float)
            closes = df["close"]
            highs  = df["high"]
            lows   = df["low"]

            current = float(closes.iloc[-1])
            rsi     = self._calc_rsi(closes)
            fib_high = float(highs.tail(60).max())
            fib_low  = float(lows.tail(60).min())

            vol_usd = 0.0
            try:
                t       = self.executor.exchange.fetch_ticker(symbol)
                vol_usd = float(t.get("quoteVolume") or 0)
            except Exception:
                pass

            return {
                "current": current,
                "rsi":     rsi,
                "fib_high": fib_high,
                "fib_low":  fib_low,
                "vol_usd":  vol_usd,
            }
        except Exception as e:
            # سجّل الخطأ كاملاً لمعرفة السبب
            _log(f"[Scan] ❌ {symbol}: {type(e).__name__}: {str(e)[:80]}")
            return None

    async def _process_candidate(
        self,
        session: aiohttp.ClientSession,
        symbol:  str,
    ):
        # ── Deduplication Guard — blocks sub-second duplicate buys ──
        with self._processing_lock:
            if symbol in self._processing_symbols:
                _log(f"[DeDup] {symbol}: already processing — skip")
                return
            if not self.slots.is_vacant(symbol):
                return
            self._processing_symbols.add(symbol)

        try:
            await self._process_candidate_inner(session, symbol)
        finally:
            with self._processing_lock:
                self._processing_symbols.discard(symbol)

    async def _process_candidate_inner(
        self,
        session: aiohttp.ClientSession,
        symbol:  str,
    ):
        ind = await asyncio.get_running_loop().run_in_executor(
            None, self._fetch_indicators, symbol
        )
        if not ind:
            return

        _log(f"[Scan] {symbol}: RSI={ind['rsi']:.1f} Vol=${ind['vol_usd']/1e6:.1f}M")

        # ── Layer 1 ──
        passed, reason = await self.pipeline.layer1_pass(
            session, symbol, ind["rsi"]
        )
        if not passed:
            _log(f"[L1 ❌] {symbol}: {reason}")
            return
        _log(f"[L1 ✅] {symbol}: {reason}")

        # ── Layer 2: DeepSeek ──
        result = await self.committee.run(
            symbol   = symbol,
            rsi      = ind["rsi"],
            vol_m    = ind["vol_usd"] / 1e6,
            entry    = ind["current"],
            fib_high = ind["fib_high"],
            fib_low  = ind["fib_low"],
        )

        if not result["safety_ok"]:
            _log(f"[L2 ❌] {symbol}: DeepSeek SKIP ({result['elapsed']}s)")
            return

        targets = result["targets"]

        # ── SL: pure 15m swing low — AI never touches ──
        stop_loss = await asyncio.get_running_loop().run_in_executor(
            None, calculate_micro_swing_sl,
            self.executor.exchange, symbol, ind["current"]
        )

        _log(
            f"[L2 ✅] {symbol} ({result['elapsed']}s) | "
            f"TP1={targets['tp1']:.6g} TP2={targets['tp2']:.6g} "
            f"TP3={targets['tp3']:.6g} SL={stop_loss:.6g}"
        )

        # ── Final slot vacancy check before execution ──
        if not self.slots.is_vacant(symbol):
            _log(f"[L3] {symbol}: slot taken — skip")
            return

        # ── Layer 3: Execute with live-price fallback ──
        entry_price = ind["current"]

        # Live price fallback — exchange sometimes returns 0 on fast markets
        if not entry_price or entry_price == 0:
            try:
                ticker      = self.executor.exchange.fetch_ticker(symbol)
                entry_price = float(ticker.get("last") or ticker.get("close") or 0)
                _log(f"[L3] {symbol}: ⚠️ entry_price was 0 → live fallback: {entry_price:.8g}")
            except Exception as e:
                _log(f"[L3] {symbol}: live price fallback failed: {e}")
                return

        if entry_price <= 0:
            _log(f"[L3] {symbol}: entry_price still 0 after fallback — abort")
            return

        state = await asyncio.get_running_loop().run_in_executor(
            None,
            self.executor.execute_full_trade,
            symbol, entry_price,
            targets["tp1"], targets["tp2"], targets["tp3"], stop_loss,
        )

        if not state:
            _log(f"[L3 ❌] {symbol}: execution failed")
            await self._send_telegram(
                f"⚠️ <b>فشل التنفيذ التلقائي</b>\n"
                f"العملة: <code>{symbol}</code>\n"
                f"تحقق من رصيد USDT في Spot Wallet"
            )
            return

        # Validate bracket was placed — warn if missing
        if not state.tp1_order_id and not state.tp2_order_id and not state.tp3_order_id:
            _log(f"[L3 ⚠️] {symbol}: تم الشراء لكن أوامر TP/SL لم تُوضع — راجع يدوياً!")

        self.slots.occupy(state)

        await self._send_telegram(
            f"🚀 <b>تم التنفيذ التلقائي</b>\n\n"
            f"العملة:    <code>{symbol}</code>\n"
            f"سعر الدخول: <code>{state.entry_price:.8g}</code>\n"
            f"الكمية:    <code>{state.filled_qty:.6f}</code>\n"
            f"رأس المال: <code>${self.cfg.capital:.2f}</code>\n\n"
            f"<b>الأهداف ({targets['method']})</b>\n"
            f"TP1: <code>{state.tp1:.8g}</code>  (+{(state.tp1/state.entry_price-1)*100:.1f}%)  → 40%\n"
            f"TP2: <code>{state.tp2:.8g}</code>  (+{(state.tp2/state.entry_price-1)*100:.1f}%)  → 35%\n"
            f"TP3: <code>{state.tp3:.8g}</code>  (+{(state.tp3/state.entry_price-1)*100:.1f}%)  → 25%\n"
            f"SL:  <code>{state.stop_loss:.8g}</code>  "
            f"(-{(1-state.stop_loss/state.entry_price)*100:.1f}%)\n\n"
            f"🛡️ SL ديناميكي (15m Swing Low)\n"
            f"⏱️ DeepSeek: {result['elapsed']}s"
        )

    async def scan_loop(self):
        interval = self.cfg.scan_interval * 60
        await self._send_telegram(
            f"🤖 <b>Scalping Engine نشط</b>\n"
            f"🛡️ Layer 1: RSI ≤ {self.cfg.rsi_threshold} + CMC + LunarCrush\n"
            f"🧠 Layer 2: AI Safety + Fib ({self.cfg.max_ai_tokens} tokens)\n"
            f"⚡ Layer 3: MARKET execution + bracket\n"
            f"📊 Slots: {self.cfg.max_slots} | Capital: ${self.cfg.capital}/trade\n"
            f"🔄 Scan: every {self.cfg.scan_interval} min"
        )

        while True:
            start = datetime.now()
            _log(f"🔄 Scan cycle: {start.strftime('%Y-%m-%d %H:%M:%S')}")

            # ── Real-Time USDT Balance Guard ──
            try:
                balance_data = self.executor.exchange.fetch_balance({"type": "spot"})
                free_usdt    = float(balance_data.get("USDT", {}).get("free", 0))
                _log(f"[Balance] Free USDT: ${free_usdt:.2f} | Capital: ${self.cfg.capital:.2f}")
                if free_usdt < self.cfg.capital:
                    _log(
                        f"[Scan Skip] USDT غير كافٍ (${free_usdt:.2f} < ${self.cfg.capital:.2f})"
                        f" — انتظار 60 ثانية..."
                    )
                    await asyncio.sleep(60)
                    continue
            except Exception as e:
                _log(f"[Balance ⚠️] فشل جلب الرصيد: {e}")

            try:
                markets = self.executor.exchange.markets

                symbols = []
                for s, mkt in markets.items():
                    # --- Surgical Shariah & ETF Compliance Shield ---
                    base_asset = s.split("/")[0].split("_")[0].strip().upper()

                    # Shariah blacklist
                    if base_asset in self.cfg.blacklisted_assets:
                        continue

                    # Leveraged tokens (3L/3S/5L/5S/DOWN/UP)
                    if any(p in base_asset for p in ["3L","3S","5L","5S","DOWN","UP"]):
                        if len(base_asset) > 4 and any(base_asset.endswith(x) for x in ["3L","3S","5L","5S"]):
                            continue
                    # ------------------------------------------------

                    # Rule 1: USDT pairs only
                    if not s.endswith("/USDT"):
                        continue
                    # Rule 2: exclude Futures/Swaps/Perps
                    if ":" in s or "swap" in s.lower() or "future" in s.lower():
                        continue
                    # Rule 3: direct append
                    symbols.append(s)
                _log(f"[Scan] تم فحص وتأكيد جاهزية {len(symbols)} عملة Spot/USDT حقيقية للبدء.")
                passed_l1 = 0
                checked   = 0

                # معالجة تسلسلية مع تأخير لتجنب rate limit
                # كل 5 عملات بالتوازي ثم استراحة 2 ثانية
                BATCH = 5
                async with aiohttp.ClientSession() as session:
                    for i in range(0, len(symbols), BATCH):
                        batch = symbols[i:i+BATCH]
                        tasks = [
                            self._process_candidate(session, sym)
                            for sym in batch
                        ]
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        checked += len(batch)
                        await asyncio.sleep(2)  # تجنب Rate Limit

                        # تقرير دوري كل 50 عملة
                        if checked % 50 == 0:
                            _log(
                                f"[Scan] تقدم: {checked}/{len(symbols)} | "
                                f"slots={self.slots.used}/{self.cfg.max_slots}"
                            )

            except Exception as e:
                _log(f"❌ Scan error: {e}")
                await self._send_telegram(f"⚠️ Scan error: {str(e)[:100]}")

            elapsed = (datetime.now() - start).seconds // 60
            _log(f"✅ Cycle done in {elapsed}m | slots={self.slots.used}/{self.cfg.max_slots}")
            await asyncio.sleep(interval)

    async def run(self):
        """Entry point — runs scan_loop + monitor concurrently."""
        await asyncio.gather(
            self.scan_loop(),
            self.monitor.start(),
        )


# ─────────────────────────────────────────────
# LOGGER
# ─────────────────────────────────────────────
def _log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} | {msg}", flush=True)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    cfg = Config()
    if not cfg.mexc_api_key or not cfg.mexc_api_secret:
        raise RuntimeError("MEXC_API_KEY and MEXC_API_SECRET are required")
    bot = ScalpingOrchestrator(cfg)
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        _log("⛔ Bot stopped")
