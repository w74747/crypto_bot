"""
scalping_bot.py
===============
Production-Grade Crypto Scalping Engine
Classes: Config | SlotManager | DataPipeline | ParallelAICommittee
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
    groq_api_key:      str   = field(default_factory=lambda: os.environ.get("GROQ_API_KEY", ""))
    together_api_key:  str   = field(default_factory=lambda: os.environ.get("TOGETHER_API_KEY") or os.environ.get("TOGATHER_API_KEY", ""))
    deepseek_api_key:  str   = field(default_factory=lambda: os.environ.get("DEEPSEEK_API_KEY", ""))
    cmc_api_key:       str   = field(default_factory=lambda: os.environ.get("COINMARKETCAP_API_KEY", ""))
    lunar_api_key:     str   = field(default_factory=lambda: os.environ.get("LUNARCRUSH_API_KEY", ""))
    database_url:      str   = field(default_factory=lambda: os.environ.get("DATABASE_URL", ""))

    capital:           float = field(default_factory=lambda: float(os.environ.get("TRADE_INVESTMENT_AMOUNT", "30")))
    max_slots:         int   = field(default_factory=lambda: int(os.environ.get("MAX_CONCURRENT_TRADES", "3")))
    scan_interval:     int   = field(default_factory=lambda: int(os.environ.get("SCAN_INTERVAL_MINUTES", "60")))
    rsi_threshold:     int   = field(default_factory=lambda: int(os.environ.get("RSI_OVERSOLD_THRESHOLD", "31")))
    min_volume_usd:    float = field(default_factory=lambda: float(os.environ.get("MIN_DAILY_VOLUME_USD", "1000000")))
    monitor_interval:  int   = 30   # ثوانٍ بين كل فحص للأوامر
    max_ai_tokens:     int   = 120
    groq_model:        str   = field(default_factory=lambda: os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile"))
    together_model:    str   = field(default_factory=lambda: os.environ.get("TOGETHER_MODEL", "meta-llama/Llama-3.3-70B-Instruct-Turbo"))
    deepseek_model:    str   = field(default_factory=lambda: os.environ.get("DEEPSEEK_MODEL", "deepseek-chat"))


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
    opened_at:      float = field(default_factory=time.time)


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
# 6. PARALLEL AI COMMITTEE
# ─────────────────────────────────────────────
class ParallelAICommittee:
    """
    3 specialists run concurrently via asyncio.gather.
    SL is NOT asked from AI — calculated purely in code.
    Fib targets are suggested by AI but overridden by cascading engine.
    Target latency: < 3 seconds.
    """

    SAFETY_SYS = (
        "Safety Guard. Is 24h spot volume > $1M and order book depth adequate? "
        "One sentence only, then on the last line: [SAFETY_VOTE: YES] or [SAFETY_VOTE: NO]"
    )
    FIB_SYS = (
        "Fib Planner. Compute Fibonacci Internal Retracement targets for this 15m scalp. "
        "One sentence, then:\n[TP1_PRICE: X.XXXX]\n[TP2_PRICE: X.XXXX]\n[TP3_PRICE: X.XXXX]"
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
                    _log(f"[{label}] 429 Rate Limit")
                    return ""
                resp.raise_for_status()
                data = await resp.json()
                return data["choices"][0]["message"]["content"] or ""
        except asyncio.TimeoutError:
            _log(f"[{label}] Timeout")
            return ""
        except Exception as e:
            _log(f"[{label}] {str(e)[:60]}")
            return ""

    async def _call_fallback(
        self,
        session:   aiohttp.ClientSession,
        providers: list[tuple],
        system:    str,
        user_msg:  str,
        role:      str,
    ) -> str:
        for label, key, url, model in providers:
            if not key:
                continue
            text = await self._call(session, key, url, model, system, user_msg, label)
            if text:
                _log(f"[{role}] {label} ✅")
                return text
        return ""

    async def specialist_safety(
        self, session: aiohttp.ClientSession, symbol: str,
        rsi: float, vol_m: float
    ) -> bool:
        msg = (
            f"Symbol: {symbol} | RSI: {rsi:.1f} | "
            f"24h Volume: ${vol_m:.1f}M\n"
            f"Is liquidity safe for a $30 scalp entry?"
        )
        text = await self._call_fallback(
            session,
            providers=[
                ("Groq",     self.cfg.groq_api_key,     "https://api.groq.com/openai/v1", self.cfg.groq_model),
                ("DeepSeek", self.cfg.deepseek_api_key, "https://api.deepseek.com/v1",    self.cfg.deepseek_model),
            ],
            system=self.SAFETY_SYS, user_msg=msg, role="Safety",
        )
        m = re.search(r'\[SAFETY_VOTE:\s*(YES|NO)\]', text, re.IGNORECASE)
        voted = (m.group(1).upper() == "YES") if m else False
        _log(f"[Safety] {symbol}: {'✅ YES' if voted else '❌ NO'}")
        return voted

    async def specialist_fib(
        self, session: aiohttp.ClientSession,
        symbol: str, entry: float, fib_high: float, fib_low: float, rsi: float
    ) -> dict:
        msg = (
            f"Symbol: {symbol} | Entry: {entry:.8g} | "
            f"Local High: {fib_high:.8g} | Local Low: {fib_low:.8g} | RSI: {rsi:.1f}\n"
            f"Compute 15m Fibonacci Internal Retracement targets."
        )
        text = await self._call_fallback(
            session,
            providers=[
                ("DeepSeek", self.cfg.deepseek_api_key, "https://api.deepseek.com/v1",    self.cfg.deepseek_model),
                ("Together", self.cfg.together_api_key, "https://api.together.xyz/v1",    self.cfg.together_model),
            ],
            system=self.FIB_SYS, user_msg=msg, role="Fib",
        )

        def extract(tag, default):
            m = re.search(rf'\[{tag}:\s*([\d.]+)\]', text)
            return float(m.group(1)) if m else default

        fib_range = fib_high - fib_low
        ai_tp1 = extract("TP1_PRICE", fib_low + fib_range * 0.382)
        ai_tp2 = extract("TP2_PRICE", fib_low + fib_range * 0.500)
        ai_tp3 = extract("TP3_PRICE", fib_low + fib_range * 0.618)

        # always run through cascading engine — AI output is a hint only
        return calculate_cascading_targets(
            fib_high=max(ai_tp3, fib_high),
            fib_low=min(ai_tp1, fib_low),
            entry_price=entry,
        )

    async def run(
        self,
        symbol:    str,
        rsi:       float,
        vol_m:     float,
        entry:     float,
        fib_high:  float,
        fib_low:   float,
    ) -> dict:
        start = time.time()
        async with aiohttp.ClientSession() as session:
            safety_coro = self.specialist_safety(session, symbol, rsi, vol_m)
            fib_coro    = self.specialist_fib(session, symbol, entry, fib_high, fib_low, rsi)
            safety_ok, targets = await asyncio.gather(safety_coro, fib_coro, return_exceptions=True)

        if isinstance(safety_ok, Exception):
            safety_ok = False
        if isinstance(targets, Exception):
            targets = calculate_cascading_targets(fib_high, fib_low, entry)

        elapsed = time.time() - start
        _log(
            f"[Committee] {symbol}: safety={'✅' if safety_ok else '❌'} "
            f"TP1={targets['tp1']:.6g} TP2={targets['tp2']:.6g} "
            f"TP3={targets['tp3']:.6g} | {elapsed:.1f}s"
        )
        return {
            "safety_ok": safety_ok,
            "targets":   targets,
            "elapsed":   round(elapsed, 2),
        }


# ─────────────────────────────────────────────
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
        symbol:      str,
        filled_qty:  float,
        filled_price: float,
        tp1: float, tp2: float, tp3: float,
        stop_loss:   float,
    ) -> dict:
        """Places TP1/TP2/TP3 limit sells + stop-loss. Returns order IDs."""
        q1 = self._apply_step_size(symbol, filled_qty * TP1_QTY_PCT)
        q2 = self._apply_step_size(symbol, filled_qty * TP2_QTY_PCT)
        q3 = self._apply_step_size(symbol, filled_qty * TP3_QTY_PCT)

        ids: dict = {}
        for label, qty, price in [("tp1", q1, tp1), ("tp2", q2, tp2), ("tp3", q3, tp3)]:
            try:
                o = self.exchange.create_limit_sell_order(symbol, qty, price)
                ids[f"{label}_order_id"] = o["id"]
                _log(f"✅ {label.upper()}: {price:.8g} ×{qty} ID:{o['id']}")
            except Exception as e:
                _log(f"❌ {label.upper()} FAILED {symbol}: {e}")

        try:
            o = self.exchange.create_order(
                symbol, "STOP_LOSS_LIMIT", "sell", filled_qty,
                stop_loss * 0.999,
                {"stopPrice": stop_loss, "type": "spot"},
            )
            ids["sl_order_id"] = o["id"]
            _log(f"✅ SL: {stop_loss:.8g} ID:{o['id']}")
        except Exception as e:
            _log(f"❌ SL FAILED {symbol}: {e} — review manually!")

        return ids

    def move_sl_to_breakeven(self, symbol: str, entry_price: float, remaining_qty: float):
        """Moves SL to entry price the moment TP1 fills."""
        try:
            qty = self._apply_step_size(symbol, remaining_qty)
            o   = self.exchange.create_order(
                symbol, "STOP_LOSS_LIMIT", "sell", qty,
                entry_price * 0.999,
                {"stopPrice": entry_price, "type": "spot"},
            )
            _log(f"✅ Break-Even {symbol}: SL→{entry_price:.8g} ID:{o['id']}")
            return o["id"]
        except Exception as e:
            _log(f"❌ Break-Even FAILED {symbol}: {e}")
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
        symbol:    str,
        entry_price: float,
        tp1: float, tp2: float, tp3: float,
        stop_loss: float,
    ) -> Optional[SlotState]:
        """Executes buy + bracket. Returns SlotState or None on failure."""
        buy = self.market_buy(symbol, entry_price)
        if not buy:
            return None

        bracket = self.place_bracket(
            symbol, buy["filled_qty"], buy["filled_price"],
            tp1, tp2, tp3, stop_loss,
        )

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
        self._running = True
        _log("[TradeMonitor] ✅ started — polling every 30s (bot IDs only)")
        while self._running:
            await self._check_all_slots()
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

        # ── Check TP1 → move SL to break-even ──
        if state.tp1_order_id:
            tp1_status = await asyncio.get_running_loop().run_in_executor(
                None, self.executor.fetch_order_status, symbol, state.tp1_order_id
            )
            if tp1_status == "closed":
                remaining_qty = state.filled_qty * (TP2_QTY_PCT + TP3_QTY_PCT)
                _log(f"[Monitor] 🎯 TP1 HIT: {symbol} → break-even SL")
                await asyncio.get_running_loop().run_in_executor(
                    None, self.executor.move_sl_to_breakeven,
                    symbol, state.entry_price, remaining_qty
                )
                # cancel old SL
                if state.sl_order_id:
                    await asyncio.get_running_loop().run_in_executor(
                        None, self.executor.cancel_order, symbol, state.sl_order_id
                    )

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

    async def _notify(self, text: str):
        if not self.cfg.telegram_token or not self.cfg.telegram_chat_id:
            return
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
        self.cfg       = cfg
        self.slots     = SlotManager(cfg)
        self.pipeline  = DataPipeline(cfg)
        self.committee = ParallelAICommittee(cfg)
        self.executor  = HighSpeedExecutor(cfg)
        self.monitor   = TradeMonitor(cfg, self.executor, self.slots)

    async def _send_telegram(self, text: str):
        if not self.cfg.telegram_token or not self.cfg.telegram_chat_id:
            return
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
        if not self.slots.is_vacant(symbol):
            return

        ind = await asyncio.get_running_loop().run_in_executor(
            None, self._fetch_indicators, symbol
        )
        if not ind:
            return  # خطأ مسجّل في _fetch_indicators

        _log(f"[Scan] {symbol}: RSI={ind['rsi']:.1f} Vol=${ind['vol_usd']/1e6:.1f}M")

        # ── Layer 1 ──
        passed, reason = await self.pipeline.layer1_pass(
            session, symbol, ind["rsi"]
        )
        if not passed:
            _log(f"[L1 ❌] {symbol}: {reason}")
            return
        _log(f"[L1 ✅] {symbol}: {reason}")

        # ── Layer 2: AI Committee ──
        result = await self.committee.run(
            symbol   = symbol,
            rsi      = ind["rsi"],
            vol_m    = ind["vol_usd"] / 1e6,
            entry    = ind["current"],
            fib_high = ind["fib_high"],
            fib_low  = ind["fib_low"],
        )

        if not result["safety_ok"]:
            _log(f"[L2 ❌] {symbol}: Safety rejected ({result['elapsed']}s)")
            return

        targets = result["targets"]

        # ── SL: pure code calculation — AI never touches this ──
        stop_loss = await asyncio.get_running_loop().run_in_executor(
            None, calculate_micro_swing_sl,
            self.executor.exchange, symbol, ind["current"]
        )

        _log(
            f"[L2 ✅] {symbol} ({result['elapsed']}s) | "
            f"TP1={targets['tp1']:.6g} TP2={targets['tp2']:.6g} "
            f"TP3={targets['tp3']:.6g} SL={stop_loss:.6g}"
        )

        # ── Double-check slot still vacant (race condition guard) ──
        if not self.slots.is_vacant(symbol):
            _log(f"[L3] {symbol}: slot taken by concurrent process — skip")
            return

        # ── Layer 3: Execute ──
        state = await asyncio.get_running_loop().run_in_executor(
            None,
            self.executor.execute_full_trade,
            symbol, ind["current"],
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
            f"⏱️ AI Committee: {result['elapsed']}s"
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

            try:
                markets = self.executor.exchange.markets

                # MEXC ccxt: يدعم /USDT و _USDT كلاهما
                symbols = []
                for s, mkt in markets.items():
                    if not (s.endswith("/USDT") or s.endswith("_USDT")):
                        continue
                    if ":" in s:
                        continue
                    if mkt.get("active") is False:
                        continue
                    symbols.append(s)

                if not symbols:
                    sample = list(markets.keys())[:8]
                    _log(f"[Scan] ⚠️ 0 عملة — عينة مفاتيح: {sample}")
                else:
                    _log(f"[Scan] فحص {len(symbols)} عملة...")
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
