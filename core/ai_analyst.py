"""
core/ai_analyst.py
==================
Parallel AI Workers — 3 متخصصون يعملون في آن واحد
بدل 9 طلبات خطية → 3 طلبات متوازية في < 3 ثوانٍ

Specialist 1 (Safety Guard)    → DeepSeek via Groq
Specialist 2 (SL Engineer)     → Together AI
Specialist 3 (Fib Planner)     → DeepSeek
"""

import os
import re
import asyncio
import aiohttp
import json
from utils.logger import logger
from core.scanner import TradeOpportunity

GROQ_API_KEY     = os.getenv("GROQ_API_KEY", "")
TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY") or os.getenv("TOGATHER_API_KEY", "")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")

GROQ_MODEL     = os.getenv("GROQ_MODEL",     "llama-3.3-70b-versatile")
TOGETHER_MODEL = os.getenv("TOGETHER_MODEL", "meta-llama/Llama-3.3-70B-Instruct-Turbo")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

MAX_TOKENS = 120


# ==========================================
# Async HTTP call
# ==========================================
async def _async_call(
    session:  aiohttp.ClientSession,
    api_key:  str,
    base_url: str,
    model:    str,
    system:   str,
    user_msg: str,
    label:    str,
) -> str:
    """طلب HTTP غير متزامن — لا blocking"""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":       model,
        "max_tokens":  MAX_TOKENS,
        "temperature": 0.1,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": user_msg},
        ],
    }
    try:
        async with session.post(
            f"{base_url}/chat/completions",
            headers = headers,
            json    = payload,
            timeout = aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status == 429:
                logger.warning(f"[{label}] 429 Rate Limit")
                return ""
            resp.raise_for_status()
            data = await resp.json()
            return data["choices"][0]["message"]["content"] or ""
    except asyncio.TimeoutError:
        logger.warning(f"[{label}] Timeout (8s)")
        return ""
    except Exception as e:
        logger.warning(f"[{label}] خطأ: {str(e)[:60]}")
        return ""


async def _async_call_with_fallback(
    session:   aiohttp.ClientSession,
    providers: list[tuple],
    system:    str,
    user_msg:  str,
    role:      str,
) -> tuple[str, str]:
    """يجرب المزودين بالترتيب — يُعيد أول رد ناجح"""
    for label, key, url, model in providers:
        if not key:
            continue
        text = await _async_call(session, key, url, model, system, user_msg, label)
        if text:
            logger.info(f"[{role}] {label} ✅")
            return text, label
    return f"لا يوجد مزود. [SAFETY_VOTE: NO]", "—"


# ==========================================
# Specialist 1 — Safety Guard (Liquidity)
# ==========================================
SAFETY_SYS = """Safety Guard. تحقق: هل الحجم > $1M وعمق دفتر الأوامر كافٍ؟
جملة واحدة فقط ثم في آخر سطر: [SAFETY_VOTE: YES] أو [SAFETY_VOTE: NO]"""

async def specialist_safety(session, opp: TradeOpportunity) -> tuple[str, bool]:
    vol_m = opp.volume_24h_usd / 1e6
    msg   = (
        f"العملة: {opp.symbol} | حجم: ${vol_m:.1f}M | "
        f"RSI: {opp.rsi_daily:.1f} | انهيار: {opp.crash_pct_60d:.0%}\n"
        f"هل الحجم والسيولة كافيان للدخول الآمن؟"
    )
    text, _ = await _async_call_with_fallback(
        session,
        providers=[
            ("Groq",     GROQ_API_KEY,     "https://api.groq.com/openai/v1",  GROQ_MODEL),
            ("DeepSeek", DEEPSEEK_API_KEY, "https://api.deepseek.com/v1",     DEEPSEEK_MODEL),
        ],
        system   = SAFETY_SYS,
        user_msg = msg,
        role     = "Safety",
    )
    m     = re.search(r'\[SAFETY_VOTE:\s*(YES|NO)\]', text, re.IGNORECASE)
    voted = (m.group(1).upper() == "YES") if m else False
    logger.info(f"[Safety] {opp.symbol}: {'✅ YES' if voted else '❌ NO'}")
    return text, voted


# ==========================================
# Specialist 2 — SL Engineer
# ==========================================
SL_SYS = """SL Engineer. احسب SL = 0.15% تحت آخر قاع محلي في آخر 5 شمعات.
أجب بجملة واحدة ثم: [CALCULATED_SL_PRICE: X.XXXX]"""

async def specialist_sl(session, opp: TradeOpportunity) -> tuple[str, float]:
    msg = (
        f"العملة: {opp.symbol} | دخول: {opp.entry_price:.8g} | "
        f"أقرب دعم: {opp.nearest_support:.8g} | RSI: {opp.rsi_daily:.1f}\n"
        f"احسب SL = 0.15% تحت أقرب قاع محلي."
    )
    text, _ = await _async_call_with_fallback(
        session,
        providers=[
            ("Together", TOGETHER_API_KEY, "https://api.together.xyz/v1",    TOGETHER_MODEL),
            ("DeepSeek", DEEPSEEK_API_KEY, "https://api.deepseek.com/v1",    DEEPSEEK_MODEL),
        ],
        system   = SL_SYS,
        user_msg = msg,
        role     = "SL",
    )
    m  = re.search(r'\[CALCULATED_SL_PRICE:\s*([\d.]+)\]', text)
    sl = float(m.group(1)) if m else opp.nearest_support * 0.9985

    # تحقق: SL منطقي (بين 1% و 20% تحت السعر)
    min_sl = opp.entry_price * 0.80
    max_sl = opp.entry_price * 0.99
    sl     = max(min_sl, min(sl, max_sl))

    logger.info(f"[SL] {opp.symbol}: SL={sl:.8g}")
    return text, sl


# ==========================================
# Specialist 3 — Fibonacci Planner
# ==========================================
FIB_SYS = """Fib Planner. احسب أهداف Fibonacci Internal Retracement من الموجة الحالية.
أجب بجملة واحدة ثم:
[TP1_PRICE: X.XXXX]
[TP2_PRICE: X.XXXX]
[TP3_PRICE: X.XXXX]"""

async def specialist_fib(session, opp: TradeOpportunity) -> tuple[str, dict]:
    fib_range = opp.fib_high - opp.fib_low
    tp1_calc  = opp.fib_low + fib_range * 0.382
    tp2_calc  = opp.fib_low + fib_range * 0.500
    tp3_calc  = opp.fib_low + fib_range * 0.618

    msg = (
        f"العملة: {opp.symbol} | دخول: {opp.entry_price:.8g}\n"
        f"قمة محلية: {opp.fib_high:.8g} | قاع: {opp.fib_low:.8g}\n"
        f"RSI: {opp.rsi_daily:.1f} | احسب أهداف Fib Internal Retracement."
    )
    text, _ = await _async_call_with_fallback(
        session,
        providers=[
            ("DeepSeek", DEEPSEEK_API_KEY, "https://api.deepseek.com/v1",    DEEPSEEK_MODEL),
            ("Together", TOGETHER_API_KEY, "https://api.together.xyz/v1",    TOGETHER_MODEL),
        ],
        system   = FIB_SYS,
        user_msg = msg,
        role     = "Fib",
    )

    def extract(tag, default):
        m = re.search(rf'\[{tag}:\s*([\d.]+)\]', text)
        return float(m.group(1)) if m else default

    tp1 = extract("TP1_PRICE", tp1_calc)
    tp2 = extract("TP2_PRICE", tp2_calc)
    tp3 = extract("TP3_PRICE", tp3_calc)

    # الحد الأقصى 15% للـ Scalping
    cap = opp.entry_price * 1.15
    tp1 = min(max(tp1, opp.entry_price * 1.02), cap)
    tp2 = min(max(tp2, tp1 * 1.02), cap)
    tp3 = min(max(tp3, tp2 * 1.02), cap)

    targets = {
        "tp1": round(tp1, 10),
        "tp2": round(tp2, 10),
        "tp3": round(tp3, 10),
    }
    logger.info(
        f"[Fib] {opp.symbol}: "
        f"TP1={tp1:.6g} TP2={tp2:.6g} TP3={tp3:.6g}"
    )
    return text, targets


# ==========================================
# Pipeline الرئيسي — 3 متخصصون بالتوازي
# ==========================================
async def run_parallel_committee(opp: TradeOpportunity) -> dict:
    """
    يُشغّل 3 متخصصين في آن واحد عبر asyncio.gather
    المدة المستهدفة: < 3 ثوانٍ
    """
    import time
    start = time.time()

    async with aiohttp.ClientSession() as session:
        # الثلاثة يعملون بالتوازي
        safety_task = specialist_safety(session, opp)
        sl_task     = specialist_sl(session, opp)
        fib_task    = specialist_fib(session, opp)

        results = await asyncio.gather(
            safety_task, sl_task, fib_task,
            return_exceptions=True,
        )

    elapsed = time.time() - start
    logger.info(f"[Committee] {opp.symbol}: انتهى في {elapsed:.1f}s")

    # استخراج النتائج
    safety_text, safety_ok = (results[0] if not isinstance(results[0], Exception)
                               else ("خطأ", False))
    sl_text,     calc_sl   = (results[1] if not isinstance(results[1], Exception)
                               else ("خطأ", opp.nearest_support * 0.9985))
    fib_text,    targets   = (results[2] if not isinstance(results[2], Exception)
                               else ("خطأ", {"tp1": opp.tp1, "tp2": opp.tp2, "tp3": opp.tp3}))

    # القرار: Safety YES كافٍ للتنفيذ
    send_signal = safety_ok

    if send_signal:
        label = "✅ Safety موافق — تنفيذ فوري"
        emoji = "🟢"
    else:
        label = "❌ Safety رفض — لا تنفيذ"
        emoji = "🔴"

    votes = f"Safety {'✅' if safety_ok else '❌'}  SL 📐  Fib 🎯"

    logger.info(f"[Committee] {opp.symbol}: {label} | {elapsed:.1f}s")

    return {
        "symbol":      opp.symbol,
        "elapsed_sec": round(elapsed, 2),
        "debate_log": [
            {"round": 1, "speaker": "Safety",  "text": safety_text,
             "vote": "approve" if safety_ok else "reject"},
            {"round": 1, "speaker": "SL",      "text": sl_text,     "vote": "approve"},
            {"round": 1, "speaker": "Fib",     "text": fib_text,    "vote": "approve"},
        ],
        "calculated_sl":   calc_sl,
        "fib_targets":     targets,
        "tech_model":      "Parallel",
        "risk_model":      "Parallel",
        "market_model":    "Parallel",
        "recommendation": {
            "send_signal": send_signal,
            "label":       label,
            "emoji":       emoji,
            "confidence":  f"{elapsed:.1f}s",
            "votes":       votes,
        },
    }


# wrapper للاستدعاء من كود sync
def run_expert_debate(opp: TradeOpportunity) -> dict:
    """
    Wrapper لتشغيل الـ committee من كود غير async
    يُعيد نفس هيكل البيانات القديم للتوافق مع باقي الكود
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, run_parallel_committee(opp))
                result = future.result(timeout=15)
        else:
            result = loop.run_until_complete(run_parallel_committee(opp))
    except Exception as e:
        logger.error(f"[Committee] خطأ: {e}")
        # Fallback
        result = {
            "symbol":      opp.symbol,
            "elapsed_sec": 0,
            "debate_log":  [],
            "calculated_sl": opp.stop_loss,
            "fib_targets": {"tp1": opp.tp1, "tp2": opp.tp2, "tp3": opp.tp3},
            "tech_model":  "—", "risk_model": "—", "market_model": "—",
            "recommendation": {
                "send_signal": False,
                "label":       f"خطأ: {str(e)[:50]}",
                "emoji":       "🔴",
                "confidence":  "—",
                "votes":       "❌ ❌ ❌",
            },
        }
    return result
