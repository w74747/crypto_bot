"""
core/ai_analyst.py
==================
نقاش الخبراء الثلاثي — Claude + Gemini + Grok
APIs مصححة بالكامل
"""

import os
import re
import requests
from utils.logger import logger
from core.scanner import TradeOpportunity

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY", "")
GROK_API_KEY      = os.getenv("GROK_API_KEY", "")

CLAUDE_MODEL = "claude-sonnet-4-20250514"
GEMINI_MODEL = "gemini-1.5-flash"
GROK_MODEL   = "grok-3-fast"
MAX_TOKENS   = 350

STYLE = (
    "اكتب بالعربية فقط. "
    "لا تستخدم ## أو ** أو --- أو أي رموز Markdown. "
    "جمل قصيرة ومباشرة. لا تتجاوز 80 كلمة. "
    "في نهاية ردك اكتب موقفك: موافق أو محايد أو رافض."
)

# ==========================================
# بيانات الفرصة
# ==========================================
def build_data(opp: TradeOpportunity) -> str:
    return (
        f"العملة: {opp.symbol} | "
        f"RSI: {opp.rsi_daily:.1f} | "
        f"انهيار 60 يوم: {opp.crash_pct_60d:.0%} | "
        f"الحجم: ${opp.volume_24h_usd/1e6:.2f}M | "
        f"السعر: {opp.current_price:.8g} | "
        f"SL: {opp.stop_loss:.8g} | "
        f"R/R: 1:{opp.risk_reward_ratio} | "
        f"إشارة: {opp.signal_type}"
    )


# ==========================================
# تنظيف النص
# ==========================================
def _clean(text: str) -> str:
    text = re.sub(r'#{1,6}\s*', '', text)
    text = re.sub(r'\*{1,3}([^*\n]+)\*{1,3}', r'\1', text)
    text = re.sub(r'_{1,2}([^_\n]+)_{1,2}', r'\1', text)
    text = re.sub(r'`([^`]+)`', r'\1', text)
    text = re.sub(r'---+', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

def _trim(text: str, n: int = 90) -> str:
    text = _clean(text.strip())
    if len(text) <= n:
        return text
    cut = text[:n]
    for sep in ['،', '.', '!', '؟']:
        pos = cut.rfind(sep)
        if pos > n * 0.6:
            return cut[:pos+1]
    return cut + "..."


# ==========================================
# Claude API
# ==========================================
def ask_claude(system: str, messages: list[dict]) -> str:
    if not ANTHROPIC_API_KEY:
        return "مفتاح Claude غير موجود"
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      CLAUDE_MODEL,
                "max_tokens": MAX_TOKENS,
                "system":     system,
                "messages":   messages,
            },
            timeout=30,
        )
        r.raise_for_status()
        return _clean(r.json()["content"][0]["text"])
    except Exception as e:
        logger.error(f"[Claude] {e}")
        return "خطأ في Claude"


# ==========================================
# Gemini API — الصيغة الصحيحة
# ==========================================
def ask_gemini(system: str, user_msg: str) -> str:
    if not GEMINI_API_KEY:
        return "مفتاح Gemini غير موجود"
    try:
        # الصيغة الصحيحة: system في أول رسالة من النموذج
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        )
        body = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": f"{system}\n\n{user_msg}"}]
                }
            ],
            "generationConfig": {
                "maxOutputTokens": MAX_TOKENS,
                "temperature":     0.2,
            },
        }
        r = requests.post(url, json=body, timeout=30)
        r.raise_for_status()
        text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
        return _clean(text)
    except Exception as e:
        logger.error(f"[Gemini] {e}")
        return "خطأ في Gemini"


# ==========================================
# Grok API — صيغة OpenAI الصحيحة
# ==========================================
def ask_grok(system: str, user_msg: str, use_x_search: bool = False) -> str:
    if not GROK_API_KEY:
        return "مفتاح Grok غير موجود"
    try:
        # Grok يستخدم OpenAI Chat Completions format
        body = {
            "model":    GROK_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user",   "content": user_msg},
            ],
            "max_tokens":  MAX_TOKENS,
            "temperature": 0.2,
        }

        if use_x_search:
            body["tools"] = [{
                "type": "function",
                "function": {
                    "name":        "search_x",
                    "description": "Search X/Twitter for recent posts",
                    "parameters": {
                        "type":       "object",
                        "properties": {"query": {"type": "string"}},
                        "required":   ["query"],
                    },
                },
            }]

        r = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROK_API_KEY}",
                "Content-Type":  "application/json",
            },
            json=body,
            timeout=40,
        )
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"] or ""
        return _clean(text)
    except Exception as e:
        logger.error(f"[Grok] {e}")
        return "خطأ في Grok"


# ==========================================
# System Prompts
# ==========================================
CLAUDE_SYS = f"أنت محلل فني للعملات الرقمية. ركّز على المؤشرات الفنية فقط. {STYLE}"
GEMINI_SYS = f"أنت خبير مخاطر للعملات الرقمية. ركّز على المخاطر المالية فقط. {STYLE}"
GROK_SYS   = f"أنت محلل مجتمع X للعملات الرقمية. أبلّغ عن مزاج X والمؤسسين فقط. {STYLE}"


# ==========================================
# النقاش الرئيسي
# ==========================================
def run_expert_debate(opp: TradeOpportunity) -> dict:
    coin = opp.symbol.replace("/USDT", "")
    data = build_data(opp)
    log  = []

    def rec(round_num, speaker, text):
        log.append({"round": round_num, "speaker": speaker, "text": text})
        logger.info(f"[Debate R{round_num}] {speaker}: {text[:60]}...")

    # ── جولة 1: تحليل مستقل ──
    logger.info(f"[Debate] جولة 1 — {opp.symbol}")

    c1 = ask_claude(CLAUDE_SYS, (
        f"البيانات: {data}\n\n"
        f"حلّل المؤشرات الفنية. هل الإشارة قوية؟ هل نقاط الدخول منطقية؟"
    ))
    rec(1, "Claude", c1)

    g1 = ask_gemini(GEMINI_SYS, (
        f"البيانات: {data}\n\n"
        f"قيّم المخاطر. هل الحجم كافٍ؟ هل وقف الخسارة منطقي؟"
    ))
    rec(1, "Gemini", g1)

    grok1 = ask_grok(GROK_SYS, (
        f"ابحث عن {coin} crypto على X. "
        f"ما مزاج المجتمع؟ هل المؤسسون نشطون؟ أي أخبار حديثة؟\n"
        f"البيانات: {data}"
    ), use_x_search=False)  # نعطّل X Search مؤقتاً لتجنب أخطاء التنسيق
    rec(1, "Grok", grok1)

    # ── جولة 2: ردود متقاطعة ──
    logger.info(f"[Debate] جولة 2 — {opp.symbol}")

    c2 = ask_claude(CLAUDE_SYS, (
        f"البيانات: {data}\n\n"
        f"رأي خبير المخاطر: {_trim(g1)}\n"
        f"رأي محلل X: {_trim(grok1)}\n\n"
        f"هل تتفق معهما فنياً؟ ما الذي يغير رأيك؟"
    ))
    rec(2, "Claude", c2)

    g2 = ask_gemini(GEMINI_SYS, (
        f"البيانات: {data}\n\n"
        f"رأي المحلل الفني: {_trim(c1)}\n"
        f"رأي محلل X: {_trim(grok1)}\n\n"
        f"هل التحليل الفني يطمئنك من ناحية المخاطر؟"
    ))
    rec(2, "Gemini", g2)

    grok2 = ask_grok(GROK_SYS, (
        f"البيانات: {data}\n\n"
        f"رأي المحلل الفني: {_trim(c1)}\n"
        f"رأي خبير المخاطر: {_trim(g1)}\n\n"
        f"هل ما تعرفه عن {coin} على X يدعم أو يعارض تحليلهما؟"
    ))
    rec(2, "Grok", grok2)

    # ── جولة 3: الحكم النهائي ──
    logger.info(f"[Debate] جولة 3 — {opp.symbol}")

    c3 = ask_claude(CLAUDE_SYS, (
        f"البيانات: {data}\n\n"
        f"بعد النقاش مع خبير المخاطر والمحلل الاجتماعي:\n"
        f"المخاطر: {_trim(g2)}\n"
        f"X: {_trim(grok2)}\n\n"
        f"اكتب: نقطة اتفاق واحدة، نقطة خلاف واحدة، التوصية، درجة الثقة."
    ))
    rec(3, "Claude", c3)

    g3 = ask_gemini(GEMINI_SYS, (
        f"الحكم النهائي للمحلل الفني: {_trim(c3)}\n\n"
        f"البيانات: {data}\n\n"
        f"هل توافق؟ ما أكبر خطر قائم؟ جملة واحدة للمتداول."
    ))
    rec(3, "Gemini", g3)

    grok3 = ask_grok(GROK_SYS, (
        f"حكم المحلل الفني: {_trim(c3)}\n"
        f"رأي خبير المخاطر: {_trim(g3)}\n\n"
        f"هل مجتمع X يدعم هذا؟ جملة ختامية واحدة."
    ))
    rec(3, "Grok", grok3)

    recommendation = _extract_recommendation(c3, g3, grok3)
    logger.info(
        f"[Debate] ✅ {opp.symbol} | "
        f"{recommendation['label']} | "
        f"{recommendation['votes']}"
    )

    return {
        "symbol":         opp.symbol,
        "debate_log":     log,
        "claude_final":   c3,
        "gemini_final":   g3,
        "grok_final":     grok3,
        "recommendation": recommendation,
    }


# ==========================================
# استخراج التوصية
# ==========================================
def _extract_recommendation(c: str, g: str, grok: str) -> dict:
    def vote(text: str) -> str:
        t = text.lower()
        if any(w in t for w in ["رافض", "لا أنصح", "تجنب", "خطر عالٍ", "ارفض", "رفض"]):
            return "reject"
        if any(w in t for w in ["موافق", "أنصح بالدخول", "إيجابي", "ادخل", "شراء"]):
            return "approve"
        return "neutral"

    votes = {"Claude": vote(c), "Gemini": vote(g), "Grok": vote(grok)}
    ap    = sum(1 for v in votes.values() if v == "approve")
    rj    = sum(1 for v in votes.values() if v == "reject")

    if   ap == 3:           label, emoji, conf = "إجماع على الدخول",   "🟢", "عالية جداً 🔥🔥"
    elif ap == 2 and rj==0: label, emoji, conf = "أغلبية موافقة",      "🟢", "عالية 🔥"
    elif ap == 2 and rj==1: label, emoji, conf = "موافقة مع تحفظ",    "🟡", "متوسطة 💧"
    elif rj == 3:           label, emoji, conf = "إجماع على الرفض",   "🔴", "رفض مؤكد ❄️"
    elif rj >= 2:           label, emoji, conf = "أغلبية رافضة",       "🔴", "منخفضة ❄️"
    else:                   label, emoji, conf = "محايد — انتظار",     "🟡", "منخفضة 💧"

    def ve(v): return "✅" if v=="approve" else ("❌" if v=="reject" else "⚠️")
    votes_str = f"Claude {ve(votes['Claude'])}  Gemini {ve(votes['Gemini'])}  Grok {ve(votes['Grok'])}"

    return {"label": label, "emoji": emoji, "confidence": conf, "votes": votes_str}
