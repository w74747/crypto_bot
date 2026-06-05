"""
core/ai_analyst.py
==================
نقاش الخبراء الثلاثي — Claude + Gemini + Grok
مع System Prompts تمنع استخدام Markdown
"""

import os
import requests
from utils.logger import logger
from core.scanner import TradeOpportunity

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY", "")
GROK_API_KEY      = os.getenv("GROK_API_KEY", "")

CLAUDE_MODEL = "claude-sonnet-4-20250514"
GEMINI_MODEL = "gemini-1.5-flash"
GROK_MODEL   = "grok-3-fast"
MAX_TOKENS   = 400


# ==========================================
# بيانات الفرصة
# ==========================================
def build_opportunity_data(opp: TradeOpportunity) -> str:
    return (
        f"العملة: {opp.symbol} | "
        f"السعر: {opp.current_price:.8g} | "
        f"RSI: {opp.rsi_daily:.1f} | "
        f"انهيار 60 يوم: {opp.crash_pct_60d:.0%} | "
        f"البعد عن القاع: {opp.distance_from_lod:.1%} | "
        f"الحجم: ${opp.volume_24h_usd/1e6:.2f}M | "
        f"دعم: {opp.nearest_support:.8g} | "
        f"SL: {opp.stop_loss:.8g} | "
        f"R/R: 1:{opp.risk_reward_ratio} | "
        f"إشارة: {opp.signal_type}"
    )


# ==========================================
# قواعد الأسلوب — مشتركة لجميع النماذج
# ==========================================
STYLE_RULES = """
قواعد الكتابة الإلزامية — لا استثناء:
- اكتب بالعربية فقط
- لا تستخدم ## أو ** أو __ أو --- أو أي Markdown إطلاقاً
- لا تستخدم نقاط ترقيم زخرفية
- اكتب بجمل قصيرة ومباشرة
- لا تتجاوز 80 كلمة
- في نهاية ردك: موقفك في كلمة واحدة: موافق أو محايد أو رافض
"""

CLAUDE_SYSTEM = f"""أنت محلل فني للعملات الرقمية. دورك: تحليل المؤشرات الفنية فقط.
{STYLE_RULES}"""

GEMINI_SYSTEM = f"""أنت خبير مخاطر للعملات الرقمية. دورك: تقييم المخاطر المالية فقط.
{STYLE_RULES}"""

GROK_SYSTEM = f"""أنت محلل مجتمع X للعملات الرقمية. دورك: تقرير مزاج X والمؤسسين فقط.
{STYLE_RULES}"""


# ==========================================
# استدعاء النماذج
# ==========================================
def ask_claude(messages: list[dict], system: str) -> str:
    if not ANTHROPIC_API_KEY:
        return "غير متاح: مفتاح Claude مفقود"
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={"model": CLAUDE_MODEL, "max_tokens": MAX_TOKENS,
                  "system": system, "messages": messages},
            timeout=30,
        )
        r.raise_for_status()
        return _clean(r.json()["content"][0]["text"])
    except Exception as e:
        logger.error(f"[Claude] {e}")
        return f"خطأ في Claude"


def ask_gemini(prompt: str) -> str:
    if not GEMINI_API_KEY:
        return "غير متاح: مفتاح Gemini مفقود"
    try:
        # الرابط الصحيح لـ Gemini 1.5 Flash
        url = (
            "https://generativelanguage.googleapis.com/v1/models/"
            f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        )
        r = requests.post(
            url,
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "maxOutputTokens": MAX_TOKENS,
                    "temperature":     0.2,
                },
                "systemInstruction": {
                    "parts": [{"text": GEMINI_SYSTEM}]
                },
            },
            timeout=30,
        )
        r.raise_for_status()
        text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
        return _clean(text)
    except Exception as e:
        logger.error(f"[Gemini] {e}")
        return "خطأ في Gemini"


def ask_grok(messages: list[dict], use_x_search: bool = True) -> str:
    if not GROK_API_KEY:
        return "غير متاح: مفتاح Grok مفقود"
    try:
        body = {
            "model":      GROK_MODEL,
            "max_tokens": MAX_TOKENS,
            "system":     GROK_SYSTEM,
            "messages":   messages,
        }
        if use_x_search:
            body["tools"]       = [{"type": "x_search"}]
            body["tool_choice"] = "auto"

        r = requests.post(
            "https://api.x.ai/v1/messages",
            headers={
                "x-api-key":         GROK_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json=body,
            timeout=40,
        )
        r.raise_for_status()
        content = r.json().get("content", [])
        texts   = [c.get("text","") for c in content if c.get("type") == "text"]
        return _clean("\n".join(texts))
    except Exception as e:
        logger.error(f"[Grok] {e}")
        return "خطأ في Grok"


def _clean(text: str) -> str:
    """يزيل رموز Markdown من النص"""
    import re
    text = re.sub(r'#{1,6}\s*', '', text)       # ## العناوين
    text = re.sub(r'\*{1,3}([^*]+)\*{1,3}', r'\1', text)  # **bold** *italic*
    text = re.sub(r'_{1,2}([^_]+)_{1,2}', r'\1', text)    # __bold__ _italic_
    text = re.sub(r'`([^`]+)`', r'\1', text)    # `code`
    text = re.sub(r'---+', '', text)             # ---
    text = re.sub(r'\n{3,}', '\n\n', text)       # سطور فارغة متعددة
    return text.strip()


def _trim(text: str, limit: int = 120) -> str:
    """يختصر النص للحد المطلوب"""
    text = text.strip()
    if len(text) <= limit:
        return text
    # قطع عند آخر جملة كاملة
    cut = text[:limit]
    last_period = max(cut.rfind('。'), cut.rfind('.'), cut.rfind('،'), cut.rfind('!'))
    if last_period > limit * 0.6:
        return cut[:last_period+1]
    return cut + "..."


# ==========================================
# دالة النقاش الرئيسية — 3 جولات
# ==========================================
def run_expert_debate(opp: TradeOpportunity) -> dict:
    coin     = opp.symbol.replace("/USDT", "")
    data     = build_opportunity_data(opp)
    log      = []

    def record(round_num, speaker, text):
        log.append({"round": round_num, "speaker": speaker, "text": text})
        logger.info(f"[Debate R{round_num}] {speaker}: {text[:60]}...")

    # ─── الجولة الأولى: كل خبير يحلل مستقلاً ───
    logger.info(f"[Debate] جولة 1 — {opp.symbol}")

    c1 = ask_claude(
        messages=[{"role": "user", "content":
            f"البيانات: {data}\n\n"
            f"حلل المؤشرات الفنية: RSI والانهيار ومستويات الدعم. "
            f"هل الإشارة قوية؟ هل نقاط الدخول والأهداف منطقية؟"
        }],
        system=CLAUDE_SYSTEM,
    )
    record(1, "Claude", c1)

    g1 = ask_gemini(
        f"البيانات: {data}\n\n"
        f"قيّم المخاطر: هل الحجم كافٍ للخروج الآمن؟ "
        f"ما نسبة المخاطرة؟ هل وقف الخسارة منطقي؟"
    )
    record(1, "Gemini", g1)

    grok1 = ask_grok(
        messages=[{"role": "user", "content":
            f"ابحث على X عن {coin} crypto. "
            f"ما مزاج المجتمع؟ هل المؤسسون نشطون؟ "
            f"هل هناك أخبار سلبية أو إيجابية حديثة؟"
        }],
        use_x_search=True,
    )
    record(1, "Grok", grok1)

    # ─── الجولة الثانية: ردود متقاطعة ───
    logger.info(f"[Debate] جولة 2 — {opp.symbol}")

    c2 = ask_claude(
        messages=[
            {"role": "user",      "content": f"البيانات: {data}"},
            {"role": "assistant", "content": c1},
            {"role": "user",      "content":
                f"Gemini قال: {_trim(g1, 80)}\n"
                f"Grok قال: {_trim(grok1, 80)}\n\n"
                f"هل تتفق معهما؟ ما الذي يغير رأيك؟"
            },
        ],
        system=CLAUDE_SYSTEM,
    )
    record(2, "Claude", c2)

    g2 = ask_gemini(
        f"البيانات: {data}\n\n"
        f"Claude قال: {_trim(c1, 80)}\n"
        f"Grok قال: {_trim(grok1, 80)}\n\n"
        f"هل تحليل Claude الفني يطمئنك؟ "
        f"هل بيانات X تزيد أو تقلل المخاطر؟"
    )
    record(2, "Gemini", g2)

    grok2 = ask_grok(
        messages=[{"role": "user", "content":
            f"Claude قال: {_trim(c1, 80)}\n"
            f"Gemini قال: {_trim(g1, 80)}\n\n"
            f"هل ما وجدته على X يدعم أو يعارض تحليلهما؟"
        }],
        use_x_search=False,
    )
    record(2, "Grok", grok2)

    # ─── الجولة الثالثة: الحكم النهائي ───
    logger.info(f"[Debate] جولة 3 — {opp.symbol}")

    c3 = ask_claude(
        messages=[
            {"role": "user",      "content": f"البيانات: {data}"},
            {"role": "assistant", "content": c1},
            {"role": "user",      "content": f"ردود الجولة 2: Gemini: {_trim(g2,60)} | Grok: {_trim(grok2,60)}"},
            {"role": "assistant", "content": c2},
            {"role": "user",      "content":
                "الحكم النهائي: نقطة اتفاق واحدة، نقطة خلاف واحدة، "
                "التوصية، ودرجة الثقة في جملة واحدة لكل بند."
            },
        ],
        system=CLAUDE_SYSTEM,
    )
    record(3, "Claude", c3)

    g3 = ask_gemini(
        f"حكم Claude النهائي: {_trim(c3, 100)}\n\n"
        f"هل توافق؟ ما أكبر خطر لا يزال قائماً؟ "
        f"جملة واحدة فقط للمتداول."
    )
    record(3, "Gemini", g3)

    grok3 = ask_grok(
        messages=[{"role": "user", "content":
            f"حكم Claude: {_trim(c3, 80)}\n"
            f"رأي Gemini: {_trim(g3, 80)}\n\n"
            f"هل X يدعم هذا الحكم؟ جملة ختامية واحدة."
        }],
        use_x_search=False,
    )
    record(3, "Grok", grok3)

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
        if any(w in t for w in ["رافض", "لا أنصح", "تجنب", "خطر عالٍ", "ارفض"]):
            return "reject"
        elif any(w in t for w in ["موافق", "أنصح", "إيجابي", "ادخل"]):
            return "approve"
        return "neutral"

    votes = {"Claude": vote(c), "Gemini": vote(g), "Grok": vote(grok)}
    ap    = sum(1 for v in votes.values() if v == "approve")
    rj    = sum(1 for v in votes.values() if v == "reject")

    if   ap == 3:           label, emoji, conf = "إجماع على الدخول ✅",   "🟢", "عالية جداً 🔥🔥"
    elif ap == 2 and rj==0: label, emoji, conf = "أغلبية موافقة ✅",      "🟢", "عالية 🔥"
    elif ap == 2 and rj==1: label, emoji, conf = "موافقة مع تحفظ ⚠️",   "🟡", "متوسطة 💧"
    elif rj == 3:           label, emoji, conf = "إجماع على الرفض ❌",   "🔴", "رفض مؤكد ❄️"
    elif rj >= 2:           label, emoji, conf = "أغلبية رافضة ❌",       "🔴", "منخفضة ❄️"
    else:                   label, emoji, conf = "محايد — انتظار ⚠️",    "🟡", "منخفضة 💧"

    def v_emoji(v): return "✅" if v=="approve" else ("❌" if v=="reject" else "⚠️")
    votes_str = (
        f"Claude {v_emoji(votes['Claude'])}  "
        f"Gemini {v_emoji(votes['Gemini'])}  "
        f"Grok {v_emoji(votes['Grok'])}"
    )

    return {"label": label, "emoji": emoji, "confidence": conf, "votes": votes_str}
