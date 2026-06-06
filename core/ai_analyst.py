"""
core/ai_analyst.py
==================
نقاش الخبراء الرباعي:
Claude (فني) + DeepSeek/Groq (مخاطر) + Grok (X) + Reddit (مجتمع)
DeepSeek أساسي — Groq احتياطي تلقائي
"""

import os
import re
import requests
from utils.logger import logger
from core.scanner import TradeOpportunity

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
DEEPSEEK_API_KEY  = os.getenv("DEEPSEEK_API_KEY", "")
GROQ_API_KEY      = os.getenv("GROQ_API_KEY", "")
GROK_API_KEY      = os.getenv("GROK_API_KEY", "")

CLAUDE_MODEL   = os.getenv("CLAUDE_MODEL",   "claude-sonnet-4-5")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
GROQ_MODEL     = os.getenv("GROQ_MODEL",     "llama-3.3-70b-versatile")
GROK_MODEL     = os.getenv("GROK_MODEL",     "grok-3-fast")
MAX_TOKENS     = 350

STYLE = (
    "اكتب بالعربية فقط. "
    "لا تستخدم ## أو ** أو --- أو أي رموز Markdown. "
    "جمل قصيرة ومباشرة. لا تتجاوز 70 كلمة. "
    "في نهاية ردك اكتب موقفك في كلمة واحدة: موافق أو محايد أو رافض."
)

CLAUDE_SYS   = f"أنت محلل فني للعملات الرقمية. ركّز على المؤشرات الفنية فقط. {STYLE}"
RISK_SYS     = f"أنت خبير مخاطر مالية للعملات الرقمية. ركّز على المخاطر والسيولة فقط. {STYLE}"
GROK_SYS     = f"أنت محلل مجتمع X للعملات الرقمية. أبلّغ عن مزاج X والمؤسسين فقط. {STYLE}"


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
# Reddit RSS — بدون مفاتيح
# ==========================================
def get_reddit_sentiment(coin_symbol: str) -> str:
    coin = coin_symbol.replace("/USDT", "").lower()
    subreddits = ["CryptoCurrency", "CryptoMarkets", "altcoin"]
    posts_found = []
    headers = {"User-Agent": "CryptoBot/1.0"}

    for sub in subreddits:
        try:
            url = f"https://www.reddit.com/r/{sub}/search.json?q={coin}&sort=new&limit=3&t=week"
            r = requests.get(url, headers=headers, timeout=8)
            if r.status_code != 200:
                continue
            posts = r.json().get("data", {}).get("children", [])
            for post in posts:
                p = post.get("data", {})
                title = p.get("title", "")
                if coin.upper() in title.upper() or coin.lower() in title.lower():
                    posts_found.append({
                        "title":    title[:80],
                        "score":    p.get("score", 0),
                        "comments": p.get("num_comments", 0),
                        "sub":      sub,
                    })
        except Exception:
            continue

    if not posts_found:
        return f"لا توجد منشورات حديثة عن {coin_symbol} على Reddit هذا الأسبوع."

    positive_words = ["bull", "moon", "buy", "opportunity", "pump", "recovery", "surge"]
    negative_words = ["bear", "dump", "sell", "crash", "down", "rug", "scam", "falling"]

    pos_count = sum(1 for p in posts_found for w in positive_words if w in p["title"].lower())
    neg_count = sum(1 for p in posts_found for w in negative_words if w in p["title"].lower())

    sentiment = "إيجابي" if pos_count > neg_count else ("سلبي" if neg_count > pos_count else "محايد")
    lines = [f"r/{p['sub']}: {p['title']} (👍{p['score']})" for p in posts_found[:3]]

    return f"مزاج Reddit: {sentiment}\n" + "\n".join(lines)


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
# OpenAI-compatible API (DeepSeek أو Groq)
# ==========================================
def _call_openai_compatible(
    api_key: str,
    base_url: str,
    model: str,
    system: str,
    user_msg: str,
    label: str,
) -> str:
    """دالة مشتركة لأي API متوافق مع OpenAI"""
    try:
        r = requests.post(
            f"{base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type":  "application/json",
            },
            json={
                "model":       model,
                "max_tokens":  MAX_TOKENS,
                "temperature": 0.2,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user",   "content": user_msg},
                ],
            },
            timeout=30,
        )
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"] or ""
        return _clean(text)
    except Exception as e:
        logger.error(f"[{label}] {e}")
        raise


def ask_risk_expert(system: str, user_msg: str) -> tuple[str, str]:
    """
    يحاول DeepSeek أولاً — إذا فشل يستخدم Groq تلقائياً
    Returns: (النص, اسم النموذج المستخدم)
    """
    # المحاولة الأولى: DeepSeek
    if DEEPSEEK_API_KEY:
        try:
            text = _call_openai_compatible(
                api_key  = DEEPSEEK_API_KEY,
                base_url = "https://api.deepseek.com/v1",
                model    = DEEPSEEK_MODEL,
                system   = system,
                user_msg = user_msg,
                label    = "DeepSeek",
            )
            logger.info("[Risk] DeepSeek ✅")
            return text, "DeepSeek"
        except Exception as e:
            logger.warning(f"[Risk] DeepSeek فشل ({e}) — جاري التبديل لـ Groq...")

    # الاحتياطي: Groq
    if GROQ_API_KEY:
        try:
            text = _call_openai_compatible(
                api_key  = GROQ_API_KEY,
                base_url = "https://api.groq.com/openai/v1",
                model    = GROQ_MODEL,
                system   = system,
                user_msg = user_msg,
                label    = "Groq",
            )
            logger.info("[Risk] Groq ✅ (احتياطي)")
            return text, "Groq"
        except Exception as e:
            logger.error(f"[Risk] Groq فشل أيضاً: {e}")

    return "خطأ في نماذج المخاطر", "—"


# ==========================================
# Grok API
# ==========================================
def ask_grok(system: str, user_msg: str) -> str:
    if not GROK_API_KEY:
        return "مفتاح Grok غير موجود"
    try:
        return _call_openai_compatible(
            api_key  = GROK_API_KEY,
            base_url = "https://api.x.ai/v1",
            model    = GROK_MODEL,
            system   = system,
            user_msg = user_msg,
            label    = "Grok",
        )
    except Exception:
        return "خطأ في Grok"


# ==========================================
# النقاش الرئيسي — 3 جولات + Reddit
# ==========================================
def run_expert_debate(opp: TradeOpportunity) -> dict:
    coin = opp.symbol.replace("/USDT", "")
    data = build_data(opp)
    log  = []
    risk_model_used = "DeepSeek"

    def rec(round_num, speaker, text):
        log.append({"round": round_num, "speaker": speaker, "text": text})
        logger.info(f"[Debate R{round_num}] {speaker}: {text[:60]}...")

    # Reddit
    logger.info(f"[Debate] جلب Reddit — {opp.symbol}")
    reddit_data = get_reddit_sentiment(coin)
    rec(0, "Reddit", reddit_data)

    # ── جولة 1 ──
    logger.info(f"[Debate] جولة 1 — {opp.symbol}")

    c1 = ask_claude(CLAUDE_SYS, [{"role": "user", "content":
        f"البيانات: {data}\n\n"
        f"حلّل المؤشرات الفنية. هل الإشارة قوية؟ هل نقاط الدخول منطقية؟"
    }])
    rec(1, "Claude", c1)

    d1_text, risk_model_used = ask_risk_expert(RISK_SYS,
        f"البيانات: {data}\n\n"
        f"قيّم المخاطر المالية. هل الحجم كافٍ للخروج؟ "
        f"هل وقف الخسارة منطقي؟ ما احتمالية الخسارة؟"
    )
    rec(1, risk_model_used, d1_text)

    grok1 = ask_grok(GROK_SYS,
        f"ما مزاج مجتمع X تجاه {coin} crypto؟ "
        f"هل المؤسسون نشطون؟ أي أخبار حديثة؟\n"
        f"Reddit: {_trim(reddit_data, 80)}"
    )
    rec(1, "Grok", grok1)

    # ── جولة 2 ──
    logger.info(f"[Debate] جولة 2 — {opp.symbol}")

    c2 = ask_claude(CLAUDE_SYS, [{"role": "user", "content":
        f"البيانات: {data}\n\n"
        f"رأي خبير المخاطر ({risk_model_used}): {_trim(d1_text)}\n"
        f"رأي محلل X: {_trim(grok1)}\n"
        f"Reddit: {_trim(reddit_data, 60)}\n\n"
        f"هل تتفق معهم؟ ما الذي يغير رأيك الفني؟"
    }])
    rec(2, "Claude", c2)

    d2_text, _ = ask_risk_expert(RISK_SYS,
        f"البيانات: {data}\n\n"
        f"رأي المحلل الفني: {_trim(c1)}\n"
        f"رأي محلل X: {_trim(grok1)}\n"
        f"Reddit: {_trim(reddit_data, 60)}\n\n"
        f"هل التحليل الفني يطمئنك من ناحية المخاطر؟"
    )
    rec(2, risk_model_used, d2_text)

    grok2 = ask_grok(GROK_SYS,
        f"البيانات: {data}\n\n"
        f"رأي المحلل الفني: {_trim(c1)}\n"
        f"رأي خبير المخاطر: {_trim(d1_text)}\n\n"
        f"هل ما تعرفه عن {coin} على X يدعم أو يعارض تحليلهما؟"
    )
    rec(2, "Grok", grok2)

    # ── جولة 3: الحكم النهائي ──
    logger.info(f"[Debate] جولة 3 — {opp.symbol}")

    c3 = ask_claude(CLAUDE_SYS, [{"role": "user", "content":
        f"البيانات: {data}\n\n"
        f"بعد النقاش الكامل:\n"
        f"المخاطر ({risk_model_used}): {_trim(d2_text)}\n"
        f"X: {_trim(grok2)}\n"
        f"Reddit: {_trim(reddit_data, 60)}\n\n"
        f"اكتب: نقطة اتفاق، نقطة خلاف، التوصية، درجة الثقة."
    }])
    rec(3, "Claude", c3)

    d3_text, _ = ask_risk_expert(RISK_SYS,
        f"حكم المحلل الفني: {_trim(c3)}\n\n"
        f"البيانات: {data}\n\n"
        f"هل توافق؟ ما أكبر خطر قائم؟ جملة واحدة للمتداول."
    )
    rec(3, risk_model_used, d3_text)

    grok3 = ask_grok(GROK_SYS,
        f"حكم المحلل الفني: {_trim(c3)}\n"
        f"رأي خبير المخاطر: {_trim(d3_text)}\n\n"
        f"هل مجتمع X يدعم هذا الحكم؟ جملة ختامية واحدة."
    )
    rec(3, "Grok", grok3)

    recommendation = _extract_recommendation(c3, d3_text, grok3, reddit_data)
    logger.info(
        f"[Debate] ✅ {opp.symbol} | "
        f"{recommendation['label']} | "
        f"{recommendation['votes']} | "
        f"نموذج المخاطر: {risk_model_used}"
    )

    return {
        "symbol":          opp.symbol,
        "debate_log":      log,
        "reddit_data":     reddit_data,
        "claude_final":    c3,
        "risk_final":      d3_text,
        "risk_model":      risk_model_used,
        "grok_final":      grok3,
        "recommendation":  recommendation,
    }


# ==========================================
# استخراج التوصية — تصويت رباعي
# ==========================================
def _extract_recommendation(c: str, d: str, grok: str, reddit: str) -> dict:
    def vote(text: str) -> str:
        t = text.lower()
        if any(w in t for w in ["رافض", "لا أنصح", "تجنب", "خطر عالٍ", "ارفض", "رفض"]):
            return "reject"
        if any(w in t for w in ["موافق", "أنصح", "إيجابي", "ادخل", "شراء", "فرصة"]):
            return "approve"
        return "neutral"

    def reddit_vote(text: str) -> str:
        if "إيجابي" in text: return "approve"
        if "سلبي"   in text: return "reject"
        return "neutral"

    votes = {
        "Claude": vote(c),
        "Risk":   vote(d),
        "Grok":   vote(grok),
        "Reddit": reddit_vote(reddit),
    }

    ap = sum(1 for v in votes.values() if v == "approve")
    rj = sum(1 for v in votes.values() if v == "reject")

    if   ap == 4:           label, emoji, conf = "إجماع كامل على الدخول",   "🟢", "عالية جداً 🔥🔥🔥"
    elif ap == 3 and rj==0: label, emoji, conf = "أغلبية قوية موافقة",      "🟢", "عالية جداً 🔥🔥"
    elif ap == 3 and rj==1: label, emoji, conf = "أغلبية موافقة مع تحفظ",  "🟢", "عالية 🔥"
    elif ap == 2 and rj==0: label, emoji, conf = "ميل للموافقة",            "🟡", "متوسطة 💧"
    elif ap == 2 and rj==1: label, emoji, conf = "موافقة مع تحفظ",         "🟡", "متوسطة 💧"
    elif ap == 2 and rj==2: label, emoji, conf = "انقسام — انتظار",         "🟡", "منخفضة 💧"
    elif rj == 4:           label, emoji, conf = "إجماع على الرفض",         "🔴", "رفض مؤكد ❄️"
    elif rj >= 3:           label, emoji, conf = "أغلبية رافضة",            "🔴", "منخفضة ❄️"
    else:                   label, emoji, conf = "محايد — انتظار",          "🟡", "منخفضة 💧"

    def ve(v): return "✅" if v=="approve" else ("❌" if v=="reject" else "⚠️")
    votes_str = (
        f"Claude {ve(votes['Claude'])}  "
        f"Risk {ve(votes['Risk'])}  "
        f"Grok {ve(votes['Grok'])}  "
        f"Reddit {ve(votes['Reddit'])}"
    )

    return {"label": label, "emoji": emoji, "confidence": conf, "votes": votes_str}
