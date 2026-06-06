"""
core/ai_analyst.py
==================
نقاش الخبراء الرباعي:
Claude (فني) + DeepSeek (مخاطر) + Grok (X) + Reddit (مجتمع)
"""

import os
import re
import requests
from utils.logger import logger
from core.scanner import TradeOpportunity

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
DEEPSEEK_API_KEY  = os.getenv("DEEPSEEK_API_KEY", "")
GROK_API_KEY      = os.getenv("GROK_API_KEY", "")

CLAUDE_MODEL   = os.getenv("CLAUDE_MODEL",   "claude-sonnet-4-5")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
GROK_MODEL     = os.getenv("GROK_MODEL",     "grok-3-fast")
MAX_TOKENS     = 350

STYLE = (
    "اكتب بالعربية فقط. "
    "لا تستخدم ## أو ** أو --- أو أي رموز Markdown. "
    "جمل قصيرة ومباشرة. لا تتجاوز 70 كلمة. "
    "في نهاية ردك اكتب موقفك في كلمة واحدة: موافق أو محايد أو رافض."
)

CLAUDE_SYS   = f"أنت محلل فني للعملات الرقمية. ركّز على المؤشرات الفنية فقط. {STYLE}"
DEEPSEEK_SYS = f"أنت خبير مخاطر مالية للعملات الرقمية. ركّز على المخاطر والسيولة فقط. {STYLE}"
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
    """يجلب آخر posts من Reddit عن العملة بدون API keys"""
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

            data = r.json()
            posts = data.get("data", {}).get("children", [])

            for post in posts:
                p = post.get("data", {})
                title = p.get("title", "")
                score = p.get("score", 0)
                comments = p.get("num_comments", 0)
                # فقط posts ذات صلة
                if coin.upper() in title.upper() or coin.lower() in title.lower():
                    posts_found.append({
                        "title":    title[:80],
                        "score":    score,
                        "comments": comments,
                        "sub":      sub,
                    })
        except Exception as e:
            logger.debug(f"[Reddit] {sub}: {e}")
            continue

    if not posts_found:
        return f"لا توجد منشورات حديثة عن {coin_symbol} على Reddit هذا الأسبوع."

    # تحليل المزاج من العناوين
    positive_words = ["bull", "moon", "buy", "opportunity", "pump", "recovery", "surge", "up"]
    negative_words = ["bear", "dump", "sell", "crash", "down", "rug", "scam", "dead", "falling"]

    pos_count = 0
    neg_count = 0
    summary_lines = []

    for p in posts_found[:4]:
        title_lower = p["title"].lower()
        pos = sum(1 for w in positive_words if w in title_lower)
        neg = sum(1 for w in negative_words if w in title_lower)
        pos_count += pos
        neg_count += neg
        summary_lines.append(f"r/{p['sub']}: {p['title']} (👍{p['score']} 💬{p['comments']})")

    if pos_count > neg_count:
        sentiment = "إيجابي"
    elif neg_count > pos_count:
        sentiment = "سلبي"
    else:
        sentiment = "محايد"

    result = f"مزاج Reddit: {sentiment}\n" + "\n".join(summary_lines[:3])
    return result


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
# DeepSeek API — متوافق مع OpenAI format
# ==========================================
def ask_deepseek(system: str, user_msg: str) -> str:
    if not DEEPSEEK_API_KEY:
        return "مفتاح DeepSeek غير موجود"
    try:
        r = requests.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":       DEEPSEEK_MODEL,
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
        logger.error(f"[DeepSeek] {e}")
        return "خطأ في DeepSeek"


# ==========================================
# Grok API — OpenAI format
# ==========================================
def ask_grok(system: str, user_msg: str) -> str:
    if not GROK_API_KEY:
        return "مفتاح Grok غير موجود"
    try:
        r = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROK_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":       GROK_MODEL,
                "max_tokens":  MAX_TOKENS,
                "temperature": 0.2,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user",   "content": user_msg},
                ],
            },
            timeout=40,
        )
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"] or ""
        return _clean(text)
    except Exception as e:
        logger.error(f"[Grok] {e}")
        return "خطأ في Grok"


# ==========================================
# النقاش الرئيسي — 3 جولات + Reddit
# ==========================================
def run_expert_debate(opp: TradeOpportunity) -> dict:
    coin = opp.symbol.replace("/USDT", "")
    data = build_data(opp)
    log  = []

    def rec(round_num, speaker, text):
        log.append({"round": round_num, "speaker": speaker, "text": text})
        logger.info(f"[Debate R{round_num}] {speaker}: {text[:60]}...")

    # ── Reddit أولاً (بيانات خارجية) ──
    logger.info(f"[Debate] جلب Reddit — {opp.symbol}")
    reddit_data = get_reddit_sentiment(coin)
    rec(0, "Reddit", reddit_data)

    # ── جولة 1: تحليل مستقل ──
    logger.info(f"[Debate] جولة 1 — {opp.symbol}")

    c1 = ask_claude(CLAUDE_SYS, [{"role": "user", "content":
        f"البيانات: {data}\n\n"
        f"حلّل المؤشرات الفنية. هل الإشارة قوية؟ هل نقاط الدخول والأهداف منطقية؟"
    }])
    rec(1, "Claude", c1)

    d1 = ask_deepseek(DEEPSEEK_SYS,
        f"البيانات: {data}\n\n"
        f"قيّم المخاطر المالية. هل الحجم كافٍ للخروج؟ "
        f"هل وقف الخسارة منطقي؟ ما احتمالية الخسارة؟"
    )
    rec(1, "DeepSeek", d1)

    grok1 = ask_grok(GROK_SYS,
        f"ما مزاج مجتمع X تجاه {coin} crypto؟ "
        f"هل المؤسسون نشطون؟ أي أخبار حديثة؟\n"
        f"بيانات Reddit للسياق: {_trim(reddit_data, 100)}"
    )
    rec(1, "Grok", grok1)

    # ── جولة 2: ردود متقاطعة ──
    logger.info(f"[Debate] جولة 2 — {opp.symbol}")

    c2 = ask_claude(CLAUDE_SYS, [{"role": "user", "content":
        f"البيانات: {data}\n\n"
        f"رأي خبير المخاطر: {_trim(d1)}\n"
        f"رأي محلل X: {_trim(grok1)}\n"
        f"Reddit: {_trim(reddit_data, 60)}\n\n"
        f"هل تتفق معهم؟ ما الذي يغير رأيك الفني؟"
    }])
    rec(2, "Claude", c2)

    d2 = ask_deepseek(DEEPSEEK_SYS,
        f"البيانات: {data}\n\n"
        f"رأي المحلل الفني: {_trim(c1)}\n"
        f"رأي محلل X: {_trim(grok1)}\n"
        f"Reddit: {_trim(reddit_data, 60)}\n\n"
        f"هل التحليل الفني يطمئنك من ناحية المخاطر؟"
    )
    rec(2, "DeepSeek", d2)

    grok2 = ask_grok(GROK_SYS,
        f"البيانات: {data}\n\n"
        f"رأي المحلل الفني: {_trim(c1)}\n"
        f"رأي خبير المخاطر: {_trim(d1)}\n\n"
        f"هل ما تعرفه عن {coin} على X يدعم أو يعارض تحليلهما؟"
    )
    rec(2, "Grok", grok2)

    # ── جولة 3: الحكم النهائي ──
    logger.info(f"[Debate] جولة 3 — {opp.symbol}")

    c3 = ask_claude(CLAUDE_SYS, [{"role": "user", "content":
        f"البيانات: {data}\n\n"
        f"بعد النقاش الكامل:\n"
        f"المخاطر: {_trim(d2)}\n"
        f"X: {_trim(grok2)}\n"
        f"Reddit: {_trim(reddit_data, 60)}\n\n"
        f"اكتب: نقطة اتفاق، نقطة خلاف، التوصية، درجة الثقة."
    }])
    rec(3, "Claude", c3)

    d3 = ask_deepseek(DEEPSEEK_SYS,
        f"حكم المحلل الفني: {_trim(c3)}\n\n"
        f"البيانات: {data}\n\n"
        f"هل توافق؟ ما أكبر خطر قائم؟ جملة واحدة للمتداول."
    )
    rec(3, "DeepSeek", d3)

    grok3 = ask_grok(GROK_SYS,
        f"حكم المحلل الفني: {_trim(c3)}\n"
        f"رأي خبير المخاطر: {_trim(d3)}\n\n"
        f"هل مجتمع X يدعم هذا الحكم؟ جملة ختامية واحدة."
    )
    rec(3, "Grok", grok3)

    recommendation = _extract_recommendation(c3, d3, grok3, reddit_data)
    logger.info(
        f"[Debate] ✅ {opp.symbol} | "
        f"{recommendation['label']} | "
        f"{recommendation['votes']}"
    )

    return {
        "symbol":         opp.symbol,
        "debate_log":     log,
        "reddit_data":    reddit_data,
        "claude_final":   c3,
        "deepseek_final": d3,
        "grok_final":     grok3,
        "recommendation": recommendation,
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
        if "إيجابي" in text:
            return "approve"
        if "سلبي" in text:
            return "reject"
        return "neutral"

    votes = {
        "Claude":   vote(c),
        "DeepSeek": vote(d),
        "Grok":     vote(grok),
        "Reddit":   reddit_vote(reddit),
    }

    ap = sum(1 for v in votes.values() if v == "approve")
    rj = sum(1 for v in votes.values() if v == "reject")

    if   ap == 4:           label, emoji, conf = "إجماع كامل على الدخول",  "🟢", "عالية جداً 🔥🔥🔥"
    elif ap == 3 and rj==0: label, emoji, conf = "أغلبية قوية موافقة",     "🟢", "عالية جداً 🔥🔥"
    elif ap == 3 and rj==1: label, emoji, conf = "أغلبية موافقة مع تحفظ", "🟢", "عالية 🔥"
    elif ap == 2 and rj==0: label, emoji, conf = "ميل للموافقة",           "🟡", "متوسطة 💧"
    elif ap == 2 and rj==1: label, emoji, conf = "موافقة مع تحفظ",        "🟡", "متوسطة 💧"
    elif ap == 2 and rj==2: label, emoji, conf = "انقسام — انتظار",        "🟡", "منخفضة 💧"
    elif rj == 4:           label, emoji, conf = "إجماع على الرفض",        "🔴", "رفض مؤكد ❄️"
    elif rj >= 3:           label, emoji, conf = "أغلبية رافضة",           "🔴", "منخفضة ❄️"
    else:                   label, emoji, conf = "محايد — انتظار",         "🟡", "منخفضة 💧"

    def ve(v): return "✅" if v=="approve" else ("❌" if v=="reject" else "⚠️")
    votes_str = (
        f"Claude {ve(votes['Claude'])}  "
        f"DeepSeek {ve(votes['DeepSeek'])}  "
        f"Grok {ve(votes['Grok'])}  "
        f"Reddit {ve(votes['Reddit'])}"
    )

    return {
        "label":      label,
        "emoji":      emoji,
        "confidence": conf,
        "votes":      votes_str,
    }
