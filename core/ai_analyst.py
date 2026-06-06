"""
core/ai_analyst.py
==================
نظام نقاش الخبراء — بدون Claude أو Grok
Groq + Together + DeepSeek + Reddit RSS
تكلفة شبه صفر مع جودة عالية
"""

import os
import re
import requests
from utils.logger import logger
from core.scanner import TradeOpportunity

# ==========================================
# مفاتيح API
# ==========================================
GROQ_API_KEY     = os.getenv("GROQ_API_KEY", "")
TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY", "")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")

# النماذج
GROQ_MODEL     = os.getenv("GROQ_MODEL",     "llama-3.3-70b-versatile")
TOGETHER_MODEL = os.getenv("TOGETHER_MODEL", "meta-llama/Llama-3.3-70B-Instruct-Turbo")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

MAX_TOKENS = 280

# ==========================================
# System Prompts — بعقلية مستثمر حقيقي
# ==========================================
TECHNICAL_SYS = """أنت محلل تقني متخصص في العملات الرقمية مع 10 سنوات خبرة.
أنت تحلل هذه الفرصة لاتخاذ قرار استثمار حقيقي.
قواعد إلزامية:
- اكتب بالعربية فقط بدون أي رموز Markdown
- لا تتجاوز 60 كلمة
- كن حازماً — لا "ربما" أو "قد يكون"
- ركّز على: RSI، الأهداف الديناميكية، وقف الخسارة، R/R
- آخر سطر حتماً: موافق أو رافض"""

RISK_SYS = """أنت مدير مخاطر في صندوق استثمار متخصص في العملات الرقمية.
مهمتك: حماية رأس المال أولاً ثم تعظيم العائد.
قواعد إلزامية:
- اكتب بالعربية فقط بدون أي رموز Markdown
- لا تتجاوز 60 كلمة
- قيّم: السيولة، وقف الخسارة، نسبة R/R، السيناريو الأسوأ
- آخر سطر حتماً: موافق أو رافض"""

MARKET_SYS = """أنت محلل أسواق متخصص في العملات الرقمية يراقب السوق الكلي.
قواعد إلزامية:
- اكتب بالعربية فقط بدون أي رموز Markdown
- لا تتجاوز 60 كلمة
- قيّم: اتجاه السوق الكلي، قوة العملة مقارنة بـ BTC، مزاج المتداولين
- آخر سطر حتماً: موافق أو رافض"""


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

def _trim(text: str, n: int = 80) -> str:
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
# بيانات الفرصة
# ==========================================
def build_data(opp: TradeOpportunity) -> str:
    return (
        f"العملة: {opp.symbol} | "
        f"RSI اليومي: {opp.rsi_daily:.1f} | "
        f"انهيار 60 يوم: {opp.crash_pct_60d:.0%} | "
        f"الحجم: ${opp.volume_24h_usd/1e6:.2f}M | "
        f"دخول: {opp.entry_price:.8g} | "
        f"SL: {opp.stop_loss:.8g} | "
        f"TP1: {opp.tp1:.8g} (+{opp.tp1_pct}%) | "
        f"TP2: {opp.tp2:.8g} (+{opp.tp2_pct}%) | "
        f"TP3: {opp.tp3:.8g} (+{opp.tp3_pct}%) | "
        f"R/R: 1:{opp.risk_reward_ratio} | "
        f"طريقة الأهداف: {opp.tp_method} | "
        f"إشارة: {opp.signal_type}"
    )


# ==========================================
# Reddit RSS — مجاني كلياً
# ==========================================
def get_reddit_sentiment(coin_symbol: str) -> tuple[str, str]:
    """
    يجلب منشورات Reddit ويحلل المزاج
    Returns: (النص الكامل, الموقف: approve/neutral/reject)
    """
    coin = coin_symbol.replace("/USDT", "").lower()
    subreddits = ["CryptoCurrency", "CryptoMarkets", "altcoin"]
    posts_found = []
    headers = {"User-Agent": "CryptoBot/1.0"}

    for sub in subreddits:
        try:
            url = (
                f"https://www.reddit.com/r/{sub}/search.json"
                f"?q={coin}&sort=new&limit=5&t=week"
            )
            r = requests.get(url, headers=headers, timeout=8)
            if r.status_code != 200:
                continue
            for post in r.json().get("data", {}).get("children", []):
                p = post.get("data", {})
                title = p.get("title", "")
                if coin.upper() in title.upper():
                    posts_found.append({
                        "title":    title[:90],
                        "score":    p.get("score", 0),
                        "comments": p.get("num_comments", 0),
                        "sub":      sub,
                    })
        except Exception:
            continue

    if not posts_found:
        return f"لا توجد منشورات حديثة عن {coin_symbol} على Reddit.", "neutral"

    positive_words = ["bull","moon","buy","pump","recovery","surge","bullish","gem","opportunity"]
    negative_words = ["bear","dump","crash","down","rug","scam","falling","dead","bearish","warning"]

    pos = sum(1 for p in posts_found
              for w in positive_words if w in p["title"].lower())
    neg = sum(1 for p in posts_found
              for w in negative_words if w in p["title"].lower())

    if pos > neg:
        sentiment = "إيجابي"
        vote = "approve"
    elif neg > pos:
        sentiment = "سلبي"
        vote = "reject"
    else:
        sentiment = "محايد"
        vote = "neutral"

    lines = [
        f"r/{p['sub']}: {p['title']} (👍{p['score']} 💬{p['comments']})"
        for p in posts_found[:3]
    ]
    text = f"مزاج Reddit: {sentiment}\n" + "\n".join(lines)
    return text, vote


# ==========================================
# API Caller — صيغة OpenAI المشتركة
# ==========================================
def _call_api(
    api_key:  str,
    base_url: str,
    model:    str,
    system:   str,
    user_msg: str,
    label:    str,
) -> str:
    r = requests.post(
        f"{base_url}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
        },
        json={
            "model":       model,
            "max_tokens":  MAX_TOKENS,
            "temperature": 0.1,
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


# ==========================================
# الخبراء الثلاثة مع Fallback
# ==========================================
def ask_technical_analyst(user_msg: str) -> tuple[str, str]:
    """
    المحلل الفني — Groq أساسي، Together احتياطي، DeepSeek ثالث
    """
    providers = []
    if GROQ_API_KEY:
        providers.append(("Groq", GROQ_API_KEY,
                          "https://api.groq.com/openai/v1", GROQ_MODEL))
    if TOGETHER_API_KEY:
        providers.append(("Together", TOGETHER_API_KEY,
                          "https://api.together.xyz/v1", TOGETHER_MODEL))
    if DEEPSEEK_API_KEY:
        providers.append(("DeepSeek", DEEPSEEK_API_KEY,
                          "https://api.deepseek.com/v1", DEEPSEEK_MODEL))

    for label, key, url, model in providers:
        try:
            text = _call_api(key, url, model, TECHNICAL_SYS, user_msg, label)
            logger.info(f"[Technical] {label} ✅")
            return text, label
        except Exception as e:
            logger.warning(f"[Technical] {label} فشل: {str(e)[:50]}")

    return "لا يوجد محلل فني متاح. رافض", "—"


def ask_risk_expert(user_msg: str) -> tuple[str, str]:
    """
    خبير المخاطر — Together أساسي، DeepSeek احتياطي، Groq ثالث
    """
    providers = []
    if TOGETHER_API_KEY:
        providers.append(("Together", TOGETHER_API_KEY,
                          "https://api.together.xyz/v1", TOGETHER_MODEL))
    if DEEPSEEK_API_KEY:
        providers.append(("DeepSeek", DEEPSEEK_API_KEY,
                          "https://api.deepseek.com/v1", DEEPSEEK_MODEL))
    if GROQ_API_KEY:
        providers.append(("Groq", GROQ_API_KEY,
                          "https://api.groq.com/openai/v1", GROQ_MODEL))

    for label, key, url, model in providers:
        try:
            text = _call_api(key, url, model, RISK_SYS, user_msg, label)
            logger.info(f"[Risk] {label} ✅")
            return text, label
        except Exception as e:
            logger.warning(f"[Risk] {label} فشل: {str(e)[:50]}")

    return "لا يوجد خبير مخاطر متاح. رافض", "—"


def ask_market_analyst(user_msg: str) -> tuple[str, str]:
    """
    محلل السوق الكلي — DeepSeek أساسي، Groq احتياطي، Together ثالث
    """
    providers = []
    if DEEPSEEK_API_KEY:
        providers.append(("DeepSeek", DEEPSEEK_API_KEY,
                          "https://api.deepseek.com/v1", DEEPSEEK_MODEL))
    if GROQ_API_KEY:
        providers.append(("Groq", GROQ_API_KEY,
                          "https://api.groq.com/openai/v1", GROQ_MODEL))
    if TOGETHER_API_KEY:
        providers.append(("Together", TOGETHER_API_KEY,
                          "https://api.together.xyz/v1", TOGETHER_MODEL))

    for label, key, url, model in providers:
        try:
            text = _call_api(key, url, model, MARKET_SYS, user_msg, label)
            logger.info(f"[Market] {label} ✅")
            return text, label
        except Exception as e:
            logger.warning(f"[Market] {label} فشل: {str(e)[:50]}")

    return "لا يوجد محلل سوق متاح. محايد", "—"


# ==========================================
# استخراج الموقف
# ==========================================
def extract_vote(text: str) -> str:
    lines = [l.strip() for l in text.strip().split('\n') if l.strip()]
    for line in reversed(lines[-3:]):
        t = line.lower()
        if any(w in t for w in ["موافق", "أوافق", "ادخل", "شراء"]):
            return "approve"
        if any(w in t for w in ["رافض", "أرفض", "تجنب", "لا تدخل"]):
            return "reject"
        if "محايد" in t:
            return "neutral"
    full = text.lower()
    ap = sum(1 for w in ["موافق", "فرصة جيدة", "ادخل"] if w in full)
    rj = sum(1 for w in ["رافض", "تجنب", "خطر", "لا أنصح"] if w in full)
    if ap > rj: return "approve"
    if rj > ap: return "reject"
    return "neutral"


# ==========================================
# النقاش الرئيسي — 3 جولات
# ==========================================
def run_expert_debate(opp: TradeOpportunity) -> dict:
    coin = opp.symbol.replace("/USDT", "")
    data = build_data(opp)
    log  = []

    tech_model   = "—"
    risk_model   = "—"
    market_model = "—"

    def rec(round_num: int, speaker: str, text: str):
        vote = extract_vote(text)
        log.append({
            "round":   round_num,
            "speaker": speaker,
            "text":    text,
            "vote":    vote,
        })
        logger.info(f"[Debate R{round_num}] {speaker} [{vote}]: {text[:55]}...")

    # Reddit
    reddit_text, reddit_vote = get_reddit_sentiment(coin)
    log.append({"round": 0, "speaker": "Reddit",
                "text": reddit_text, "vote": reddit_vote})
    logger.info(f"[Debate R0] Reddit [{reddit_vote}]: {reddit_text[:55]}...")

    # ── جولة 1: تحليل مستقل ──
    logger.info(f"[Debate] جولة 1 — {opp.symbol}")

    t1_text, tech_model = ask_technical_analyst(
        f"بيانات الصفقة الكاملة:\n{data}\n\n"
        f"حكمك الفني: هل المؤشرات تدعم الدخول؟\n"
        f"حلّل RSI والأهداف الديناميكية ووقف الخسارة.\n"
        f"آخر سطر: موافق أو رافض"
    )
    rec(1, f"فني/{tech_model}", t1_text)

    r1_text, risk_model = ask_risk_expert(
        f"بيانات الصفقة:\n{data}\n\n"
        f"قيّم المخاطر: هل R/R={opp.risk_reward_ratio} مقبول؟\n"
        f"هل الحجم ${opp.volume_24h_usd/1e6:.1f}M كافٍ للخروج؟\n"
        f"آخر سطر: موافق أو رافض"
    )
    rec(1, f"مخاطر/{risk_model}", r1_text)

    m1_text, market_model = ask_market_analyst(
        f"بيانات: {data}\n"
        f"Reddit: {_trim(reddit_text, 80)}\n\n"
        f"قيّم السياق الكلي للسوق لهذه العملة.\n"
        f"آخر سطر: موافق أو رافض"
    )
    rec(1, f"سوق/{market_model}", m1_text)

    # إنهاء مبكر إذا رفض الجميع
    votes_r1 = [extract_vote(t1_text), extract_vote(r1_text), extract_vote(m1_text)]
    if votes_r1.count("reject") >= 3:
        logger.info(f"[Debate] إجماع رفض جولة 1 — إنهاء مبكر")
        for sp, tx in [(f"فني/{tech_model}", t1_text),
                       (f"مخاطر/{risk_model}", r1_text),
                       (f"سوق/{market_model}", m1_text)]:
            rec(3, sp, tx)
        return _finalize(opp, log, reddit_text, tech_model, risk_model, market_model)

    # ── جولة 2: تحدٍّ متقاطع ──
    logger.info(f"[Debate] جولة 2 — {opp.symbol}")

    t2_text, _ = ask_technical_analyst(
        f"بيانات: {data}\n\n"
        f"خبير المخاطر قال: {_trim(r1_text)}\n"
        f"محلل السوق قال: {_trim(m1_text)}\n\n"
        f"هل تغيّر رأيك الفني؟ إذا بقيت موقفك أعطِ سبباً تقنياً محدداً.\n"
        f"آخر سطر: موافق أو رافض"
    )
    rec(2, f"فني/{tech_model}", t2_text)

    r2_text, _ = ask_risk_expert(
        f"بيانات: {data}\n\n"
        f"المحلل الفني قال: {_trim(t1_text)}\n"
        f"محلل السوق قال: {_trim(m1_text)}\n\n"
        f"بعد سماعهما، هل تعدّل تقييم المخاطر؟\n"
        f"آخر سطر: موافق أو رافض"
    )
    rec(2, f"مخاطر/{risk_model}", r2_text)

    m2_text, _ = ask_market_analyst(
        f"بيانات: {data}\n\n"
        f"الفني: {_trim(t1_text, 50)} | المخاطر: {_trim(r1_text, 50)}\n\n"
        f"هل تحليلهما يغيّر نظرتك للسوق الكلي؟\n"
        f"آخر سطر: موافق أو رافض"
    )
    rec(2, f"سوق/{market_model}", m2_text)

    # ── جولة 3: الحكم النهائي ──
    logger.info(f"[Debate] جولة 3 — {opp.symbol}")

    t3_text, _ = ask_technical_analyst(
        f"بيانات: {data}\n\n"
        f"بعد النقاش الكامل:\n"
        f"المخاطر: {_trim(r2_text)}\n"
        f"السوق: {_trim(m2_text)}\n"
        f"Reddit: {_trim(reddit_text, 50)}\n\n"
        f"قرارك النهائي القاطع — هل تدخل بأموالك؟\n"
        f"سبب واحد فقط.\n"
        f"آخر سطر: موافق أو رافض"
    )
    rec(3, f"فني/{tech_model}", t3_text)

    r3_text, _ = ask_risk_expert(
        f"حكم المحلل الفني: {_trim(t3_text)}\n"
        f"بيانات: {data}\n\n"
        f"قرارك النهائي كمدير مخاطر — هل تُجيز الصفقة؟\n"
        f"آخر سطر: موافق أو رافض"
    )
    rec(3, f"مخاطر/{risk_model}", r3_text)

    m3_text, _ = ask_market_analyst(
        f"الفني قرر: {_trim(t3_text, 50)}\n"
        f"المخاطر قرر: {_trim(r3_text, 50)}\n\n"
        f"تصويتك الأخير بناءً على السوق الكلي.\n"
        f"آخر سطر: موافق أو رافض"
    )
    rec(3, f"سوق/{market_model}", m3_text)

    return _finalize(opp, log, reddit_text, tech_model, risk_model, market_model)


# ==========================================
# بناء النتيجة
# ==========================================
def _finalize(opp, log, reddit_text, tech_model, risk_model, market_model) -> dict:
    r3 = {e["speaker"]: e["vote"] for e in log if e["round"] == 3}

    vote_tech   = r3.get(f"فني/{tech_model}",   "neutral")
    vote_risk   = r3.get(f"مخاطر/{risk_model}", "neutral")
    vote_market = r3.get(f"سوق/{market_model}", "neutral")
    vote_reddit = next((e["vote"] for e in log if e["round"] == 0), "neutral")

    all_votes = [vote_tech, vote_risk, vote_market, vote_reddit]
    ap = all_votes.count("approve")
    rj = all_votes.count("reject")

    # الشرط الأساسي: الفني + المخاطر موافقان
    core_ok = (vote_tech == "approve" and vote_risk == "approve")

    if   core_ok and ap == 4: label, emoji, conf = "إجماع كامل — دخول قوي",     "🟢", "عالية جداً 🔥🔥🔥"
    elif core_ok and ap == 3: label, emoji, conf = "أغلبية قوية موافقة",         "🟢", "عالية جداً 🔥🔥"
    elif core_ok and ap == 2: label, emoji, conf = "موافقة أساسية — مقبول",     "🟢", "عالية 🔥"
    elif rj >= 3:             label, emoji, conf = "رفض واسع — تجنب",           "🔴", "رفض مؤكد ❄️"
    elif vote_tech == "reject" or vote_risk == "reject":
                              label, emoji, conf = "رفض أساسي — لا تدخل",       "🔴", "منخفضة ❄️"
    else:                     label, emoji, conf = "غير محسوم — انتظار إشارة", "🟡", "منخفضة 💧"

    def ve(v): return "✅" if v=="approve" else ("❌" if v=="reject" else "⚠️")
    votes_str = (
        f"فني {ve(vote_tech)}  "
        f"مخاطر {ve(vote_risk)}  "
        f"سوق {ve(vote_market)}  "
        f"Reddit {ve(vote_reddit)}"
    )

    rec = {
        "label":        label,
        "emoji":        emoji,
        "confidence":   conf,
        "votes":        votes_str,
        "send_signal":  core_ok,
    }

    logger.info(f"[Debate] ✅ {opp.symbol} | {label} | {votes_str}")

    return {
        "symbol":         opp.symbol,
        "debate_log":     log,
        "reddit_data":    reddit_text,
        "tech_model":     tech_model,
        "risk_model":     risk_model,
        "market_model":   market_model,
        "recommendation": rec,
    }
