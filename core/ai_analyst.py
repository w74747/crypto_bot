"""
core/ai_analyst.py
==================
نظام نقاش الخبراء — مع نظام تصويت منظم
كل نموذج يُلزَم بإخراج [VOTE: YES] أو [VOTE: NO] فقط
يمنع الخلط بين "موافق على رأي الخبير" و "موافق على الصفقة"
"""

import os
import re
import requests
from utils.logger import logger
from core.scanner import TradeOpportunity

GROQ_API_KEY     = os.getenv("GROQ_API_KEY", "")
TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY", "")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")

GROQ_MODEL     = os.getenv("GROQ_MODEL",     "llama-3.3-70b-versatile")
TOGETHER_MODEL = os.getenv("TOGETHER_MODEL", "meta-llama/Llama-3.3-70B-Instruct-Turbo")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")

MAX_TOKENS = 180

# ==========================================
# VOTE TAG — قاعدة لا استثناء فيها
# ==========================================
VOTE_INSTRUCTION = """
آخر سطر فقط: [VOTE: YES] أو [VOTE: NO]
"""

# ==========================================
# System Prompts
# ==========================================
TECHNICAL_SYS = f"""محلل تقني. ركّز: RSI + R/R + دعم/مقاومة. 40 كلمة max. لا Markdown.
{VOTE_INSTRUCTION}"""

RISK_SYS = f"""مدير مخاطر. السياق: صفقة $30-100، Dynamic SL ضيق لذا R/R مرتفع طبيعي.
قيّم: حجم > $1M؟ SL عند دعم حقيقي؟ 40 كلمة max. لا Markdown.
{VOTE_INSTRUCTION}"""

MARKET_SYS = f"""محلل سوق. ارفض فقط إذا BTC انهار >15% في أسبوع. القيعان = فرص.
هل العملة عند قاع تاريخي؟ 40 كلمة max. لا Markdown.
{VOTE_INSTRUCTION}"""


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
    # أزل الـ VOTE tag من النص المختصر
    text = re.sub(r'\[VOTE:\s*(YES|NO)\]', '', text).strip()
    if len(text) <= n:
        return text
    cut = text[:n]
    for sep in ['،', '.', '!', '؟']:
        pos = cut.rfind(sep)
        if pos > n * 0.6:
            return cut[:pos+1]
    return cut + "..."


# ==========================================
# استخراج التصويت — STRUCTURED ONLY
# ==========================================
def extract_vote(text: str) -> str:
    """
    يستخرج التصويت من الـ structured tag فقط
    [VOTE: YES] → approve
    [VOTE: NO]  → reject
    إذا لم يجد tag → يحلل النص بحذر شديد كـ fallback
    """
    if not text:
        return "reject"  # بدون رد → رفض آمن

    # البحث عن الـ structured tag
    match = re.search(r'\[VOTE:\s*(YES|NO)\]', text, re.IGNORECASE)
    if match:
        vote = match.group(1).upper()
        result = "approve" if vote == "YES" else "reject"
        logger.debug(f"[Vote] Tag وُجد: [VOTE: {vote}] → {result}")
        return result

    # Fallback: النموذج لم يلتزم بالـ tag
    # نستخدم تحليلاً محافظاً — الشك يُحسب رفضاً
    logger.warning(f"[Vote] لم يُعثر على [VOTE] tag — تحليل محافظ: {text[:60]}")

    text_lower = text.lower()

    # كلمات رفض صريحة وواضحة السياق
    hard_reject = [
        "لا أنصح بالدخول", "لا توصية بالدخول",
        "تجنب هذه الصفقة", "لا تدخل",
        "خطر عالٍ جداً", "مخاطرة عالية جداً",
        "i do not recommend", "do not enter",
    ]
    for phrase in hard_reject:
        if phrase in text_lower:
            logger.debug(f"[Vote Fallback] رفض صريح: '{phrase}'")
            return "reject"

    # كلمات موافقة صريحة وواضحة السياق
    hard_approve = [
        "أنصح بالدخول", "توصية بالدخول",
        "فرصة ممتازة للدخول", "يستحق الدخول",
        "i recommend entering", "good entry opportunity",
    ]
    for phrase in hard_approve:
        if phrase in text_lower:
            logger.debug(f"[Vote Fallback] موافقة صريحة: '{phrase}'")
            return "approve"

    # إذا لم يجد شيئاً واضحاً → رفض آمن
    logger.debug(f"[Vote Fallback] غير محدد → رفض آمن")
    return "reject"


def extract_reddit_vote(text: str) -> str:
    """Reddit لا يستخدم VOTE tag — يحلل المزاج المباشر"""
    if "إيجابي" in text: return "approve"
    if "سلبي"   in text: return "reject"
    return "neutral"


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
# Reddit RSS
# ==========================================
def get_reddit_sentiment(coin_symbol: str) -> tuple[str, str]:
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
                    })
        except Exception:
            continue

    if not posts_found:
        return f"لا توجد منشورات حديثة عن {coin_symbol} على Reddit.", "neutral"

    pos = sum(1 for p in posts_found
              for w in ["bull","moon","buy","pump","recovery","bullish","gem"]
              if w in p["title"].lower())
    neg = sum(1 for p in posts_found
              for w in ["bear","dump","crash","rug","scam","bearish","warning","dead"]
              if w in p["title"].lower())

    if pos > neg:
        sentiment, vote = "إيجابي", "approve"
    elif neg > pos:
        sentiment, vote = "سلبي", "reject"
    else:
        sentiment, vote = "محايد", "neutral"

    lines = [f"{p['title']} (👍{p['score']})" for p in posts_found[:2]]
    text  = f"Reddit {sentiment}: " + " | ".join(lines)
    return text, vote


# ==========================================
# API Caller
# ==========================================
def _call_api(api_key: str, base_url: str, model: str,
               system: str, user_msg: str, label: str) -> str:
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
    return _clean(r.json()["choices"][0]["message"]["content"] or "")


# ==========================================
# الخبراء مع Fallback Chain
# ==========================================
def _ask_with_fallback(
    providers: list[tuple],
    system:    str,
    user_msg:  str,
    role:      str,
) -> tuple[str, str]:
    """
    يجرب المزودين بالترتيب ويُعيد (النص, اسم المزود)
    يتحقق أن الرد يحتوي على [VOTE] tag
    """
    for i, (label, key, url, model) in enumerate(providers):
        if not key:
            continue
        # تأخير 2 ثانية بين المحاولات لمنع 429
        if i > 0:
            import time
            time.sleep(2)
        try:
            text = _call_api(key, url, model, system, user_msg, label)

            if re.search(r'\[VOTE:\s*(YES|NO)\]', text, re.IGNORECASE):
                logger.info(f"[{role}] {label} ✅ (tag موجود)")
                return text, label

            logger.warning(f"[{role}] {label}: لا VOTE tag — retry")
            retry_msg = user_msg + "\n[VOTE: YES] أو [VOTE: NO] في آخر سطر"
            text2 = _call_api(key, url, model, system, retry_msg, label)
            logger.info(f"[{role}] {label} ✅ (retry)")
            return text2, label

        except Exception as e:
            logger.warning(f"[{role}] {label} فشل: {str(e)[:60]}")

    return f"لا يوجد مزود متاح. [VOTE: NO]", "—"


def ask_technical(user_msg: str) -> tuple[str, str]:
    return _ask_with_fallback(
        providers=[
            ("Groq",     GROQ_API_KEY,     "https://api.groq.com/openai/v1",  GROQ_MODEL),
            ("Together", TOGETHER_API_KEY, "https://api.together.xyz/v1",     TOGETHER_MODEL),
            ("DeepSeek", DEEPSEEK_API_KEY, "https://api.deepseek.com/v1",     DEEPSEEK_MODEL),
        ],
        system   = TECHNICAL_SYS,
        user_msg = user_msg,
        role     = "Technical",
    )

def ask_risk(user_msg: str) -> tuple[str, str]:
    return _ask_with_fallback(
        providers=[
            ("Together", TOGETHER_API_KEY, "https://api.together.xyz/v1",     TOGETHER_MODEL),
            ("DeepSeek", DEEPSEEK_API_KEY, "https://api.deepseek.com/v1",     DEEPSEEK_MODEL),
            ("Groq",     GROQ_API_KEY,     "https://api.groq.com/openai/v1",  GROQ_MODEL),
        ],
        system   = RISK_SYS,
        user_msg = user_msg,
        role     = "Risk",
    )

def ask_market(user_msg: str) -> tuple[str, str]:
    return _ask_with_fallback(
        providers=[
            ("DeepSeek", DEEPSEEK_API_KEY, "https://api.deepseek.com/v1",     DEEPSEEK_MODEL),
            ("Groq",     GROQ_API_KEY,     "https://api.groq.com/openai/v1",  GROQ_MODEL),
            ("Together", TOGETHER_API_KEY, "https://api.together.xyz/v1",     TOGETHER_MODEL),
        ],
        system   = MARKET_SYS,
        user_msg = user_msg,
        role     = "Market",
    )


# ==========================================
# النقاش الرئيسي — 3 جولات
# ==========================================
def run_expert_debate(opp: TradeOpportunity) -> dict:
    coin = opp.symbol.replace("/USDT", "")
    data = build_data(opp)
    log  = []

    tech_model = risk_model = market_model = "—"

    def rec(round_num: int, speaker: str, text: str):
        vote = extract_vote(text)
        log.append({
            "round":   round_num,
            "speaker": speaker,
            "text":    text,
            "vote":    vote,
        })
        # أظهر الـ VOTE tag في الـ log بوضوح
        tag = re.search(r'\[VOTE:\s*(YES|NO)\]', text, re.IGNORECASE)
        tag_str = f"[VOTE: {tag.group(1)}]" if tag else "[NO TAG]"
        logger.info(
            f"[Debate R{round_num}] {speaker} {tag_str} → {vote}: "
            f"{_trim(text, 50)}"
        )

    # Reddit
    reddit_text, reddit_vote = get_reddit_sentiment(coin)
    log.append({"round": 0, "speaker": "Reddit",
                "text": reddit_text, "vote": reddit_vote})

    # ── جولة 1 ──
    logger.info(f"[Debate] جولة 1 — {opp.symbol}")

    t1, tech_model = ask_technical(
        f"بيانات الصفقة:\n{data}\n\n"
        f"حكمك الفني: هل المؤشرات تدعم الدخول؟\n"
        f"حلّل RSI والأهداف الديناميكية ووقف الخسارة.\n"
        f"اكتب تحليلك ثم في السطر الأخير: [VOTE: YES] أو [VOTE: NO]"
    )
    rec(1, f"فني/{tech_model}", t1)

    r1, risk_model = ask_risk(
        f"بيانات الصفقة:\n{data}\n\n"
        f"قيّم المخاطر: هل R/R={opp.risk_reward_ratio} مقبول؟\n"
        f"هل الحجم ${opp.volume_24h_usd/1e6:.1f}M كافٍ للخروج الآمن؟\n"
        f"اكتب تحليلك ثم في السطر الأخير: [VOTE: YES] أو [VOTE: NO]"
    )
    rec(1, f"مخاطر/{risk_model}", r1)

    m1, market_model = ask_market(
        f"بيانات: {data}\n"
        f"Reddit: {_trim(reddit_text, 80)}\n\n"
        f"قيّم السياق الكلي للسوق لهذه العملة.\n"
        f"اكتب تحليلك ثم في السطر الأخير: [VOTE: YES] أو [VOTE: NO]"
    )
    rec(1, f"سوق/{market_model}", m1)

    # إنهاء مبكر إذا رفض الجميع
    v_t1 = extract_vote(t1)
    v_r1 = extract_vote(r1)
    if v_t1 == "reject" and v_r1 == "reject":
        logger.info(f"[Debate] الفني والمخاطر رفضا → إنهاء مبكر")
        for sp, tx in [(f"فني/{tech_model}", t1),
                       (f"مخاطر/{risk_model}", r1),
                       (f"سوق/{market_model}", m1)]:
            rec(3, sp, tx)
        return _finalize(opp, log, reddit_text, tech_model, risk_model, market_model)

    # ── جولة 2 ──
    logger.info(f"[Debate] جولة 2 — {opp.symbol}")

    t2, _ = ask_technical(
        f"بيانات: {data}\n\n"
        f"خبير المخاطر قال: {_trim(r1)}\n"
        f"محلل السوق قال: {_trim(m1)}\n\n"
        f"هل تُعدّل رأيك الفني بعد سماعهما؟\n"
        f"اكتب تحليلك ثم في السطر الأخير: [VOTE: YES] أو [VOTE: NO]"
    )
    rec(2, f"فني/{tech_model}", t2)

    r2, _ = ask_risk(
        f"بيانات: {data}\n\n"
        f"المحلل الفني قال: {_trim(t1)}\n"
        f"محلل السوق قال: {_trim(m1)}\n\n"
        f"بعد سماع التحليل الفني، هل تُعدّل تقييم المخاطر؟\n"
        f"اكتب تحليلك ثم في السطر الأخير: [VOTE: YES] أو [VOTE: NO]"
    )
    rec(2, f"مخاطر/{risk_model}", r2)

    m2, _ = ask_market(
        f"بيانات: {data[:120]}\n\n"
        f"الفني: {_trim(t1, 50)} | المخاطر: {_trim(r1, 50)}\n\n"
        f"هل تُعدّل نظرتك للسوق الكلي؟\n"
        f"اكتب تحليلك ثم في السطر الأخير: [VOTE: YES] أو [VOTE: NO]"
    )
    rec(2, f"سوق/{market_model}", m2)

    # ── جولة 3: الحكم النهائي ──
    logger.info(f"[Debate] جولة 3 — {opp.symbol}")

    t3, _ = ask_technical(
        f"بيانات: {data}\n\n"
        f"بعد النقاش الكامل:\n"
        f"المخاطر: {_trim(r2)}\n"
        f"السوق: {_trim(m2)}\n"
        f"Reddit: {_trim(reddit_text, 50)}\n\n"
        f"قرارك النهائي القاطع — هل تدخل هذه الصفقة بأموالك الحقيقية؟\n"
        f"سبب واحد فقط، ثم في السطر الأخير: [VOTE: YES] أو [VOTE: NO]"
    )
    rec(3, f"فني/{tech_model}", t3)

    r3, _ = ask_risk(
        f"حكم المحلل الفني: {_trim(t3)}\n"
        f"بيانات: {data}\n\n"
        f"قرارك النهائي كمدير مخاطر — هل تُجيز هذه الصفقة؟\n"
        f"في السطر الأخير: [VOTE: YES] أو [VOTE: NO]"
    )
    rec(3, f"مخاطر/{risk_model}", r3)

    m3, _ = ask_market(
        f"الفني: {_trim(t3, 50)} | المخاطر: {_trim(r3, 50)}\n\n"
        f"تصويتك الأخير بناءً على السوق الكلي.\n"
        f"في السطر الأخير: [VOTE: YES] أو [VOTE: NO]"
    )
    rec(3, f"سوق/{market_model}", m3)

    return _finalize(opp, log, reddit_text, tech_model, risk_model, market_model)


# ==========================================
# بناء النتيجة النهائية
# ==========================================
def _finalize(opp, log, reddit_text, tech_model, risk_model, market_model) -> dict:
    r3 = {e["speaker"]: e["vote"] for e in log if e["round"] == 3}

    v_tech   = r3.get(f"فني/{tech_model}",   "reject")
    v_risk   = r3.get(f"مخاطر/{risk_model}", "reject")
    v_market = r3.get(f"سوق/{market_model}", "neutral")
    v_reddit = next((e["vote"] for e in log if e["round"] == 0), "neutral")

    all_v = [v_tech, v_risk, v_market, v_reddit]
    ap = all_v.count("approve")
    rj = all_v.count("reject")

    # الشرط الأساسي: الفني + المخاطر كلاهما [VOTE: YES]
    core_ok = (v_tech == "approve" and v_risk == "approve")

    if   core_ok and ap == 4: label, emoji, conf = "إجماع كامل — دخول قوي",    "🟢", "عالية جداً 🔥🔥🔥"
    elif core_ok and ap == 3: label, emoji, conf = "أغلبية قوية موافقة",        "🟢", "عالية جداً 🔥🔥"
    elif core_ok and ap == 2: label, emoji, conf = "موافقة أساسية — مقبول",    "🟢", "عالية 🔥"
    elif rj >= 3:             label, emoji, conf = "رفض واسع — تجنب",          "🔴", "رفض مؤكد ❄️"
    elif not core_ok and rj >= 1:
                              label, emoji, conf = "رفض أساسي — لا تدخل",      "🔴", "منخفضة ❄️"
    else:                     label, emoji, conf = "غير محسوم — انتظار",       "🟡", "منخفضة 💧"

    def ve(v): return "✅" if v=="approve" else ("❌" if v=="reject" else "⚠️")
    votes_str = (
        f"فني {ve(v_tech)}  "
        f"مخاطر {ve(v_risk)}  "
        f"سوق {ve(v_market)}  "
        f"Reddit {ve(v_reddit)}"
    )

    rec = {
        "label":        label,
        "emoji":        emoji,
        "confidence":   conf,
        "votes":        votes_str,
        "send_signal":  core_ok,
    }

    logger.info(
        f"[Debate] ✅ {opp.symbol} | {label} | {votes_str} | "
        f"core_ok={core_ok}"
    )

    return {
        "symbol":         opp.symbol,
        "debate_log":     log,
        "reddit_data":    reddit_text,
        "tech_model":     tech_model,
        "risk_model":     risk_model,
        "market_model":   market_model,
        "recommendation": rec,
    }
