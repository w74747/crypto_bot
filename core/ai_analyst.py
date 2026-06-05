"""
core/ai_analyst.py
==================
نظام نقاش الخبراء الثلاثي
Claude (فني) + Gemini (مخاطر) + Grok (مجتمع X)
3 جولات للوصول لرأي مشترك
"""

import os
import requests
from utils.logger import logger
from core.scanner import TradeOpportunity

# ==========================================
# إعدادات API
# ==========================================
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY", "")
GROK_API_KEY      = os.getenv("GROK_API_KEY", "")

CLAUDE_MODEL = "claude-sonnet-4-20250514"
GEMINI_MODEL = "gemini-1.5-flash"
GROK_MODEL   = "grok-3-fast"   # الأرخص مع X Search

MAX_TOKENS = 600  # مختصر لكل رد


# ==========================================
# بيانات الفرصة
# ==========================================
def build_opportunity_data(opp: TradeOpportunity) -> str:
    return f"""
بيانات العملة:
  الرمز:               {opp.symbol}
  السعر الحالي:        {opp.current_price:.6f} USDT
  نوع الإشارة:         {opp.signal_type}
  RSI اليومي:          {opp.rsi_daily:.1f}
  الانهيار (60 يوم):   {opp.crash_pct_60d:.1%}
  البعد عن القاع:      {opp.distance_from_lod:.1%}
  أدنى سعر 180 يوم:    {opp.lod_180:.6f}
  أقرب دعم:            {opp.nearest_support:.6f}
  حجم التداول 24h:     ${opp.volume_24h_usd:,.0f}
  سعر الدخول:          {opp.entry_price:.6f}
  وقف الخسارة:         {opp.stop_loss:.6f} (-{((opp.entry_price-opp.stop_loss)/opp.entry_price)*100:.1f}%)
  TP1 (30%):           {opp.tp1:.6f}
  TP2 (60%):           {opp.tp2:.6f}
  TP3 (100%):          {opp.tp3:.6f}
  نسبة R/R:            1:{opp.risk_reward_ratio}
  نشاط GitHub:         {'✅ نشط' if opp.github_active else '❌ غير نشط'}
"""


# ==========================================
# استدعاء Claude
# ==========================================
def ask_claude(messages: list[dict], system: str) -> str:
    if not ANTHROPIC_API_KEY:
        return "⚠️ ANTHROPIC_API_KEY غير موجود"
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
        return r.json()["content"][0]["text"].strip()
    except Exception as e:
        logger.error(f"[Claude] خطأ: {e}")
        return f"⚠️ خطأ Claude: {str(e)[:80]}"


# ==========================================
# استدعاء Gemini
# ==========================================
def ask_gemini(prompt: str) -> str:
    if not GEMINI_API_KEY:
        return "⚠️ GEMINI_API_KEY غير موجود"
    try:
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": MAX_TOKENS, "temperature": 0.3},
            },
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as e:
        logger.error(f"[Gemini] خطأ: {e}")
        return f"⚠️ خطأ Gemini: {str(e)[:80]}"


# ==========================================
# استدعاء Grok مع X Search
# ==========================================
def ask_grok(messages: list[dict], system: str, use_x_search: bool = True) -> str:
    if not GROK_API_KEY:
        return "⚠️ GROK_API_KEY غير موجود"
    try:
        body = {
            "model":      GROK_MODEL,
            "max_tokens": MAX_TOKENS,
            "system":     system,
            "messages":   messages,
        }

        # تفعيل X Search للجولة الأولى فقط (لتوفير التكلفة)
        if use_x_search:
            body["tools"] = [{"type": "x_search"}]
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

        # Grok قد يُعيد نصاً أو نتيجة tool_use
        content = r.json().get("content", [])
        texts   = [c["text"] for c in content if c.get("type") == "text"]
        return "\n".join(texts).strip() or "لا يوجد رد من Grok"

    except Exception as e:
        logger.error(f"[Grok] خطأ: {e}")
        return f"⚠️ خطأ Grok: {str(e)[:80]}"


# ==========================================
# System Prompts لكل خبير
# ==========================================
CLAUDE_SYSTEM = """أنت محلل فني متخصص في العملات الرقمية — خبير المؤشرات والأرقام.
دورك في هذا النقاش: تحليل الجانب الفني البحت.
قواعد:
- اختصر ردودك (لا تتجاوز 150 كلمة)
- استند للأرقام فقط: RSI، الانهيار، R/R، مستويات الدعم
- حدد موقفك بوضوح: ✅ موافق / ⚠️ محايد / ❌ رافض
- عند الرد على الآخرين: اذكر نقاط اتفاق واختلاف محددة"""

GEMINI_SYSTEM = """أنت خبير إدارة مخاطر متخصص في العملات الرقمية.
دورك في هذا النقاش: تقييم المخاطر المالية وسلامة الصفقة.
قواعد:
- اختصر ردودك (لا تتجاوز 150 كلمة)
- ركز على: المخاطر، السيولة، حجم التداول، احتمالية الخسارة
- حدد موقفك بوضوح: ✅ موافق / ⚠️ محايد / ❌ رافض
- عند الرد على الآخرين: ناقش بموضوعية وأضف زاوية مخاطر جديدة"""

GROK_SYSTEM = """أنت محلل مجتمع ومشاعر (Sentiment Analyst) متخصص في العملات الرقمية.
لديك وصول لـ X/Twitter في الوقت الحقيقي.
دورك في هذا النقاش: تقرير ما يقوله المجتمع والمؤسسون.
قواعد:
- اختصر ردودك (لا تتجاوز 150 كلمة)
- ابحث على X عن: العملة + المؤسسون + المستثمرون الكبار
- أبلّغ عن: مزاج المجتمع، آخر تغريدات المؤسسين، أي أخبار مخفية
- حدد موقفك: ✅ مجتمع إيجابي / ⚠️ محايد / ❌ مجتمع سلبي
- لا تخترع معلومات — إذا لم تجد شيئاً قل ذلك بوضوح"""


# ==========================================
# دالة النقاش الرئيسية
# ==========================================
def run_expert_debate(opp: TradeOpportunity) -> dict:
    """
    3 جولات نقاش بين Claude وGemini وGrok
    """
    logger.info(f"[AI Debate] بدء النقاش الثلاثي لـ {opp.symbol}")
    coin      = opp.symbol.replace("/USDT", "")
    opp_data  = build_opportunity_data(opp)
    debate_log = []

    def log_entry(round_num, speaker, text):
        debate_log.append({"round": round_num, "speaker": speaker, "text": text})
        logger.info(f"[Debate R{round_num}] {speaker}: {text[:80]}...")

    # ─────────────────────────────────
    # الجولة الأولى — كل خبير يحلل مستقلاً
    # ─────────────────────────────────
    logger.info("[AI Debate] ── الجولة الأولى ──")

    # Claude: التحليل الفني
    c1 = ask_claude(
        messages=[{"role": "user", "content":
            f"حلّل هذه الفرصة فنياً:\n{opp_data}\n\n"
            f"أجب على:\n"
            f"1. قوة الإشارة الفنية (RSI، الانهيار، الدعم)\n"
            f"2. جودة نقاط الدخول والأهداف\n"
            f"3. موقفك: ✅ موافق / ⚠️ محايد / ❌ رافض\n"
            f"كن مختصراً ومباشراً."
        }],
        system=CLAUDE_SYSTEM,
    )
    log_entry(1, "Claude", c1)

    # Gemini: تحليل المخاطر
    g1 = ask_gemini(
        f"أنت خبير مخاطر. {GEMINI_SYSTEM}\n\n"
        f"قيّم مخاطر هذه الصفقة:\n{opp_data}\n\n"
        f"أجب على:\n"
        f"1. مستوى المخاطرة (عالي/متوسط/منخفض) ولماذا\n"
        f"2. هل حجم التداول كافٍ للخروج الآمن؟\n"
        f"3. أكبر خطر يهددها\n"
        f"4. موقفك: ✅ موافق / ⚠️ محايد / ❌ رافض"
    )
    log_entry(1, "Gemini", g1)

    # Grok: تقرير X مع البحث الحقيقي
    grok1 = ask_grok(
        messages=[{"role": "user", "content":
            f"ابحث على X/Twitter عن عملة {coin} وأبلّغ عن:\n"
            f"1. آخر تغريدات المؤسسين أو الفريق الرسمي (آخر 7 أيام)\n"
            f"2. مزاج المجتمع: إيجابي أم سلبي؟\n"
            f"3. هل يتحدث الـ Whales أو المستثمرون الكبار عنها؟\n"
            f"4. هل هناك أخبار سلبية أو مشاكل تقنية تُناقش؟\n"
            f"5. موقفك بناءً على X: ✅ / ⚠️ / ❌\n\n"
            f"بيانات السوق للسياق:\n{opp_data}"
        }],
        system=GROK_SYSTEM,
        use_x_search=True,  # بحث حقيقي على X
    )
    log_entry(1, "Grok", grok1)

    # ─────────────────────────────────
    # الجولة الثانية — كل خبير يرد على الآخرين
    # ─────────────────────────────────
    logger.info("[AI Debate] ── الجولة الثانية ──")

    c2 = ask_claude(
        messages=[
            {"role": "user",      "content": f"حلّل هذه الفرصة:\n{opp_data}"},
            {"role": "assistant", "content": c1},
            {"role": "user",      "content":
                f"قال Gemini (خبير المخاطر):\n{g1}\n\n"
                f"وقال Grok (محلل X):\n{grok1}\n\n"
                f"ردّ عليهما:\n"
                f"1. ما الذي يقولانه وهو صحيح؟\n"
                f"2. أين تختلف معهما؟\n"
                f"3. هل معطيات X من Grok تؤثر على تحليلك الفني؟\n"
                f"4. موقفك المحدّث"
            },
        ],
        system=CLAUDE_SYSTEM,
    )
    log_entry(2, "Claude", c2)

    g2 = ask_gemini(
        f"أنت خبير مخاطر. {GEMINI_SYSTEM}\n\n"
        f"البيانات:\n{opp_data}\n\n"
        f"قال Claude (المحلل الفني):\n{c1}\n\n"
        f"قال Grok (محلل X):\n{grok1}\n\n"
        f"ردّ عليهما:\n"
        f"1. هل تحليل Claude الفني يطمئنك أم يقلقك؟\n"
        f"2. هل معطيات X من Grok تزيد أو تقلل المخاطر؟\n"
        f"3. ما المخاطرة التي لم يذكراها؟\n"
        f"4. موقفك المحدّث"
    )
    log_entry(2, "Gemini", g2)

    grok2 = ask_grok(
        messages=[{"role": "user", "content":
            f"قال Claude (المحلل الفني):\n{c1}\n\n"
            f"قال Gemini (خبير المخاطر):\n{g1}\n\n"
            f"ردّ عليهما بناءً على ما وجدته على X:\n"
            f"1. هل ما تحدثا عنه يتوافق مع مزاج X؟\n"
            f"2. هل هناك معلومة من X تدعم أو تعارض تحليلهما؟\n"
            f"3. موقفك المحدّث بناءً على النقاش"
        }],
        system=GROK_SYSTEM,
        use_x_search=False,  # لا نكرر البحث توفيراً للتكلفة
    )
    log_entry(2, "Grok", grok2)

    # ─────────────────────────────────
    # الجولة الثالثة — الحكم النهائي
    # ─────────────────────────────────
    logger.info("[AI Debate] ── الجولة الثالثة — الحكم ──")

    c3 = ask_claude(
        messages=[
            {"role": "user",      "content": f"حلّل هذه الفرصة:\n{opp_data}"},
            {"role": "assistant", "content": c1},
            {"role": "user",      "content": f"ردودهما:\nGemini: {g1}\nGrok: {grok1}"},
            {"role": "assistant", "content": c2},
            {"role": "user",      "content":
                f"ردودهما الثانية:\nGemini: {g2}\nGrok: {grok2}\n\n"
                f"الحكم النهائي المطلوب منك:\n"
                f"1. نقاط الاتفاق الثلاثة بينكم\n"
                f"2. نقاط الخلاف المتبقية\n"
                f"3. التوصية النهائية: هل تستحق الدخول؟\n"
                f"4. درجة الثقة: عالية / متوسطة / منخفضة\n"
                f"5. أهم شرط لنجاح الصفقة\n"
                f"كن حاسماً وواضحاً."
            },
        ],
        system=CLAUDE_SYSTEM,
    )
    log_entry(3, "Claude", c3)

    g3 = ask_gemini(
        f"أنت خبير مخاطر. {GEMINI_SYSTEM}\n\n"
        f"ملخص النقاش:\n"
        f"Claude قال: {c2}\n"
        f"Grok قال: {grok2}\n"
        f"الحكم النهائي من Claude: {c3}\n\n"
        f"دورك الأخير:\n"
        f"1. هل توافق على حكم Claude النهائي؟\n"
        f"2. ما أكبر خطر لا يزال قائماً؟\n"
        f"3. خلاصتك في جملتين للمتداول"
    )
    log_entry(3, "Gemini", g3)

    grok3 = ask_grok(
        messages=[{"role": "user", "content":
            f"الحكم النهائي من Claude: {c3}\n"
            f"رأي Gemini الأخير: {g3}\n\n"
            f"خلاصتك الأخيرة:\n"
            f"1. هل X يدعم أو يعارض هذا الحكم؟\n"
            f"2. جملة واحدة تلخص ما وجدته على X\n"
            f"3. تصويتك النهائي: ✅ / ⚠️ / ❌"
        }],
        system=GROK_SYSTEM,
        use_x_search=False,
    )
    log_entry(3, "Grok", grok3)

    # ─────────────────────────────────
    # استخراج النتيجة المشتركة
    # ─────────────────────────────────
    recommendation = _extract_recommendation(c3, g3, grok3)

    logger.info(
        f"[AI Debate] ✅ اكتمل النقاش لـ {opp.symbol} | "
        f"التوصية: {recommendation['label']} | "
        f"الأصوات: {recommendation['votes']}"
    )

    return {
        "symbol":         opp.symbol,
        "debate_log":     debate_log,
        "claude_final":   c3,
        "gemini_final":   g3,
        "grok_final":     grok3,
        "recommendation": recommendation,
    }


# ==========================================
# استخراج التوصية بنظام التصويت
# ==========================================
def _extract_recommendation(claude_text: str, gemini_text: str, grok_text: str) -> dict:
    """نظام تصويت 3/3 — كل خبير صوت واحد"""

    def get_vote(text: str) -> str:
        t = text.lower()
        if any(w in t for w in ["❌", "رافض", "لا أنصح", "تجنب", "خطر عالٍ"]):
            return "reject"
        elif any(w in t for w in ["⚠️", "محايد", "بحذر", "انتظار"]):
            return "neutral"
        else:
            return "approve"

    votes = {
        "Claude": get_vote(claude_text),
        "Gemini": get_vote(gemini_text),
        "Grok":   get_vote(grok_text),
    }

    approve_count = sum(1 for v in votes.values() if v == "approve")
    reject_count  = sum(1 for v in votes.values() if v == "reject")

    if approve_count == 3:
        label = "✅ إجماع على الدخول"
        emoji = "🟢"
        confidence = "عالية جداً 🔥🔥"
    elif approve_count == 2 and reject_count == 0:
        label = "✅ أغلبية موافقة"
        emoji = "🟢"
        confidence = "عالية 🔥"
    elif approve_count == 2 and reject_count == 1:
        label = "⚠️ موافقة مع تحفظ"
        emoji = "🟡"
        confidence = "متوسطة 💧"
    elif reject_count == 3:
        label = "❌ إجماع على الرفض"
        emoji = "🔴"
        confidence = "عالية (رفض) ❄️"
    elif reject_count >= 2:
        label = "❌ أغلبية رافضة"
        emoji = "🔴"
        confidence = "منخفضة ❄️"
    else:
        label = "⚠️ محايد — انتظار"
        emoji = "🟡"
        confidence = "منخفضة 💧"

    votes_str = (
        f"Claude {'✅' if votes['Claude']=='approve' else '⚠️' if votes['Claude']=='neutral' else '❌'} | "
        f"Gemini {'✅' if votes['Gemini']=='approve' else '⚠️' if votes['Gemini']=='neutral' else '❌'} | "
        f"Grok {'✅' if votes['Grok']=='approve' else '⚠️' if votes['Grok']=='neutral' else '❌'}"
    )

    return {
        "label":      label,
        "emoji":      emoji,
        "confidence": confidence,
        "votes":      votes_str,
        "raw_votes":  votes,
    }
