# في MarketScanner — دالة جديدة
def calculate_dynamic_targets(
    self,
    df: pd.DataFrame,
    entry_price: float,
    nearest_support: float,
) -> dict:
    """
    يحسب الأهداف ديناميكياً بناءً على:
    1. Swing Highs (القمم التاريخية)
    2. Fibonacci Retracement
    ويختار الأنسب تلقائياً
    """
    closes = df["close"]
    highs  = df["high"]
    lows   = df["low"]

    # ── الطريقة 1: Swing Highs ──
    # نبحث في آخر 50 شمعة عن القمم المحلية
    swing_highs = []
    window = min(50, len(df))
    recent = highs.tail(window)

    for i in range(2, len(recent) - 2):
        val = recent.iloc[i]
        # قمة محلية: أعلى من الشمعتين قبلها وبعدها
        if (val > recent.iloc[i-1] and
            val > recent.iloc[i-2] and
            val > recent.iloc[i+1] and
            val > recent.iloc[i+2] and
            val > entry_price):       # فقط القمم فوق سعر الدخول
            swing_highs.append(val)

    swing_highs = sorted(set(swing_highs))  # رتّب تصاعدياً وأزل المكرر

    # ── الطريقة 2: Fibonacci Retracement ──
    # القمة: أعلى سعر قبل الانهيار (آخر 60 شمعة)
    local_high = float(highs.tail(60).max())
    # القاع: أدنى سعر (مستوى الدعم الحالي)
    local_low  = nearest_support

    fib_range = local_high - local_low

    fib_tp1 = local_low + fib_range * 0.236
    fib_tp2 = local_low + fib_range * 0.382
    fib_tp3 = local_low + fib_range * 0.618

    # ── اختيار الطريقة ──
    # إذا وُجدت 3 قمم أو أكثر فوق سعر الدخول → Swing Highs
    if len(swing_highs) >= 3:
        tp1 = swing_highs[0]   # أقرب قمة
        tp2 = swing_highs[1]   # القمة الثانية
        tp3 = swing_highs[2]   # القمة الثالثة
        method = "Swing Highs"

    # إذا وُجدت قمتان فقط → نمزج مع Fibonacci
    elif len(swing_highs) == 2:
        tp1 = swing_highs[0]
        tp2 = swing_highs[1]
        tp3 = max(fib_tp3, swing_highs[1] * 1.1)
        method = "Swing + Fib"

    elif len(swing_highs) == 1:
        tp1 = swing_highs[0]
        tp2 = max(fib_tp2, tp1 * 1.15)
        tp3 = max(fib_tp3, tp1 * 1.30)
        method = "Swing + Fib"

    # إذا لم توجد قمم واضحة → Fibonacci خالص
    else:
        tp1 = fib_tp1
        tp2 = fib_tp2
        tp3 = fib_tp3
        method = "Fibonacci"

    # ── التحقق من المنطق ──
    # يجب أن تكون الأهداف تصاعدية وفوق سعر الدخول
    tp1 = max(tp1, entry_price * 1.05)   # على الأقل 5% ربح
    tp2 = max(tp2, tp1 * 1.10)           # TP2 أعلى من TP1 بـ 10%
    tp3 = max(tp3, tp2 * 1.15)           # TP3 أعلى من TP2 بـ 15%

    return {
        "tp1":    round(tp1, 8),
        "tp2":    round(tp2, 8),
        "tp3":    round(tp3, 8),
        "method": method,
        "fib_levels": {
            "high": local_high,
            "low":  local_low,
            "0.236": round(fib_tp1, 8),
            "0.382": round(fib_tp2, 8),
            "0.618": round(fib_tp3, 8),
        }
    }
