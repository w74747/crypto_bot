"""
core/database.py
================
Trading Brain — قاعدة بيانات ذاكرة البوت
SQLite للتخزين المحلي (قابل للترقية لـ PostgreSQL)

الجداول:
  market_context     — بيانات السوق عند الإشارة
  expert_discussions — نقاشات الخبراء وقراراتهم
  trade_outcome      — نتيجة كل صفقة
  price_tracking     — تتبع السعر كل 6 ساعات
"""

import os
import sqlite3
import json
import time
import threading
import requests
from datetime import datetime, timezone, timedelta
from utils.logger import logger

DB_PATH = os.getenv("DB_PATH", "data/trading_brain.db")


# ==========================================
# إنشاء قاعدة البيانات والجداول
# ==========================================
def init_db():
    """ينشئ قاعدة البيانات والجداول إذا لم تكن موجودة"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = _get_conn()
    cur  = conn.cursor()

    # ── جدول 1: سياق السوق ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS market_context (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id       TEXT    UNIQUE NOT NULL,  -- معرف فريد للإشارة
        symbol          TEXT    NOT NULL,
        timestamp_utc   TEXT    NOT NULL,         -- وقت الإشارة
        entry_price     REAL    NOT NULL,
        volume_24h_usd  REAL,
        rsi_daily       REAL,
        crash_pct_60d   REAL,
        signal_type     TEXT,                     -- 🅰️ 🅱️ 🅲 🅳
        tp_method       TEXT,                     -- Fibonacci / Swing Highs
        tp1             REAL,
        tp2             REAL,
        tp3             REAL,
        stop_loss       REAL,
        fib_high        REAL,
        fib_low         REAL,
        risk_reward     REAL,
        btc_price       REAL,                     -- سعر BTC عند الإشارة
        btc_trend       TEXT,                     -- safe / danger
        galaxy_score    REAL,                     -- LunarCrush (إذا متاح)
        reddit_sentiment TEXT                     -- positive / neutral / negative
    )
    """)

    # ── جدول 2: نقاشات الخبراء ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS expert_discussions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id       TEXT    NOT NULL REFERENCES market_context(signal_id),
        timestamp_utc   TEXT    NOT NULL,
        tech_model      TEXT,                     -- Groq / DeepSeek / Together
        risk_model      TEXT,
        market_model    TEXT,
        tech_round1     TEXT,                     -- رد الجولة الأولى
        risk_round1     TEXT,
        market_round1   TEXT,
        tech_round3     TEXT,                     -- الحكم النهائي
        risk_round3     TEXT,
        market_round3   TEXT,
        tech_vote       TEXT,                     -- YES / NO
        risk_vote       TEXT,
        market_vote     TEXT,
        reddit_vote     TEXT,
        final_decision  TEXT,                     -- approved / rejected
        confidence      TEXT,                     -- label مثل "عالية 🔥"
        full_log        TEXT                      -- JSON كامل للنقاش
    )
    """)

    # ── جدول 3: نتيجة الصفقة ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trade_outcome (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id           TEXT    NOT NULL REFERENCES market_context(signal_id),
        symbol              TEXT    NOT NULL,
        user_decision       TEXT    NOT NULL,     -- executed / ignored
        execution_timestamp TEXT,                 -- وقت الشراء الفعلي
        filled_price        REAL,                 -- سعر الشراء الفعلي
        filled_qty          REAL,
        trade_amount_usd    REAL,

        -- النتائج (تُحدَّث تلقائياً بعد 48 ساعة)
        outcome_status      TEXT    DEFAULT 'pending',
        -- pending / hit_tp1 / hit_tp2 / hit_tp3 / hit_stop / cancelled

        max_price_reached   REAL,                 -- أعلى سعر وصل إليه
        max_pct_reached     REAL,                 -- أعلى نسبة ربح وصلت
        outcome_timestamp   TEXT,                 -- وقت تحديث النتيجة
        notes               TEXT                  -- ملاحظات إضافية
    )
    """)

    # ── جدول 4: تتبع السعر (للتحديث التلقائي) ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS price_tracking (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id       TEXT    NOT NULL,
        symbol          TEXT    NOT NULL,
        check_timestamp TEXT    NOT NULL,
        current_price   REAL    NOT NULL,
        pct_change      REAL,                     -- نسبة التغيير من الدخول
        tp1_hit         INTEGER DEFAULT 0,        -- 0 أو 1
        tp2_hit         INTEGER DEFAULT 0,
        tp3_hit         INTEGER DEFAULT 0,
        sl_hit          INTEGER DEFAULT 0
    )
    """)

    # ── جدول 5: إحصائيات الأداء (للتعلم الذاتي) ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS performance_stats (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        calculated_at   TEXT    NOT NULL,
        total_signals   INTEGER DEFAULT 0,
        total_executed  INTEGER DEFAULT 0,
        total_ignored   INTEGER DEFAULT 0,
        tp1_hit_rate    REAL,                     -- نسبة الوصول لـ TP1
        tp2_hit_rate    REAL,
        tp3_hit_rate    REAL,
        sl_hit_rate     REAL,
        avg_max_pct     REAL,                     -- متوسط أعلى ربح وصل
        best_signal_type TEXT,                    -- أفضل نوع إشارة
        best_tp_method  TEXT,                     -- أفضل طريقة أهداف
        notes           TEXT
    )
    """)

    conn.commit()
    conn.close()
    logger.info(f"✅ قاعدة البيانات جاهزة: {DB_PATH}")


def _get_conn() -> sqlite3.Connection:
    """يُعيد اتصال بقاعدة البيانات"""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row  # نتائج كـ dict
    return conn


def _generate_signal_id(symbol: str) -> str:
    """ينشئ معرفاً فريداً للإشارة"""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    sym = symbol.replace("/", "").replace("USDT", "")
    return f"{sym}_{ts}"


# ==========================================
# حفظ إشارة جديدة
# ==========================================
def log_signal(
    opp,              # TradeOpportunity
    debate: dict,
    btc_price: float = 0.0,
    btc_trend: str   = "safe",
    galaxy_score: float = 0.0,
) -> str:
    """
    يحفظ إشارة جديدة في قاعدة البيانات
    يُعيد signal_id للاستخدام لاحقاً
    """
    signal_id = _generate_signal_id(opp.symbol)
    now       = datetime.now(timezone.utc).isoformat()
    rec       = debate.get("recommendation", {})
    log_list  = debate.get("debate_log", [])

    # استخراج الأصوات من الجولة الثالثة
    r3      = {e["speaker"]: e for e in log_list if e["round"] == 3}
    r3_vote = {e["speaker"]: e.get("vote","") for e in log_list if e["round"] == 3}

    tech_model   = debate.get("tech_model",   "—")
    risk_model   = debate.get("risk_model",   "—")
    market_model = debate.get("market_model", "—")

    # نص كل رد
    def get_text(speaker, round_num):
        for e in log_list:
            if e["round"] == round_num and e["speaker"] == speaker:
                return e.get("text", "")
        return ""

    # Reddit
    reddit_entry = next((e for e in log_list if e["round"] == 0), {})
    reddit_sent  = "neutral"
    rt = reddit_entry.get("text", "")
    if "إيجابي" in rt: reddit_sent = "positive"
    elif "سلبي" in rt: reddit_sent = "negative"

    try:
        conn = _get_conn()
        cur  = conn.cursor()

        # ── market_context ──
        cur.execute("""
        INSERT OR IGNORE INTO market_context
        (signal_id, symbol, timestamp_utc, entry_price, volume_24h_usd,
         rsi_daily, crash_pct_60d, signal_type, tp_method,
         tp1, tp2, tp3, stop_loss, fib_high, fib_low, risk_reward,
         btc_price, btc_trend, galaxy_score, reddit_sentiment)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            signal_id, opp.symbol, now,
            opp.entry_price, opp.volume_24h_usd,
            opp.rsi_daily, opp.crash_pct_60d,
            opp.signal_type, opp.tp_method,
            opp.tp1, opp.tp2, opp.tp3, opp.stop_loss,
            opp.fib_high, opp.fib_low, opp.risk_reward_ratio,
            btc_price, btc_trend, galaxy_score, reddit_sent,
        ))

        # ── expert_discussions ──
        cur.execute("""
        INSERT INTO expert_discussions
        (signal_id, timestamp_utc,
         tech_model, risk_model, market_model,
         tech_round1, risk_round1, market_round1,
         tech_round3, risk_round3, market_round3,
         tech_vote, risk_vote, market_vote, reddit_vote,
         final_decision, confidence, full_log)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            signal_id, now,
            tech_model, risk_model, market_model,
            get_text(f"فني/{tech_model}",   1),
            get_text(f"مخاطر/{risk_model}", 1),
            get_text(f"سوق/{market_model}", 1),
            get_text(f"فني/{tech_model}",   3),
            get_text(f"مخاطر/{risk_model}", 3),
            get_text(f"سوق/{market_model}", 3),
            r3_vote.get(f"فني/{tech_model}",   ""),
            r3_vote.get(f"مخاطر/{risk_model}", ""),
            r3_vote.get(f"سوق/{market_model}", ""),
            reddit_entry.get("vote", ""),
            "approved" if rec.get("send_signal") else "rejected",
            rec.get("confidence", ""),
            json.dumps(log_list, ensure_ascii=False),
        ))

        conn.commit()
        conn.close()
        logger.info(f"[DB] إشارة محفوظة: {signal_id}")
        return signal_id

    except Exception as e:
        logger.error(f"[DB] خطأ في حفظ الإشارة: {e}")
        return ""


# ==========================================
# تسجيل قرار المستخدم (تنفيذ أو تجاهل)
# ==========================================
def log_user_decision(
    signal_id:    str,
    decision:     str,    # "executed" أو "ignored"
    opp           = None,
    filled_price: float = 0.0,
    filled_qty:   float = 0.0,
):
    """يسجل قرار المستخدم في trade_outcome"""
    if not signal_id:
        return

    now = datetime.now(timezone.utc).isoformat()

    try:
        conn = _get_conn()
        cur  = conn.cursor()

        from config.settings import TRADE_AMOUNT_USD
        symbol = opp.symbol if opp else ""

        cur.execute("""
        INSERT OR REPLACE INTO trade_outcome
        (signal_id, symbol, user_decision, execution_timestamp,
         filled_price, filled_qty, trade_amount_usd, outcome_status)
        VALUES (?,?,?,?,?,?,?,?)
        """, (
            signal_id, symbol, decision, now,
            filled_price, filled_qty, TRADE_AMOUNT_USD,
            "pending" if decision == "executed" else "cancelled",
        ))

        conn.commit()
        conn.close()
        logger.info(f"[DB] قرار مسجّل: {signal_id} → {decision}")

    except Exception as e:
        logger.error(f"[DB] خطأ في تسجيل القرار: {e}")


# ==========================================
# تحديث نتيجة الصفقة (يعمل في الخلفية)
# ==========================================
def _fetch_current_price(symbol: str, exchange) -> float:
    """يجلب السعر الحالي من MEXC"""
    try:
        ticker = exchange.fetch_ticker(symbol)
        return float(ticker.get("last") or ticker.get("close") or 0)
    except Exception as e:
        logger.warning(f"[DB Tracker] فشل جلب سعر {symbol}: {e}")
        return 0.0


def update_trade_outcome(signal_id: str, exchange):
    """
    يتحقق من نتيجة الصفقة ويحدّث قاعدة البيانات
    يُستدعى من background thread بعد 48 ساعة
    """
    try:
        conn = _get_conn()
        cur  = conn.cursor()

        # جلب بيانات الصفقة والسياق
        cur.execute("""
        SELECT t.symbol, t.filled_price, t.outcome_status,
               m.tp1, m.tp2, m.tp3, m.stop_loss
        FROM trade_outcome t
        JOIN market_context m ON t.signal_id = m.signal_id
        WHERE t.signal_id = ? AND t.user_decision = 'executed'
        """, (signal_id,))

        row = cur.fetchone()
        if not row:
            conn.close()
            return

        symbol       = row["symbol"]
        filled_price = row["filled_price"]
        tp1, tp2, tp3, sl = row["tp1"], row["tp2"], row["tp3"], row["stop_loss"]

        # جلب السعر الحالي
        current_price = _fetch_current_price(symbol, exchange)
        if current_price <= 0:
            conn.close()
            return

        # حساب نسبة التغيير
        pct_change = ((current_price - filled_price) / filled_price) * 100

        # تحديد النتيجة
        now = datetime.now(timezone.utc).isoformat()

        if current_price >= tp3:
            status = "hit_tp3"
        elif current_price >= tp2:
            status = "hit_tp2"
        elif current_price >= tp1:
            status = "hit_tp1"
        elif current_price <= sl:
            status = "hit_stop"
        else:
            status = "pending"

        # تحديث السجل
        cur.execute("""
        UPDATE trade_outcome
        SET outcome_status    = ?,
            max_price_reached = MAX(COALESCE(max_price_reached, 0), ?),
            max_pct_reached   = MAX(COALESCE(max_pct_reached, -999), ?),
            outcome_timestamp = ?
        WHERE signal_id = ?
        """, (status, current_price, pct_change, now, signal_id))

        # حفظ في price_tracking
        cur.execute("""
        INSERT INTO price_tracking
        (signal_id, symbol, check_timestamp, current_price, pct_change,
         tp1_hit, tp2_hit, tp3_hit, sl_hit)
        VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            signal_id, symbol, now, current_price, pct_change,
            1 if current_price >= tp1 else 0,
            1 if current_price >= tp2 else 0,
            1 if current_price >= tp3 else 0,
            1 if current_price <= sl  else 0,
        ))

        conn.commit()
        conn.close()

        logger.info(
            f"[DB Tracker] {symbol} | {signal_id}\n"
            f"  دخول: {filled_price:.6f} | الحالي: {current_price:.6f}\n"
            f"  التغيير: {pct_change:+.1f}% | الحالة: {status}"
        )

    except Exception as e:
        logger.error(f"[DB Tracker] خطأ في تحديث {signal_id}: {e}")


# ==========================================
# Background Tracker — يعمل كل 6 ساعات
# ==========================================
def start_outcome_tracker(exchange):
    """
    يشغّل thread في الخلفية يتحقق من نتائج الصفقات
    كل 6 ساعات لمدة 48 ساعة من كل صفقة
    """
    def _tracker_loop():
        logger.info("[DB Tracker] بدء تتبع الصفقات في الخلفية")
        while True:
            try:
                conn = _get_conn()
                cur  = conn.cursor()

                # جلب الصفقات المنفّذة التي لم تنته بعد
                # وعمرها أقل من 72 ساعة (3 أيام)
                cutoff = (
                    datetime.now(timezone.utc) - timedelta(hours=72)
                ).isoformat()

                cur.execute("""
                SELECT signal_id FROM trade_outcome
                WHERE user_decision = 'executed'
                AND outcome_status IN ('pending', 'hit_tp1', 'hit_tp2')
                AND execution_timestamp > ?
                """, (cutoff,))

                rows = cur.fetchall()
                conn.close()

                if rows:
                    logger.info(f"[DB Tracker] تتبع {len(rows)} صفقة نشطة...")
                    for row in rows:
                        update_trade_outcome(row["signal_id"], exchange)
                        time.sleep(1)  # تجنب Rate Limiting

            except Exception as e:
                logger.error(f"[DB Tracker] خطأ في الـ loop: {e}")

            # كل 6 ساعات
            time.sleep(6 * 3600)

    thread = threading.Thread(target=_tracker_loop, daemon=True)
    thread.start()
    logger.info("[DB Tracker] ✅ Thread نشط")
    return thread


# ==========================================
# إحصائيات الأداء (للتعلم الذاتي)
# ==========================================
def calculate_performance_stats() -> dict:
    """
    يحسب إحصائيات الأداء الكاملة
    يُستخدم لاحقاً لتحسين الـ Prompts تلقائياً
    """
    try:
        conn = _get_conn()
        cur  = conn.cursor()

        # إجمالي الإشارات
        cur.execute("SELECT COUNT(*) as total FROM trade_outcome")
        total = cur.fetchone()["total"]

        if total == 0:
            conn.close()
            return {"total": 0, "message": "لا توجد صفقات بعد"}

        # الصفقات المنفّذة
        cur.execute("""
        SELECT
            COUNT(*) as executed,
            SUM(CASE WHEN outcome_status = 'hit_tp1' THEN 1 ELSE 0 END) as tp1_count,
            SUM(CASE WHEN outcome_status = 'hit_tp2' THEN 1 ELSE 0 END) as tp2_count,
            SUM(CASE WHEN outcome_status = 'hit_tp3' THEN 1 ELSE 0 END) as tp3_count,
            SUM(CASE WHEN outcome_status = 'hit_stop' THEN 1 ELSE 0 END) as sl_count,
            AVG(max_pct_reached) as avg_max_pct,
            MAX(max_pct_reached) as best_trade_pct
        FROM trade_outcome
        WHERE user_decision = 'executed'
        AND outcome_status != 'pending'
        """)
        row = cur.fetchone()

        executed = row["executed"] or 0
        if executed == 0:
            conn.close()
            return {"total": total, "executed": 0}

        # أفضل نوع إشارة
        cur.execute("""
        SELECT m.signal_type, COUNT(*) as count,
               AVG(t.max_pct_reached) as avg_pct
        FROM trade_outcome t
        JOIN market_context m ON t.signal_id = m.signal_id
        WHERE t.user_decision = 'executed'
        AND t.outcome_status NOT IN ('pending', 'hit_stop')
        GROUP BY m.signal_type
        ORDER BY avg_pct DESC LIMIT 1
        """)
        best_signal = cur.fetchone()

        # أفضل طريقة أهداف
        cur.execute("""
        SELECT m.tp_method, COUNT(*) as count,
               AVG(t.max_pct_reached) as avg_pct
        FROM trade_outcome t
        JOIN market_context m ON t.signal_id = m.signal_id
        WHERE t.user_decision = 'executed'
        AND t.outcome_status NOT IN ('pending', 'hit_stop')
        GROUP BY m.tp_method
        ORDER BY avg_pct DESC LIMIT 1
        """)
        best_method = cur.fetchone()

        stats = {
            "total_signals":   total,
            "total_executed":  executed,
            "total_ignored":   total - executed,
            "tp1_hit_rate":    round((row["tp1_count"] or 0) / executed * 100, 1),
            "tp2_hit_rate":    round((row["tp2_count"] or 0) / executed * 100, 1),
            "tp3_hit_rate":    round((row["tp3_count"] or 0) / executed * 100, 1),
            "sl_hit_rate":     round((row["sl_count"]  or 0) / executed * 100, 1),
            "avg_max_pct":     round(row["avg_max_pct"] or 0, 1),
            "best_trade_pct":  round(row["best_trade_pct"] or 0, 1),
            "best_signal_type": best_signal["signal_type"] if best_signal else "—",
            "best_tp_method":   best_method["tp_method"] if best_method else "—",
        }

        # حفظ في performance_stats
        now = datetime.now(timezone.utc).isoformat()
        cur.execute("""
        INSERT INTO performance_stats
        (calculated_at, total_signals, total_executed, total_ignored,
         tp1_hit_rate, tp2_hit_rate, tp3_hit_rate, sl_hit_rate,
         avg_max_pct, best_signal_type, best_tp_method)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now, stats["total_signals"], stats["total_executed"],
            stats["total_ignored"], stats["tp1_hit_rate"],
            stats["tp2_hit_rate"], stats["tp3_hit_rate"],
            stats["sl_hit_rate"], stats["avg_max_pct"],
            stats["best_signal_type"], stats["best_tp_method"],
        ))

        conn.commit()
        conn.close()

        logger.info(f"[DB Stats] {stats}")
        return stats

    except Exception as e:
        logger.error(f"[DB Stats] خطأ: {e}")
        return {}


def get_performance_summary_text() -> str:
    """
    يُعيد ملخص الأداء كنص للـ Telegram
    أو لحقن السياق في Prompts الخبراء
    """
    stats = calculate_performance_stats()
    if not stats or stats.get("total_signals", 0) == 0:
        return "لا توجد بيانات تاريخية بعد."

    return (
        f"📊 سجل الأداء التاريخي:\n"
        f"  إجمالي الإشارات: {stats['total_signals']}\n"
        f"  منفّذة: {stats['total_executed']} | "
        f"مُتجاهلة: {stats['total_ignored']}\n"
        f"  معدل الوصول لـ TP1: {stats['tp1_hit_rate']}%\n"
        f"  معدل الوصول لـ TP2: {stats['tp2_hit_rate']}%\n"
        f"  معدل الوصول لـ TP3: {stats['tp3_hit_rate']}%\n"
        f"  معدل وقف الخسارة: {stats['sl_hit_rate']}%\n"
        f"  متوسط أعلى ربح: {stats['avg_max_pct']}%\n"
        f"  أفضل نوع إشارة: {stats['best_signal_type']}\n"
        f"  أفضل طريقة أهداف: {stats['best_tp_method']}"
    )
