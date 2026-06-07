"""
core/database.py
================
Trading Brain — PostgreSQL على Supabase
ذاكرة دائمة ومستقلة عن Railway
"""

import os
import json
import time
import threading
from datetime import datetime, timezone, timedelta
from utils.logger import logger

# ==========================================
# الاتصال بـ Supabase PostgreSQL
# ==========================================
DATABASE_URL = os.getenv("DATABASE_URL", "")

def _get_conn():
    """يُعيد اتصال بـ PostgreSQL مع دعم IPv4 Pooler"""
    try:
        import psycopg2

        # حاول الاتصال — إذا كان الرابط يحتوي على pooler استخدمه بدون sslmode إجباري
        url = DATABASE_URL
        if "pooler.supabase.com" in url:
            conn = psycopg2.connect(url)
        else:
            conn = psycopg2.connect(url, sslmode="require")

        conn.autocommit = False
        return conn
    except ImportError:
        raise RuntimeError("psycopg2 غير مثبت")
    except Exception as e:
        logger.error(f"[DB] فشل الاتصال بـ Supabase: {e}")
        raise


# ==========================================
# إنشاء الجداول
# ==========================================
def init_db():
    """
    ينشئ الجداول في Supabase
    لا يُوقف البوت عند الفشل — يسجّل تحذيراً فقط
    """
    if not DATABASE_URL:
        logger.warning("[DB] DATABASE_URL غير موجود — قاعدة البيانات معطّلة")
        return

    try:
        conn = _get_conn()
    cur  = conn.cursor()

    # ── market_context ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS market_context (
        id              SERIAL PRIMARY KEY,
        signal_id       TEXT    UNIQUE NOT NULL,
        symbol          TEXT    NOT NULL,
        timestamp_utc   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        entry_price     REAL    NOT NULL,
        volume_24h_usd  REAL,
        rsi_daily       REAL,
        crash_pct_60d   REAL,
        signal_type     TEXT,
        tp_method       TEXT,
        tp1             REAL,
        tp2             REAL,
        tp3             REAL,
        stop_loss       REAL,
        fib_high        REAL,
        fib_low         REAL,
        risk_reward     REAL,
        btc_price       REAL,
        btc_trend       TEXT,
        galaxy_score    REAL,
        reddit_sentiment TEXT
    )
    """)

    # ── expert_discussions ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS expert_discussions (
        id              SERIAL PRIMARY KEY,
        signal_id       TEXT    NOT NULL REFERENCES market_context(signal_id),
        timestamp_utc   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        tech_model      TEXT,
        risk_model      TEXT,
        market_model    TEXT,
        tech_round1     TEXT,
        risk_round1     TEXT,
        market_round1   TEXT,
        tech_round3     TEXT,
        risk_round3     TEXT,
        market_round3   TEXT,
        tech_vote       TEXT,
        risk_vote       TEXT,
        market_vote     TEXT,
        reddit_vote     TEXT,
        final_decision  TEXT,
        confidence      TEXT,
        full_log        JSONB
    )
    """)

    # ── trade_outcome ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trade_outcome (
        id                  SERIAL PRIMARY KEY,
        signal_id           TEXT    NOT NULL REFERENCES market_context(signal_id),
        symbol              TEXT    NOT NULL,
        user_decision       TEXT    NOT NULL,
        execution_timestamp TIMESTAMPTZ DEFAULT NOW(),
        filled_price        REAL,
        filled_qty          REAL,
        trade_amount_usd    REAL,
        outcome_status      TEXT    DEFAULT 'pending',
        max_price_reached   REAL,
        max_pct_reached     REAL,
        outcome_timestamp   TIMESTAMPTZ,
        notes               TEXT
    )
    """)

    # ── price_tracking ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS price_tracking (
        id              SERIAL PRIMARY KEY,
        signal_id       TEXT    NOT NULL,
        symbol          TEXT    NOT NULL,
        check_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        current_price   REAL    NOT NULL,
        pct_change      REAL,
        tp1_hit         BOOLEAN DEFAULT FALSE,
        tp2_hit         BOOLEAN DEFAULT FALSE,
        tp3_hit         BOOLEAN DEFAULT FALSE,
        sl_hit          BOOLEAN DEFAULT FALSE
    )
    """)

    # ── performance_stats ──
    cur.execute("""
    CREATE TABLE IF NOT EXISTS performance_stats (
        id               SERIAL PRIMARY KEY,
        calculated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        total_signals    INTEGER DEFAULT 0,
        total_executed   INTEGER DEFAULT 0,
        total_ignored    INTEGER DEFAULT 0,
        tp1_hit_rate     REAL,
        tp2_hit_rate     REAL,
        tp3_hit_rate     REAL,
        sl_hit_rate      REAL,
        avg_max_pct      REAL,
        best_signal_type TEXT,
        best_tp_method   TEXT,
        notes            TEXT
    )
    """)

        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ Supabase PostgreSQL جاهز — جميع الجداول محدّثة")

    except Exception as e:
        logger.error(
            f"[DB] ⚠️ فشل الاتصال بـ Supabase — البوت سيعمل بدون قاعدة بيانات\n"
            f"السبب: {e}\n"
            f"تلميح: استخدم Session Pooler من Supabase بدل Direct Connection"
        )
        # لا نوقف البوت — نكتفي بالتحذير


def _generate_signal_id(symbol: str) -> str:
    ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    sym = symbol.replace("/", "").replace("USDT", "")
    return f"{sym}_{ts}"


# ==========================================
# حفظ إشارة جديدة
# ==========================================
def log_signal(
    opp,
    debate:       dict,
    btc_price:    float = 0.0,
    btc_trend:    str   = "safe",
    galaxy_score: float = 0.0,
) -> str:
    if not DATABASE_URL:
        return ""

    signal_id = _generate_signal_id(opp.symbol)
    now       = datetime.now(timezone.utc)
    rec       = debate.get("recommendation", {})
    log_list  = debate.get("debate_log", [])

    tech_model   = debate.get("tech_model",   "—")
    risk_model   = debate.get("risk_model",   "—")
    market_model = debate.get("market_model", "—")

    def get_text(speaker, round_num):
        for e in log_list:
            if e["round"] == round_num and e["speaker"] == speaker:
                return e.get("text", "")[:2000]  # حد 2000 حرف
        return ""

    def get_vote(speaker, round_num):
        for e in log_list:
            if e["round"] == round_num and e["speaker"] == speaker:
                return e.get("vote", "")
        return ""

    reddit_entry = next((e for e in log_list if e["round"] == 0), {})
    reddit_sent  = "neutral"
    rt = reddit_entry.get("text", "")
    if "إيجابي" in rt: reddit_sent = "positive"
    elif "سلبي" in rt:  reddit_sent = "negative"

    try:
        conn = _get_conn()
        cur  = conn.cursor()

        # market_context
        cur.execute("""
        INSERT INTO market_context
        (signal_id, symbol, timestamp_utc, entry_price, volume_24h_usd,
         rsi_daily, crash_pct_60d, signal_type, tp_method,
         tp1, tp2, tp3, stop_loss, fib_high, fib_low, risk_reward,
         btc_price, btc_trend, galaxy_score, reddit_sentiment)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (signal_id) DO NOTHING
        """, (
            signal_id, opp.symbol, now,
            opp.entry_price, opp.volume_24h_usd,
            opp.rsi_daily, opp.crash_pct_60d,
            opp.signal_type, opp.tp_method,
            opp.tp1, opp.tp2, opp.tp3, opp.stop_loss,
            opp.fib_high, opp.fib_low, opp.risk_reward_ratio,
            btc_price, btc_trend, galaxy_score, reddit_sent,
        ))

        # expert_discussions
        cur.execute("""
        INSERT INTO expert_discussions
        (signal_id, timestamp_utc,
         tech_model, risk_model, market_model,
         tech_round1, risk_round1, market_round1,
         tech_round3, risk_round3, market_round3,
         tech_vote, risk_vote, market_vote, reddit_vote,
         final_decision, confidence, full_log)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            signal_id, now,
            tech_model, risk_model, market_model,
            get_text(f"فني/{tech_model}",   1),
            get_text(f"مخاطر/{risk_model}", 1),
            get_text(f"سوق/{market_model}", 1),
            get_text(f"فني/{tech_model}",   3),
            get_text(f"مخاطر/{risk_model}", 3),
            get_text(f"سوق/{market_model}", 3),
            get_vote(f"فني/{tech_model}",   3),
            get_vote(f"مخاطر/{risk_model}", 3),
            get_vote(f"سوق/{market_model}", 3),
            reddit_entry.get("vote", ""),
            "approved" if rec.get("send_signal") else "rejected",
            rec.get("confidence", ""),
            json.dumps(log_list, ensure_ascii=False),
        ))

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"[DB] ✅ إشارة محفوظة في Supabase: {signal_id}")
        return signal_id

    except Exception as e:
        logger.error(f"[DB] خطأ في حفظ الإشارة: {e}")
        return ""


# ==========================================
# تسجيل قرار المستخدم
# ==========================================
def log_user_decision(
    signal_id:    str,
    decision:     str,
    opp           = None,
    filled_price: float = 0.0,
    filled_qty:   float = 0.0,
):
    if not DATABASE_URL or not signal_id:
        return

    try:
        from config.settings import TRADE_AMOUNT_USD
        conn = _get_conn()
        cur  = conn.cursor()

        cur.execute("""
        INSERT INTO trade_outcome
        (signal_id, symbol, user_decision, execution_timestamp,
         filled_price, filled_qty, trade_amount_usd, outcome_status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
        """, (
            signal_id,
            opp.symbol if opp else "",
            decision,
            datetime.now(timezone.utc),
            filled_price, filled_qty, TRADE_AMOUNT_USD,
            "pending" if decision == "executed" else "cancelled",
        ))

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"[DB] قرار مسجّل: {signal_id} → {decision}")

    except Exception as e:
        logger.error(f"[DB] خطأ في تسجيل القرار: {e}")


# ==========================================
# تحديث نتيجة الصفقة
# ==========================================
def update_trade_outcome(signal_id: str, exchange):
    if not DATABASE_URL:
        return

    try:
        conn = _get_conn()
        cur  = conn.cursor()

        cur.execute("""
        SELECT t.symbol, t.filled_price,
               m.tp1, m.tp2, m.tp3, m.stop_loss
        FROM trade_outcome t
        JOIN market_context m ON t.signal_id = m.signal_id
        WHERE t.signal_id = %s AND t.user_decision = 'executed'
        """, (signal_id,))

        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return

        symbol, filled_price, tp1, tp2, tp3, sl = row

        # السعر الحالي
        try:
            ticker        = exchange.fetch_ticker(symbol)
            current_price = float(ticker.get("last") or ticker.get("close") or 0)
        except Exception:
            cur.close(); conn.close()
            return

        if current_price <= 0:
            cur.close(); conn.close()
            return

        pct_change = ((current_price - filled_price) / filled_price) * 100
        now        = datetime.now(timezone.utc)

        if   current_price >= tp3: status = "hit_tp3"
        elif current_price >= tp2: status = "hit_tp2"
        elif current_price >= tp1: status = "hit_tp1"
        elif current_price <= sl:  status = "hit_stop"
        else:                      status = "pending"

        cur.execute("""
        UPDATE trade_outcome SET
            outcome_status    = %s,
            max_price_reached = GREATEST(COALESCE(max_price_reached, 0), %s),
            max_pct_reached   = GREATEST(COALESCE(max_pct_reached, -999), %s),
            outcome_timestamp = %s
        WHERE signal_id = %s
        """, (status, current_price, pct_change, now, signal_id))

        cur.execute("""
        INSERT INTO price_tracking
        (signal_id, symbol, check_timestamp, current_price,
         pct_change, tp1_hit, tp2_hit, tp3_hit, sl_hit)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            signal_id, symbol, now, current_price, pct_change,
            current_price >= tp1, current_price >= tp2,
            current_price >= tp3, current_price <= sl,
        ))

        conn.commit()
        cur.close()
        conn.close()

        logger.info(
            f"[DB Tracker] {symbol} | {pct_change:+.1f}% | {status}"
        )

    except Exception as e:
        logger.error(f"[DB Tracker] خطأ في {signal_id}: {e}")


# ==========================================
# Background Tracker
# ==========================================
def start_outcome_tracker(exchange):
    def _loop():
        logger.info("[DB Tracker] ✅ Thread نشط — يتحقق كل 6 ساعات")
        while True:
            try:
                if DATABASE_URL:
                    conn = _get_conn()
                    cur  = conn.cursor()
                    cutoff = datetime.now(timezone.utc) - timedelta(hours=72)
                    cur.execute("""
                    SELECT signal_id FROM trade_outcome
                    WHERE user_decision = 'executed'
                    AND outcome_status IN ('pending','hit_tp1','hit_tp2')
                    AND execution_timestamp > %s
                    """, (cutoff,))
                    rows = cur.fetchall()
                    cur.close(); conn.close()

                    for row in rows:
                        update_trade_outcome(row[0], exchange)
                        time.sleep(1)

            except Exception as e:
                logger.error(f"[DB Tracker] خطأ: {e}")

            time.sleep(6 * 3600)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return t


# ==========================================
# إحصائيات الأداء
# ==========================================
def calculate_performance_stats() -> dict:
    if not DATABASE_URL:
        return {}

    try:
        conn = _get_conn()
        cur  = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM trade_outcome")
        total = cur.fetchone()[0]
        if total == 0:
            cur.close(); conn.close()
            return {"total": 0}

        cur.execute("""
        SELECT
            COUNT(*) as executed,
            SUM(CASE WHEN outcome_status='hit_tp1' THEN 1 ELSE 0 END) as tp1,
            SUM(CASE WHEN outcome_status='hit_tp2' THEN 1 ELSE 0 END) as tp2,
            SUM(CASE WHEN outcome_status='hit_tp3' THEN 1 ELSE 0 END) as tp3,
            SUM(CASE WHEN outcome_status='hit_stop' THEN 1 ELSE 0 END) as sl,
            AVG(max_pct_reached) as avg_pct,
            MAX(max_pct_reached) as best_pct
        FROM trade_outcome
        WHERE user_decision='executed' AND outcome_status != 'pending'
        """)
        r = cur.fetchone()
        executed = r[0] or 0

        best_signal = None
        if executed > 0:
            cur.execute("""
            SELECT m.signal_type, AVG(t.max_pct_reached) as avg_pct
            FROM trade_outcome t JOIN market_context m ON t.signal_id=m.signal_id
            WHERE t.user_decision='executed' AND t.outcome_status NOT IN ('pending','hit_stop')
            GROUP BY m.signal_type ORDER BY avg_pct DESC LIMIT 1
            """)
            best_signal = cur.fetchone()

        cur.close(); conn.close()

        stats = {
            "total_signals":  total,
            "total_executed": executed,
            "tp1_hit_rate":   round((r[1] or 0) / max(executed,1) * 100, 1),
            "tp2_hit_rate":   round((r[2] or 0) / max(executed,1) * 100, 1),
            "tp3_hit_rate":   round((r[3] or 0) / max(executed,1) * 100, 1),
            "sl_hit_rate":    round((r[4] or 0) / max(executed,1) * 100, 1),
            "avg_max_pct":    round(r[5] or 0, 1),
            "best_trade_pct": round(r[6] or 0, 1),
            "best_signal_type": best_signal[0] if best_signal else "—",
        }
        return stats

    except Exception as e:
        logger.error(f"[DB Stats] {e}")
        return {}


def get_performance_summary_text() -> str:
    stats = calculate_performance_stats()
    if not stats or stats.get("total_signals", 0) == 0:
        return "لا توجد بيانات تاريخية بعد."

    return (
        f"إجمالي الإشارات: {stats['total_signals']} | "
        f"منفّذة: {stats['total_executed']}\n"
        f"TP1: {stats['tp1_hit_rate']}% | "
        f"TP2: {stats['tp2_hit_rate']}% | "
        f"TP3: {stats['tp3_hit_rate']}% | "
        f"SL: {stats['sl_hit_rate']}%\n"
        f"متوسط أعلى ربح: {stats['avg_max_pct']}% | "
        f"أفضل إشارة: {stats['best_signal_type']}"
    )
