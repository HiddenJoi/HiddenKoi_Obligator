"""
Notification system: tables, generation logic, and CRUD.
"""
from typing import Optional, List
import psycopg2


def _db_config():
    import os
    return {
        'host': os.getenv("DB_HOST", "localhost"),
        'port': int(os.getenv("DB_PORT", 5432)),
        'database': os.getenv("DB_NAME", "TulaHack"),
        'user': os.getenv("DB_USER", "postgres"),
        'password': os.getenv("DB_PASSWORD", "15021502"),
        'options': '-c client_encoding=UTF8',
    }


def init_notification_tables():
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id         SERIAL PRIMARY KEY,
                    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    type       TEXT NOT NULL CHECK (type IN ('coupon', 'risk', 'target', 'system')),
                    title      TEXT NOT NULL,
                    message    TEXT NOT NULL,
                    is_read    BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMP NOT NULL DEFAULT now()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS notification_settings (
                    user_id         INTEGER NOT NULL PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                    coupon_alerts   BOOLEAN NOT NULL DEFAULT TRUE,
                    risk_alerts     BOOLEAN NOT NULL DEFAULT TRUE,
                    target_alerts   BOOLEAN NOT NULL DEFAULT TRUE,
                    target_threshold NUMERIC NOT NULL DEFAULT 1.0
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications(user_id, created_at DESC)")
        conn.commit()


# ── Settings ───────────────────────────────────────────────────────────────────

def db_get_notification_settings(user_id: int) -> dict:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO notification_settings (user_id, coupon_alerts, risk_alerts, target_alerts, target_threshold)
                VALUES (%s, TRUE, TRUE, TRUE, 1.0)
                ON CONFLICT (user_id) DO NOTHING
                """,
                (user_id,),
            )
            conn.commit()
            cur.execute(
                "SELECT coupon_alerts, risk_alerts, target_alerts, target_threshold FROM notification_settings WHERE user_id = %s",
                (user_id,),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            return dict(zip(cols, row)) if row else None


def db_update_notification_settings(
    user_id: int,
    coupon_alerts: Optional[bool] = None,
    risk_alerts: Optional[bool] = None,
    target_alerts: Optional[bool] = None,
    target_threshold: Optional[float] = None,
) -> dict:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            fields = []
            params: List = []
            if coupon_alerts is not None:
                fields.append("coupon_alerts = %s")
                params.append(coupon_alerts)
            if risk_alerts is not None:
                fields.append("risk_alerts = %s")
                params.append(risk_alerts)
            if target_alerts is not None:
                fields.append("target_alerts = %s")
                params.append(target_alerts)
            if target_threshold is not None:
                fields.append("target_threshold = %s")
                params.append(target_threshold)
            if not fields:
                return db_get_notification_settings(user_id)
            params.append(user_id)
            cur.execute(
                f"""
                INSERT INTO notification_settings (user_id, coupon_alerts, risk_alerts, target_alerts, target_threshold)
                VALUES (%s, COALESCE(%s, TRUE), COALESCE(%s, TRUE), COALESCE(%s, TRUE), COALESCE(%s, 1.0))
                ON CONFLICT (user_id) DO UPDATE SET {', '.join(fields)}
                RETURNING coupon_alerts, risk_alerts, target_alerts, target_threshold
                """,
                (user_id,
                 coupon_alerts if coupon_alerts is not None else None,
                 risk_alerts if risk_alerts is not None else None,
                 target_alerts if target_alerts is not None else None,
                 target_threshold if target_threshold is not None else None,
                 user_id),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            conn.commit()
            return dict(zip(cols, row))


# ── Notifications CRUD ────────────────────────────────────────────────────────

def db_list_notifications(
    user_id: int,
    limit: int = 50,
    offset: int = 0,
    unread_only: bool = False,
) -> List[dict]:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            where = "WHERE user_id = %s AND is_read = FALSE" if unread_only else "WHERE user_id = %s"
            cur.execute(
                f"""
                SELECT id, user_id, type, title, message, is_read, created_at
                FROM notifications
                {where}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (user_id, limit, offset) if unread_only else (user_id, limit, offset),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def db_mark_notification_read(user_id: int, notification_id: int) -> Optional[dict]:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE notifications SET is_read = TRUE
                WHERE id = %s AND user_id = %s
                RETURNING id, user_id, type, title, message, is_read, created_at
                """,
                (notification_id, user_id),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            conn.commit()
            return dict(zip(cols, row)) if row else None


def db_mark_all_read(user_id: int) -> int:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE notifications SET is_read = TRUE WHERE user_id = %s AND is_read = FALSE",
                (user_id,),
            )
            affected = cur.rowcount
            conn.commit()
            return affected


def db_delete_notification(user_id: int, notification_id: int) -> bool:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM notifications WHERE id = %s AND user_id = %s",
                (notification_id, user_id),
            )
            affected = cur.rowcount
            conn.commit()
            return affected > 0


# ── Notification Generation ────────────────────────────────────────────────────

def _notification_exists(user_id: int, ntype: str, message_hash: str) -> bool:
    """Check if a similar notification was created in the last 24 hours."""
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM notifications
                WHERE user_id = %s AND type = %s AND message = %s
                  AND created_at > now() - INTERVAL '24 hours'
                LIMIT 1
                """,
                (user_id, ntype, message_hash),
            )
            return cur.fetchone() is not None


def _insert_notification(user_id: int, ntype: str, title: str, message: str) -> None:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO notifications (user_id, type, title, message)
                VALUES (%s, %s, %s, %s)
                """,
                (user_id, ntype, title, message),
            )
        conn.commit()


def generate_notifications(user_id: int) -> dict:
    """
    Scan user's positions, prices and goals; create notifications per rules.
    Returns a summary of what was created.
    """
    settings = db_get_notification_settings(user_id)
    coupon_enabled = settings.get("coupon_alerts", True)
    risk_enabled   = settings.get("risk_alerts", True)
    target_enabled = settings.get("target_alerts", True)
    target_threshold = float(settings.get("target_threshold", 1.0))

    created = {"coupon": 0, "risk": 0, "target": 0, "system": 0}

    # ── COUPON: payment in 3 days ───────────────────────────────────────────
    if coupon_enabled:
        with psycopg2.connect(**_db_config()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        b.secid, b.name, b.coupon_value, b.coupon_period,
                        b.next_coupon_date, b.face_value,
                        pp.quantity
                    FROM portfolio_positions pp
                    JOIN portfolios p ON p.id = pp.portfolio_id
                    JOIN bonds b ON b.secid = pp.secid
                    WHERE p.user_id = %s
                      AND b.next_coupon_date IS NOT NULL
                      AND b.next_coupon_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '3 days'
                    """,
                    (user_id,),
                )
                for row in cur.fetchall():
                    cols = [d[0] for d in cur.description]
                    pos = dict(zip(cols, row))
                    secid = pos["secid"]
                    coupon_date = pos["next_coupon_date"]
                    qty = float(pos["quantity"])
                    face = float(pos["face_value"] or 1000)
                    coupon_val = float(pos["coupon_value"] or 0)
                    amount = coupon_val / 100.0 * face * qty
                    msg = (
                        f"Выплата по {secid} ({pos['name']}) через купон "
                        f"{coupon_date.strftime('%d.%m.%Y')} — начисление ~{amount:,.2f} ₽"
                    )
                    if not _notification_exists(user_id, "coupon", msg):
                        _insert_notification(
                            user_id, "coupon",
                            f"Купонная выплата {secid}",
                            msg,
                        )
                        created["coupon"] += 1

    # ── RISK: became junk or reliability < 30 ───────────────────────────────
    if risk_enabled:
        with psycopg2.connect(**_db_config()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH latest_price AS (
                        SELECT DISTINCT ON (secid)
                               secid, reliability_score, is_junk
                        FROM bond_prices
                        WHERE price_date = (
                            SELECT MAX(price_date) FROM bond_prices
                            WHERE price_date <= CURRENT_DATE
                        )
                        ORDER BY secid, price_date DESC
                    )
                    SELECT
                        pp.secid, b.name,
                        lp.reliability_score, lp.is_junk,
                        pp.quantity
                    FROM portfolio_positions pp
                    JOIN portfolios p ON p.id = pp.portfolio_id
                    JOIN bonds b ON b.secid = pp.secid
                    LEFT JOIN latest_price lp ON lp.secid = pp.secid
                    WHERE p.user_id = %s
                      AND (lp.is_junk = TRUE
                           OR lp.reliability_score < 30
                           OR lp.reliability_score IS NULL)
                    """,
                    (user_id,),
                )
                for row in cur.fetchall():
                    cols = [d[0] for d in cur.description]
                    pos = dict(zip(cols, row))
                    secid = pos["secid"]
                    score = pos["reliability_score"]
                    is_junk = pos["is_junk"]

                    if is_junk:
                        title = f"⚠️ {secid} — облигация стала мусорной"
                        msg = (
                            f"Облигация {secid} ({pos['name']}) в вашем портфеле "
                            f"классифицирована как мусорная. Рекомендуется рассмотреть замену."
                        )
                    else:
                        title = f"⚠️ {secid} — низкая надёжность"
                        msg = (
                            f"Надёжность {secid} ({pos['name']}) опустилась "
                            f"до {score}. Рекомендуется обратить внимание."
                        )
                    if not _notification_exists(user_id, "risk", msg):
                        _insert_notification(user_id, "risk", title, msg)
                        created["risk"] += 1

    # ── TARGET: deviation from goal exceeds threshold ───────────────────────
    if target_enabled:
        with psycopg2.connect(**_db_config()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH latest_price AS (
                        SELECT DISTINCT ON (secid)
                               secid, yield, duration
                        FROM bond_prices
                        WHERE price_date = (
                            SELECT MAX(price_date) FROM bond_prices
                            WHERE price_date <= CURRENT_DATE
                        )
                        ORDER BY secid, price_date DESC
                    ),
                    positions AS (
                        SELECT
                            COALESCE(lp.yield, 0)               AS bond_yield,
                            COALESCE(lp.duration, 0)            AS bond_duration,
                            (
                                COALESCE(lp.close_price, 0) / 100.0
                                * COALESCE(b.face_value, 1000)
                            ) * pp.quantity                     AS current_value
                        FROM portfolio_positions pp
                        JOIN portfolios p ON p.id = pp.portfolio_id
                        JOIN bonds b ON b.secid = pp.secid
                        LEFT JOIN latest_price lp ON lp.secid = pp.secid
                        WHERE p.user_id = %s
                    ),
                    stats AS (
                        SELECT
                            SUM(current_value)                          AS total_value,
                            SUM(bond_yield  * current_value)
                                / NULLIF(SUM(current_value), 0)         AS current_yield,
                            SUM(bond_duration * current_value)
                                / NULLIF(SUM(current_value), 0)        AS current_duration
                        FROM positions
                    ),
                    goal AS (
                        SELECT target_yield, max_duration, target_monthly_income
                        FROM goals WHERE user_id = %s
                        ORDER BY created_at DESC LIMIT 1
                    )
                    SELECT
                        COALESCE(s.total_value, 0)           AS total_value,
                        COALESCE(s.current_yield, 0)         AS current_yield,
                        COALESCE(s.current_duration, 0)      AS current_duration,
                        g.target_yield,
                        g.max_duration,
                        g.target_monthly_income
                    FROM stats s, goal g
                    """,
                    (user_id, user_id),
                )
                row = cur.fetchone()
                if row:
                    cols = [d[0] for d in cur.description]
                    data = dict(zip(cols, row))

                    total_value     = float(data.get("total_value") or 0)
                    current_yield    = float(data.get("current_yield") or 0)
                    current_duration = float(data.get("current_duration") or 0)
                    target_yield     = float(data.get("target_yield") or 0)
                    max_duration    = float(data.get("max_duration") or 9999)
                    target_monthly  = float(data.get("target_monthly_income") or 0)

                    if total_value == 0:
                        pass
                    elif abs(current_yield - target_yield) > target_threshold:
                        title = "Отклонение доходности от цели"
                        msg = (
                            f"Текущая доходность портфеля ({current_yield:.2f}%) "
                            f"отклоняется от целевой ({target_yield:.2f}%) "
                            f"более чем на {target_threshold:.1f}%. "
                            f"Рассмотрите корректировку портфеля."
                        )
                        if not _notification_exists(user_id, "target", msg):
                            _insert_notification(user_id, "target", title, msg)
                            created["target"] += 1

                    elif current_duration > max_duration + target_threshold:
                        title = "Превышена целевая дюрация"
                        msg = (
                            f"Текущая дюрация портфеля ({current_duration:.0f} дней) "
                            f"превышает целевую ({max_duration:.0f} дней). "
                            f"Это увеличивает процентный риск."
                        )
                        if not _notification_exists(user_id, "target", msg):
                            _insert_notification(user_id, "target", title, msg)
                            created["target"] += 1

    return created
