"""
Portfolio logic: tables, CRUD, position aggregation.
"""
from typing import Optional, List, Literal
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


def init_portfolio_tables():
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS portfolios (
                    id         SERIAL PRIMARY KEY,
                    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    name       TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT now()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_positions (
                    id            SERIAL PRIMARY KEY,
                    portfolio_id  INTEGER NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
                    secid         TEXT    NOT NULL REFERENCES bonds(secid) ON DELETE CASCADE,
                    quantity      NUMERIC NOT NULL,
                    avg_price     NUMERIC NOT NULL,
                    created_at    TIMESTAMP DEFAULT now(),
                    UNIQUE(portfolio_id, secid)
                )
            """)
        conn.commit()


def init_transactions_tables():
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cash_accounts (
                    id         SERIAL PRIMARY KEY,
                    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE UNIQUE,
                    balance    NUMERIC NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT now()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id         SERIAL PRIMARY KEY,
                    user_id    INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    secid      TEXT,
                    type       TEXT NOT NULL CHECK (type IN ('buy', 'sell', 'coupon', 'deposit', 'withdraw')),
                    quantity   NUMERIC,
                    price      NUMERIC,
                    amount     NUMERIC NOT NULL,
                    commission NUMERIC NOT NULL DEFAULT 0,
                    date       TIMESTAMP NOT NULL DEFAULT now(),
                    created_at TIMESTAMP DEFAULT now(),
                    CONSTRAINT fk_secid FOREIGN KEY (secid) REFERENCES bonds(secid) ON DELETE SET NULL
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(user_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(user_id, date DESC)")
        conn.commit()


# ── Portfolios ────────────────────────────────────────────────────────────────

def db_list_portfolios(user_id: int) -> List[dict]:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, user_id, name, created_at FROM portfolios WHERE user_id = %s ORDER BY created_at",
                (user_id,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def db_create_portfolio(user_id: int, name: str) -> dict:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO portfolios (user_id, name) VALUES (%s, %s) RETURNING id, user_id, name, created_at",
                (user_id, name),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            conn.commit()
            return dict(zip(cols, row))


def db_get_portfolio(portfolio_id: int, user_id: int) -> Optional[dict]:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, user_id, name, created_at FROM portfolios WHERE id = %s AND user_id = %s",
                (portfolio_id, user_id),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            return dict(zip(cols, row)) if row else None


# ── Positions ────────────────────────────────────────────────────────────────

def db_add_position(
    portfolio_id: int,
    secid: str,
    quantity: float,
    avg_price: float,
) -> dict:
    """
    Insert or update a position. If it exists — weighted average.
    Returns the resulting row.
    """
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO portfolio_positions (portfolio_id, secid, quantity, avg_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (portfolio_id, secid) DO UPDATE SET
                    quantity  = portfolio_positions.quantity  + EXCLUDED.quantity,
                    avg_price = (
                        portfolio_positions.quantity  * portfolio_positions.avg_price
                        + EXCLUDED.quantity          * EXCLUDED.avg_price
                    ) / (portfolio_positions.quantity + EXCLUDED.quantity)
                RETURNING id, portfolio_id, secid, quantity, avg_price, created_at
                """,
                (portfolio_id, secid, quantity, avg_price),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            conn.commit()
            return dict(zip(cols, row))


def db_get_positions_with_prices(portfolio_id: int) -> List[dict]:
    """
    Join positions with latest bond prices to compute current value.
    """
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH latest_price AS (
                    SELECT DISTINCT ON (secid)
                           secid, close_price, yield, nkd, duration, price_date,
                           reliability_score, is_junk
                    FROM bond_prices
                    WHERE price_date = (
                        SELECT MAX(price_date) FROM bond_prices
                        WHERE price_date <= CURRENT_DATE
                    )
                    ORDER BY secid, price_date DESC
                )
                SELECT
                    pp.id,
                    pp.secid,
                    pp.quantity,
                    pp.avg_price,
                    lp.close_price,
                    lp.nkd,
                    lp.price_date,
                    b.name,
                    b.face_value,
                    b.coupon_type,
                    b.coupon_value,
                    b.coupon_period,
                    b.maturity_date,
                    lp.reliability_score,
                    lp.is_junk,
                    (
                        COALESCE(lp.close_price, 0) / 100.0 * COALESCE(b.face_value, 1000)
                        + COALESCE(lp.nkd, 0)
                    ) * pp.quantity AS current_value
                FROM portfolio_positions pp
                JOIN bonds b            ON b.secid  = pp.secid
                LEFT JOIN latest_price lp ON lp.secid = pp.secid
                WHERE pp.portfolio_id = %s
                ORDER BY pp.id
                """,
                (portfolio_id,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Goals ─────────────────────────────────────────────────────────────────────

def init_goals_table():
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS goals (
                    id                    SERIAL PRIMARY KEY,
                    user_id               INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    target_yield          NUMERIC NOT NULL,
                    max_duration          INTEGER NOT NULL,
                    target_monthly_income NUMERIC NOT NULL,
                    created_at            TIMESTAMP DEFAULT now()
                )
            """)
        conn.commit()


def db_create_goal(
    user_id: int,
    target_yield: float,
    max_duration: int,
    target_monthly_income: float,
) -> dict:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO goals (user_id, target_yield, max_duration, target_monthly_income)
                VALUES (%s, %s, %s, %s)
                RETURNING id, user_id, target_yield, max_duration, target_monthly_income, created_at
                """,
                (user_id, target_yield, max_duration, target_monthly_income),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            conn.commit()
            return dict(zip(cols, row))


def db_get_goal(user_id: int) -> Optional[dict]:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, user_id, target_yield, max_duration, target_monthly_income, created_at
                FROM goals
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (user_id,),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            return dict(zip(cols, row)) if row else None


# ── Dashboard ────────────────────────────────────────────────────────────────

def db_get_dashboard(user_id: int) -> dict:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH latest_price AS (
                    SELECT DISTINCT ON (secid)
                           secid, close_price, yield, nkd, duration
                    FROM bond_prices
                    WHERE price_date = (
                        SELECT MAX(price_date) FROM bond_prices
                        WHERE price_date <= CURRENT_DATE
                    )
                    ORDER BY secid, price_date DESC
                ),
                positions AS (
                    SELECT
                        pp.secid,
                        pp.quantity,
                        pp.avg_price,
                        COALESCE(lp.close_price, 0)                  AS current_price,
                        COALESCE(lp.yield, 0)                        AS bond_yield,
                        COALESCE(lp.duration, 0)                     AS bond_duration,
                        b.face_value,
                        b.coupon_type,
                        b.name,
                        (
                            COALESCE(lp.close_price, 0) / 100.0
                            * COALESCE(b.face_value, 1000)
                            + COALESCE(lp.nkd, 0)
                        ) * pp.quantity                               AS current_value,
                        pp.avg_price / 100.0
                            * COALESCE(b.face_value, 1000)
                            * pp.quantity                             AS invested_value,
                        (
                            COALESCE(lp.close_price, 0) / 100.0
                            * COALESCE(b.face_value, 1000)
                            + COALESCE(lp.nkd, 0)
                        ) * pp.quantity
                        - pp.avg_price / 100.0
                            * COALESCE(b.face_value, 1000)
                            * pp.quantity                             AS pnl
                    FROM portfolio_positions pp
                    JOIN portfolios p ON p.id = pp.portfolio_id
                    JOIN bonds b     ON b.secid = pp.secid
                    LEFT JOIN latest_price lp ON lp.secid = pp.secid
                    WHERE p.user_id = %s
                ),
                totals AS (
                    SELECT
                        SUM(current_value)                        AS total_value,
                        SUM(invested_value)                       AS total_invested,
                        SUM(pnl)                                 AS total_pnl,
                        SUM(bond_yield  * current_value)
                            / NULLIF(SUM(current_value), 0)     AS weighted_ytm,
                        SUM(bond_duration * current_value)
                            / NULLIF(SUM(current_value), 0)     AS weighted_duration
                    FROM positions
                ),
                by_type AS (
                    SELECT
                        COALESCE(coupon_type, 'unknown') AS coupon_type,
                        SUM(current_value)              AS value
                    FROM positions
                    GROUP BY coupon_type
                ),
                goal AS (
                    SELECT target_yield, max_duration, target_monthly_income
                    FROM goals
                    WHERE user_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                ),
                monthly_cashflow AS (
                    SELECT
                        COALESCE(
                            SUM(
                                b.coupon_value * pp.quantity
                                / NULLIF(b.coupon_period, 0)
                                * CASE
                                    WHEN b.coupon_period = 30   THEN 12.0
                                    WHEN b.coupon_period = 91   THEN 4.0
                                    WHEN b.coupon_period = 182  THEN 2.0
                                    WHEN b.coupon_period = 364  THEN 1.0
                                    ELSE 365.0 / NULLIF(b.coupon_period, 0)
                                  END
                            ), 0
                        ) AS monthly_income
                    FROM portfolio_positions pp
                    JOIN portfolios p  ON p.id = pp.portfolio_id
                    JOIN bonds b       ON b.secid = pp.secid
                    WHERE p.user_id = %s
                      AND b.coupon_type NOT IN ('FLOAT', 'VARIABLE', 'ZERO')
                      AND b.coupon_value > 0
                      AND b.coupon_period > 0
                )
                SELECT
                    t.total_value,
                    t.total_invested,
                    t.total_pnl,
                    CASE WHEN t.total_invested > 0
                         THEN (t.total_pnl / t.total_invested) * 100
                         ELSE 0 END                           AS total_pnl_pct,
                    t.weighted_ytm,
                    t.weighted_duration,
                    (SELECT json_agg(json_build_object(
                        'coupon_type', coupon_type,
                        'value',       value,
                        'pct',         CASE WHEN t.total_value > 0
                                          THEN (value / t.total_value) * 100
                                          ELSE 0 END
                    )) FROM by_type)                            AS allocation,
                    (SELECT json_agg(json_build_object(
                        'secid',          secid,
                        'name',           name,
                        'quantity',       quantity,
                        'avg_price',      avg_price,
                        'current_price',  current_price,
                        'pnl',            pnl,
                        'yield_',         bond_yield,
                        'duration',       bond_duration
                    ) ORDER BY current_value DESC)
                     FROM positions)                             AS positions,
                    g.target_yield,
                    g.max_duration,
                    g.target_monthly_income,
                    mc.monthly_income                            AS current_monthly_income
                FROM totals t
                LEFT JOIN goal g ON TRUE
                LEFT JOIN monthly_cashflow mc ON TRUE
                """,
                (user_id, user_id, user_id),
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                cols = [d[0] for d in cur.description]
                result = dict(zip(cols, row))
                for key in ('total_value', 'total_invested', 'total_pnl',
                            'total_pnl_pct', 'weighted_ytm', 'weighted_duration',
                            'target_yield', 'max_duration', 'target_monthly_income',
                            'current_monthly_income'):
                    if result.get(key) is not None:
                        result[key] = float(result[key])
                return result
            return {
                'total_value': 0, 'total_invested': 0, 'total_pnl': 0,
                'total_pnl_pct': 0, 'weighted_ytm': 0, 'weighted_duration': 0,
                'allocation': [], 'positions': [],
                'target_yield': None, 'max_duration': None,
                'target_monthly_income': None, 'current_monthly_income': None,
            }


# ── Cashflow ────────────────────────────────────────────────────────────────

def db_get_positions_for_cashflow(user_id: int) -> List[dict]:
    """Fetch positions with bond details needed for cashflow calculation."""
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    pp.id,
                    pp.secid,
                    pp.quantity,
                    b.coupon_value,
                    b.coupon_period,
                    b.coupon_type,
                    b.maturity_date,
                    b.face_value
                FROM portfolio_positions pp
                JOIN portfolios p ON p.id = pp.portfolio_id
                JOIN bonds b     ON b.secid = pp.secid
                WHERE p.user_id = %s
                  AND b.coupon_type NOT IN ('FLOAT', 'VARIABLE', 'ZERO')
                  AND b.coupon_value > 0
                  AND b.coupon_period > 0
                  AND b.maturity_date > CURRENT_DATE
                ORDER BY pp.id
                """,
                (user_id,),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Portfolio Adjustment ───────────────────────────────────────────────────────

def db_get_portfolio_adjustment(user_id: int) -> List[dict]:
    """
    Analyze portfolio gaps vs goal and return buy/sell recommendations.
    Returns a list of {action, secid, name, reason, impact} dicts.
    """
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH latest_price AS (
                    SELECT DISTINCT ON (secid)
                           secid, close_price, yield, duration, price_date,
                           reliability_score, is_junk
                    FROM bond_prices
                    WHERE price_date = (
                        SELECT MAX(price_date) FROM bond_prices
                        WHERE price_date <= CURRENT_DATE
                    )
                    ORDER BY secid, price_date DESC
                ),
                goal AS (
                    SELECT target_yield, max_duration, target_monthly_income
                    FROM goals WHERE user_id = %s
                    ORDER BY created_at DESC LIMIT 1
                ),
                positions AS (
                    SELECT
                        pp.secid,
                        pp.quantity,
                        COALESCE(lp.close_price, 0)      AS current_price,
                        COALESCE(lp.yield, 0)            AS bond_yield,
                        COALESCE(lp.duration, 0)         AS bond_duration,
                        b.face_value,
                        b.coupon_value,
                        b.coupon_period,
                        b.coupon_type,
                        b.maturity_date,
                        (
                            COALESCE(lp.close_price, 0) / 100.0
                            * COALESCE(b.face_value, 1000)
                        ) * pp.quantity                  AS current_value
                    FROM portfolio_positions pp
                    JOIN portfolios p ON p.id = pp.portfolio_id
                    JOIN bonds b ON b.secid = pp.secid
                    LEFT JOIN latest_price lp ON lp.secid = pp.secid
                    WHERE p.user_id = %s
                ),
                portfolio_stats AS (
                    SELECT
                        SUM(current_value)                          AS total_value,
                        SUM(bond_yield  * current_value)
                            / NULLIF(SUM(current_value), 0)         AS weighted_ytm,
                        SUM(bond_duration * current_value)
                            / NULLIF(SUM(current_value), 0)        AS weighted_duration,
                        SUM(
                            b.coupon_value * pp.quantity
                            / NULLIF(b.coupon_period, 0)
                            * CASE
                                WHEN b.coupon_period = 30   THEN 12.0
                                WHEN b.coupon_period = 91   THEN 4.0
                                WHEN b.coupon_period = 182  THEN 2.0
                                WHEN b.coupon_period = 364  THEN 1.0
                                ELSE 365.0 / NULLIF(b.coupon_period, 0)
                              END
                        )                                           AS current_monthly_income,
                        SUM(CASE WHEN bond_duration <= 365  THEN current_value ELSE 0 END) AS short_value,
                        SUM(CASE WHEN bond_duration > 365   THEN current_value ELSE 0 END) AS long_value
                    FROM positions pp
                    JOIN bonds b ON b.secid = pp.secid
                ),
                gaps AS (
                    SELECT
                        COALESCE(ps.total_value, 0)           AS total_value,
                        COALESCE(ps.weighted_ytm, 0)          AS current_yield,
                        COALESCE(g.target_yield, 0)          AS target_yield,
                        COALESCE(g.target_monthly_income, 0)  AS target_monthly_income,
                        COALESCE(ps.current_monthly_income, 0) AS current_monthly_income,
                        COALESCE(ps.weighted_duration, 0)     AS current_duration,
                        COALESCE(g.max_duration, 9999)        AS max_duration,
                        COALESCE(ps.short_value, 0)           AS short_value,
                        COALESCE(ps.long_value, 0)            AS long_value
                    FROM portfolio_stats ps, goal g
                )
                SELECT
                    g.total_value,
                    g.current_yield,
                    g.target_yield,
                    g.target_monthly_income,
                    g.current_monthly_income,
                    g.current_duration,
                    g.max_duration,
                    g.short_value,
                    g.long_value,
                    (SELECT json_agg(json_build_object(
                        'secid',    pp.secid,
                        'name',     b.name,
                        'quantity', pp.quantity,
                        'yield',    COALESCE(lp.yield, 0),
                        'duration', COALESCE(lp.duration, 0)
                    )) FROM positions pp
                       JOIN bonds b ON b.secid = pp.secid
                       LEFT JOIN latest_price lp ON lp.secid = pp.secid) AS positions_json
                FROM gaps g
                """,
                (user_id, user_id),
            )
            row = cur.fetchone()
            if not row:
                return []
            cols = [d[0] for d in cur.description]
            data = dict(zip(cols, row))

            total_value     = float(data.get("total_value") or 0)
            current_yield    = float(data.get("current_yield") or 0)
            target_yield     = float(data.get("target_yield") or 0)
            target_monthly   = float(data.get("target_monthly_income") or 0)
            current_monthly  = float(data.get("current_monthly_income") or 0)
            current_duration = float(data.get("current_duration") or 0)
            max_duration     = float(data.get("max_duration") or 9999)
            short_value      = float(data.get("short_value") or 0)
            long_value       = float(data.get("long_value") or 0)
            positions_json   = data.get("positions_json") or []

            yield_gap    = target_yield - current_yield
            cashflow_gap = target_monthly - current_monthly

            existing_secids = {p["secid"] for p in positions_json}
            portfolio_duration = current_duration

            # Score expression (reuse existing logic)
            score_expr = """
                GREATEST(0, 1 - ABS(lp.yield - %s) / %s) * 0.6
                + GREATEST(0, 1 - lp.duration / %s) * 0.4
            """

            # 1) Yield gap — need higher-yielding bonds
            if yield_gap > 0.5:
                cur.execute(
                    f"""
                    WITH latest_price AS (
                        SELECT DISTINCT ON (secid)
                               secid, yield, duration, close_price, nkd, price_date,
                               reliability_score, is_junk
                        FROM bond_prices
                        WHERE price_date = (
                            SELECT MAX(price_date) FROM bond_prices
                            WHERE price_date <= CURRENT_DATE
                        )
                        ORDER BY secid, price_date DESC
                    )
                    SELECT
                        b.secid, b.name, b.coupon_type, b.coupon_value, b.coupon_period,
                        lp.yield, lp.duration, lp.close_price, lp.reliability_score,
                        lp.is_junk,
                        (
                            GREATEST(0, 1 - ABS(lp.yield - %s) / %s) * 0.6
                            + GREATEST(0, 1 - lp.duration / %s) * 0.4
                        ) AS score
                    FROM bonds b
                    JOIN latest_price lp ON lp.secid = b.secid
                    WHERE lp.yield IS NOT NULL
                      AND lp.yield > %s
                      AND lp.duration <= %s
                      AND (lp.is_junk = FALSE OR lp.is_junk IS NULL)
                      AND lp.reliability_score >= 30
                      AND b.secid NOT IN ({','.join(['%s'] * len(existing_secids)) if existing_secids else "''"})
                    ORDER BY score DESC, lp.yield DESC
                    LIMIT 3
                    """,
                    (target_yield, target_yield * 1.5, max_duration,
                     yield_gap, max_duration) + tuple(existing_secids) if existing_secids else ()
                )
            else:
                cur.execute("SELECT 1 WHERE FALSE")

            yield_recs = []
            for r in cur.fetchall():
                cols2 = [d[0] for d in cur.description]
                rec = dict(zip(cols2, r))
                yield_recs.append(rec)

            # 2) Cashflow gap — need higher-coupon, frequent bonds
            if cashflow_gap > 100:
                cur.execute(
                    f"""
                    WITH latest_price AS (
                        SELECT DISTINCT ON (secid)
                               secid, yield, duration, close_price, nkd, price_date,
                               reliability_score, is_junk
                        FROM bond_prices
                        WHERE price_date = (
                            SELECT MAX(price_date) FROM bond_prices
                            WHERE price_date <= CURRENT_DATE
                        )
                        ORDER BY secid, price_date DESC
                    )
                    SELECT
                        b.secid, b.name, b.coupon_type, b.coupon_value, b.coupon_period,
                        lp.yield, lp.duration, lp.close_price, lp.reliability_score,
                        lp.is_junk,
                        (b.coupon_value * 1.0 / NULLIF(b.coupon_period, 0)
                         * CASE WHEN b.coupon_period = 30 THEN 12.0
                                WHEN b.coupon_period = 91  THEN 4.0
                                WHEN b.coupon_period = 182 THEN 2.0
                                ELSE 1.0 END) AS monthly_coupon_per_lot
                    FROM bonds b
                    JOIN latest_price lp ON lp.secid = b.secid
                    WHERE lp.yield IS NOT NULL
                      AND b.coupon_type NOT IN ('FLOAT', 'VARIABLE', 'ZERO')
                      AND b.coupon_value > 0
                      AND b.coupon_period > 0
                      AND lp.duration <= %s
                      AND (lp.is_junk = FALSE OR lp.is_junk IS NULL)
                      AND lp.reliability_score >= 30
                      AND b.secid NOT IN ({','.join(['%s'] * len(existing_secids)) if existing_secids else "''"})
                    ORDER BY monthly_coupon_per_lot DESC
                    LIMIT 3
                    """,
                    (max_duration,) + tuple(existing_secids) if existing_secids else ()
                )
            else:
                cur.execute("SELECT 1 WHERE FALSE")

            cf_recs = []
            for r in cur.fetchall():
                cols2 = [d[0] for d in cur.description]
                rec = dict(zip(cols2, r))
                cf_recs.append(rec)

            # 3) Duration imbalance — need short (duration <= 365)
            short_pct = short_value / total_value if total_value > 0 else 0
            if short_pct < 0.15:  # less than 15% short — need short bonds
                cur.execute(
                    f"""
                    WITH latest_price AS (
                        SELECT DISTINCT ON (secid)
                               secid, yield, duration, close_price, nkd, price_date,
                               reliability_score, is_junk
                        FROM bond_prices
                        WHERE price_date = (
                            SELECT MAX(price_date) FROM bond_prices
                            WHERE price_date <= CURRENT_DATE
                        )
                        ORDER BY secid, price_date DESC
                    )
                    SELECT
                        b.secid, b.name, b.coupon_type, b.coupon_value, b.coupon_period,
                        lp.yield, lp.duration, lp.close_price, lp.reliability_score,
                        lp.is_junk
                    FROM bonds b
                    JOIN latest_price lp ON lp.secid = b.secid
                    WHERE lp.yield IS NOT NULL
                      AND lp.duration <= 365
                      AND lp.duration > 0
                      AND lp.duration <= %s
                      AND (lp.is_junk = FALSE OR lp.is_junk IS NULL)
                      AND lp.reliability_score >= 30
                      AND b.secid NOT IN ({','.join(['%s'] * len(existing_secids)) if existing_secids else "''"})
                    ORDER BY lp.yield DESC
                    LIMIT 2
                    """,
                    (max_duration,) + tuple(existing_secids) if existing_secids else ()
                )
            else:
                cur.execute("SELECT 1 WHERE FALSE")

            short_recs = []
            for r in cur.fetchall():
                cols2 = [d[0] for d in cur.description]
                rec = dict(zip(cols2, r))
                short_recs.append(rec)

            # 4) Duration imbalance — need long bonds if short > 60%
            if short_pct > 0.6:  # more than 60% short — need long bonds
                cur.execute(
                    f"""
                    WITH latest_price AS (
                        SELECT DISTINCT ON (secid)
                               secid, yield, duration, close_price, nkd, price_date,
                               reliability_score, is_junk
                        FROM bond_prices
                        WHERE price_date = (
                            SELECT MAX(price_date) FROM bond_prices
                            WHERE price_date <= CURRENT_DATE
                        )
                        ORDER BY secid, price_date DESC
                    )
                    SELECT
                        b.secid, b.name, b.coupon_type, b.coupon_value, b.coupon_period,
                        lp.yield, lp.duration, lp.close_price, lp.reliability_score,
                        lp.is_junk
                    FROM bonds b
                    JOIN latest_price lp ON lp.secid = b.secid
                    WHERE lp.yield IS NOT NULL
                      AND lp.duration > 730
                      AND lp.duration <= %s
                      AND (lp.is_junk = FALSE OR lp.is_junk IS NULL)
                      AND lp.reliability_score >= 30
                      AND b.secid NOT IN ({','.join(['%s'] * len(existing_secids)) if existing_secids else "''"})
                    ORDER BY lp.yield DESC
                    LIMIT 2
                    """,
                    (max_duration,) + tuple(existing_secids) if existing_secids else ()
                )
            else:
                cur.execute("SELECT 1 WHERE FALSE")

            long_recs = []
            for r in cur.fetchall():
                cols2 = [d[0] for d in cur.description]
                rec = dict(zip(cols2, r))
                long_recs.append(rec)

            results = []

            seen = set()
            for rec_list, reason, key_func in [
                (yield_recs,  "увеличивает доходность портфеля",          lambda r: r["yield"]),
                (cf_recs,    "увеличивает ежемесячный cashflow",         lambda r: r.get("monthly_coupon_per_lot", r["yield"])),
                (short_recs, "добавляет короткие бумаги в портфель",      lambda r: r["duration"]),
                (long_recs,  "добавляет длинные бумаги в портфель",       lambda r: -r["duration"]),
            ]:
                for rec in rec_list:
                    secid = rec["secid"]
                    if secid in seen:
                        continue
                    seen.add(secid)
                    results.append({
                        "action": "buy",
                        "secid":  secid,
                        "name":   rec.get("name"),
                        "reason": reason,
                        "impact": {
                            "yield":    round(float(rec["yield"] or 0) - current_yield, 2),
                            "duration": round(float(rec["duration"] or 0) - portfolio_duration, 2),
                        },
                        "score": round(float(rec.get("score") or 0), 6),
                    })

            return results


# ── Cash Account ───────────────────────────────────────────────────────────────

def db_get_cash_account(user_id: int) -> Optional[dict]:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cash_accounts (user_id, balance)
                VALUES (%s, 0)
                ON CONFLICT (user_id) DO UPDATE SET user_id = EXCLUDED.user_id
                RETURNING id, user_id, balance, updated_at
                """,
                (user_id,),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            conn.commit()
            return dict(zip(cols, row))


# ── Transactions ───────────────────────────────────────────────────────────────

class TransactionError(Exception):
    pass


def db_create_transaction(
    user_id: int,
    tx_type: str,
    amount: float,
    secid: Optional[str] = None,
    quantity: Optional[float] = None,
    price: Optional[float] = None,
    commission: float = 0,
    tx_date: Optional[str] = None,
) -> dict:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cash_accounts (user_id, balance) VALUES (%s, 0) "
                "ON CONFLICT (user_id) DO NOTHING",
                (user_id,),
            )

            if tx_type == "buy":
                total_cost = amount + commission
                cur.execute(
                    "SELECT balance FROM cash_accounts WHERE user_id = %s FOR UPDATE",
                    (user_id,),
                )
                row = cur.fetchone()
                if not row or float(row[0]) < total_cost:
                    raise TransactionError(
                        f"Недостаточно средств: нужно {total_cost:.2f}, доступно {row[0] if row else 0:.2f}"
                    )
                cur.execute(
                    "UPDATE cash_accounts SET balance = balance - %s, updated_at = now() WHERE user_id = %s",
                    (total_cost, user_id),
                )
                cur.execute(
                    """
                    INSERT INTO portfolio_positions (portfolio_id, secid, quantity, avg_price)
                    SELECT p.id, %s, %s, %s
                    FROM portfolios p WHERE p.user_id = %s LIMIT 1
                    ON CONFLICT (portfolio_id, secid) DO UPDATE SET
                        quantity  = portfolio_positions.quantity  + EXCLUDED.quantity,
                        avg_price = (
                            portfolio_positions.quantity  * portfolio_positions.avg_price
                            + EXCLUDED.quantity         * EXCLUDED.avg_price
                        ) / (portfolio_positions.quantity + EXCLUDED.quantity)
                    """,
                    (secid, quantity, price, user_id),
                )

            elif tx_type == "sell":
                cur.execute(
                    """
                    SELECT pp.quantity, p.id
                    FROM portfolio_positions pp
                    JOIN portfolios p ON p.id = pp.portfolio_id
                    WHERE p.user_id = %s AND pp.secid = %s
                    FOR UPDATE OF pp
                    """,
                    (user_id, secid),
                )
                row = cur.fetchone()
                if not row or float(row[0]) < float(quantity or 0):
                    raise TransactionError(
                        f"Недостаточно бумаг: нужно {quantity}, в наличии {row[0] if row else 0}"
                    )
                net_proceeds = amount - commission
                cur.execute(
                    "UPDATE cash_accounts SET balance = balance + %s, updated_at = now() WHERE user_id = %s",
                    (net_proceeds, user_id),
                )
                cur.execute(
                    "UPDATE portfolio_positions SET quantity = quantity - %s WHERE portfolio_id = %s AND secid = %s",
                    (quantity, row[1], secid),
                )
                cur.execute(
                    "DELETE FROM portfolio_positions WHERE portfolio_id = %s AND secid = %s AND quantity <= 0",
                    (row[1], secid),
                )

            elif tx_type in ("coupon", "deposit"):
                cur.execute(
                    "UPDATE cash_accounts SET balance = balance + %s, updated_at = now() WHERE user_id = %s",
                    (amount, user_id),
                )

            elif tx_type == "withdraw":
                cur.execute(
                    "SELECT balance FROM cash_accounts WHERE user_id = %s FOR UPDATE",
                    (user_id,),
                )
                row = cur.fetchone()
                if not row or float(row[0]) < amount:
                    raise TransactionError(
                        f"Недостаточно средств для вывода: нужно {amount:.2f}, доступно {row[0] if row else 0:.2f}"
                    )
                cur.execute(
                    "UPDATE cash_accounts SET balance = balance - %s, updated_at = now() WHERE user_id = %s",
                    (amount, user_id),
                )

            cur.execute(
                """
                INSERT INTO transactions (user_id, secid, type, quantity, price, amount, commission, date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, COALESCE(%s::timestamp, now()))
                RETURNING id, user_id, secid, type, quantity, price, amount, commission, date, created_at
                """,
                (user_id, secid, tx_type, quantity, price, amount, commission, tx_date),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            conn.commit()
            return dict(zip(cols, row))


def db_list_transactions(
    user_id: int,
    limit: int = 100,
    offset: int = 0,
    tx_type: Optional[str] = None,
    secid: Optional[str] = None,
) -> List[dict]:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            conditions = ["user_id = %s"]
            params: List = [user_id]
            if tx_type:
                conditions.append("type = %s")
                params.append(tx_type)
            if secid:
                conditions.append("secid = %s")
                params.append(secid)
            where = " AND ".join(conditions)
            cur.execute(
                f"""
                SELECT id, user_id, secid, type, quantity, price, amount, commission, date, created_at
                FROM transactions
                WHERE {where}
                ORDER BY date DESC, created_at DESC
                LIMIT %s OFFSET %s
                """,
                params + [limit, offset],
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Portfolio Snapshots (time-series) ─────────────────────────────────────────

def init_snapshots_table():
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                    id              SERIAL PRIMARY KEY,
                    user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    date            DATE    NOT NULL,
                    total_value     NUMERIC NOT NULL DEFAULT 0,
                    cash            NUMERIC NOT NULL DEFAULT 0,
                    invested_value  NUMERIC NOT NULL DEFAULT 0,
                    pnl             NUMERIC NOT NULL DEFAULT 0,
                    created_at      TIMESTAMP DEFAULT now(),
                    UNIQUE(user_id, date)
                )
            """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_snapshots_user_date "
                "ON portfolio_snapshots(user_id, date DESC)"
            )
        conn.commit()


def compute_portfolio_value(user_id: int, as_of_date) -> dict:
    """
    Compute portfolio value for user_id on a specific date.
    Uses bond_prices for that date (or nearest prior if not available),
    plus cash_account balance.
    """
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH prices_on_date AS (
                    SELECT DISTINCT ON (secid)
                           secid, close_price, nkd
                    FROM bond_prices
                    WHERE price_date = %s
                       OR price_date = (
                          SELECT MAX(price_date) FROM bond_prices
                          WHERE price_date <= %s
                       )
                    ORDER BY secid, price_date DESC
                ),
                positions AS (
                    SELECT
                        pp.secid,
                        pp.quantity,
                        pp.avg_price,
                        COALESCE(pd.close_price, 0)  AS current_price,
                        COALESCE(pd.nkd, 0)           AS nkd,
                        b.face_value
                    FROM portfolio_positions pp
                    JOIN portfolios p  ON p.id = pp.portfolio_id
                    JOIN bonds b       ON b.secid = pp.secid
                    LEFT JOIN prices_on_date pd ON pd.secid = pp.secid
                    WHERE p.user_id = %s
                ),
                cash AS (
                    SELECT COALESCE(balance, 0) AS balance
                    FROM cash_accounts
                    WHERE user_id = %s
                )
                SELECT
                    COALESCE(SUM(
                        (COALESCE(close_price, 0) / 100.0 * COALESCE(face_value, 1000)
                         + COALESCE(nkd, 0)) * quantity
                    ), 0)                                          AS total_value,
                    COALESCE(SUM(
                        avg_price / 100.0 * COALESCE(face_value, 1000) * quantity
                    ), 0)                                          AS invested_value,
                    COALESCE(SUM(
                        ((COALESCE(close_price, 0) / 100.0 * COALESCE(face_value, 1000)
                          + COALESCE(nkd, 0)) - avg_price / 100.0 * COALESCE(face_value, 1000))
                        * quantity
                    ), 0)                                          AS pnl,
                    COALESCE(c.balance, 0)                         AS cash
                FROM positions
                LEFT JOIN cash c ON TRUE
                """,
                (as_of_date, as_of_date, user_id, user_id),
            )
            row = cur.fetchone()
            if row:
                return {
                    'total_value':    float(row[0]) if row[0] is not None else 0.0,
                    'invested_value': float(row[1]) if row[1] is not None else 0.0,
                    'pnl':            float(row[2]) if row[2] is not None else 0.0,
                    'cash':           float(row[3]) if row[3] is not None else 0.0,
                }
            return {'total_value': 0.0, 'invested_value': 0.0, 'pnl': 0.0, 'cash': 0.0}


def db_save_snapshot(user_id: int, as_of_date) -> dict:
    """
    Compute and upsert a portfolio snapshot for user_id on as_of_date.
    Returns the saved row.
    """
    vals = compute_portfolio_value(user_id, as_of_date)
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO portfolio_snapshots (user_id, date, total_value, cash, invested_value, pnl)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, date) DO UPDATE SET
                    total_value    = EXCLUDED.total_value,
                    cash           = EXCLUDED.cash,
                    invested_value = EXCLUDED.invested_value,
                    pnl            = EXCLUDED.pnl,
                    created_at     = now()
                RETURNING id, user_id, date, total_value, cash, invested_value, pnl, created_at
                """,
                (user_id, as_of_date, vals['total_value'], vals['cash'],
                 vals['invested_value'], vals['pnl']),
            )
            cols = [d[0] for d in cur.description]
            row = cur.fetchone()
            conn.commit()
            return dict(zip(cols, row))


def db_get_snapshots(
    user_id: int,
    period: str = "daily",
    limit: int = 365,
) -> List[dict]:
    """
    Return snapshots for user_id, optionally aggregated.
    period: 'daily' | 'weekly' | 'monthly'
    Uses DB-level aggregation, no on-the-fly recompute.
    """
    if period == "daily":
        trunc = "day"
    elif period == "weekly":
        trunc = "week"
    elif period == "monthly":
        trunc = "month"
    else:
        trunc = "day"

    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH truncated AS (
                    SELECT
                        user_id,
                        date_trunc('{trunc}', date)::date AS period_date,
                        SUM(total_value)    AS total_value,
                        SUM(cash)           AS cash,
                        SUM(invested_value) AS invested_value,
                        SUM(pnl)            AS pnl
                    FROM portfolio_snapshots
                    WHERE user_id = %s
                    GROUP BY user_id, date_trunc('{trunc}', date)
                )
                SELECT
                    period_date,
                    total_value,
                    cash,
                    invested_value,
                    pnl
                FROM truncated
                ORDER BY period_date DESC
                LIMIT %s
                """,
                (user_id, limit),
            )
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return [
                {
                    'date':           r[0].isoformat() if r[0] else None,
                    'total_value':    float(r[1]) if r[1] is not None else 0.0,
                    'cash':           float(r[2]) if r[2] is not None else 0.0,
                    'invested_value': float(r[3]) if r[3] is not None else 0.0,
                    'pnl':            float(r[4]) if r[4] is not None else 0.0,
                }
                for r in rows
            ]


def db_sync_positions_from_transactions(user_id: int) -> None:
    with psycopg2.connect(**_db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM portfolios WHERE user_id = %s LIMIT 1", (user_id,))
            row = cur.fetchone()
            if not row:
                cur.execute(
                    "INSERT INTO portfolios (user_id, name) VALUES (%s, 'Main') RETURNING id",
                    (user_id,),
                )
                portfolio_id = cur.fetchone()[0]
            else:
                portfolio_id = row[0]
            cur.execute("DELETE FROM portfolio_positions WHERE portfolio_id = %s", (portfolio_id,))
            cur.execute(
                """
                INSERT INTO portfolio_positions (portfolio_id, secid, quantity, avg_price)
                SELECT %s, secid, SUM(quantity), SUM(quantity * price) / NULLIF(SUM(quantity), 0)
                FROM transactions
                WHERE user_id = %s AND type = 'buy'
                GROUP BY secid
                HAVING SUM(quantity) > 0
                """,
                (portfolio_id, user_id),
            )
            conn.commit()