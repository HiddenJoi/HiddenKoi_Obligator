import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from enum import Enum
from typing import Optional, List, Tuple

import psycopg2
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from cache import cached_get, cached_set, make_key, init_redis, close_redis
from auth import init_users_table
from portfolio import init_portfolio_tables, init_goals_table, init_transactions_tables, init_snapshots_table, db_get_portfolio_adjustment
from notifications import init_notification_tables
from routes.auth import router as auth_router
from routes.portfolio import router as portfolio_router
from routes.notifications import router as notifications_router

# ── Thread pool for sync psycopg2 → async bridge ────────────────────────────
_DB_EXECUTOR = ThreadPoolExecutor(max_workers=10)

# ── Coupons Enum ───────────────────────────────────────────────────────────
class CouponType(str, Enum):
    FIXED  = "fixed"
    FLOAT  = "float"
    ZERO   = "zero"

# ── DB Config ────────────────────────────────────────────────────────────────
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'TulaHack',
    'user': 'postgres',
    'password': '15021502',
    'options': '-c client_encoding=UTF8',
}


# ── Pydantic Models ──────────────────────────────────────────────────────────
class BondPriceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    price_date: str
    close_price: Optional[float] = None
    yield_rate: Optional[float] = None
    nkd: Optional[float] = None
    duration: Optional[float] = None


class BondDetailResponse(BaseModel):
    secid: str
    name: str
    issuer: Optional[str] = None
    face_value: Optional[float] = None
    coupon_type: Optional[str] = None
    coupon_value: Optional[float] = None
    coupon_period: Optional[int] = None
    maturity_date: Optional[str] = None
    last_price: Optional[BondPriceResponse] = None

    model_config = ConfigDict(from_attributes=True)


class BondListResponse(BaseModel):
    secid: str
    name: str
    issuer: Optional[str] = None
    face_value: Optional[float] = None
    maturity_date: Optional[str] = None
    # price fields
    close_price: Optional[float] = None
    yield_rate: Optional[float] = None
    ytw: Optional[float] = None       # Yield to Worst
    nkd: Optional[float] = None
    duration: Optional[float] = None
    price_date: Optional[str] = None
    # bond-specific fields for quick reference
    has_offer: Optional[bool] = None
    has_amortization: Optional[bool] = None
    coupon_type: Optional[str] = None
    # reliability fields
    reliability_score: Optional[float] = None  # 0-100, чем выше - тем надёжнее
    is_junk: Optional[bool] = None             # True = мусорная облигация
    junk_reason: Optional[str] = None          # причина отнесения к мусору


class BondListPaginatedResponse(BaseModel):
    total: int
    limit: int
    offset: int
    bonds: list[BondListResponse]


# ── Risk Profile Enum ────────────────────────────────────────────────────────
class RiskProfile(str, Enum):
    CONSERVATIVE = "conservative"
    MODERATE     = "moderate"
    AGGRESSIVE   = "aggressive"


# ── Recommendations Response Model ──────────────────────────────────────────
class BondRecommendation(BaseModel):
    secid: str
    name: str
    yield_rate: float
    duration: float
    score: float

    model_config = ConfigDict(from_attributes=True)


class Recommendation(BaseModel):
    action: str
    secid: str
    name: Optional[str] = None
    reason: Optional[str] = None
    score: Optional[float] = None
    impact: Optional[dict] = None


class RecommendationsResponse(BaseModel):
    total: int
    limit: int
    bonds: list[BondRecommendation]


class AdjustmentRecommendation(BaseModel):
    action: str
    secid: str
    name: Optional[str] = None
    reason: Optional[str] = None
    score: Optional[float] = None
    impact: Optional[dict] = None


class PortfolioAdjustmentResponse(BaseModel):
    recommendations: list[AdjustmentRecommendation]


# ── App + lifecycle ──────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Redis via the existing cache module
    try:
        await init_redis()
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("Redis unavailable, caching disabled: %s", e)
    init_users_table()
    init_portfolio_tables()
    init_goals_table()
    init_transactions_tables()
    init_snapshots_table()
    init_notification_tables()
    yield
    await close_redis()
    _DB_EXECUTOR.shutdown(wait=False)


app = FastAPI(title="Bonds API", lifespan=lifespan)
app.include_router(auth_router)
app.include_router(portfolio_router)
app.include_router(notifications_router)

# ── CORS ─────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Low-level sync DB helpers (run inside ThreadPoolExecutor) ────────────────

def _sync_fetch(conn, query: str, params: tuple) -> List[dict]:
    """Execute query and return list of dicts."""
    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _sync_fetchrow(conn, query: str, params: tuple) -> Optional[dict]:
    """Execute query and return one dict or None."""
    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        return dict(zip(cols, row)) if row else None


def _sync_fetchval(conn, query: str, params: tuple):
    """Execute query and return a single scalar value."""
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchone()[0]


def _get_conn():
    """Create a new psycopg2 connection."""
    return psycopg2.connect(**DB_CONFIG)


async def _db_fetch(query: str, params: tuple = ()) -> List[dict]:
    return await asyncio.to_thread(_sync_fetch, _get_conn(), query, params)


async def _db_fetchrow(query: str, params: tuple = ()) -> Optional[dict]:
    return await asyncio.to_thread(_sync_fetchrow, _get_conn(), query, params)


async def _db_fetchval(query: str, params: tuple = ()):
    return await asyncio.to_thread(_sync_fetchval, _get_conn(), query, params)


# ── SQL helpers ──────────────────────────────────────────────────────────────
def _build_filter(
    min_yield: Optional[float] = None,
    max_yield: Optional[float] = None,
    min_duration: Optional[float] = None,
    max_duration: Optional[float] = None,
    min_maturity_days: Optional[int] = None,
    max_maturity_days: Optional[int] = None,
    coupon_type: Optional[CouponType] = None,
    min_coupon: Optional[float] = None,
    max_coupon: Optional[float] = None,
    has_offer: Optional[bool] = None,
    has_amortization: Optional[bool] = None,
    search: Optional[str] = None,
    # Надёжность и мусорные фильтры
    exclude_junk: Optional[bool] = None,        # исключить мусорные облигации
    min_reliability: Optional[float] = None,   # мин. надёжность (0-100)
    max_reliability: Optional[float] = None,   # макс. надёжность (0-100)
) -> Tuple[str, List]:
    """
    Build SQL WHERE clause and params list.
    Uses %s positional placeholders (psycopg2 style).
    """
    conditions = []
    params: List = []

    def add(sql: str, val):
        conditions.append(sql)
        if val is not None:
            params.append(val)

    if min_yield is not None:
        add(f"lp.yield >= %s", min_yield)
    if max_yield is not None:
        add(f"lp.yield <= %s", max_yield)
    if min_duration is not None:
        add(f"lp.duration >= %s", min_duration)
    if max_duration is not None:
        add(f"lp.duration <= %s", max_duration)

    if min_maturity_days is not None:
        add(f"(b.maturity_date - CURRENT_DATE) >= %s", min_maturity_days)
    if max_maturity_days is not None:
        add(f"(b.maturity_date - CURRENT_DATE) <= %s", max_maturity_days)

    if coupon_type is not None:
        add(f"LOWER(b.coupon_type) = %s", coupon_type.value)
    if min_coupon is not None:
        add(f"b.coupon_value >= %s", min_coupon)
    if max_coupon is not None:
        add(f"b.coupon_value <= %s", max_coupon)

    if has_offer is True:
        conditions.append("b.has_offer = TRUE")
    elif has_offer is False:
        conditions.append("b.has_offer = FALSE")

    if has_amortization is not None:
        add(f"b.has_amortization = %s", has_amortization)

    if search:
        search_pattern = f"%{search}%"
        conditions.append("(b.name ILIKE %s OR b.issuer ILIKE %s)")
        params.append(search_pattern)
        params.append(search_pattern)

    # Фильтры по надёжности и мусору
    if exclude_junk is True:
        conditions.append("(lp.is_junk = FALSE OR lp.is_junk IS NULL)")
    elif exclude_junk is False:
        conditions.append("(lp.is_junk = TRUE)")

    if min_reliability is not None:
        add(f"lp.reliability_score >= %s", min_reliability)
    if max_reliability is not None:
        add(f"lp.reliability_score <= %s", max_reliability)

    if not conditions:
        return "", []
    return "WHERE " + " AND ".join(conditions), params


# ── Endpoints ────────────────────────────────────────────────────────────────
@app.get("/bonds", response_model=BondListPaginatedResponse)
async def get_bonds(
    request: Request,
    limit:   int = Query(default=20, ge=1, le=100),
    offset:  int = Query(default=0, ge=0),
    # ── Filters ────────────────────────────────────────────────────────────────
    min_yield:         Optional[float] = Query(default=None, ge=0, description="Мин. YTM (%)"),
    max_yield:         Optional[float] = Query(default=None, ge=0, description="Макс. YTM (%)"),
    min_duration:      Optional[float] = Query(default=None, ge=0, description="Мин. дюрация (дни)"),
    max_duration:      Optional[float] = Query(default=None, ge=0, description="Макс. дюрация (дни)"),
    min_maturity_days: Optional[int]   = Query(default=None, ge=0, description="Мин. дней до погашения"),
    max_maturity_days: Optional[int]   = Query(default=None, ge=0, description="Макс. дней до погашения"),
    coupon_type:       Optional[CouponType] = Query(default=None, description="fixed / float / zero"),
    min_coupon:        Optional[float] = Query(default=None, ge=0, description="Мин. купон"),
    max_coupon:        Optional[float] = Query(default=None, ge=0, description="Макс. купон"),
    has_offer:         Optional[bool]  = Query(default=None, description="Наличие оферты"),
    has_amortization:  Optional[bool]  = Query(default=None, description="Наличие амортизации"),
    search:            Optional[str]   = Query(default=None, max_length=200, description="Поиск по name / issuer"),
    # ── Sorting ────────────────────────────────────────────────────────────────
    sort_by: Optional[str] = Query(default=None, description="Сортировка"),
    # ── Reliability filters ────────────────────────────────────────────────────
    exclude_junk:      Optional[bool]  = Query(default=True, description="Исключить мусорные облигации"),
    min_reliability:   Optional[float] = Query(default=None, ge=0, le=100, description="Мин. надёжность (0-100)"),
    max_reliability:   Optional[float] = Query(default=None, ge=0, le=100, description="Макс. надёжность (0-100)"),
):
    """Возвращает список облигаций с фильтрацией, сортировкой и пагинацией.

    Кэшируется в Redis на 60 с. Ключ = MD5(query params без limit/offset).
    Заголовки X-Cache-Hit и X-Cache-TTL добавляются к каждому ответу.
    """
    cache_key = make_key("bonds", request, exclude_pagination=True)
    cached_body, headers = await cached_get("bonds", request, exclude_pagination=True)
    if cached_body is not None:
        return JSONResponse(content=json.loads(cached_body), headers=headers)

    where_clause, filter_params = _build_filter(
        min_yield=min_yield,
        max_yield=max_yield,
        min_duration=min_duration,
        max_duration=max_duration,
        min_maturity_days=min_maturity_days,
        max_maturity_days=max_maturity_days,
        coupon_type=coupon_type,
        min_coupon=min_coupon,
        max_coupon=max_coupon,
        has_offer=has_offer,
        has_amortization=has_amortization,
        search=search,
        exclude_junk=exclude_junk,
        min_reliability=min_reliability,
        max_reliability=max_reliability,
    )

    # Build ORDER BY clause
    ORDER_MAP = {
        'yield_desc':    'lp.yield DESC NULLS LAST',
        'yield_asc':     'lp.yield ASC NULLS LAST',
        'duration_desc': 'lp.duration DESC NULLS LAST',
        'duration_asc':  'lp.duration ASC NULLS LAST',
        'maturity_desc': 'b.maturity_date DESC NULLS LAST',
        'maturity_asc':  'b.maturity_date ASC NULLS LAST',
        'name_desc':     'b.name DESC NULLS LAST',
        'name_asc':      'b.name ASC NULLS LAST',
    }
    order_clause = ORDER_MAP.get(sort_by, 'b.secid ASC')

    base_query = f"""
        WITH latest_price AS (
            SELECT DISTINCT ON (secid)
                   secid, price_date, close_price, yield, ytw, nkd, duration,
                   reliability_score, is_junk, junk_reason
            FROM bond_prices
            WHERE price_date = (
                SELECT MAX(price_date) FROM bond_prices
                WHERE price_date <= CURRENT_DATE
            )
            ORDER BY secid, price_date DESC
        )
        SELECT
            b.secid,
            b.name,
            b.issuer,
            b.face_value,
            b.maturity_date,
            b.coupon_type,
            b.has_offer,
            b.has_amortization,
            lp.close_price,
            lp.yield,
            lp.ytw,
            lp.nkd,
            lp.duration,
            lp.price_date,
            lp.reliability_score,
            lp.is_junk,
            lp.junk_reason
        FROM bonds b
        LEFT JOIN latest_price lp ON lp.secid = b.secid
        {where_clause}
        ORDER BY {order_clause}
        LIMIT %s OFFSET %s
    """

    count_query = f"""
        WITH latest_price AS (
            SELECT DISTINCT ON (secid)
                   secid, yield, duration, reliability_score, is_junk
            FROM bond_prices
            WHERE price_date = (
                SELECT MAX(price_date) FROM bond_prices
                WHERE price_date <= CURRENT_DATE
            )
            ORDER BY secid, price_date DESC
        )
        SELECT COUNT(*)
        FROM bonds b
        LEFT JOIN latest_price lp ON lp.secid = b.secid
        {where_clause}
    """

    # Run both queries concurrently via gather
    total_row, rows = await asyncio.gather(
        _db_fetchval(count_query, tuple(filter_params)),
        _db_fetch(base_query, tuple(filter_params) + (limit, offset)),
    )

    bonds = [
        BondListResponse(
            secid=r["secid"],
            name=r["name"],
            issuer=r["issuer"],
            face_value=r["face_value"],
            maturity_date=r["maturity_date"].isoformat() if r["maturity_date"] else None,
            coupon_type=r["coupon_type"],
            has_offer=r["has_offer"],
            has_amortization=r["has_amortization"],
            close_price=r["close_price"],
            yield_rate=r["yield"],
            ytw=r["ytw"],
            nkd=r["nkd"],
            duration=r["duration"],
            price_date=r["price_date"].isoformat() if r["price_date"] else None,
            reliability_score=r.get("reliability_score"),
            is_junk=r.get("is_junk"),
            junk_reason=r.get("junk_reason"),
        )
        for r in rows
    ]
    response = BondListPaginatedResponse(
        total=total_row or 0,
        limit=limit,
        offset=offset,
        bonds=bonds,
    )

    await cached_set(cache_key, response, ttl=60)
    return response


@app.get("/bonds/{secid}", response_model=BondDetailResponse)
async def get_bond(secid: str):
    """Возвращает детали облигации + последнюю цену из bond_prices."""
    bond, price = await asyncio.gather(
        _db_fetchrow(
            """
            SELECT secid, name, issuer, face_value,
                   coupon_type, coupon_value, coupon_period, maturity_date
            FROM bonds
            WHERE secid = %s
            """,
            (secid,),
        ),
        _db_fetchrow(
            """
            SELECT price_date, close_price, yield, nkd, duration
            FROM bond_prices
            WHERE secid = %s
            ORDER BY price_date DESC
            LIMIT 1
            """,
            (secid,),
        ),
    )

    if not bond:
        raise HTTPException(status_code=404, detail=f"Bond '{secid}' not found")

    last_price = None
    if price:
        price_date = price["price_date"]
        last_price = BondPriceResponse(
            price_date=price_date.isoformat() if price_date else None,
            close_price=price["close_price"],
            yield_rate=price["yield"],
            nkd=price["nkd"],
            duration=price["duration"],
        )

    # Исправление здесь: преобразуем date в строку
    return BondDetailResponse(
        secid=bond["secid"],
        name=bond["name"],
        issuer=bond["issuer"],
        face_value=bond["face_value"],
        coupon_type=bond["coupon_type"],
        coupon_value=bond["coupon_value"],
        coupon_period=bond["coupon_period"],
        maturity_date=bond["maturity_date"].isoformat() if bond["maturity_date"] else None,
        last_price=last_price,
    )


# ── Scoring helpers ───────────────────────────────────────────────────────────
def _build_score_formula(risk_profile: RiskProfile) -> str:
    """
    Build SQL expression for composite score.
    Uses %s placeholders: %s = target_yield, %s = max_duration (filled by psycopg2).
    """
    score_yield = "GREATEST(0, 1 - ABS(lp.yield - %s) / %s)"
    score_dur   = "GREATEST(0, 1 - lp.duration / %s)"

    if risk_profile == RiskProfile.AGGRESSIVE:
        score = f"({score_yield} * 0.85 + {score_dur} * 0.15)"
    else:
        score = f"({score_yield} * 0.6 + {score_dur} * 0.4)"

    return f"({score}) AS score"


@app.get("/recommendations", response_model=RecommendationsResponse)
async def get_recommendations(
    request: Request,
    target_yield: float = Query(..., ge=0, description="Желаемая доходность (%)"),
    max_duration: float = Query(..., ge=1, description="Максимальная дюрация (дни)"),
    risk_profile: RiskProfile = Query(
        default=RiskProfile.MODERATE,
        description="conservative | moderate | aggressive",
    ),
    investment_horizon: Optional[int] = Query(
        default=None,
        ge=1,
        description="Срок инвестирования (дни) — фильтрует по maturity_days",
    ),
    limit: int = Query(default=10, ge=1, le=50, description="Топ-N облигаций"),
    min_reliability: Optional[float] = Query(
        default=30.0,
        ge=0, le=100,
        description="Мин. надёжность для рекомендаций (0-100, по умолчанию 30)",
    ),
    mode: Optional[str] = Query(
        default=None,
        description="portfolio_adjustment — анализ пробелов в портфеле vs цель",
    ),
):
    """
    Подбирает топ-N облигаций, отсортированных по кастомному score.

    По умолчанию исключает мусорные облигации и бумаги с надёжностью < 30.

    Кэшируется в Redis на 300 с. Кэшируются ВСЕ параметры (включая limit).
    Заголовки X-Cache-Hit и X-Cache-TTL добавляются к каждому ответу.

    ?mode=portfolio_adjustment — анализ отклонений портфеля от цели и рекомендации
    по сделкам.
    """
    # Portfolio adjustment mode — requires auth, no caching
    if mode == "portfolio_adjustment":
        return await _portfolio_adjustment(request)

    cache_key = make_key("recs", request, exclude_pagination=False)
    cached_body, headers = await cached_get("recs", request, exclude_pagination=False)
    if cached_body is not None:
        return JSONResponse(content=json.loads(cached_body), headers=headers)

    score_expr = _build_score_formula(risk_profile)

    # Count query — needs (target_yield, max_duration, investment_horizon, min_reliability)
    count_sql = f"""
        WITH latest_price AS (
            SELECT DISTINCT ON (secid)
                   secid, yield, duration, reliability_score, is_junk
            FROM bond_prices
            WHERE price_date = (
                SELECT MAX(price_date) FROM bond_prices
                WHERE price_date <= CURRENT_DATE
            )
            ORDER BY secid, price_date DESC
        )
        SELECT COUNT(*)
        FROM bonds b
        INNER JOIN latest_price lp ON lp.secid = b.secid
        WHERE lp.duration <= %s
          AND lp.yield IS NOT NULL
          AND (b.maturity_date - CURRENT_DATE) <= COALESCE(%s, 999999)
          AND (lp.is_junk = FALSE OR lp.is_junk IS NULL)
          AND lp.reliability_score >= %s
    """
    count_params: Tuple = (max_duration, investment_horizon, min_reliability)

    # Base query — needs (target_yield × 3 for score, max_duration, investment_horizon, limit, min_reliability)
    base_sql = f"""
        WITH latest_price AS (
            SELECT DISTINCT ON (secid)
                   secid, yield, duration, reliability_score, is_junk
            FROM bond_prices
            WHERE price_date = (
                SELECT MAX(price_date) FROM bond_prices
                WHERE price_date <= CURRENT_DATE
            )
            ORDER BY secid, price_date DESC
        )
        SELECT
            b.secid,
            b.name,
            lp.yield,
            lp.duration,
            lp.reliability_score,
            {score_expr}
        FROM bonds b
        INNER JOIN latest_price lp ON lp.secid = b.secid
        WHERE lp.duration <= %s
          AND lp.yield IS NOT NULL
          AND (b.maturity_date - CURRENT_DATE) <= COALESCE(%s, 999999)
          AND (lp.is_junk = FALSE OR lp.is_junk IS NULL)
          AND lp.reliability_score >= %s
        ORDER BY score DESC, b.secid ASC
        LIMIT %s
    """
    base_params: Tuple = (target_yield, target_yield, target_yield, max_duration, investment_horizon, min_reliability, limit)

    total_row, rows = await asyncio.gather(
        _db_fetchval(count_sql, count_params),
        _db_fetch(base_sql, base_params),
    )

    bonds = [
        BondRecommendation(
            secid=r["secid"],
            name=r["name"],
            yield_rate=r["yield"],
            duration=r["duration"],
            score=round(float(r["score"]), 6),
        )
        for r in rows
    ]

    response = RecommendationsResponse(total=total_row or 0, limit=limit, bonds=bonds)
    await cached_set(cache_key, response, ttl=300)
    return response


async def _portfolio_adjustment(request: Request) -> JSONResponse:
    """Portfolio adjustment mode — requires auth, returns buy/sell recommendations."""
    from auth import get_user_from_token

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    token = auth_header[7:]
    user = get_user_from_token(token)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    recs = await asyncio.to_thread(db_get_portfolio_adjustment, user["id"])

    items = [
        AdjustmentRecommendation(
            action=r["action"],
            secid=r["secid"],
            name=r.get("name"),
            reason=r.get("reason"),
            score=r.get("score"),
            impact=r.get("impact"),
        )
        for r in recs
    ]
    return JSONResponse(content=PortfolioAdjustmentResponse(recommendations=items).model_dump())
