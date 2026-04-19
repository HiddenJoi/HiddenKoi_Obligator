"""
Portfolio routes: CRUD for portfolios and positions.
All endpoints require JWT via Depends(get_current_user).
"""
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import List, Optional
from datetime import date, timedelta

from auth import get_current_user
from portfolio import (
    db_list_portfolios,
    db_create_portfolio,
    db_get_portfolio,
    db_add_position,
    db_get_positions_with_prices,
    db_get_dashboard,
    db_get_positions_for_cashflow,
    db_create_goal,
    db_get_goal,
    db_get_portfolio_adjustment,
    db_get_cash_account,
    db_create_transaction,
    db_list_transactions,
    db_save_snapshot,
    db_get_snapshots,
    compute_portfolio_value,
    TransactionError,
)

router = APIRouter(prefix="/portfolio", tags=["portfolio"])


# ── Request / Response models ────────────────────────────────────────────────

class CreatePortfolioRequest(BaseModel):
    name: str


class PortfolioResponse(BaseModel):
    id: int
    user_id: int
    name: str


class PositionRequest(BaseModel):
    secid: str
    quantity: float
    avg_price: float


class PositionResponse(BaseModel):
    id: int
    secid: str
    quantity: float
    avg_price: float


class PositionDetailResponse(BaseModel):
    id: int
    secid: str
    quantity: float
    avg_price: float
    close_price: Optional[float]
    nkd: Optional[float]
    price_date: Optional[str]
    name: Optional[str]
    face_value: Optional[float]
    coupon_type: Optional[str]
    coupon_value: Optional[float]
    coupon_period: Optional[int]
    maturity_date: Optional[str]
    reliability_score: Optional[float]
    is_junk: Optional[bool]
    current_value: Optional[float]


class CreateGoalRequest(BaseModel):
    target_yield: float
    max_duration: int
    target_monthly_income: float


class GoalResponse(BaseModel):
    id: int
    user_id: int
    target_yield: float
    max_duration: int
    target_monthly_income: float
    created_at: Optional[str] = None


class YieldDelta(BaseModel):
    target_yield: float
    current_yield: float
    delta: float


class CashflowDelta(BaseModel):
    target_monthly_income: float
    current_monthly_income: float
    delta: float


class GoalDeviationResponse(BaseModel):
    target_yield: float
    current_yield: float
    delta: float
    target_monthly_income: float
    current_monthly_income: float
    cashflow_delta: float


class PortfolioDetailResponse(BaseModel):
    id: int
    user_id: int
    name: str
    total_value: Optional[float]
    positions: List[PositionDetailResponse]


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("", response_model=PortfolioResponse)
def create_portfolio(body: CreatePortfolioRequest, user: dict = Depends(get_current_user)):
    portfolio = db_create_portfolio(user["id"], body.name)
    return PortfolioResponse(**portfolio)


@router.get("", response_model=List[PortfolioResponse])
def list_portfolios(user: dict = Depends(get_current_user)):
    return [PortfolioResponse(**p) for p in db_list_portfolios(user["id"])]


@router.post("/{portfolio_id}/positions", response_model=PositionResponse)
def add_position(
    portfolio_id: int,
    body: PositionRequest,
    user: dict = Depends(get_current_user),
):
    portfolio = db_get_portfolio(portfolio_id, user["id"])
    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")
    position = db_add_position(
        portfolio_id=portfolio_id,
        secid=body.secid,
        quantity=body.quantity,
        avg_price=body.avg_price,
    )
    return PositionResponse(**position)



# ── Dashboard ────────────────────────────────────────────────────────────────

class AllocationItem(BaseModel):
    coupon_type: str
    value: float
    pct: float


class PositionItem(BaseModel):
    secid: str
    name: Optional[str]
    quantity: float
    avg_price: float
    current_price: float
    pnl: float
    yield_: float
    duration: float

    class Config:
        populate_by_name = True


class DashboardResponse(BaseModel):
    total_value: float
    total_invested: float
    total_pnl: float
    total_pnl_pct: float
    weighted_ytm: float
    weighted_duration: float
    allocation: List[AllocationItem]
    positions: List[PositionItem]
    goals_deviation: Optional[GoalDeviationResponse] = None


@router.get("/dashboard", response_model=DashboardResponse)
def get_dashboard(user: dict = Depends(get_current_user)):
    data = db_get_dashboard(user["id"])

    allocation = [
        AllocationItem(
            coupon_type=a["coupon_type"],
            value=float(a["value"]),
            pct=float(a["pct"]),
        )
        for a in (data.get("allocation") or [])
    ]

    positions = [
        PositionItem(
            secid=p["secid"],
            name=p.get("name"),
            quantity=float(p["quantity"]),
            avg_price=float(p["avg_price"]),
            current_price=float(p["current_price"]),
            pnl=float(p["pnl"]),
            yield_=float(p["yield_"]),
            duration=float(p["duration"]),
        )
        for p in (data.get("positions") or [])
    ]

    goals_deviation = None
    if data.get("target_yield") is not None and data.get("weighted_ytm") is not None:
        current_yield = data["weighted_ytm"]
        target_yield = data["target_yield"]
        yield_delta = round(current_yield - target_yield, 2)

        current_monthly = data.get("current_monthly_income") or 0
        target_monthly = data["target_monthly_income"]
        cashflow_delta = round(current_monthly - target_monthly, 2)

        goals_deviation = GoalDeviationResponse(
            target_yield=target_yield,
            current_yield=current_yield,
            delta=yield_delta,
            target_monthly_income=target_monthly,
            current_monthly_income=current_monthly,
            cashflow_delta=cashflow_delta,
        )

    return DashboardResponse(
        total_value=data["total_value"],
        total_invested=data["total_invested"],
        total_pnl=data["total_pnl"],
        total_pnl_pct=data["total_pnl_pct"],
        weighted_ytm=data["weighted_ytm"],
        weighted_duration=data["weighted_duration"],
        allocation=allocation,
        positions=positions,
        goals_deviation=goals_deviation,
    )


# ── Goals ────────────────────────────────────────────────────────────────────

@router.post("/goals", response_model=GoalResponse)
def create_goal(body: CreateGoalRequest, user: dict = Depends(get_current_user)):
    goal = db_create_goal(
        user_id=user["id"],
        target_yield=body.target_yield,
        max_duration=body.max_duration,
        target_monthly_income=body.target_monthly_income,
    )
    return GoalResponse(
        id=goal["id"],
        user_id=goal["user_id"],
        target_yield=float(goal["target_yield"]),
        max_duration=goal["max_duration"],
        target_monthly_income=float(goal["target_monthly_income"]),
        created_at=goal["created_at"].isoformat() if goal.get("created_at") else None,
    )


@router.get("/goals", response_model=GoalResponse)
def get_goal(user: dict = Depends(get_current_user)):
    goal = db_get_goal(user["id"])
    if not goal:
        raise HTTPException(status_code=404, detail="Goal not found")
    return GoalResponse(
        id=goal["id"],
        user_id=goal["user_id"],
        target_yield=float(goal["target_yield"]),
        max_duration=goal["max_duration"],
        target_monthly_income=float(goal["target_monthly_income"]),
        created_at=goal["created_at"].isoformat() if goal.get("created_at") else None,
    )


# ── Cashflow ─────────────────────────────────────────────────────────────────

class CashflowItem(BaseModel):
    date: str
    secid: str
    amount: float


class MonthlyCashflow(BaseModel):
    year_month: str
    total: float
    items: List[CashflowItem]


class CashflowCalendarResponse(BaseModel):
    items: List[CashflowItem]
    by_month: List[MonthlyCashflow]


@router.get("/dashboard/cashflow", response_model=CashflowCalendarResponse)
def get_cashflow(user: dict = Depends(get_current_user)):
    positions = db_get_positions_for_cashflow(user["id"])
    all_items: List[CashflowItem] = []

    from datetime import date, timedelta
    today = date.today()
    horizon = date(today.year + 1, today.month, today.day)

    for pos in positions:
        coupon_days = int(pos["coupon_period"])
        if coupon_days <= 0:
            continue

        maturity = pos["maturity_date"]
        if maturity is None or maturity <= today:
            continue

        # Находим ближайшую купонную дату от maturity назад
        t = maturity
        while t >= today:
            diff = (t - today).days
            if diff >= 0 and diff % coupon_days == 0:
                # Есть выплата в пределах года
                break
            t -= timedelta(days=1)

        # Генерируем вперёд от t до horizon
        t_next = t
        while t_next <= maturity and t_next <= horizon and t_next >= today:
            if t_next >= today:
                amount = float(pos["coupon_value"]) * float(pos["quantity"])
                all_items.append(CashflowItem(
                    date=t_next.isoformat(),
                    secid=pos["secid"],
                    amount=round(amount, 2),
                ))
            t_next += timedelta(days=coupon_days)

    all_items.sort(key=lambda x: x.date)

    # Группировка по месяцам
    month_map: dict = {}
    for item in all_items:
        ym = item.date[:7]
        if ym not in month_map:
            month_map[ym] = []
        month_map[ym].append(item)

    by_month = [
        MonthlyCashflow(
            year_month=ym,
            total=round(sum(i.amount for i in group), 2),
            items=group,
        )
        for ym, group in sorted(month_map.items())
    ]

    return CashflowCalendarResponse(items=all_items, by_month=by_month)


# ── Cash Account ───────────────────────────────────────────────────────────────

class CashAccountResponse(BaseModel):
    id: int
    user_id: int
    balance: float
    updated_at: Optional[str] = None


@router.get("/cash", response_model=CashAccountResponse)
def get_cash(user: dict = Depends(get_current_user)):
    acc = db_get_cash_account(user["id"])
    return CashAccountResponse(
        id=acc["id"],
        user_id=acc["user_id"],
        balance=float(acc["balance"]),
        updated_at=acc["updated_at"].isoformat() if acc.get("updated_at") else None,
    )


# ── Transactions ──────────────────────────────────────────────────────────────

class CreateTransactionRequest(BaseModel):
    type: str
    amount: float
    secid: Optional[str] = None
    quantity: Optional[float] = None
    price: Optional[float] = None
    commission: float = 0
    date: Optional[str] = None


class TransactionResponse(BaseModel):
    id: int
    user_id: int
    secid: Optional[str]
    type: str
    quantity: Optional[float]
    price: Optional[float]
    amount: float
    commission: float
    date: str
    created_at: str


@router.post("/transactions", response_model=TransactionResponse)
def create_transaction(body: CreateTransactionRequest, user: dict = Depends(get_current_user)):
    valid_types = ("buy", "sell", "coupon", "deposit", "withdraw")
    if body.type not in valid_types:
        raise HTTPException(status_code=400, detail=f"type must be one of: {valid_types}")

    try:
        tx = db_create_transaction(
            user_id=user["id"],
            tx_type=body.type,
            amount=body.amount,
            secid=body.secid,
            quantity=body.quantity,
            price=body.price,
            commission=body.commission,
            tx_date=body.date,
        )
    except TransactionError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return TransactionResponse(
        id=tx["id"],
        user_id=tx["user_id"],
        secid=tx["secid"],
        type=tx["type"],
        quantity=float(tx["quantity"]) if tx["quantity"] is not None else None,
        price=float(tx["price"]) if tx["price"] is not None else None,
        amount=float(tx["amount"]),
        commission=float(tx["commission"]),
        date=tx["date"].isoformat() if tx["date"] else "",
        created_at=tx["created_at"].isoformat() if tx["created_at"] else "",
    )


class TransactionListResponse(BaseModel):
    total: int
    transactions: List[TransactionResponse]


@router.get("/transactions", response_model=TransactionListResponse)
def list_transactions(
    user: dict = Depends(get_current_user),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    type: Optional[str] = Query(default=None),
    secid: Optional[str] = Query(default=None),
):
    rows = db_list_transactions(user["id"], limit=limit, offset=offset, tx_type=type, secid=secid)
    return TransactionListResponse(
        total=len(rows),
        transactions=[
            TransactionResponse(
                id=r["id"],
                user_id=r["user_id"],
                secid=r["secid"],
                type=r["type"],
                quantity=float(r["quantity"]) if r["quantity"] is not None else None,
                price=float(r["price"]) if r["price"] is not None else None,
                amount=float(r["amount"]),
                commission=float(r["commission"]),
                date=r["date"].isoformat() if r["date"] else "",
                created_at=r["created_at"].isoformat() if r["created_at"] else "",
            )
            for r in rows
        ],
    )


# ── Portfolio History (snapshots) ─────────────────────────────────────────────

class HistoryPoint(BaseModel):
    date: str
    value: float
    cash: Optional[float] = None
    invested_value: Optional[float] = None
    pnl: Optional[float] = None


class HistoryResponse(BaseModel):
    period: str
    points: List[HistoryPoint]


@router.get("/history", response_model=HistoryResponse)
def get_portfolio_history(
    user: dict = Depends(get_current_user),
    period: str = Query(default="daily", pattern="^(daily|weekly|monthly)$"),
    limit: int = Query(default=365, ge=1, le=3650),
):
    rows = db_get_snapshots(user["id"], period=period, limit=limit)
    return HistoryResponse(
        period=period,
        points=[
            HistoryPoint(
                date=r["date"],
                value=round(r["total_value"], 2),
                cash=round(r["cash"], 2),
                invested_value=round(r["invested_value"], 2),
                pnl=round(r["pnl"], 2),
            )
            for r in rows
        ],
    )


@router.post("/history/take-snapshot")
def take_snapshot(user: dict = Depends(get_current_user)):
    """
    Take and save a portfolio snapshot for today (or compute + upsert).
    Call this after a transaction or as a daily cron job.
    """
    today = date.today()
    snap = db_save_snapshot(user["id"], today)
    return {
        "id": snap["id"],
        "date": snap["date"].isoformat() if snap.get("date") else None,
        "total_value": round(float(snap["total_value"]), 2),
        "cash": round(float(snap["cash"]), 2),
        "invested_value": round(float(snap["invested_value"]), 2),
        "pnl": round(float(snap["pnl"]), 2),
    }


@router.post("/history/backfill")
def backfill_history(
    user: dict = Depends(get_current_user),
    days: int = Query(default=30, ge=1, le=365),
):
    """
    Backfill snapshots for the past N days.
    Useful for initial population when snapshots table is empty.
    """
    today = date.today()
    saved = 0
    for i in range(days):
        d = today - timedelta(days=i)
        try:
            db_save_snapshot(user["id"], d)
            saved += 1
        except Exception:
            pass
    return {"saved": saved}

@router.get("/{portfolio_id}", response_model=PortfolioDetailResponse)
def get_portfolio(portfolio_id: int, user: dict = Depends(get_current_user)):
    portfolio = db_get_portfolio(portfolio_id, user["id"])
    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")

    positions = db_get_positions_with_prices(portfolio_id)
    total_value = sum(p["current_value"] for p in positions if p["current_value"])

    detail_positions = [
        PositionDetailResponse(
            id=p["id"],
            secid=p["secid"],
            quantity=float(p["quantity"]),
            avg_price=float(p["avg_price"]),
            close_price=float(p["close_price"]) if p["close_price"] else None,
            nkd=float(p["nkd"]) if p["nkd"] else None,
            price_date=p["price_date"].isoformat() if p["price_date"] else None,
            name=p["name"],
            face_value=float(p["face_value"]) if p["face_value"] else None,
            coupon_type=p["coupon_type"],
            coupon_value=float(p["coupon_value"]) if p["coupon_value"] else None,
            coupon_period=p["coupon_period"],
            maturity_date=p["maturity_date"].isoformat() if p["maturity_date"] else None,
            reliability_score=float(p["reliability_score"]) if p["reliability_score"] else None,
            is_junk=p["is_junk"],
            current_value=float(p["current_value"]) if p["current_value"] else None,
        )
        for p in positions
    ]

    return PortfolioDetailResponse(
        id=portfolio["id"],
        user_id=portfolio["user_id"],
        name=portfolio["name"],
        total_value=total_value,
        positions=detail_positions,
    )

