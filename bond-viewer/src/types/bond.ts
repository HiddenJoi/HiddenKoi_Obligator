// ── Enums & Literal Types ──────────────────────────────────────────────────

export type CouponType = 'fixed' | 'float' | 'zero'
export type RiskProfile = 'conservative' | 'moderate' | 'aggressive'
export type SortBy =
  | 'yield_desc' | 'yield_asc'
  | 'maturity_desc' | 'maturity_asc'
  | 'duration_desc' | 'duration_asc'
  | 'coupon_desc' | 'coupon_asc'
  | 'name_desc' | 'name_asc'

export type SortField = 'yield' | 'maturity' | 'duration' | 'coupon' | 'name'

// ── Domain Models ──────────────────────────────────────────────────────────

export interface Bond {
  secid: string
  name: string
  issuer: string | null
  face_value: number | null
  coupon_type: CouponType | null
  coupon_value: number | null
  coupon_period: number | null
  maturity_date: string | null
}

export interface BondPrice {
  price_date: string
  close_price: number | null
  yield: number | null
  yield_simple: number | null
  duration: number | null
  nkd: number | null
  full_price_rub: number | null
}

export interface BondWithPrice extends Bond {
  last_price: BondPrice | null
}

// ── Paginated List ─────────────────────────────────────────────────────────

export interface BondListItem {
  secid: string
  name: string
  issuer: string | null
  face_value: number | null
  maturity_date: string | null
  // from latest_price join
  close_price?: number | null
  yield_rate?: number | null   // YTM, % годовых
  ytw?: number | null          // Yield to Worst
  nkd?: number | null
  duration?: number | null
  price_date?: string | null
  // bond-specific
  coupon_type?: string | null
  has_offer?: boolean | null
  has_amortization?: boolean | null
}

export interface BondListResponse {
  total: number
  limit: number
  offset: number
  bonds: BondListItem[]
}

// ── Filter / Query Params ───────────────────────────────────────────────────

export interface BondsFilterParams {
  limit?: number
  offset?: number
  sort_by?: SortBy
  search?: string
  min_yield?: number
  max_yield?: number
  min_duration?: number
  max_duration?: number
  min_maturity_days?: number
  max_maturity_days?: number
  coupon_type?: CouponType
  min_coupon?: number
  max_coupon?: number
  has_offer?: boolean
  has_amortization?: boolean
}

export interface FilterState {
  minYield: number | undefined
  maxYield: number | undefined
  minDuration: number | undefined
  maxDuration: number | undefined
  couponType: CouponType | undefined
  search: string
  hasOffer: boolean
  hasAmortization: boolean
}

// ── Recommendations ────────────────────────────────────────────────────────

export interface BondRecommendation {
  secid: string
  name: string
  yield_rate: number
  duration: number
  score: number
}

export interface RecommendationsResponse {
  total: number
  limit: number
  bonds: BondRecommendation[]
}

export interface RecommendationParams {
  target_yield: number
  max_duration: number
  risk_profile?: RiskProfile
  investment_horizon?: number
  limit?: number
}

// ── API Error ──────────────────────────────────────────────────────────────

export interface ApiError {
  detail: string
}

// ── Portfolio ──────────────────────────────────────────────────────────────

export interface Portfolio {
  id: number
  user_id: number
  name: string
}

export interface PositionDetail {
  id: number
  secid: string
  quantity: number
  avg_price: number
  close_price: number | null
  nkd: number | null
  price_date: string | null
  name: string | null
  face_value: number | null
  coupon_type: string | null
  coupon_value: number | null
  coupon_period: number | null
  maturity_date: string | null
  reliability_score: number | null
  is_junk: boolean | null
  current_value: number | null
}

export interface PortfolioDetail {
  id: number
  user_id: number
  name: string
  total_value: number | null
  positions: PositionDetail[]
}

export interface AllocationItem {
  coupon_type: string
  value: number
  pct: number
}

export interface PositionItem {
  secid: string
  name: string | null
  quantity: number
  avg_price: number
  current_price: number
  pnl: number
  yield_: number
  duration: number
}

export interface GoalDeviation {
  target_yield: number
  current_yield: number
  delta: number
  target_monthly_income: number
  current_monthly_income: number
  cashflow_delta: number
}

export interface DashboardData {
  total_value: number
  total_invested: number
  total_pnl: number
  total_pnl_pct: number
  weighted_ytm: number
  weighted_duration: number
  allocation: AllocationItem[]
  positions: PositionItem[]
  goals_deviation: GoalDeviation | null
}

export interface Goal {
  id: number
  user_id: number
  target_yield: number
  max_duration: number
  target_monthly_income: number
  created_at: string | null
}

export interface CreateGoalParams {
  target_yield: number
  max_duration: number
  target_monthly_income: number
}

export interface CashAccount {
  id: number
  user_id: number
  balance: number
  updated_at: string | null
}

export interface Transaction {
  id: number
  user_id: number
  secid: string | null
  type: 'buy' | 'sell' | 'coupon' | 'deposit' | 'withdraw'
  quantity: number | null
  price: number | null
  amount: number
  commission: number
  date: string
  created_at: string
}

export interface CreateTransactionParams {
  type: 'buy' | 'sell' | 'coupon' | 'deposit' | 'withdraw'
  amount: number
  secid?: string
  quantity?: number
  price?: number
  commission?: number
  date?: string
}

export interface TransactionListData {
  total: number
  transactions: Transaction[]
}

export interface CashflowItem {
  date: string
  secid: string
  amount: number
}

export interface MonthlyCashflow {
  year_month: string
  total: number
  items: CashflowItem[]
}

export interface CashflowData {
  items: CashflowItem[]
  by_month: MonthlyCashflow[]
}

export interface HistoryPoint {
  date: string
  value: number
  cash: number | null
  invested_value: number | null
  pnl: number | null
}

export interface HistoryData {
  period: string
  points: HistoryPoint[]
}

export interface AdjustmentRecommendation {
  action: 'buy' | 'sell'
  secid: string
  name: string | null
  reason: string | null
  score: number | null
  impact: { yield?: number; duration?: number } | null
}

export interface PortfolioAdjustmentData {
  recommendations: AdjustmentRecommendation[]
}