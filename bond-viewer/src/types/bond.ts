// ── Enums & Literal Types ──────────────────────────────────────────────────

export type CouponType = 'fixed' | 'float' | 'zero'
export type RiskProfile = 'conservative' | 'moderate' | 'aggressive'
export type SortBy =
  | 'yield_desc' | 'yield_asc'
  | 'maturity_desc' | 'maturity_asc'
  | 'duration_desc' | 'duration_asc'
  | 'coupon_desc' | 'coupon_asc'
  | 'name_desc' | 'name_asc'

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
