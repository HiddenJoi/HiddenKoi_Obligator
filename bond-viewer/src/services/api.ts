import axios, { AxiosError, type InternalAxiosRequestConfig } from 'axios'
import type {
  BondListResponse,
  BondWithPrice,
  BondsFilterParams,
  RecommendationsResponse,
  RecommendationParams,
  DashboardData,
  Goal,
  CreateGoalParams,
  CashAccount,
  Transaction,
  CreateTransactionParams,
  TransactionListData,
  CashflowData,
  HistoryData,
  PortfolioAdjustmentData,
} from '../types'

// ── Axios instance ─────────────────────────────────────────────────────────

const apiClient = axios.create({
  baseURL: import.meta.env.VITE_API_URL as string,
  timeout: 15_000,
  headers: { 'Content-Type': 'application/json' },
})

// ── Logging interceptor ─────────────────────────────────────────────────────

apiClient.interceptors.request.use(
  (config: InternalAxiosRequestConfig) => {
    const method = config.method?.toUpperCase() ?? '?'
    const url = config.url ?? ''
    const params = JSON.stringify(config.params ?? {})
    console.debug(`[API →] ${method} ${url}  params=${params}`)
    return config
  },
  (err) => {
    console.error('[API REQ ERROR]', err)
    return Promise.reject(err)
  },
)

apiClient.interceptors.response.use(
  (response) => {
    const method = response.config.method?.toUpperCase() ?? '?'
    const url = response.config.url ?? ''
    console.debug(`[API ←] ${method} ${url}  status=${response.status}`)
    return response
  },
  (err: AxiosError) => {
    const method = err.config?.method?.toUpperCase() ?? '?'
    const url = err.config?.url ?? ''
    const status = err.response?.status ?? 'no-response'
    const detail = err.response?.data?.detail ?? err.message
    console.error(`[API ←] ${method} ${url}  status=${status}  detail="${detail}"`)
    return Promise.reject(err)
  },
)

// ── Helpers ────────────────────────────────────────────────────────────────

function stringifyFilterParams(params: BondsFilterParams): Record<string, string> {
  const result: Record<string, string> = {}

  if (params.limit !== undefined) result.limit = String(params.limit)
  if (params.offset !== undefined) result.offset = String(params.offset)
  if (params.sort_by) result.sort_by = params.sort_by
  if (params.search) result.search = params.search
  if (params.min_yield !== undefined) result.min_yield = String(params.min_yield)
  if (params.max_yield !== undefined) result.max_yield = String(params.max_yield)
  if (params.min_duration !== undefined) result.min_duration = String(params.min_duration)
  if (params.max_duration !== undefined) result.max_duration = String(params.max_duration)
  if (params.min_maturity_days !== undefined) result.min_maturity_days = String(params.min_maturity_days)
  if (params.max_maturity_days !== undefined) result.max_maturity_days = String(params.max_maturity_days)
  if (params.coupon_type) result.coupon_type = params.coupon_type
  if (params.min_coupon !== undefined) result.min_coupon = String(params.min_coupon)
  if (params.max_coupon !== undefined) result.max_coupon = String(params.max_coupon)
  if (params.has_offer !== undefined) result.has_offer = String(params.has_offer)
  if (params.has_amortization !== undefined) result.has_amortization = String(params.has_amortization)

  return result
}

function stringifyRecParams(params: RecommendationParams): Record<string, string> {
  return {
    target_yield: String(params.target_yield),
    max_duration: String(params.max_duration),
    ...(params.risk_profile && { risk_profile: params.risk_profile }),
    ...(params.investment_horizon !== undefined && {
      investment_horizon: String(params.investment_horizon),
    }),
    ...(params.limit !== undefined && { limit: String(params.limit) }),
  }
}

// ── Auth token helper (reads from localStorage) ─────────────────────────────

export function getAuthHeaders(): Record<string, string> {
  const token = localStorage.getItem('token')
  return token ? { Authorization: `Bearer ${token}` } : {}
}

// ── API Methods ─────────────────────────────────────────────────────────────

export async function getBonds(params: BondsFilterParams = {}): Promise<BondListResponse> {
  try {
    const { data } = await apiClient.get<BondListResponse>('/bonds', {
      params: stringifyFilterParams(params),
    })
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

export async function getBondById(secid: string): Promise<BondWithPrice> {
  try {
    const { data } = await apiClient.get<BondWithPrice>(`/bonds/${secid}`)
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

export async function getRecommendations(
  params: RecommendationParams,
): Promise<RecommendationsResponse> {
  try {
    const { data } = await apiClient.get<RecommendationsResponse>('/recommendations', {
      params: stringifyRecParams(params),
    })
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

// ── Error normalization ─────────────────────────────────────────────────────

function toFriendlyError(err: unknown): Error {
  if (err instanceof AxiosError) {
    const detail = (err.response?.data as any)?.detail
    if (err.response?.status === 404 && detail) {
      return new Error(`Облигация не найдена: ${detail}`)
    }
    if (err.response?.status === 422) {
      return new Error(`Неверные параметры запроса: ${detail}`)
    }
    if (err.response?.status === 500) {
      return new Error('Ошибка сервера. Попробуйте позже.')
    }
    if (!err.response) {
      return new Error('Нет связи с сервером. Проверьте, что API запущен.')
    }
  }
  if (err instanceof Error) return err
  return new Error('Неизвестная ошибка')
}

// ── Portfolio API Methods ───────────────────────────────────────────────────

export async function getDashboard(): Promise<DashboardData> {
  try {
    const { data } = await apiClient.get<DashboardData>('/portfolio/dashboard', {
      headers: getAuthHeaders(),
    })
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

export async function createGoal(params: CreateGoalParams): Promise<Goal> {
  try {
    const { data } = await apiClient.post<Goal>('/portfolio/goals', params, {
      headers: getAuthHeaders(),
    })
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

export async function getGoal(): Promise<Goal> {
  try {
    const { data } = await apiClient.get<Goal>('/portfolio/goals', {
      headers: getAuthHeaders(),
    })
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

export async function getCash(): Promise<CashAccount> {
  try {
    const { data } = await apiClient.get<CashAccount>('/portfolio/cash', {
      headers: getAuthHeaders(),
    })
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

export async function createTransaction(params: CreateTransactionParams): Promise<Transaction> {
  try {
    const { data } = await apiClient.post<Transaction>('/portfolio/transactions', params, {
      headers: getAuthHeaders(),
    })
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

export async function getTransactions(params: {
  limit?: number
  offset?: number
  type?: string
  secid?: string
} = {}): Promise<TransactionListData> {
  try {
    const { data } = await apiClient.get<TransactionListData>('/portfolio/transactions', {
      params,
      headers: getAuthHeaders(),
    })
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

export async function getCashflow(): Promise<CashflowData> {
  try {
    const { data } = await apiClient.get<CashflowData>('/portfolio/dashboard/cashflow', {
      headers: getAuthHeaders(),
    })
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

export async function getHistory(params: {
  period?: string
  limit?: number
} = {}): Promise<HistoryData> {
  try {
    const { data } = await apiClient.get<HistoryData>('/portfolio/history', {
      params,
      headers: getAuthHeaders(),
    })
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

export async function takeSnapshot(): Promise<{
  id: number
  date: string
  total_value: number
  cash: number
  invested_value: number
  pnl: number
}> {
  try {
    const { data } = await apiClient.post('/portfolio/history/take-snapshot', {}, {
      headers: getAuthHeaders(),
    })
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

export async function getPortfolioAdjustment(): Promise<PortfolioAdjustmentData> {
  try {
    const { data } = await apiClient.get<PortfolioAdjustmentData>('/recommendations', {
      params: { mode: 'portfolio_adjustment' },
      headers: getAuthHeaders(),
    })
    return data
  } catch (err) {
    throw toFriendlyError(err)
  }
}

export { apiClient }