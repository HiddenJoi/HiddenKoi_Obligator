import { useState, useCallback, useMemo } from 'react'
import type { BondsFilterParams, CouponType, SortBy } from '../types'

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

const DEFAULT_FILTERS: FilterState = {
  minYield: undefined,
  maxYield: undefined,
  minDuration: undefined,
  maxDuration: undefined,
  couponType: undefined,
  search: '',
  hasOffer: false,
  hasAmortization: false,
}

function isDefault(f: FilterState): boolean {
  return (
    f.minYield === undefined &&
    f.maxYield === undefined &&
    f.minDuration === undefined &&
    f.maxDuration === undefined &&
    f.couponType === undefined &&
    f.search === '' &&
    !f.hasOffer &&
    !f.hasAmortization
  )
}

function countActive(f: FilterState): number {
  let n = 0
  if (f.minYield !== undefined || f.maxYield !== undefined) n++
  if (f.minDuration !== undefined || f.maxDuration !== undefined) n++
  if (f.couponType !== undefined) n++
  if (f.search !== '') n++
  if (f.hasOffer) n++
  if (f.hasAmortization) n++
  return n
}

export function filtersToParams(f: FilterState, sortBy: SortBy | '', page: number): URLSearchParams {
  const p = new URLSearchParams()
  if (f.minYield !== undefined) p.set('min_yield', String(f.minYield))
  if (f.maxYield !== undefined) p.set('max_yield', String(f.maxYield))
  if (f.minDuration !== undefined) p.set('min_duration', String(f.minDuration))
  if (f.maxDuration !== undefined) p.set('max_duration', String(f.maxDuration))
  if (f.couponType) p.set('coupon_type', f.couponType)
  if (f.search) p.set('search', f.search)
  if (f.hasOffer) p.set('has_offer', '1')
  if (f.hasAmortization) p.set('has_amortization', '1')
  if (sortBy) p.set('sort_by', sortBy)
  if (page > 0) p.set('offset', String(page))
  return p
}

export function filtersFromUrl(p: URLSearchParams): FilterState {
  const num = (v: string | null): number | undefined => {
    if (v === null || v === '') return undefined
    const n = Number(v)
    return isNaN(n) ? undefined : n
  }
  return {
    minYield: num(p.get('min_yield')),
    maxYield: num(p.get('max_yield')),
    minDuration: num(p.get('min_duration')),
    maxDuration: num(p.get('max_duration')),
    couponType: (p.get('coupon_type') ?? undefined) as CouponType | undefined,
    search: p.get('search') ?? '',
    hasOffer: p.get('has_offer') === '1',
    hasAmortization: p.get('has_amortization') === '1',
  }
}

export function useFilters(): {
  pending: FilterState
  applied: FilterState
  setPending: (f: FilterState) => void
  updatePending: <K extends keyof FilterState>(k: K, v: FilterState[K]) => void
  apply: () => void
  reset: () => void
  sortBy: SortBy | ''
  setSortBy: (v: SortBy | '') => void
  page: number
  setPage: (p: number) => void
  toParams: () => URLSearchParams
  activeCount: number
  isDefault: boolean
} {
  const [pending, setPending] = useState<FilterState>(DEFAULT_FILTERS)
  const [applied, setApplied] = useState<FilterState>(DEFAULT_FILTERS)
  const [sortBy, setSortBy] = useState<SortBy | ''>('yield_desc')
  const [page, setPage] = useState(0)

  const updatePending = useCallback(<K extends keyof FilterState>(k: K, v: FilterState[K]) => {
    setPending(prev => ({ ...prev, [k]: v }))
  }, [])

  const apply = useCallback(() => {
    setApplied(pending)
    setPage(0)
  }, [pending])

  const reset = useCallback(() => {
    setPending(DEFAULT_FILTERS)
    setApplied(DEFAULT_FILTERS)
    setPage(0)
  }, [])

  const toParams = useCallback(() => {
    return filtersToParams(applied, sortBy, page)
  }, [applied, sortBy, page])

  return {
    pending,
    applied,
    setPending,
    updatePending,
    apply,
    reset,
    sortBy,
    setSortBy,
    page,
    setPage,
    toParams,
    activeCount: countActive(applied),
    isDefault: isDefault(applied),
  }
}
