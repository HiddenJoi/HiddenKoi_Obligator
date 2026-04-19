import { useEffect, useRef } from 'react'
import { useSearchParams } from 'react-router-dom'
import type { FilterState, SortBy } from '../types'

interface UseUrlParamsOptions {
  pending: FilterState
  applied: FilterState
  sortBy: SortBy | ''
  page: number
  limit: number
  onFiltersFromUrl: (f: FilterState) => void
  onSortByFromUrl: (s: SortBy | '') => void
  onPageFromUrl: (p: number) => void
}

export function useUrlParams({
  pending,
  applied,
  sortBy,
  page,
  limit,
  onFiltersFromUrl,
  onSortByFromUrl,
  onPageFromUrl,
}: UseUrlParamsOptions): { syncToUrl: (f: FilterState, s: SortBy | '', p: number) => void } {
  const [searchParams, setSearchParams] = useSearchParams()

  // Keep mutable refs so effects always read the latest values without causing dependency loops
  const limitRef = useRef(limit)
  limitRef.current = limit

  const onPageFromUrlRef = useRef(onPageFromUrl)
  onPageFromUrlRef.current = onPageFromUrl

  // ── Init from URL on mount ────────────────────────────────────────────
  const initDoneRef = useRef(false)
  useEffect(() => {
    if (initDoneRef.current) return
    initDoneRef.current = true

    const p = searchParams
    const f = filtersFromUrl(p)
    const s = (p.get('sort_by') ?? '') as SortBy | ''
    const rawOffset = Number(p.get('offset') ?? '0')
    onFiltersFromUrl(f)
    onSortByFromUrl(s || 'yield_desc')
    const l = limitRef.current
    const calculatedPage = l > 0 ? Math.floor(rawOffset / l) : 0
    onPageFromUrlRef.current(isNaN(calculatedPage) || calculatedPage < 0 ? 0 : calculatedPage)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []) // run once on mount only

  // ── Recalculate page when limit changes (e.g. from localStorage) ─────
  useEffect(() => {
    if (!initDoneRef.current) return
    const rawOffset = Number(searchParams.get('offset') ?? '0')
    const l = limitRef.current
    const calculatedPage = l > 0 ? Math.floor(rawOffset / l) : 0
    onPageFromUrlRef.current(isNaN(calculatedPage) || calculatedPage < 0 ? 0 : calculatedPage)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [limit]) // re-evaluate page when limit changes

  // ── Sync applied filters + sort + page to URL ───────────────────────
  const syncToUrl = (f: FilterState, s: SortBy | '', p: number) => {
    const params = filtersToParams(f, s, p, limitRef.current)
    setSearchParams(params, { replace: true })
  }

  return { syncToUrl }
}

function num(v: string | null): number | undefined {
  if (v === null || v === '') return undefined
  const n = Number(v)
  return isNaN(n) ? undefined : n
}

function filtersFromUrl(p: URLSearchParams) {
  return {
    minYield: num(p.get('min_yield')),
    maxYield: num(p.get('max_yield')),
    minDuration: num(p.get('min_duration')),
    maxDuration: num(p.get('max_duration')),
    couponType: (p.get('coupon_type') ?? undefined) as any,
    search: p.get('search') ?? '',
    hasOffer: p.get('has_offer') === '1',
    hasAmortization: p.get('has_amortization') === '1',
  }
}

function filtersToParams(f: FilterState, sortBy: SortBy | '', page: number, limitForCalc: number): URLSearchParams {
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
  p.set('offset', String(page * limitForCalc))
  return p
}
