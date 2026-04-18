import { useEffect } from 'react'
import { useSearchParams } from 'react-router-dom'
import { useDebounce } from './useDebounce'
import { filtersFromUrl, filtersToParams } from './useFilters'
import type { FilterState, SortBy } from '../types'

interface UseUrlParamsOptions {
  pending: FilterState
  applied: FilterState
  sortBy: SortBy | ''
  page: number
  onFiltersFromUrl: (f: FilterState) => void
  onSortByFromUrl: (s: SortBy | '') => void
  onPageFromUrl: (p: number) => void
}

export function useUrlParams({
  pending,
  applied,
  sortBy,
  page,
  onFiltersFromUrl,
  onSortByFromUrl,
  onPageFromUrl,
}: UseUrlParamsOptions): { syncToUrl: (f: FilterState, s: SortBy | '', p: number) => void } {
  const [searchParams, setSearchParams] = useSearchParams()

  // Initialize state from URL on mount
  useEffect(() => {
    const f = filtersFromUrl(searchParams)
    const s = (searchParams.get('sort_by') ?? '') as SortBy | ''
    const o = Number(searchParams.get('offset') ?? '0')
    onFiltersFromUrl(f)
    onSortByFromUrl(s || 'yield_desc')
    onPageFromUrl(isNaN(o) ? 0 : o)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []) // run once on mount

  // Sync applied filters + sort + page to URL
  const syncToUrl = (f: FilterState, s: SortBy | '', p: number) => {
    const params = filtersToParams(f, s, p)
    setSearchParams(params, { replace: true })
  }

  return { syncToUrl }
}
