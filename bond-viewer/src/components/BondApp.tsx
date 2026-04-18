import { useState, useEffect, useMemo } from "react"
import { getBonds } from "../services/api"
import type { BondListItem, BondListResponse } from "../types"
import { useFilters } from "../hooks/useFilters"
import { useUrlParams } from "../hooks/useUrlParams"
import { FilterPanel } from "./FilterPanel"
import { BondTable } from "./BondTable"
import { Pagination, PaginationInfo, LimitSelect } from "./Pagination"
import { TableSettings } from "./TableSettings"

const STORAGE_KEY_FAVORITES = "bond_favorites"
const STORAGE_KEY_LIMIT = "bond_table_limit"
const STORAGE_KEY_FAVORITES_ONLY = "bond_show_favorites_only"
const STORAGE_KEY_HIDE_EMPTY = "bond_hide_empty"

const DEFAULT_LIMIT = 25

function loadFromStorage<T>(key: string, fallback: T): T {
  try {
    const stored = localStorage.getItem(key)
    if (stored !== null) {
      return JSON.parse(stored) as T
    }
  } catch {
    // ignore parse errors
  }
  return fallback
}

function saveToStorage<T>(key: string, value: T): void {
  try {
    localStorage.setItem(key, JSON.stringify(value))
  } catch {
    // ignore storage errors
  }
}

export function BondApp() {
  // ── Core filter state ─────────────────────────────────────────────
  const {
    pending, applied, updatePending, apply, reset,
    sortBy: defaultSortBy, setSortBy: setDefaultSortBy,
    page, setPage,
    activeCount, isDefault,
  } = useFilters()

  // ── Pagination state ───────────────────────────────────────────────
  const [limit, setLimit] = useState<number>(() =>
    loadFromStorage(STORAGE_KEY_LIMIT, DEFAULT_LIMIT)
  )
  const [total, setTotal] = useState(0)

  // ── Favorites state ────────────────────────────────────────────────
  const [favorites, setFavorites] = useState<Set<string>>(() => {
    const stored = loadFromStorage<string[]>(STORAGE_KEY_FAVORITES, [])
    return new Set(stored)
  })
  const [showFavoritesOnly, setShowFavoritesOnly] = useState<boolean>(() =>
    loadFromStorage(STORAGE_KEY_FAVORITES_ONLY, false)
  )
  const [hideEmpty, setHideEmpty] = useState<boolean>(() =>
    loadFromStorage(STORAGE_KEY_HIDE_EMPTY, false)
  )

  function toggleFavorite(secid: string) {
    setFavorites((prev) => {
      const next = new Set(prev)
      if (next.has(secid)) next.delete(secid)
      else next.add(secid)
      return next
    })
  }

  // ── Table settings from localStorage ─────────────────────────────

  // ── Data state ───────────────────────────────────────────────────
  const [rawBonds, setRawBonds] = useState<BondListItem[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // ── Computed values ───────────────────────────────────────────────
  const totalPages = Math.ceil(total / limit) || 1
  const currentPage = Math.min(page, totalPages - 1)
  const from = total > 0 ? page * limit + 1 : 0
  const to = Math.min((page + 1) * limit, total)

  // Filter bonds by favorites
  const displayedBonds = useMemo(() => {
    let result = rawBonds
    if (showFavoritesOnly) result = result.filter((b) => favorites.has(b.secid))
    return result
  }, [rawBonds, showFavoritesOnly, favorites])

  // ── Sync URL <- local state ──────────────────────────────────────────
  const { syncToUrl } = useUrlParams({
    pending, applied,
    sortBy: defaultSortBy, page: currentPage,
    onFiltersFromUrl: updatePending,
    onSortByFromUrl: setDefaultSortBy,
    onPageFromUrl: setPage,
  })

  // ── Persist table settings to localStorage ────────────────────────
  useEffect(() => {
    saveToStorage(STORAGE_KEY_LIMIT, limit)
  }, [limit])

  useEffect(() => {
    saveToStorage(STORAGE_KEY_FAVORITES_ONLY, showFavoritesOnly)
  }, [showFavoritesOnly])

  useEffect(() => {
    saveToStorage(STORAGE_KEY_FAVORITES, Array.from(favorites))
  }, [favorites])

  useEffect(() => {
    saveToStorage(STORAGE_KEY_HIDE_EMPTY, hideEmpty)
  }, [hideEmpty])

  // ── Fetch bonds from API ──────────────────────────────────────────
  useEffect(() => {
    async function load() {
      setLoading(true)
      setError(null)
      try {
        // Use limit from state, not from toParams
        const data: BondListResponse = await getBonds({
          limit,
          offset: currentPage * limit,
          sort_by: defaultSortBy || undefined,
          search: applied.search || undefined,
          min_yield: applied.minYield,
          max_yield: applied.maxYield,
          min_duration: applied.minDuration,
          max_duration: applied.maxDuration,
          coupon_type: applied.couponType,
          has_offer: applied.hasOffer || undefined,
          has_amortization: applied.hasAmortization || undefined,
        })
        setRawBonds(data.bonds)
        setTotal(data.total)
        syncToUrl(applied, defaultSortBy, currentPage)
      } catch (err) {
        setError(err instanceof Error ? err.message : "Неизвестная ошибка")
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [applied, defaultSortBy, currentPage, limit]) // eslint-disable-line

  // ── Handlers ──────────────────────────────────────────────────────
  function handleApply() {
    apply()
    setPage(0)
  }

  function handleReset() {
    reset()
    setPage(0)
  }

  function handleRetry() {
    setError(null)
  }

  function handleLimitChange(newLimit: number) {
    setLimit(newLimit)
    setPage(0)
  }

  function handlePageChange(newPage: number) {
    setPage(newPage)
    window.scrollTo({ top: 0, behavior: "smooth" })
  }

  const sortDir: "asc" | "desc" = defaultSortBy?.endsWith("_asc") ? "asc" : "desc"

  return (
    <div className="space-y-6">
      <FilterPanel
        pending={pending}
        onUpdate={updatePending}
        onApply={handleApply}
        onReset={handleReset}
        activeCount={activeCount}
        isDefault={isDefault}
      />

      {/* Table settings bar */}
      <TableSettings
        showFavoritesOnly={showFavoritesOnly}
        onShowFavoritesChange={setShowFavoritesOnly}
        hideEmpty={hideEmpty}
        onHideEmptyChange={setHideEmpty}
        data={displayedBonds}
        favorites={favorites}
      />

      {/* Pagination info + limit selector */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <PaginationInfo from={from} to={to} total={total} />
        <LimitSelect value={limit} onChange={handleLimitChange} />
      </div>

      <BondTable
        bonds={displayedBonds}
        loading={loading}
        error={error}
        sortBy={defaultSortBy}
        sortDir={sortDir}
        onSort={setDefaultSortBy}
        onRetry={handleRetry}
        favorites={favorites}
        onToggleFavorite={toggleFavorite}
        hideEmpty={hideEmpty}
      />

      <div className="flex justify-center">
        <Pagination
          currentPage={currentPage}
          totalPages={totalPages}
          onPageChange={handlePageChange}
        />
      </div>
    </div>
  )
}
