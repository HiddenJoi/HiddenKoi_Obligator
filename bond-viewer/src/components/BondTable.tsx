import { useState } from "react"
import { formatYield, formatDuration, formatNkd, formatPrice } from "../utils/format"
import type { BondListItem, SortBy } from "../types"

type ColKey = "star" | "name" | "issuer" | "yield" | "duration" | "nkd" | "price" | "coupon_type" | "offer" | "amortization"

interface ColDef {
  key: ColKey
  label: string
  sortBy?: SortBy
  align: "left" | "right" | "center"
  className?: string
}

const COLUMNS: ColDef[] = [
  { key: "star",        label: "",            sortBy: undefined,       align: "center", className: "w-10" },
  { key: "name",        label: "Название",    sortBy: "name_desc",    align: "left"   },
  { key: "issuer",      label: "Эмитент",     sortBy: undefined,       align: "left"   },
  { key: "yield",       label: "YTM (%)",     sortBy: "yield_desc",   align: "right"  },
  { key: "duration",    label: "Дюрация",     sortBy: "duration_desc", align: "right" },
  { key: "nkd",         label: "НКД (руб)",   sortBy: undefined,      align: "right" },
  { key: "price",       label: "Цена (%)",    sortBy: undefined,      align: "right" },
  { key: "coupon_type", label: "Купон",       sortBy: undefined,      align: "center", className: "w-16" },
  { key: "offer",       label: "Оферта",      sortBy: undefined,      align: "center", className: "w-14" },
  { key: "amortization",label: "Аморт.",       sortBy: undefined,      align: "center", className: "w-14" },
]

function isBondEmpty(bond: BondListItem): boolean {
  return (
    (bond.close_price ?? null) === null &&
    (bond.yield_rate ?? null) === null &&
    (bond.ytw ?? null) === null &&
    (bond.nkd ?? null) === null &&
    (bond.duration ?? null) === null
  )
}

function CouponBadge({ couponType }: { couponType: string | null | undefined }) {
  const map: Record<string, string> = {
    fixed: "FIX",
    float: "FLT",
    zero: "ZRO",
  }
  const label = couponType ? map[couponType.toLowerCase()] ?? couponType.toUpperCase() : "—"
  const color =
    couponType?.toLowerCase() === "float"  ? "bg-teal-100 text-teal-700" :
    couponType?.toLowerCase() === "zero"   ? "bg-orange-100 text-orange-700" :
                                             "bg-slate-100 text-slate-600"
  return (
    <span className={`inline-block px-1.5 py-0.5 rounded text-xs font-semibold ${color}`}>
      {label}
    </span>
  )
}

function FlagBadge({ value }: { value: boolean | null | undefined }) {
  if (value === true) {
    return <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-green-100 text-green-600 text-xs font-bold">✓</span>
  }
  if (value === false) {
    return <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-slate-100 text-slate-400 text-xs">—</span>
  }
  return <span className="text-slate-300 text-xs">—</span>
}

interface SortableHeaderProps {
  col: ColDef
  activeCol: ColKey | null
  activeDir: "asc" | "desc"
  onSort: (sortBy: SortBy) => void
}

function SortableHeader({ col, activeCol, activeDir, onSort }: SortableHeaderProps) {
  const isActive = col.key === activeCol

  let icon = ""
  if (col.sortBy) {
    if (isActive) {
      icon = activeDir === "desc" ? " ↓" : " ↑"
    } else {
      icon = " ⇅"
    }
  }

  return (
    <th
      className={[
        "px-3 py-3 text-sm font-semibold select-none transition-colors whitespace-nowrap",
        col.align === "right" ? "text-right" : col.align === "center" ? "text-center" : "text-left",
        col.sortBy ? "cursor-pointer hover:text-blue-600" : "cursor-default",
        isActive ? "text-blue-600" : "text-slate-600",
        col.className ?? "",
      ].join(" ")}
      onClick={col.sortBy ? () => onSort(col.sortBy!) : undefined}
      aria-sort={isActive ? (activeDir === "desc" ? "descending" : "ascending") : undefined}
    >
      {col.label}
      {col.sortBy && <span className="ml-1 text-xs">{icon}</span>}
    </th>
  )
}

function SkeletonRows() {
  return (
    <>
      {Array.from({ length: 8 }).map((_, i) => (
        <tr key={i} className="animate-pulse border-b border-slate-100">
          <td className="px-4 py-3">
            <div className="h-4 bg-slate-200 rounded w-40 mb-1"/>
            <div className="h-3 bg-slate-100 rounded w-16"/>
          </td>
          <td className="px-4 py-3">
            <div className="h-4 bg-slate-200 rounded w-28"/>
          </td>
          <td className="px-4 py-3 text-right">
            <div className="h-4 bg-slate-200 rounded w-16 ml-auto"/>
          </td>
          <td className="px-4 py-3 text-right">
            <div className="h-4 bg-slate-200 rounded w-12 ml-auto"/>
          </td>
          <td className="px-4 py-3 text-right">
            <div className="h-4 bg-slate-200 rounded w-16 ml-auto"/>
          </td>
          <td className="px-4 py-3 text-right">
            <div className="h-4 bg-slate-200 rounded w-16 ml-auto"/>
          </td>
        </tr>
      ))}
    </>
  )
}

function EmptyState() {
  return (
    <tr>
      <td colSpan={10} className="px-4 py-16 text-center text-slate-400">
        <div className="flex flex-col items-center gap-3">
          <span className="text-4xl">📭</span>
          <p className="text-base font-medium">Нет данных</p>
          <p className="text-sm">Попробуйте изменить параметры фильтрации</p>
        </div>
      </td>
    </tr>
  )
}

interface ErrorStateProps {
  message: string
  onRetry: () => void
}

function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <tr>
      <td colSpan={10} className="px-4 py-12 text-center">
        <div className="flex flex-col items-center gap-3">
          <span className="text-4xl">⚠️</span>
          <p className="text-red-600 font-medium">{message}</p>
          <button
            onClick={onRetry}
            className="mt-1 px-5 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 transition-colors"
          >
            Повторить
          </button>
        </div>
      </td>
    </tr>
  )
}

export interface BondTableProps {
  bonds: BondListItem[]
  loading: boolean
  error: string | null
  sortBy: SortBy | ""
  sortDir: "asc" | "desc"
  onSort: (sortBy: SortBy) => void
  onRetry: () => void
  favorites: Set<string>
  onToggleFavorite: (secid: string) => void
  hideEmpty?: boolean   // скрыть облигации без рыночных данных
}

export function BondTable({ bonds, loading, error, sortBy, sortDir, onSort, onRetry, favorites, onToggleFavorite, hideEmpty }: BondTableProps) {
  const [hovered, setHovered] = useState<string | null>(null)

  const visibleBonds = hideEmpty ? bonds.filter((b) => !isBondEmpty(b)) : bonds

  const activeCol: ColKey | null = sortBy
    ? (sortBy.replace(/_desc$|_asc$/, "") as ColKey)
    : null

  function handleSort(colSortBy: SortBy) {
    const base = colSortBy.replace(/_desc$|_asc$/, "")
    const currentDir =
      sortBy?.startsWith(base) && sortBy?.endsWith("_asc") ? "asc" : "desc"
    const newDir: "asc" | "desc" = currentDir === "desc" ? "asc" : "desc"
    onSort((base + "_" + newDir) as SortBy)
  }

  return (
    <div className="overflow-x-auto rounded-xl border border-slate-200 shadow-sm">
      <table className="w-full text-sm">
        <thead className="bg-slate-50 border-b border-slate-200">
          <tr>
            {COLUMNS.map((col) => (
              <SortableHeader
                key={col.key}
                col={col}
                activeCol={activeCol}
                activeDir={sortDir}
                onSort={handleSort}
              />
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 bg-white">
          {loading ? (
            <SkeletonRows />
          ) : error ? (
            <ErrorState message={error} onRetry={onRetry} />
          ) : visibleBonds.length === 0 ? (
            <EmptyState />
          ) : (
            visibleBonds.map((bond) => (
              <tr
                key={bond.secid}
                className={[
                  "transition-colors cursor-default",
                  hovered === bond.secid ? "bg-blue-50" : "hover:bg-slate-50",
                ].join(" ")}
                onMouseEnter={() => setHovered(bond.secid)}
                onMouseLeave={() => setHovered(null)}
              >
                {/* Star / Favorite column */}
                <td className="px-3 py-3 text-center w-10">
                  <button
                    onClick={(e) => { e.stopPropagation(); onToggleFavorite(bond.secid) }}
                    className="text-lg leading-none focus:outline-none hover:scale-110 transition-transform"
                    title={favorites.has(bond.secid) ? "Убрать из избранного" : "Добавить в избранное"}
                  >
                    {favorites.has(bond.secid) ? "⭐" : "☆"}
                  </button>
                </td>
                <td className="px-4 py-3 max-w-xs">
                  <div className="font-medium text-slate-800 truncate">{bond.name}</div>
                  <div className="text-slate-400 text-xs">{bond.secid}</div>
                </td>
                <td className="px-4 py-3 text-slate-600 max-w-xs truncate">
                  {bond.issuer ?? "—"}
                </td>
                <td className="px-4 py-3 text-right font-semibold text-slate-700 whitespace-nowrap">
                  {formatYield(bond.yield_rate ?? bond.ytw)}
                </td>
                <td className="px-4 py-3 text-right text-slate-600 whitespace-nowrap">
                  {formatDuration(bond.duration)}
                </td>
                <td className="px-4 py-3 text-right text-slate-600 whitespace-nowrap">
                  {formatNkd(bond.nkd)}
                </td>
                <td className="px-4 py-3 text-right text-slate-600 whitespace-nowrap">
                  {formatPrice(bond.close_price)}
                </td>
                <td className="px-3 py-3 text-center">
                  <CouponBadge couponType={bond.coupon_type} />
                </td>
                <td className="px-3 py-3 text-center">
                  <FlagBadge value={bond.has_offer} />
                </td>
                <td className="px-3 py-3 text-center">
                  <FlagBadge value={bond.has_amortization} />
                </td>
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  )
}
