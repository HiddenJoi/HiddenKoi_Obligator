import React from "react"
import type { BondListItem } from "../types"

interface TableSettingsProps {
  showFavoritesOnly: boolean
  onShowFavoritesChange: (v: boolean) => void
  hideEmpty: boolean
  onHideEmptyChange: (v: boolean) => void
  data: BondListItem[]
  favorites: Set<string>
}

export function TableSettings({
  showFavoritesOnly,
  onShowFavoritesChange,
  hideEmpty,
  onHideEmptyChange,
  data,
  favorites,
}: TableSettingsProps) {
  function handleExportCSV() {
    const headers = ["SECID", "Название", "Эмитент", "Доходность (%)", "Дюрация", "НКД", "Цена (%)", "Купон", "Оферта", "Аморт."]
    const rows = data.map((b) => [
      b.secid,
      `"${(b.name || "").replace(/"/g, '""')}"`,
      `"${(b.issuer || "").replace(/"/g, '""')}"`,
      b.yield_rate ?? b.ytw ?? "",
      b.duration ?? "",
      b.nkd ?? "",
      b.close_price ?? "",
      b.coupon_type ?? "",
      b.has_offer === true ? "✓" : b.has_offer === false ? "—" : "",
      b.has_amortization === true ? "✓" : b.has_amortization === false ? "—" : "",
    ])
    const csv = [headers.join(","), ...rows.map((r) => r.join(","))].join("\n")
    const blob = new Blob(["\ufeff" + csv], { type: "text/csv;charset=utf-8" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `export_${new Date().toISOString().slice(0, 10)}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="flex items-center justify-between flex-wrap gap-3">
      {/* Left controls */}
      <div className="flex items-center gap-4 flex-wrap">
        {/* Favorites toggle */}
        <label className="flex items-center gap-2 cursor-pointer select-none">
          <input
            type="checkbox"
            checked={showFavoritesOnly}
            onChange={(e) => onShowFavoritesChange(e.target.checked)}
            className="w-4 h-4 rounded border-slate-400 text-blue-600 focus:ring-blue-500"
          />
          <span className="text-sm text-slate-700">
            ⭐ Избранное
            {favorites.size > 0 && (
              <span className="ml-1 text-xs text-slate-400">({favorites.size})</span>
            )}
          </span>
        </label>

        <label className="flex items-center gap-2 cursor-pointer select-none">
          <input
            type="checkbox"
            checked={hideEmpty}
            onChange={(e) => onHideEmptyChange(e.target.checked)}
            className="w-4 h-4 rounded border-slate-400 text-blue-600 focus:ring-blue-500"
          />
          <span className="text-sm text-slate-700">Скрыть пустые</span>
        </label>
      </div>

      {/* Export button */}
      <button
        onClick={handleExportCSV}
        className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-slate-700 border border-slate-300 rounded-lg hover:bg-slate-100 transition-colors"
        title="Экспорт текущих данных в CSV"
      >
        <span>📥</span>
        Экспорт в CSV
      </button>
    </div>
  )
}
