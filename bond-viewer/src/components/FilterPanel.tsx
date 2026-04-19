import { useEffect } from "react"
import { useDebounce } from "../hooks/useDebounce"
import type { FilterState, CouponType } from "../types"

interface FilterPanelProps {
  pending: FilterState
  onUpdate: <K extends keyof FilterState>(k: K, v: FilterState[K]) => void
  onApply: () => void
  onReset: () => void
  activeCount: number
  isDefault: boolean
}

function clamp(v: number, min: number, max: number) {
  return Math.min(max, Math.max(min, v))
}

function NumInput({
  label, value, onChange, min, max, step = 1, placeholder,
}: {
  label: string
  value: number | undefined
  onChange: (v: number | undefined) => void
  min: number
  max: number
  step?: number
  placeholder?: string
}) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-xs text-slate-500 font-medium">{label}</span>
      <input
        type="number"
        value={value ?? ""}
        placeholder={placeholder ?? String(min)}
        min={min} max={max} step={step}
        onChange={(e) => {
          const raw = e.target.value
          if (raw === "") { onChange(undefined); return }
          const n = Number(raw)
          onChange(isNaN(n) ? undefined : clamp(n, min, max))
        }}
        className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors"
      />
    </label>
  )
}

function Checkbox({ label, checked, onChange }: { label: string; checked: boolean; onChange: (v: boolean) => void }) {
  return (
    <label className="flex items-center gap-2 cursor-pointer select-none group">
      <div className="relative">
        <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} className="peer sr-only" />
        <div className="w-4 h-4 border-2 border-slate-300 rounded bg-white
          peer-checked:bg-blue-600 peer-checked:border-blue-600
          transition-colors group-hover:border-blue-400" />
        <svg className="absolute inset-0 m-auto w-2.5 h-2.5 text-white opacity-0 peer-checked:opacity-100 transition-opacity pointer-events-none"
          fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
        </svg>
      </div>
      <span className="text-sm text-slate-600 group-hover:text-slate-800 transition-colors">{label}</span>
    </label>
  )
}

export function FilterPanel({ pending, onUpdate, onApply, onReset, activeCount, isDefault }: FilterPanelProps) {
  const debouncedSearch = useDebounce(pending.search, 300)

  useEffect(() => {
    if (debouncedSearch !== pending.search) {
      onUpdate("search", debouncedSearch)
    }
  }, [debouncedSearch, pending.search, onUpdate])

  const couponOptions: { label: string; value: CouponType | undefined }[] = [
    { label: "Все типы", value: undefined },
    { label: "Fixed",    value: "fixed" },
    { label: "Float",    value: "float" },
    { label: "Zero",     value: "zero" },
  ]

  return (
    <div className="bg-white rounded-2xl shadow-md border border-slate-200 overflow-hidden">
      <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between gap-4 flex-wrap">
        <div className="flex items-center gap-2">
          <span className="text-base font-semibold text-slate-800">Фильтры</span>
          {activeCount > 0 && (
            <span className="inline-flex items-center justify-center w-5 h-5 text-xs font-bold text-white bg-blue-600 rounded-full">
              {activeCount}
            </span>
          )}
        </div>
        {!isDefault && (
          <button onClick={onReset} className="text-xs text-slate-500 hover:text-red-500 transition-colors underline underline-offset-2">
            Сбросить все
          </button>
        )}
      </div>

      <div className="p-5 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-x-6 gap-y-5">
        <div className="flex flex-col gap-2">
          <span className="text-xs text-slate-500 font-medium">YTM, %</span>
          <div className="flex gap-2 items-center">
            <NumInput label="От" value={pending.minYield} onChange={(v) => onUpdate("minYield", v)} min={0} max={30} step={0.01} placeholder="0" />
            <span className="text-slate-400 text-sm mt-4">&#8212;</span>
            <NumInput label="До" value={pending.maxYield} onChange={(v) => onUpdate("maxYield", v)} min={0} max={30} step={0.01} placeholder="30" />
          </div>
        </div>

        <div className="flex flex-col gap-2">
          <span className="text-xs text-slate-500 font-medium">Дюрация, дн</span>
          <div className="flex gap-2 items-center">
            <NumInput label="От" value={pending.minDuration} onChange={(v) => onUpdate("minDuration", v)} min={0} max={2000} step={1} placeholder="0" />
            <span className="text-slate-400 text-sm mt-4">&#8212;</span>
            <NumInput label="До" value={pending.maxDuration} onChange={(v) => onUpdate("maxDuration", v)} min={0} max={2000} step={1} placeholder="2000" />
          </div>
        </div>

        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-500 font-medium">Тип купона</label>
          <select
            value={pending.couponType ?? ""}
            onChange={(e) => onUpdate("couponType", (e.target.value || undefined) as CouponType | undefined)}
            className="border border-slate-300 rounded-lg px-3 py-1.5 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors bg-white"
          >
            {couponOptions.map((o) => (
              <option key={String(o.value)} value={o.value ?? ""}>{o.label}</option>
            ))}
          </select>
        </div>

        <div className="flex flex-col gap-1">
          <label className="text-xs text-slate-500 font-medium">
            Поиск <span className="ml-1 text-slate-400 font-normal">(debounce 300ms)</span>
          </label>
          <div className="relative">
            <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400 pointer-events-none"
              fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-4.35-4.35M17 11A6 6 0 115 11a6 6 0 0112 0z" />
            </svg>
            <input
              type="text"
              value={pending.search}
              onChange={(e) => onUpdate("search", e.target.value)}
              placeholder="ОФЗ, Сбер, Газпром&#8230;"
              className="w-full border border-slate-300 rounded-lg pl-9 pr-3 py-1.5 text-sm text-slate-700 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-colors"
            />
          </div>
        </div>

        <div className="flex flex-col gap-3 justify-end">
          <Checkbox label="Только с офертой" checked={pending.hasOffer} onChange={(v) => onUpdate("hasOffer", v)} />
          <Checkbox label="Только с амортизацией" checked={pending.hasAmortization} onChange={(v) => onUpdate("hasAmortization", v)} />
        </div>
      </div>

      <div className="px-5 py-4 border-t border-slate-100 flex justify-end">
        <button
          onClick={onApply}
          className="px-6 py-2 bg-blue-600 text-white text-sm font-medium rounded-xl hover:bg-blue-700 active:scale-95 transition-all shadow-sm"
        >
          Применить фильтры
        </button>
      </div>
    </div>
  )
}