import { useState, useEffect } from 'react'
import { getHistory } from '../services/api'
import type { HistoryData, HistoryPoint } from '../types'

export function PortfolioHistory() {
  const [data, setData] = useState<HistoryData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [period, setPeriod] = useState<'daily' | 'weekly' | 'monthly'>('daily')
  const [showCash, setShowCash] = useState(false)

  useEffect(() => {
    async function load() {
      setLoading(true)
      setError(null)
      try {
        const d = await getHistory({ period, limit: 180 })
        setData(d)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Неизвестная ошибка')
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [period])

  const fmt = (n: number) => n.toLocaleString('ru-RU', { style: 'currency', currency: 'RUB', maximumFractionDigits: 0 })

  if (loading) return (
    <div className="animate-pulse space-y-4">
      <div className="h-64 bg-slate-100 rounded-xl" />
    </div>
  )

  if (error) return (
    <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-red-700 text-sm">{error}</div>
  )

  if (!data || data.points.length === 0) {
    return (
      <div className="bg-white rounded-xl border border-slate-200 p-8 text-center text-slate-400 text-sm">
        Нет данных для отображения истории портфеля
      </div>
    )
  }

  const points = [...data.points].reverse()
  const maxValue = Math.max(...points.map(p => p.value))
  const minValue = Math.min(...points.map(p => p.value))
  const range = maxValue - minValue || 1

  return (
    <div className="space-y-4">
      {/* Controls */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div className="flex gap-2">
          {(['daily', 'weekly', 'monthly'] as const).map(p => (
            <button key={p} onClick={() => setPeriod(p)}
              className={`px-3 py-1.5 rounded-lg text-sm ${period === p ? 'bg-blue-600 text-white' : 'bg-white border border-slate-200 text-slate-600'}`}>
              {p === 'daily' ? 'Дни' : p === 'weekly' ? 'Недели' : 'Месяцы'}
            </button>
          ))}
        </div>
        <label className="flex items-center gap-2 text-sm text-slate-600">
          <input type="checkbox" checked={showCash} onChange={e => setShowCash(e.target.checked)} />
          Показывать наличные
        </label>
      </div>

      {/* Simple SVG chart */}
      <div className="bg-white rounded-xl border border-slate-200 p-6">
        <svg viewBox={`0 0 ${points.length * 8} 200`} className="w-full" style={{ height: 200, minHeight: 200 }}>
          {/* Grid lines */}
          {[0, 0.25, 0.5, 0.75, 1].map(ratio => {
            const y = 10 + ratio * 180
            const val = minValue + ratio * range
            return (
              <g key={ratio}>
                <line x1="0" y1={y} x2={points.length * 8} y2={y} stroke="#e2e8f0" strokeWidth="0.5" />
                <text x="0" y={y - 2} fill="#94a3b8" fontSize="8" textAnchor="start">{fmt(val)}</text>
              </g>
            )
          })}

          {/* Main value line */}
          <polyline
            points={points.map((p, i) => `${i * 8},${10 + (1 - (p.value - minValue) / range) * 180}`).join(' ')}
            fill="none"
            stroke="#3b82f6"
            strokeWidth="1.5"
            strokeLinejoin="round"
            strokeLinecap="round"
          />

          {/* Cash area (if enabled) */}
          {showCash && (
            <polyline
              points={points.map((p, i) => `${i * 8},${10 + (1 - ((p.cash ?? 0) - minValue) / range) * 180}`).join(' ')}
              fill="none"
              stroke="#f59e0b"
              strokeWidth="1"
              strokeDasharray="3,2"
              strokeLinejoin="round"
            />
          )}
        </svg>

        {/* Legend */}
        <div className="flex gap-4 mt-3 justify-center">
          <div className="flex items-center gap-1.5 text-xs text-slate-600">
            <div className="w-4 h-0.5 bg-blue-500 rounded" />
            Общая стоимость
          </div>
          {showCash && (
            <div className="flex items-center gap-1.5 text-xs text-slate-600">
              <div className="w-4 h-0.5 bg-amber-400 rounded" style={{ borderTop: '1px dashed #f59e0b' }} />
              Наличные
            </div>
          )}
        </div>
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-3 gap-4">
        <div className="bg-white rounded-xl border border-slate-200 p-4">
          <p className="text-sm text-slate-400">Макс. стоимость</p>
          <p className="text-lg font-bold text-slate-800">{fmt(maxValue)}</p>
        </div>
        <div className="bg-white rounded-xl border border-slate-200 p-4">
          <p className="text-sm text-slate-400">Мин. стоимость</p>
          <p className="text-lg font-bold text-slate-800">{fmt(minValue)}</p>
        </div>
        <div className="bg-white rounded-xl border border-slate-200 p-4">
          <p className="text-sm text-slate-400">Текущая</p>
          <p className="text-lg font-bold text-green-600">{fmt(points[points.length - 1].value)}</p>
        </div>
      </div>

      {/* Recent points */}
      <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-slate-200">
          <h3 className="font-semibold text-slate-800">Последние значения</h3>
        </div>
        <div className="divide-y divide-slate-100 max-h-64 overflow-y-auto">
          {points.slice(-30).reverse().map(p => (
            <div key={p.date} className="px-6 py-3 flex items-center justify-between">
              <div>
                <p className="font-medium text-slate-800 text-sm">{p.date}</p>
                {showCash && p.cash !== null && <p className="text-xs text-slate-400">Нал.: {fmt(p.cash)}</p>}
              </div>
              <div className="text-right">
                <p className="font-medium text-slate-800">{fmt(p.value)}</p>
                {p.pnl !== null && <p className={`text-xs ${p.pnl >= 0 ? 'text-green-600' : 'text-red-500'}`}>
                  PnL: {p.pnl >= 0 ? '+' : ''}{fmt(p.pnl)}
                </p>}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
