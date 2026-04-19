import { useState, useEffect } from 'react'
import { getCashflow } from '../services/api'
import type { CashflowData, MonthlyCashflow } from '../types'

export function CashflowCalendar() {
  const [data, setData] = useState<CashflowData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedMonth, setSelectedMonth] = useState<string | null>(null)

  useEffect(() => {
    async function load() {
      setLoading(true)
      setError(null)
      try {
        const d = await getCashflow()
        setData(d)
        if (d.by_month.length > 0) setSelectedMonth(d.by_month[0].year_month)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Неизвестная ошибка')
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  const fmt = (n: number) => n.toLocaleString('ru-RU', { style: 'currency', currency: 'RUB', maximumFractionDigits: 0 })

  if (loading) return (
    <div className="animate-pulse space-y-4">
      {[...Array(3)].map((_, i) => <div key={i} className="h-16 bg-slate-100 rounded-xl" />)}
    </div>
  )

  if (error) return (
    <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-red-700 text-sm">{error}</div>
  )

  if (!data) return null

  return (
    <div className="space-y-6">
      {/* Month tabs */}
      <div className="flex gap-2 overflow-x-auto pb-1">
        {data.by_month.map(m => (
          <button key={m.year_month} onClick={() => setSelectedMonth(m.year_month)}
            className={`flex-shrink-0 px-4 py-2 rounded-lg text-sm font-medium transition-colors
              ${selectedMonth === m.year_month
                ? 'bg-blue-600 text-white'
                : 'bg-white border border-slate-200 text-slate-600 hover:bg-slate-50'}`}>
            <div>{m.year_month}</div>
            <div className={`text-xs ${selectedMonth === m.year_month ? 'text-blue-200' : 'text-slate-400'}`}>
              {fmt(m.total)}
            </div>
          </button>
        ))}
      </div>

      {/* Selected month detail */}
      {selectedMonth && data.by_month.length > 0 && (
        <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
          <div className="px-6 py-4 border-b border-slate-200 flex items-center justify-between">
            <h3 className="font-semibold text-slate-800">
              Купонные выплаты — {selectedMonth}
            </h3>
            <span className="text-sm text-slate-500">
              {fmt(data.by_month.find(m => m.year_month === selectedMonth)?.total ?? 0)}
            </span>
          </div>
          <div className="divide-y divide-slate-100">
            {data.by_month
              .find(m => m.year_month === selectedMonth)
              ?.items.map(item => (
                <div key={`${item.secid}-${item.date}`} className="px-6 py-3 flex items-center justify-between hover:bg-slate-50">
                  <div>
                    <p className="font-medium text-slate-800 text-sm">{item.secid}</p>
                    <p className="text-xs text-slate-400">{item.date}</p>
                  </div>
                  <p className="font-medium text-green-600">{fmt(item.amount)}</p>
                </div>
              ))}
          </div>
        </div>
      )}

      {/* Upcoming items (next 30 days) */}
      {data.items.length > 0 && (
        <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
          <div className="px-6 py-4 border-b border-slate-200">
            <h3 className="font-semibold text-slate-800">Ближайшие выплаты</h3>
          </div>
          <div className="divide-y divide-slate-100">
            {data.items.slice(0, 10).map(item => (
              <div key={`${item.secid}-${item.date}`} className="px-6 py-3 flex items-center justify-between hover:bg-slate-50">
                <div>
                  <p className="font-medium text-slate-800 text-sm">{item.secid}</p>
                  <p className="text-xs text-slate-400">{item.date}</p>
                </div>
                <p className="font-medium text-green-600">{fmt(item.amount)}</p>
              </div>
            ))}
          </div>
        </div>
      )}

      {data.items.length === 0 && (
        <div className="bg-white rounded-xl border border-slate-200 p-8 text-center text-slate-400 text-sm">
          Нет предстоящих выплат
        </div>
      )}
    </div>
  )
}
