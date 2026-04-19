import { useState, useEffect } from 'react'
import { getDashboard, getGoal, createGoal, getCash, getPortfolioAdjustment } from '../services/api'
import type { DashboardData, Goal, CashAccount, AdjustmentRecommendation } from '../types'

export function PortfolioDashboard() {
  const [data, setData] = useState<DashboardData | null>(null)
  const [goal, setGoal] = useState<Goal | null>(null)
  const [cash, setCash] = useState<CashAccount | null>(null)
  const [recommendations, setRecommendations] = useState<AdjustmentRecommendation[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [showGoalForm, setShowGoalForm] = useState(false)
  const [goalForm, setGoalForm] = useState({ target_yield: 15, max_duration: 1000, target_monthly_income: 5000 })
  const [savingGoal, setSavingGoal] = useState(false)

  async function load() {
    setLoading(true)
    setError(null)
    try {
      const [d, g, c] = await Promise.all([getDashboard(), getGoal().catch(() => null), getCash().catch(() => null)])
      setData(d)
      setGoal(g)
      setCash(c)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Неизвестная ошибка')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  async function handleSaveGoal() {
    setSavingGoal(true)
    try {
      const g = await createGoal(goalForm)
      setGoal(g)
      setShowGoalForm(false)
      await load()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Не удалось сохранить цель')
    } finally {
      setSavingGoal(false)
    }
  }

  async function loadAdjustment() {
    try {
      const res = await getPortfolioAdjustment()
      setRecommendations(res.recommendations)
    } catch {
      // ignore — portfolio_adjustment requires auth
    }
  }

  const fmt = (n: number) => n.toLocaleString('ru-RU', { style: 'currency', currency: 'RUB', maximumFractionDigits: 0 })
  const fmtPct = (n: number) => `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`

  if (loading) return (
    <div className="animate-pulse space-y-4">
      {[...Array(4)].map((_, i) => <div key={i} className="h-24 bg-slate-100 rounded-xl" />)}
    </div>
  )

  if (error) return (
    <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-red-700 text-sm">{error}</div>
  )

  if (!data) return null

  return (
    <div className="space-y-6">
      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-white rounded-xl border border-slate-200 p-4">
          <p className="text-sm text-slate-400">Общая стоимость</p>
          <p className="text-xl font-bold text-slate-800">{fmt(data.total_value)}</p>
        </div>
        <div className="bg-white rounded-xl border border-slate-200 p-4">
          <p className="text-sm text-slate-400">Вложено</p>
          <p className="text-xl font-bold text-slate-600">{fmt(data.total_invested)}</p>
        </div>
        <div className="bg-white rounded-xl border border-slate-200 p-4">
          <p className="text-sm text-slate-400">PnL</p>
          <p className={`text-xl font-bold ${data.total_pnl >= 0 ? 'text-green-600' : 'text-red-500'}`}>
            {fmt(data.total_pnl)} <span className="text-sm font-normal">{fmtPct(data.total_pnl_pct)}</span>
          </p>
        </div>
        <div className="bg-white rounded-xl border border-slate-200 p-4">
          <p className="text-sm text-slate-400">Наличные</p>
          <p className="text-xl font-bold text-slate-800">{fmt(cash?.balance ?? 0)}</p>
        </div>
      </div>

      {/* YTM & Duration */}
      <div className="grid grid-cols-2 gap-4">
        <div className="bg-white rounded-xl border border-slate-200 p-4">
          <p className="text-sm text-slate-400">Доходность (YTM)</p>
          <p className="text-2xl font-bold text-green-600">{data.weighted_ytm.toFixed(2)}%</p>
          {data.goals_deviation && (
            <p className="text-xs mt-1" style={{ color: data.goals_deviation.delta >= 0 ? 'green' : 'red' }}>
              {data.goals_deviation.delta >= 0 ? '▲' : '▼'} цель: {data.goals_deviation.target_yield}%
            </p>
          )}
        </div>
        <div className="bg-white rounded-xl border border-slate-200 p-4">
          <p className="text-sm text-slate-400">Дюрация</p>
          <p className="text-2xl font-bold text-blue-600">{data.weighted_duration.toFixed(0)} дн.</p>
        </div>
      </div>

      {/* Goal */}
      <div className="bg-white rounded-xl border border-slate-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="font-semibold text-slate-800">Инвестиционная цель</h3>
          <button onClick={() => setShowGoalForm(!showGoalForm)}
            className="text-sm text-blue-600 hover:text-blue-700">
            {goal ? 'Изменить' : 'Установить цель'}
          </button>
        </div>
        {showGoalForm || !goal ? (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-slate-600 mb-1">Целевая доходность (%)</label>
              <input type="number" value={goalForm.target_yield} onChange={e => setGoalForm(f => ({ ...f, target_yield: Number(e.target.value) }))}
                min={0} max={50} step={0.1}
                className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-600 mb-1">Макс. дюрация (дни)</label>
              <input type="number" value={goalForm.max_duration} onChange={e => setGoalForm(f => ({ ...f, max_duration: Number(e.target.value) }))}
                min={1}
                className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-600 mb-1">Целевой ежемесячный доход</label>
              <input type="number" value={goalForm.target_monthly_income} onChange={e => setGoalForm(f => ({ ...f, target_monthly_income: Number(e.target.value) }))}
                min={0}
                className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
            </div>
            <div className="md:col-span-3 flex justify-end gap-3">
              {showGoalForm && goal && (
                <button onClick={() => setShowGoalForm(false)} className="px-4 py-2 text-slate-600 hover:text-slate-700">Отмена</button>
              )}
              <button onClick={handleSaveGoal} disabled={savingGoal}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50">
                {savingGoal ? 'Сохранение...' : 'Сохранить цель'}
              </button>
            </div>
          </div>
        ) : (
          <div className="grid grid-cols-3 gap-4 text-sm">
            <div><span className="text-slate-400">Доходность: </span><span className="font-medium">{goal.target_yield}%</span></div>
            <div><span className="text-slate-400">Макс. дюрация: </span><span className="font-medium">{goal.max_duration} дн.</span></div>
            <div><span className="text-slate-400">Ежемесячный доход: </span><span className="font-medium">{goal.target_monthly_income.toLocaleString('ru-RU')} ₽</span></div>
          </div>
        )}
      </div>

      {/* Allocation */}
      {data.allocation.length > 0 && (
        <div className="bg-white rounded-xl border border-slate-200 p-6">
          <h3 className="font-semibold text-slate-800 mb-4">Распределение по типу купона</h3>
          <div className="space-y-2">
            {data.allocation.map(a => (
              <div key={a.coupon_type} className="flex items-center justify-between">
                <span className="text-sm text-slate-600 capitalize">{a.coupon_type}</span>
                <div className="flex items-center gap-3">
                  <div className="w-32 h-2 bg-slate-100 rounded-full overflow-hidden">
                    <div className="h-full bg-blue-500 rounded-full" style={{ width: `${a.pct}%` }} />
                  </div>
                  <span className="text-sm font-medium w-20 text-right">{fmt(a.value)}</span>
                  <span className="text-xs text-slate-400 w-10">{a.pct.toFixed(1)}%</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Positions */}
      {data.positions.length > 0 && (
        <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
          <div className="px-6 py-4 border-b border-slate-200">
            <h3 className="font-semibold text-slate-800">Позиции</h3>
          </div>
          <div className="divide-y divide-slate-100">
            {data.positions.map(p => (
              <div key={p.secid} className="px-6 py-3 flex items-center justify-between hover:bg-slate-50">
                <div>
                  <p className="font-medium text-slate-800 text-sm">{p.secid}</p>
                  {p.name && <p className="text-xs text-slate-400">{p.name}</p>}
                </div>
                <div className="flex items-center gap-6 text-sm">
                  <div className="text-right"><p className="text-slate-400">Кол-во</p><p className="font-medium">{p.quantity}</p></div>
                  <div className="text-right"><p className="text-slate-400">Ср. цена</p><p className="font-medium">{p.avg_price.toFixed(2)}</p></div>
                  <div className="text-right"><p className="text-slate-400">Текущая</p><p className="font-medium">{p.current_price.toFixed(2)}</p></div>
                  <div className="text-right"><p className="text-slate-400">PnL</p><p className={`font-medium ${p.pnl >= 0 ? 'text-green-600' : 'text-red-500'}`}>{fmt(p.pnl)}</p></div>
                  <div className="text-right"><p className="text-slate-400">YTM</p><p className="font-medium text-green-600">{(p.yield_).toFixed(2)}%</p></div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Portfolio adjustment recommendations */}
      <div className="bg-white rounded-xl border border-slate-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="font-semibold text-slate-800">Рекомендации по портфелю</h3>
          <button onClick={loadAdjustment}
            className="text-sm text-blue-600 hover:text-blue-700">
            Обновить
          </button>
        </div>
        {recommendations.length === 0 ? (
          <p className="text-sm text-slate-400">Нажмите ��Обновить» для анализа отклонений от цели</p>
        ) : (
          <div className="divide-y divide-slate-100">
            {recommendations.map(r => (
              <div key={r.secid} className="py-3 flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <span className={`px-2 py-0.5 rounded text-xs font-medium ${r.action === 'buy' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>
                    {r.action === 'buy' ? 'КУПИТЬ' : 'ПРОДАТЬ'}
                  </span>
                  <div>
                    <p className="font-medium text-slate-800 text-sm">{r.secid}</p>
                    {r.name && <p className="text-xs text-slate-400">{r.name}</p>}
                    {r.reason && <p className="text-xs text-slate-500 mt-0.5">{r.reason}</p>}
                  </div>
                </div>
                {r.impact && (
                  <div className="flex gap-4 text-xs text-slate-500">
                    {r.impact.yield !== undefined && <span>YTM {r.impact.yield >= 0 ? '+' : ''}{r.impact.yield.toFixed(2)}</span>}
                    {r.impact.duration !== undefined && <span>Дюрация {r.impact.duration >= 0 ? '+' : ''}{r.impact.duration.toFixed(0)}</span>}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
