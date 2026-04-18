import { useState } from 'react'
import { getRecommendations } from '../services/api'
import type { BondRecommendation, RiskProfile } from '../types'

const RISK_PROFILES: { value: RiskProfile; label: string }[] = [
  { value: 'conservative', label: 'Консервативный' },
  { value: 'moderate', label: 'Умеренный' },
  { value: 'aggressive', label: 'Агрессивный' },
]

export function Recommendations() {
  const [targetYield, setTargetYield] = useState<number>(15)
  const [maxDuration, setMaxDuration] = useState<number>(1000)
  const [riskProfile, setRiskProfile] = useState<RiskProfile>('moderate')
  const [investmentHorizon, setInvestmentHorizon] = useState<number | ''>('')
  const [limit, setLimit] = useState<number>(10)

  const [recommendations, setRecommendations] = useState<BondRecommendation[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [hasSearched, setHasSearched] = useState(false)

  async function handleSearch() {
    setLoading(true)
    setError(null)
    try {
      const data = await getRecommendations({
        target_yield: targetYield,
        max_duration: maxDuration,
        risk_profile: riskProfile,
        investment_horizon: investmentHorizon === '' ? undefined : investmentHorizon,
        limit,
      })
      setRecommendations(data.bonds)
      setHasSearched(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Неизвестная ошибка')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
        <h2 className="text-lg font-semibold text-slate-800 mb-4">Подбор облигаций</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          <div>
            <label className="block text-sm font-medium text-slate-600 mb-1">Желаемая доходность (%)</label>
            <input type="number" value={targetYield} onChange={(e) => setTargetYield(Number(e.target.value))}
              min={0} max={100} step={0.1}
              className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-600 mb-1">Макс. дюрация (дни)</label>
            <input type="number" value={maxDuration} onChange={(e) => setMaxDuration(Number(e.target.value))}
              min={1} step={1}
              className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-600 mb-1">Профиль риска</label>
            <select value={riskProfile} onChange={(e) => setRiskProfile(e.target.value as RiskProfile)}
              className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
              {RISK_PROFILES.map((r) => (<option key={r.value} value={r.value}>{r.label}</option>))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-600 mb-1">Горизонт инвестирования (дни)</label>
            <input type="number" value={investmentHorizon}
              onChange={(e) => setInvestmentHorizon(e.target.value === '' ? '' : Number(e.target.value))}
              min={1} placeholder="Без ограничений"
              className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-600 mb-1">Кол-во результатов</label>
            <input type="number" value={limit} onChange={(e) => setLimit(Number(e.target.value))}
              min={1} max={50}
              className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
          </div>
          <div className="flex items-end">
            <button onClick={handleSearch} disabled={loading}
              className="w-full px-4 py-2 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">
              {loading ? 'Поиск...' : 'Найти'}
            </button>
          </div>
        </div>
      </div>

      {error && (<div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700 text-sm">{error}</div>)}

      {hasSearched && !loading && recommendations.length === 0 && (
        <div className="bg-white rounded-xl border border-slate-200 p-12 text-center">
          <span className="text-4xl">📭</span>
          <p className="mt-3 text-slate-600">Ничего не найдено</p>
          <p className="text-sm text-slate-400">Попробуйте изменить параметры поиска</p>
        </div>
      )}

      {recommendations.length > 0 && (
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          <div className="px-6 py-4 border-b border-slate-200">
            <h3 className="font-semibold text-slate-800">Найдено облигаций: {recommendations.length}</h3>
          </div>
          <div className="divide-y divide-slate-100">
            {recommendations.map((bond, index) => (
              <div key={bond.secid} className="px-6 py-4 flex items-center justify-between hover:bg-slate-50 transition-colors">
                <div className="flex items-center gap-4">
                  <div className="w-8 h-8 rounded-full bg-blue-100 text-blue-700 flex items-center justify-center text-sm font-bold">{index + 1}</div>
                  <div>
                    <p className="font-medium text-slate-800">{bond.name}</p>
                    <p className="text-sm text-slate-400">{bond.secid}</p>
                  </div>
                </div>
                <div className="flex items-center gap-6 text-sm">
                  <div className="text-right"><p className="text-slate-400">Доходность</p><p className="font-semibold text-green-600">{bond.yield_rate?.toFixed(2)}%</p></div>
                  <div className="text-right"><p className="text-slate-400">Дюрация</p><p className="font-semibold text-slate-700">{bond.duration?.toFixed(1)} дн.</p></div>
                  <div className="text-right"><p className="text-slate-400">Счёт</p><p className="font-semibold text-blue-600">{(bond.score * 100).toFixed(1)}%</p></div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {loading && (
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6">
          <div className="animate-pulse space-y-4">
            {[...Array(5)].map((_, i) => (
              <div key={i} className="flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <div className="w-8 h-8 bg-slate-200 rounded-full" />
                  <div><div className="h-4 bg-slate-200 rounded w-48 mb-2" /><div className="h-3 bg-slate-100 rounded w-24" /></div>
                </div>
                <div className="flex gap-6">
                  <div className="h-10 w-20 bg-slate-100 rounded" />
                  <div className="h-10 w-20 bg-slate-100 rounded" />
                  <div className="h-10 w-20 bg-slate-100 rounded" />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
