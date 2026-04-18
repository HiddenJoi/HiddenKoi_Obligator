import { memo, useMemo } from 'react'
import { formatYield, formatDuration } from '../utils/format'

interface BondCardBond {
  secid: string
  name: string
  yield_rate: number | null
  duration: number | null
  score: number
}

interface BondCardProps {
  bond: BondCardBond
  rank: number
}

function getScoreColor(pct: number): string {
  if (pct >= 80) return 'text-green-600 bg-green-50'
  if (pct >= 60) return 'text-blue-600 bg-blue-50'
  if (pct >= 40) return 'text-yellow-600 bg-yellow-50'
  return 'text-red-600 bg-red-50'
}

function getScoreBarColor(pct: number): string {
  if (pct >= 80) return 'bg-green-500'
  if (pct >= 60) return 'bg-blue-500'
  if (pct >= 40) return 'bg-yellow-500'
  return 'bg-red-500'
}

export const BondCard = memo(function BondCard({ bond, rank }: BondCardProps) {
  const scorePct = useMemo(() => Math.round(bond.score * 100), [bond.score])
  const barColorClass = useMemo(() => getScoreBarColor(scorePct), [scorePct])
  const badgeClass = useMemo(() => getScoreColor(scorePct), [scorePct])

  const yieldFormatted = useMemo(() => formatYield(bond.yield_rate), [bond.yield_rate])
  const durationFormatted = useMemo(() => formatDuration(bond.duration), [bond.duration])

  return (
    <div className="bg-white dark:bg-slate-800 rounded-2xl shadow-md hover:shadow-xl transition-all duration-300 overflow-hidden border border-slate-200 dark:border-slate-700 group">
      {/* Header */}
      <div className="bg-gradient-to-r from-slate-50 to-slate-100 dark:from-slate-700 dark:to-slate-700/50 px-5 py-4 flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span className="flex-shrink-0 w-6 h-6 rounded-full bg-blue-600 text-white text-xs font-bold flex items-center justify-center">
              {rank}
            </span>
            <span className="text-xs text-slate-400 dark:text-slate-500 font-mono">{bond.secid}</span>
          </div>
          <h4 className="font-semibold text-slate-800 dark:text-slate-100 truncate text-sm leading-tight">
            {bond.name}
          </h4>
        </div>
      </div>

      {/* Stats */}
      <div className="px-5 py-4 grid grid-cols-2 gap-3">
        <div>
          <p className="text-xs text-slate-400 dark:text-slate-500 mb-0.5">Доходность</p>
          <p className="font-bold text-slate-800 dark:text-slate-100">{yieldFormatted}</p>
        </div>
        <div>
          <p className="text-xs text-slate-400 dark:text-slate-500 mb-0.5">Дюрация</p>
          <p className="font-medium text-slate-700 dark:text-slate-300">{durationFormatted}</p>
        </div>
      </div>

      {/* Compatibility bar */}
      <div className="px-5 pb-4">
        <div className="flex items-center justify-between mb-1.5">
          <span className="text-xs text-slate-500 dark:text-slate-400">Совместимость</span>
          <span className={`text-xs font-bold px-2 py-0.5 rounded-full ${badgeClass}`}>
            {scorePct}%
          </span>
        </div>
        <div className="h-2 bg-slate-100 dark:bg-slate-700 rounded-full overflow-hidden">
          <div
            className={`h-full rounded-full transition-all duration-700 ease-out ${barColorClass}`}
            style={{ width: `${scorePct}%` }}
          />
        </div>
      </div>

      {/* Footer */}
      <div className="px-5 pb-5">
        <button
          onClick={() => { /* TODO: navigate to detail */ }}
          className="w-full py-2 px-4 rounded-lg text-sm font-medium
                     bg-blue-600 text-white
                     hover:bg-blue-700 active:bg-blue-800
                     dark:bg-blue-600 dark:hover:bg-blue-500
                     transition-colors duration-200
                     shadow-sm hover:shadow-md"
        >
          Подробнее
        </button>
      </div>
    </div>
  )
})
