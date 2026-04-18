import type { SortField } from '../types'

interface Props {
  value: SortField | ''
  onChange: (v: SortField | '') => void
}

const OPTIONS: { label: string; value: SortField | '' }[] = [
  { label: 'По умолчанию', value: '' },
  { label: 'Доходность ↑', value: 'yield_asc' },
  { label: 'Доходность ↓', value: 'yield_desc' },
  { label: 'Дюрация ↑', value: 'duration_asc' },
  { label: 'Дюрация ↓', value: 'duration_desc' },
  { label: 'Погашение ↑', value: 'maturity_asc' },
  { label: 'Погашение ↓', value: 'maturity_desc' },
  { label: 'Название А-Я', value: 'name_asc' },
  { label: 'Название Я-А', value: 'name_desc' },
]

export function BondSorter({ value, onChange }: Props) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value as SortField | '')}
      className="border border-slate-300 rounded-lg px-3 py-2 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
    >
      {OPTIONS.map((o) => (
        <option key={o.value} value={o.value}>
          {o.label}
        </option>
      ))}
    </select>
  )
}
