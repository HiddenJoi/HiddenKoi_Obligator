import { useState, useEffect, useCallback } from 'react'
import { getTransactions, createTransaction } from '../services/api'
import type { Transaction, CreateTransactionParams } from '../types'

const TX_TYPES = ['buy', 'sell', 'coupon', 'deposit', 'withdraw'] as const

export function Transactions() {
  const [transactions, setTransactions] = useState<Transaction[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [filterType, setFilterType] = useState<string>('')
  const [page, setPage] = useState(0)
  const limit = 50

  const [showForm, setShowForm] = useState(false)
  const [form, setForm] = useState<CreateTransactionParams>({
    type: 'buy',
    amount: 0,
    secid: '',
    quantity: undefined,
    price: undefined,
    commission: 0,
  })
  const [saving, setSaving] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const d = await getTransactions({
        limit,
        offset: page * limit,
        type: filterType || undefined,
      })
      setTransactions(d.transactions)
      setTotal(d.total)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Неизвестная ошибка')
    } finally {
      setLoading(false)
    }
  }, [page, filterType])

  useEffect(() => { load() }, [load])

  async function handleSubmit() {
    setSaving(true)
    setFormError(null)
    try {
      await createTransaction(form)
      setShowForm(false)
      setForm({ type: 'buy', amount: 0, secid: '', quantity: undefined, price: undefined, commission: 0 })
      setPage(0)
      await load()
    } catch (err) {
      setFormError(err instanceof Error ? err.message : 'Не удалось создать транзакцию')
    } finally {
      setSaving(false)
    }
  }

  const fmt = (n: number) => n.toLocaleString('ru-RU', { style: 'currency', currency: 'RUB', maximumFractionDigits: 2 })

  const typeLabel: Record<string, string> = {
    buy: 'Покупка', sell: 'Продажа', coupon: 'Купон', deposit: 'Депозит', withdraw: 'Вывод',
  }
  const typeColor: Record<string, string> = {
    buy: 'text-red-600', sell: 'text-green-600', coupon: 'text-blue-600', deposit: 'text-green-600', withdraw: 'text-orange-600',
  }

  return (
    <div className="space-y-6">
      {/* Header + Add button */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-slate-800">История транзакций</h2>
          <p className="text-sm text-slate-400">Всего: {total}</p>
        </div>
        <button onClick={() => setShowForm(!showForm)}
          className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700">
          + Добавить транзакцию
        </button>
      </div>

      {/* Create form */}
      {showForm && (
        <div className="bg-white rounded-xl border border-slate-200 p-6 space-y-4">
          <h3 className="font-semibold text-slate-800">Новая транзакция</h3>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <label className="block text-sm font-medium text-slate-600 mb-1">Тип</label>
              <select value={form.type} onChange={e => setForm(f => ({ ...f, type: e.target.value as CreateTransactionParams['type'] }))}
                className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500">
                {TX_TYPES.map(t => <option key={t} value={t}>{typeLabel[t]}</option>)}
              </select>
            </div>
            {(form.type === 'buy' || form.type === 'sell') && (
              <>
                <div>
                  <label className="block text-sm font-medium text-slate-600 mb-1">SECID</label>
                  <input type="text" value={form.secid ?? ''} onChange={e => setForm(f => ({ ...f, secid: e.target.value }))}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-600 mb-1">Кол-во</label>
                  <input type="number" value={form.quantity ?? ''} onChange={e => setForm(f => ({ ...f, quantity: e.target.value ? Number(e.target.value) : undefined }))}
                    min={0} step={1}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-600 mb-1">Цена</label>
                  <input type="number" value={form.price ?? ''} onChange={e => setForm(f => ({ ...f, price: e.target.value ? Number(e.target.value) : undefined }))}
                    min={0} step={0.01}
                    className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
                </div>
              </>
            )}
            <div>
              <label className="block text-sm font-medium text-slate-600 mb-1">Сумма</label>
              <input type="number" value={form.amount} onChange={e => setForm(f => ({ ...f, amount: Number(e.target.value) }))}
                min={0} step={0.01}
                className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-600 mb-1">Комиссия</label>
              <input type="number" value={form.commission ?? 0} onChange={e => setForm(f => ({ ...f, commission: Number(e.target.value) }))}
                min={0} step={0.01}
                className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500" />
            </div>
          </div>
          {formError && <p className="text-sm text-red-600">{formError}</p>}
          <div className="flex justify-end gap-3">
            <button onClick={() => setShowForm(false)} className="px-4 py-2 text-slate-600 hover:text-slate-700">Отмена</button>
            <button onClick={handleSubmit} disabled={saving}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50">
              {saving ? 'Сохранение...' : 'Сохранить'}
            </button>
          </div>
        </div>
      )}

      {/* Filter */}
      <div className="flex gap-2">
        <button onClick={() => { setFilterType(''); setPage(0) }}
          className={`px-3 py-1.5 rounded-lg text-sm ${!filterType ? 'bg-blue-100 text-blue-700' : 'bg-white border border-slate-200 text-slate-600'}`}>
          Все
        </button>
        {TX_TYPES.map(t => (
          <button key={t} onClick={() => { setFilterType(t); setPage(0) }}
            className={`px-3 py-1.5 rounded-lg text-sm ${filterType === t ? 'bg-blue-100 text-blue-700' : 'bg-white border border-slate-200 text-slate-600'}`}>
            {typeLabel[t]}
          </button>
        ))}
      </div>

      {/* Table */}
      {loading ? (
        <div className="animate-pulse space-y-3">{[...Array(5)].map((_, i) => <div key={i} className="h-12 bg-slate-100 rounded-lg" />)}</div>
      ) : error ? (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-red-700 text-sm">{error}</div>
      ) : transactions.length === 0 ? (
        <div className="bg-white rounded-xl border border-slate-200 p-8 text-center text-slate-400 text-sm">Нет транзакций</div>
      ) : (
        <div className="bg-white rounded-xl border border-slate-200 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 border-b border-slate-200">
              <tr>
                <th className="text-left px-6 py-3 text-slate-500 font-medium">Дата</th>
                <th className="text-left px-6 py-3 text-slate-500 font-medium">Тип</th>
                <th className="text-left px-6 py-3 text-slate-500 font-medium">Бумага</th>
                <th className="text-right px-6 py-3 text-slate-500 font-medium">Кол-во</th>
                <th className="text-right px-6 py-3 text-slate-500 font-medium">Цена</th>
                <th className="text-right px-6 py-3 text-slate-500 font-medium">Сумма</th>
                <th className="text-right px-6 py-3 text-slate-500 font-medium">Комиссия</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {transactions.map(tx => (
                <tr key={tx.id} className="hover:bg-slate-50">
                  <td className="px-6 py-3 text-slate-600">{tx.date}</td>
                  <td className="px-6 py-3">
                    <span className={`font-medium ${typeColor[tx.type]}`}>{typeLabel[tx.type]}</span>
                  </td>
                  <td className="px-6 py-3 text-slate-800">{tx.secid ?? '—'}</td>
                  <td className="px-6 py-3 text-right text-slate-600">{tx.quantity ?? '—'}</td>
                  <td className="px-6 py-3 text-right text-slate-600">{tx.price?.toFixed(2) ?? '—'}</td>
                  <td className={`px-6 py-3 text-right font-medium ${typeColor[tx.type]}`}>{fmt(tx.amount)}</td>
                  <td className="px-6 py-3 text-right text-slate-400">{tx.commission > 0 ? fmt(tx.commission) : '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Pagination */}
      {Math.ceil(total / limit) > 1 && (
        <div className="flex justify-center gap-2">
          <button onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0}
            className="px-3 py-1.5 bg-white border border-slate-200 rounded-lg text-sm disabled:opacity-50">
            ← Назад
          </button>
          <span className="px-3 py-1.5 text-sm text-slate-500">Стр. {page + 1} из {Math.ceil(total / limit)}</span>
          <button onClick={() => setPage(p => p + 1)} disabled={(page + 1) * limit >= total}
            className="px-3 py-1.5 bg-white border border-slate-200 rounded-lg text-sm disabled:opacity-50">
            Вперёд →
          </button>
        </div>
      )}
    </div>
  )
}
