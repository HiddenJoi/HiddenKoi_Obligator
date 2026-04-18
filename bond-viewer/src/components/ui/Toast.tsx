import { useEffect, useState, useCallback } from 'react'

export type ToastVariant = 'success' | 'error' | 'info'

interface ToastItem {
  id: number
  message: string
  variant: ToastVariant
}

let _nextId = 0
const _listeners = new Set<(items: ToastItem[]) => void>()
let _toasts: ToastItem[] = []

function _notify() {
  _listeners.forEach((l) => l([..._toasts]))
}

export function toast(message: string, variant: ToastVariant = 'info') {
  const item: ToastItem = { id: ++_nextId, message, variant }
  _toasts = [..._toasts, item]
  _notify()
  setTimeout(() => {
    _toasts = _toasts.filter((t) => t.id !== item.id)
    _notify()
  }, 4_000)
}

const VARIANT_CLASSES: Record<ToastVariant, string> = {
  success: 'bg-green-50 border-green-300 text-green-800',
  error:   'bg-red-50    border-red-300    text-red-800',
  info:    'bg-blue-50   border-blue-300   text-blue-800',
}

const ICONS: Record<ToastVariant, string> = {
  success: '✓',
  error:   '✗',
  info:    'ℹ',
}

export function ToastContainer() {
  const [items, setItems] = useState<ToastItem[]>([])

  useEffect(() => {
    _listeners.add(setItems)
    return () => { _listeners.delete(setItems) }
  }, [])

  if (items.length === 0) return null

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2 pointer-events-none">
      {items.map((t) => (
        <div
          key={t.id}
          className={`
            flex items-center gap-3 px-4 py-3 rounded-xl border shadow-lg
            min-w-72 max-w-md pointer-events-auto
            ${VARIANT_CLASSES[t.variant]}
          `}
        >
          <span className="flex-shrink-0 text-base">{ICONS[t.variant]}</span>
          <p className="text-sm font-medium">{t.message}</p>
        </div>
      ))}
    </div>
  )
}
