import { createContext, useContext, type ReactNode } from 'react'
import { toast, ToastContainer, type ToastVariant } from '../components/ui/Toast'

interface ToastContextValue {
  showToast: (message: string, variant?: ToastVariant) => void
}

const ToastContext = createContext<ToastContextValue | null>(null)

export function ToastProvider({ children }: { children: ReactNode }) {
  const showToast = (message: string, variant: ToastVariant = 'info') => {
    toast(message, variant)
  }

  return (
    <ToastContext.Provider value={{ showToast }}>
      {children}
      <ToastContainer />
    </ToastContext.Provider>
  )
}

export const useToast = (() => {
  // Simple hook that uses the global toast
  const ctx = useContext(ToastContext)
  return ctx ?? { showToast: (msg: string, v?: ToastVariant) => toast(msg, v) }
}) as () => ToastContextValue
