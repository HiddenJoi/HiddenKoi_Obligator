import type { ReactNode } from 'react'
import { Header } from './Header'

type Tab = 'bonds' | 'recommendations' | 'portfolio' | 'cashflow' | 'transactions' | 'history'

interface Props {
  children?: ReactNode
  activeTab?: Tab
  onTabChange?: (tab: Tab) => void
  tabs?: { value: Tab; label: string }[]
}

export function Layout({ children, activeTab, onTabChange, tabs }: Props) {
  return (
    <div className="min-h-screen flex flex-col bg-white">
      <Header activeTab={activeTab} onTabChange={onTabChange} tabs={tabs} />
      <main className="flex-1 px-6 py-8 max-w-6xl mx-auto w-full">
        {children}
      </main>
    </div>
  )
}
