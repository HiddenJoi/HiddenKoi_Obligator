import type { ReactNode } from 'react'
import { Header } from './Header'
import { Footer } from './Footer'

type Tab = 'bonds' | 'recommendations'

interface Props {
  children?: ReactNode
  activeTab?: Tab
  onTabChange?: (tab: Tab) => void
}

export function Layout({ children, activeTab, onTabChange }: Props) {
  return (
    <div className="min-h-screen flex flex-col bg-white">
      <Header activeTab={activeTab} onTabChange={onTabChange} />
      <main className="flex-1 px-6 py-8 max-w-6xl mx-auto w-full">
        {children}
      </main>
    </div>
  )
}
