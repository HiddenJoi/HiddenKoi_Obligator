import type { ReactNode } from 'react'

type Tab = 'bonds' | 'recommendations' | 'portfolio' | 'cashflow' | 'transactions' | 'history'

interface HeaderProps {
  activeTab?: Tab
  onTabChange?: (tab: Tab) => void
  tabs?: { value: Tab; label: string }[]
}

const TAB_ICONS: Record<string, string> = {
  bonds: '📋',
  recommendations: '💡',
  portfolio: '💼',
  cashflow: '📅',
  transactions: '💳',
  history: '📈',
}

export function Header({ activeTab = 'bonds', onTabChange, tabs }: HeaderProps) {
  const tabList = tabs ?? [
    { value: 'bonds', label: 'Все облигации' },
    { value: 'recommendations', label: 'Подбор' },
  ]

  return (
    <header className="bg-slate-800 text-white shadow-md">
      <div className="max-w-6xl mx-auto px-6 py-4">
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-semibold flex items-center gap-2">
            Облигатор
          </h1>
          <nav className="flex gap-1">
            {tabList.map(tab => (
              <NavButton
                key={tab.value}
                active={activeTab === tab.value}
                onClick={() => onTabChange?.(tab.value)}
              >
                {TAB_ICONS[tab.value] ?? '•'} {tab.label}
              </NavButton>
            ))}
          </nav>
        </div>
      </div>
    </header>
  )
}

function NavButton({
  active,
  onClick,
  children,
}: {
  active: boolean
  onClick: () => void
  children: ReactNode
}) {
  return (
    <button
      onClick={onClick}
      className={[
        'px-4 py-2 rounded-lg text-sm font-medium transition-colors',
        active
          ? 'bg-blue-600 text-white'
          : 'text-slate-300 hover:bg-slate-700 hover:text-white',
      ].join(' ')}
    >
      {children}
    </button>
  )
}
