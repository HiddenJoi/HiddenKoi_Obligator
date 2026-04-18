import type { ReactNode } from 'react'

type Tab = 'bonds' | 'recommendations'

interface HeaderProps {
  activeTab?: Tab
  onTabChange?: (tab: Tab) => void
}

export function Header({ activeTab = 'bonds', onTabChange }: HeaderProps) {
  return (
    <header className="bg-slate-800 text-white shadow-md">
      <div className="max-w-6xl mx-auto px-6 py-4">
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-semibold flex items-center gap-2">
            Облигатор
          </h1>
          <nav className="flex gap-1">
            <NavButton
              active={activeTab === 'bonds'}
              onClick={() => onTabChange?.('bonds')}
            >
              📋 Все облигации
            </NavButton>
            <NavButton
              active={activeTab === 'recommendations'}
              onClick={() => onTabChange?.('recommendations')}
            >
              💡 Рекомендации
            </NavButton>
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
