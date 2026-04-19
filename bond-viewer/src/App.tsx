import { useState } from 'react'
import { Layout } from './components/Layout'
import { BondApp } from './components/BondApp'
import { Recommendations } from './components/Recommendations'
import { PortfolioDashboard } from './components/PortfolioDashboard'
import { CashflowCalendar } from './components/CashflowCalendar'
import { Transactions } from './components/Transactions'
import { PortfolioHistory } from './components/PortfolioHistory'
import { ToastContainer } from './components/ui/Toast'

type Tab =
  | 'bonds'
  | 'recommendations'
  | 'portfolio'
  | 'cashflow'
  | 'transactions'
  | 'history'

const TABS: { value: Tab; label: string }[] = [
  { value: 'bonds', label: 'Облигации' },
  { value: 'recommendations', label: 'Подбор' },
  { value: 'portfolio', label: 'Портфель' },
  { value: 'cashflow', label: 'Календарь' },
  { value: 'transactions', label: 'Транзакции' },
  { value: 'history', label: 'История' },
]

export function App() {
  const [activeTab, setActiveTab] = useState<Tab>('bonds')

  return (
    <>
      <Layout activeTab={activeTab} onTabChange={setActiveTab} tabs={TABS}>
        <div className="space-y-6">
          {activeTab === 'bonds' && <BondApp />}
          {activeTab === 'recommendations' && <Recommendations />}
          {activeTab === 'portfolio' && <PortfolioDashboard />}
          {activeTab === 'cashflow' && <CashflowCalendar />}
          {activeTab === 'transactions' && <Transactions />}
          {activeTab === 'history' && <PortfolioHistory />}
        </div>
      </Layout>
      <ToastContainer />
    </>
  )
}
