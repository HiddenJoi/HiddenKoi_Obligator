import { useState } from 'react'
import { Layout } from './components/Layout'
import { BondApp } from './components/BondApp'
import { Recommendations } from './components/Recommendations'
import { ToastContainer } from './components/ui/Toast'

type Tab = 'bonds' | 'recommendations'

export function App() {
  const [activeTab, setActiveTab] = useState<Tab>('bonds')

  return (
    <>
      <Layout activeTab={activeTab} onTabChange={setActiveTab}>
        <div className="space-y-6">
          {activeTab === 'bonds' ? <BondApp /> : <Recommendations />}
        </div>
      </Layout>
      <ToastContainer />
    </>
  )
}
