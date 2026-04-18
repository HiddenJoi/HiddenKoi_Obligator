import { useState, useEffect, useCallback } from "react"
import { getBonds } from "../services/api"
import type { BondListItem, BondsFilterParams } from "../types"

interface UseBondsResult {
  bonds: BondListItem[]
  total: number
  loading: boolean
  error: string | null
  refetch: () => void
}

export function useBonds(initialParams: BondsFilterParams = {}): UseBondsResult {
  const [params, setParams] = useState<BondsFilterParams>(initialParams)
  const [bonds, setBonds] = useState<BondListItem[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await getBonds(params)
      setBonds(data.bonds)
      setTotal(data.total)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Неизвестная ошибка")
    } finally {
      setLoading(false)
    }
  }, [params])

  useEffect(() => { load() }, [load])

  return { bonds, total, loading, error, refetch: load }
}
