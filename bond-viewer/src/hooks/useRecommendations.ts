import { useState, useEffect, useRef } from 'react'
import { getRecommendations } from '../services/api'
import type { RecommendationParams, RecommendationsResponse } from '../types'

interface UseRecommendationsResult {
  data: RecommendationsResponse | null
  isLoading: boolean
  isError: boolean
  error: Error | null
  refetch: () => void
}

export function useRecommendations(params: RecommendationParams): UseRecommendationsResult {
  const [data, setData] = useState<RecommendationsResponse | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [isError, setIsError] = useState(false)
  const [error, setError] = useState<Error | null>(null)
  const paramsRef = useRef(params)
  paramsRef.current = params

  const fetch = () => {
    setIsLoading(true)
    setIsError(false)
    setError(null)
    getRecommendations(paramsRef.current)
      .then(setData)
      .catch((err: Error) => {
        setError(err)
        setIsError(true)
      })
      .finally(() => setIsLoading(false))
  }

  useEffect(() => {
    fetch()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  return {
    data,
    isLoading,
    isError,
    error,
    refetch: fetch,
  }
}