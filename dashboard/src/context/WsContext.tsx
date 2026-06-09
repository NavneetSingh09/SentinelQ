import { createContext, useContext, useEffect, useRef, useState, ReactNode } from 'react'
import type { WsMessage } from '../types'

interface WsCtx {
  connected: boolean
  messages: WsMessage[]
}

const Ctx = createContext<WsCtx>({ connected: false, messages: [] })

const MAX_MESSAGES = 50

export function WsProvider({ children }: { children: ReactNode }) {
  const [connected, setConnected] = useState(false)
  const [messages, setMessages] = useState<WsMessage[]>([])
  const wsRef = useRef<WebSocket | null>(null)
  const retryRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    function connect() {
      const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
      const url = (import.meta as { env: Record<string, string> }).env.VITE_WS_URL
        ?? `${proto}//${window.location.host}/ws`

      const ws = new WebSocket(url)
      wsRef.current = ws

      ws.onopen = () => setConnected(true)
      ws.onclose = () => {
        setConnected(false)
        retryRef.current = setTimeout(connect, 3000)
      }
      ws.onerror = () => ws.close()
      ws.onmessage = e => {
        try {
          const msg = JSON.parse(e.data as string) as WsMessage
          if (msg.type === 'ping') return
          setMessages(prev => [msg, ...prev].slice(0, MAX_MESSAGES))
        } catch {
          // ignore malformed frames
        }
      }
    }

    connect()

    return () => {
      if (retryRef.current) clearTimeout(retryRef.current)
      wsRef.current?.close()
    }
  }, [])

  return <Ctx.Provider value={{ connected, messages }}>{children}</Ctx.Provider>
}

export const useWs = () => useContext(Ctx)
