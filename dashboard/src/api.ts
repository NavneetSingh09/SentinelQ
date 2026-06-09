import type { AlertJob, Stats, Schedule } from './types'

const BASE = (import.meta as { env: Record<string, string> }).env.VITE_API_URL ?? '/api'

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  return res.json() as Promise<T>
}

async function post<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({})) as { detail?: { message?: string } | string }
    const msg =
      typeof err.detail === 'object' && err.detail !== null
        ? (err.detail as { message?: string }).message ?? JSON.stringify(err.detail)
        : (err.detail as string | undefined) ?? `${res.status}`
    throw new Error(msg)
  }
  return res.json() as Promise<T>
}

async function del(path: string): Promise<void> {
  const res = await fetch(`${BASE}${path}`, { method: 'DELETE' })
  if (!res.ok) throw new Error(`${res.status}`)
}

async function patch<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`, { method: 'PATCH' })
  if (!res.ok) throw new Error(`${res.status}`)
  return res.json() as Promise<T>
}

export const api = {
  getStats: () => get<Stats>('/stats'),
  listAlerts: (params?: Record<string, string>) => {
    const q = params && Object.keys(params).length > 0
      ? '?' + new URLSearchParams(params).toString()
      : ''
    return get<AlertJob[]>(`/alerts${q}`)
  },
  retryAlert: (id: string) => post<{ message: string }>(`/alerts/${id}/retry`),
  listSchedules: () => get<Schedule[]>('/schedules'),
  createSchedule: (data: unknown) => post<Schedule>('/schedules', data),
  deleteSchedule: (id: string) => del(`/schedules/${id}`),
  toggleSchedule: (id: string) => patch<{ id: string; is_active: boolean }>(`/schedules/${id}/toggle`),
}
