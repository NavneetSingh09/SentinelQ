export type AlertStatus = 'queued' | 'processing' | 'done' | 'failed' | 'dead'
export type AlertPriority = 'critical' | 'high' | 'normal' | 'low'
export type AlertSeverity = 'critical' | 'warning' | 'info'

export interface AlertJob {
  id: string
  alert_name: string
  severity: AlertSeverity
  priority: AlertPriority
  status: AlertStatus
  retry_count: number
  worker_id?: string
  created_at: string
  updated_at?: string
  error?: string
  deduplicated?: boolean
  source?: string
  payload?: Record<string, unknown>
}

export interface Stats {
  total_processed: number
  total_submitted: number
  success_rate: number
  failed_jobs: number
  dead_letter_count: number
  queue_depths: Record<string, number>
  by_priority: Record<string, number>
}

export interface Schedule {
  id: string
  alert_name: string
  severity: AlertSeverity
  source: string
  cron_expr: string
  is_active: boolean
  last_run_at?: string | null
  next_run_at?: string | null
  created_at: string
}

export interface WsMessage {
  type?: string
  job_id?: string
  status?: AlertStatus
  worker_id?: string
  error?: string
  timestamp?: string
}
