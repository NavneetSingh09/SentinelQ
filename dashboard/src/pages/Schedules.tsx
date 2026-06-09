import { useEffect, useState, useCallback } from 'react'
import { api } from '../api'
import type { Schedule } from '../types'

const CRON_PRESETS = [
  { label: 'Every 5 min',     value: '*/5 * * * *'  },
  { label: 'Every 15 min',    value: '*/15 * * * *' },
  { label: 'Every hour',      value: '0 * * * *'    },
  { label: 'Daily midnight',  value: '0 0 * * *'    },
]

const fmtDate = (s?: string | null) => {
  if (!s || s === 'None') return '—'
  const d = new Date(s)
  return isNaN(d.getTime()) ? s : d.toLocaleString()
}

const EMPTY_FORM = {
  alert_name: '',
  severity: 'warning',
  source: 'scheduler',
  cron_expr: '*/15 * * * *',
}

export default function Schedules() {
  const [schedules, setSchedules] = useState<Schedule[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [form, setForm] = useState(EMPTY_FORM)
  const [toast, setToast] = useState<{ msg: string; type: 'success' | 'error' } | null>(null)

  const showToast = useCallback((msg: string, type: 'success' | 'error') => {
    setToast({ msg, type })
    setTimeout(() => setToast(null), 3000)
  }, [])

  const fetchSchedules = useCallback(async () => {
    try {
      setSchedules(await api.listSchedules())
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load schedules')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { fetchSchedules() }, [fetchSchedules])

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!form.alert_name.trim()) return
    setSubmitting(true)
    try {
      await api.createSchedule(form)
      showToast('Schedule created', 'success')
      setForm(EMPTY_FORM)
      fetchSchedules()
    } catch (err) {
      showToast(err instanceof Error ? err.message : 'Create failed', 'error')
    } finally {
      setSubmitting(false)
    }
  }

  const handleToggle = async (id: string) => {
    try {
      const result = await api.toggleSchedule(id)
      setSchedules(s =>
        s.map(sc => (sc.id === id ? { ...sc, is_active: result.is_active } : sc))
      )
    } catch {
      showToast('Toggle failed', 'error')
    }
  }

  const handleDelete = async (id: string) => {
    try {
      await api.deleteSchedule(id)
      setSchedules(s => s.filter(sc => sc.id !== id))
      showToast('Schedule deleted', 'success')
    } catch {
      showToast('Delete failed', 'error')
    }
  }

  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">Schedules</h1>
          <p className="page-subtitle">Recurring cron-style alert schedules</p>
        </div>
      </div>

      {error && <div className="error-msg">{error}</div>}

      <div className="card" style={{ marginBottom: 20 }}>
        <div className="card-title">New Schedule</div>
        <form onSubmit={handleCreate}>
          <div className="form-grid">
            <div className="form-group">
              <label>Alert Name</label>
              <input
                type="text"
                value={form.alert_name}
                onChange={e => setForm(f => ({ ...f, alert_name: e.target.value }))}
                placeholder="e.g. High CPU Check"
                required
              />
            </div>

            <div className="form-group">
              <label>Severity</label>
              <select
                value={form.severity}
                onChange={e => setForm(f => ({ ...f, severity: e.target.value }))}
              >
                <option value="critical">critical</option>
                <option value="warning">warning</option>
                <option value="info">info</option>
              </select>
            </div>

            <div className="form-group">
              <label>Source</label>
              <input
                type="text"
                value={form.source}
                onChange={e => setForm(f => ({ ...f, source: e.target.value }))}
                placeholder="scheduler"
              />
            </div>

            <div className="form-group">
              <label>Cron Expression</label>
              <div style={{ display: 'flex', gap: 6 }}>
                <input
                  type="text"
                  value={form.cron_expr}
                  onChange={e => setForm(f => ({ ...f, cron_expr: e.target.value }))}
                  placeholder="*/15 * * * *"
                  style={{ flex: 1 }}
                  required
                />
                <select
                  value=""
                  onChange={e => {
                    if (e.target.value) setForm(f => ({ ...f, cron_expr: e.target.value }))
                  }}
                  style={{ width: 140 }}
                >
                  <option value="">Presets…</option>
                  {CRON_PRESETS.map(p => (
                    <option key={p.value} value={p.value}>{p.label}</option>
                  ))}
                </select>
              </div>
            </div>
          </div>

          <button
            type="submit"
            className="btn btn-primary"
            disabled={submitting || !form.alert_name.trim()}
          >
            {submitting ? 'Creating…' : '+ Create Schedule'}
          </button>
        </form>
      </div>

      {loading ? (
        <div className="loading">Loading…</div>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Alert Name</th>
                <th>Severity</th>
                <th>Cron</th>
                <th>Source</th>
                <th>Last Run</th>
                <th>Next Run</th>
                <th>Active</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {schedules.length === 0 ? (
                <tr>
                  <td colSpan={8}>
                    <div className="empty-state">
                      <h3>No schedules yet</h3>
                      <p>Use the form above to create your first recurring alert schedule.</p>
                    </div>
                  </td>
                </tr>
              ) : (
                schedules.map(s => (
                  <tr key={s.id}>
                    <td>
                      <div style={{ fontWeight: 500 }}>{s.alert_name}</div>
                      <div className="monospace">{s.id.slice(0, 8)}…</div>
                    </td>
                    <td>
                      <span className={`badge badge-${s.severity}`}>{s.severity}</span>
                    </td>
                    <td className="monospace">{s.cron_expr}</td>
                    <td className="monospace">{s.source}</td>
                    <td className="monospace" style={{ color: 'var(--text-muted)' }}>
                      {fmtDate(s.last_run_at)}
                    </td>
                    <td className="monospace" style={{ color: 'var(--text-muted)' }}>
                      {fmtDate(s.next_run_at)}
                    </td>
                    <td>
                      <label className="toggle">
                        <input
                          type="checkbox"
                          checked={s.is_active}
                          onChange={() => handleToggle(s.id)}
                        />
                        <span className="toggle-track" />
                        <span className="toggle-thumb" />
                      </label>
                    </td>
                    <td>
                      <button
                        className="btn btn-danger btn-sm"
                        onClick={() => handleDelete(s.id)}
                      >
                        Delete
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      )}

      {toast && (
        <div className="toast-container">
          <div className={`toast ${toast.type}`}>{toast.msg}</div>
        </div>
      )}
    </>
  )
}
