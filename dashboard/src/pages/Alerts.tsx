import { useEffect, useState, useCallback, Fragment } from 'react'
import { api } from '../api'
import type { AlertJob } from '../types'

const fmtDate = (s?: string | null) => {
  if (!s || s === 'None') return '—'
  const d = new Date(s)
  return isNaN(d.getTime()) ? s : d.toLocaleString()
}

export default function Alerts() {
  const [alerts, setAlerts] = useState<AlertJob[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [filters, setFilters] = useState({ status: '', priority: '', severity: '' })
  const [expanded, setExpanded] = useState<string | null>(null)
  const [retrying, setRetrying] = useState<string | null>(null)
  const [toast, setToast] = useState<{ msg: string; type: 'success' | 'error' } | null>(null)

  const showToast = useCallback((msg: string, type: 'success' | 'error') => {
    setToast({ msg, type })
    setTimeout(() => setToast(null), 3000)
  }, [])

  const fetchAlerts = useCallback(async () => {
    try {
      const params: Record<string, string> = {}
      if (filters.status)   params.status   = filters.status
      if (filters.priority) params.priority = filters.priority
      if (filters.severity) params.severity = filters.severity
      setAlerts(await api.listAlerts(params))
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load alerts')
    } finally {
      setLoading(false)
    }
  }, [filters])

  useEffect(() => { fetchAlerts() }, [fetchAlerts])

  const handleRetry = async (id: string, e: React.MouseEvent) => {
    e.stopPropagation()
    setRetrying(id)
    try {
      await api.retryAlert(id)
      showToast('Job requeued successfully', 'success')
      fetchAlerts()
    } catch {
      showToast('Retry failed', 'error')
    } finally {
      setRetrying(null)
    }
  }

  const toggleExpand = (id: string) =>
    setExpanded(prev => (prev === id ? null : id))

  const clearFilters = () => setFilters({ status: '', priority: '', severity: '' })
  const hasFilters = filters.status || filters.priority || filters.severity

  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">Alerts</h1>
          <p className="page-subtitle">{alerts.length} job{alerts.length !== 1 ? 's' : ''} shown</p>
        </div>
        <button className="btn btn-secondary btn-sm" onClick={fetchAlerts}>↻ Refresh</button>
      </div>

      {error && <div className="error-msg">{error}</div>}

      <div className="filter-bar">
        <select
          value={filters.status}
          onChange={e => setFilters(f => ({ ...f, status: e.target.value }))}
        >
          <option value="">All Statuses</option>
          {['queued', 'processing', 'done', 'failed', 'dead'].map(s => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>

        <select
          value={filters.priority}
          onChange={e => setFilters(f => ({ ...f, priority: e.target.value }))}
        >
          <option value="">All Priorities</option>
          {['critical', 'high', 'normal', 'low'].map(p => (
            <option key={p} value={p}>{p}</option>
          ))}
        </select>

        <select
          value={filters.severity}
          onChange={e => setFilters(f => ({ ...f, severity: e.target.value }))}
        >
          <option value="">All Severities</option>
          {['critical', 'warning', 'info'].map(s => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>

        {hasFilters && (
          <button className="btn btn-secondary btn-sm" onClick={clearFilters}>
            ✕ Clear
          </button>
        )}
      </div>

      {loading ? (
        <div className="loading">Loading…</div>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Alert Name</th>
                <th>Priority</th>
                <th>Severity</th>
                <th>Status</th>
                <th>Retries</th>
                <th>Source</th>
                <th>Created</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {alerts.length === 0 ? (
                <tr>
                  <td colSpan={8}>
                    <div className="empty-state">
                      <h3>No alerts found</h3>
                      <p>
                        {hasFilters
                          ? 'Try adjusting the filters.'
                          : 'Submit alerts via POST /alerts or run the seed script.'}
                      </p>
                    </div>
                  </td>
                </tr>
              ) : (
                alerts.map(a => (
                  <Fragment key={a.id}>
                    <tr className="clickable" onClick={() => toggleExpand(a.id)}>
                      <td>
                        <div style={{ fontWeight: 500 }}>{a.alert_name}</div>
                        <div className="monospace">{a.id.slice(0, 8)}…</div>
                      </td>
                      <td>
                        <span className={`badge badge-${a.priority}`}>{a.priority}</span>
                      </td>
                      <td>
                        <span className={`badge badge-${a.severity}`}>{a.severity}</span>
                      </td>
                      <td>
                        <span className={`badge badge-${a.status}`}>{a.status}</span>
                      </td>
                      <td style={{ color: a.retry_count > 0 ? 'var(--high)' : 'var(--text-muted)' }}>
                        {a.retry_count}
                      </td>
                      <td className="monospace">{a.source ?? '—'}</td>
                      <td className="monospace">{fmtDate(a.created_at)}</td>
                      <td onClick={e => e.stopPropagation()}>
                        {(a.status === 'failed' || a.status === 'dead') && (
                          <button
                            className="btn btn-secondary btn-sm"
                            onClick={e => handleRetry(a.id, e)}
                            disabled={retrying === a.id}
                          >
                            {retrying === a.id ? '…' : '↺ Retry'}
                          </button>
                        )}
                      </td>
                    </tr>

                    {expanded === a.id && (
                      <tr>
                        <td colSpan={8} style={{ padding: 0 }}>
                          <div className="row-detail">
                            <div className="detail-grid">
                              <div className="detail-item">
                                <label>Job ID</label>
                                <span>{a.id}</span>
                              </div>
                              <div className="detail-item">
                                <label>Worker</label>
                                <span>{a.worker_id ?? '—'}</span>
                              </div>
                              <div className="detail-item">
                                <label>Deduplicated</label>
                                <span>{a.deduplicated ? 'yes' : 'no'}</span>
                              </div>
                              {a.error && (
                                <div className="detail-item" style={{ gridColumn: '1 / -1' }}>
                                  <label>Error</label>
                                  <span style={{ color: 'var(--critical)' }}>{a.error}</span>
                                </div>
                              )}
                              {a.payload && Object.keys(a.payload).length > 0 && (
                                <div className="detail-item" style={{ gridColumn: '1 / -1' }}>
                                  <label>Payload</label>
                                  <span style={{ whiteSpace: 'pre-wrap' }}>
                                    {JSON.stringify(a.payload, null, 2)}
                                  </span>
                                </div>
                              )}
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
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
