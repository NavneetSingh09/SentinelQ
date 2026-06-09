import { useEffect, useState, useCallback } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell, Legend,
} from 'recharts'
import { api } from '../api'
import { useWs } from '../context/WsContext'
import type { Stats } from '../types'

const PRIORITY_COLORS: Record<string, string> = {
  critical: '#f85149',
  high:     '#e3b341',
  normal:   '#58a6ff',
  low:      '#3fb950',
}

const fmtTime = (ts?: string) => {
  const d = ts ? new Date(ts) : new Date()
  return isNaN(d.getTime()) ? new Date().toLocaleTimeString() : d.toLocaleTimeString()
}

export default function Overview() {
  const [stats, setStats] = useState<Stats | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const { connected, messages } = useWs()

  const fetchStats = useCallback(async () => {
    try {
      setStats(await api.getStats())
      setError(null)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load stats')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    fetchStats()
    const id = setInterval(fetchStats, 10_000)
    return () => clearInterval(id)
  }, [fetchStats])

  const queueData = stats
    ? Object.entries(stats.queue_depths).map(([topic, depth]) => ({
        name: topic.replace('alerts.', ''),
        depth,
      }))
    : []

  const priorityData = stats
    ? Object.entries(stats.by_priority)
        .filter(([, v]) => v > 0)
        .map(([name, value]) => ({ name, value }))
    : []

  if (loading) return <div className="loading">Loading…</div>

  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">Overview</h1>
          <p className="page-subtitle">Live system health — auto-refreshes every 10 s</p>
        </div>
        <button className="btn btn-secondary btn-sm" onClick={fetchStats}>↻ Refresh</button>
      </div>

      {error && <div className="error-msg">{error}</div>}

      {stats && (
        <>
          <div className="stat-grid">
            <div className="stat-card">
              <div className="stat-label">Submitted</div>
              <div className="stat-value">{stats.total_submitted.toLocaleString()}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Processed</div>
              <div className="stat-value green">{stats.total_processed.toLocaleString()}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Success Rate</div>
              <div
                className={`stat-value ${
                  stats.success_rate >= 90 ? 'green' : stats.success_rate >= 70 ? 'yellow' : 'red'
                }`}
              >
                {stats.success_rate}%
              </div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Failed</div>
              <div className="stat-value red">{stats.failed_jobs.toLocaleString()}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Dead Letter</div>
              <div className="stat-value muted">{stats.dead_letter_count.toLocaleString()}</div>
            </div>
          </div>

          <div className="charts-row">
            <div className="card">
              <div className="card-title">Queue Depth by Topic</div>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={queueData} margin={{ top: 4, right: 4, bottom: 0, left: -20 }}>
                  <XAxis
                    dataKey="name"
                    tick={{ fontSize: 11, fill: '#8b949e' }}
                    axisLine={false}
                    tickLine={false}
                  />
                  <YAxis
                    tick={{ fontSize: 11, fill: '#8b949e' }}
                    axisLine={false}
                    tickLine={false}
                    allowDecimals={false}
                  />
                  <Tooltip
                    contentStyle={{
                      background: '#161b22',
                      border: '1px solid #30363d',
                      borderRadius: '6px',
                      fontSize: 12,
                    }}
                    cursor={{ fill: 'rgba(88,166,255,0.06)' }}
                  />
                  <Bar dataKey="depth" radius={[3, 3, 0, 0]}>
                    {queueData.map(({ name }) => (
                      <Cell key={name} fill={PRIORITY_COLORS[name] ?? '#58a6ff'} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>

            <div className="card">
              <div className="card-title">Jobs by Priority</div>
              {priorityData.length === 0 ? (
                <div
                  style={{
                    textAlign: 'center',
                    padding: '60px 0',
                    color: 'var(--text-muted)',
                    fontSize: 12,
                  }}
                >
                  No data yet
                </div>
              ) : (
                <ResponsiveContainer width="100%" height={200}>
                  <PieChart>
                    <Pie
                      data={priorityData}
                      cx="50%"
                      cy="45%"
                      innerRadius={48}
                      outerRadius={74}
                      dataKey="value"
                      paddingAngle={2}
                    >
                      {priorityData.map(({ name }) => (
                        <Cell key={name} fill={PRIORITY_COLORS[name] ?? '#58a6ff'} />
                      ))}
                    </Pie>
                    <Tooltip
                      contentStyle={{
                        background: '#161b22',
                        border: '1px solid #30363d',
                        borderRadius: '6px',
                        fontSize: 12,
                      }}
                    />
                    <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 11 }} />
                  </PieChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>
        </>
      )}

      <div className="card">
        <div className="card-title">
          Live Event Feed
          <span className={`ws-dot${connected ? ' connected' : ''}`} />
          <span style={{ fontWeight: 400, textTransform: 'none', letterSpacing: 0 }}>
            {connected ? 'connected' : 'disconnected'}
          </span>
        </div>
        <div className="live-feed">
          {messages.length === 0 ? (
            <div
              style={{
                padding: '28px 0',
                textAlign: 'center',
                color: 'var(--text-muted)',
                fontSize: 12,
              }}
            >
              {connected ? 'Waiting for events…' : 'Connect to the API to see live updates.'}
            </div>
          ) : (
            messages.map((msg, i) => (
              <div key={i} className="feed-item">
                <span className="feed-time">{fmtTime(msg.timestamp)}</span>
                <div className="feed-body">
                  <span className="feed-job-id">
                    {msg.job_id ? `${msg.job_id.slice(0, 8)}…` : '—'}
                  </span>{' '}
                  {msg.status && (
                    <span className={`badge badge-${msg.status}`}>{msg.status}</span>
                  )}
                  {msg.worker_id && (
                    <span style={{ color: 'var(--text-muted)', marginLeft: 8, fontSize: 11 }}>
                      worker:{msg.worker_id}
                    </span>
                  )}
                  {msg.error && (
                    <div style={{ color: 'var(--critical)', marginTop: 2, fontSize: 11 }}>
                      {msg.error}
                    </div>
                  )}
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </>
  )
}
