import { ReactNode } from 'react'
import { NavLink } from 'react-router-dom'
import { useWs } from '../context/WsContext'

const NAV = [
  { to: '/overview',  label: 'Overview',  icon: '◈' },
  { to: '/alerts',    label: 'Alerts',    icon: '⚡' },
  { to: '/schedules', label: 'Schedules', icon: '⏱' },
]

export default function Layout({ children }: { children: ReactNode }) {
  const { connected } = useWs()

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-logo">Sentinel<span>Q</span></div>
        <nav className="sidebar-nav">
          {NAV.map(n => (
            <NavLink
              key={n.to}
              to={n.to}
              className={({ isActive }) => `nav-link${isActive ? ' active' : ''}`}
            >
              <span>{n.icon}</span>
              {n.label}
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-footer">
          <span className={`ws-dot${connected ? ' connected' : ''}`} />
          {connected ? 'Live' : 'Disconnected'}
        </div>
      </aside>
      <main className="main-content">{children}</main>
    </div>
  )
}
