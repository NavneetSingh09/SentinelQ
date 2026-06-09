import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { WsProvider } from './context/WsContext'
import Layout from './components/Layout'
import Overview from './pages/Overview'
import Alerts from './pages/Alerts'
import Schedules from './pages/Schedules'

export default function App() {
  return (
    <WsProvider>
      <BrowserRouter>
        <Layout>
          <Routes>
            <Route path="/" element={<Navigate to="/overview" replace />} />
            <Route path="/overview" element={<Overview />} />
            <Route path="/alerts" element={<Alerts />} />
            <Route path="/schedules" element={<Schedules />} />
          </Routes>
        </Layout>
      </BrowserRouter>
    </WsProvider>
  )
}
