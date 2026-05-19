import { useEffect, useState } from 'react'
import Home from './pages/Home'
import Games from './pages/Games'
import Players from './pages/Players'
import DownloadNewGames from './pages/DownloadNewGames'
import MainCharacters from './pages/MainCharacters'
import SecondaryCharacter from './pages/SecondaryCharacter'
import Positions from './pages/Positions'
import LiveAnalysis from './pages/LiveAnalysis'
import ScoredPositions from './pages/ScoredPositions'
import AnalyzeTimes from './pages/AnalyzeTimes'
import { API_BASE_URL } from './config'

async function fetchCurrentAccount() {
  const response = await fetch(`${API_BASE_URL}/auth/me`, {
    credentials: 'include'
  })
  if (!response.ok) return null
  return response.json()
}

async function unlockSuperadmin(code) {
  const response = await fetch(`${API_BASE_URL}/auth/gate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include',
    body: JSON.stringify({ code })
  })
  if (!response.ok) return null
  return response.json()
}

function SuperadminGate({ onUnlock }) {
  const [value, setValue] = useState('')

  const handleChange = async (event) => {
    const nextValue = event.target.value
    setValue(nextValue)
    const payload = await unlockSuperadmin(nextValue)
    if (payload?.ok) {
      onUnlock()
    }
  }

  return (
    <main className="superadmin-gate">
      <input
        className="superadmin-gate-input"
        type="text"
        autoFocus
        autoComplete="off"
        inputMode="numeric"
        value={value}
        onChange={handleChange}
      />
    </main>
  )
}

async function fetchAdmins() {
  const response = await fetch(`${API_BASE_URL}/auth/admins`, {
    credentials: 'include'
  })
  if (!response.ok) {
    throw new Error('Gate access required.')
  }
  return response.json()
}

async function submitAdminSignup(data) {
  const response = await fetch(`${API_BASE_URL}/auth/admins/signup`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include',
    body: JSON.stringify(data)
  })
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) {
    throw new Error(payload.detail || 'Could not create admin.')
  }
  return payload
}

async function submitAdminLogin(data) {
  const response = await fetch(`${API_BASE_URL}/auth/admins/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'include',
    body: JSON.stringify(data)
  })
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) {
    throw new Error(payload.detail || 'Could not sign in.')
  }
  return payload
}

async function logoutAdmin() {
  await fetch(`${API_BASE_URL}/auth/logout`, {
    method: 'POST',
    credentials: 'include'
  }).catch(() => {})
}

function AdminsPage({ onAuthenticated, onGateExpired }) {
  const [admins, setAdmins] = useState([])
  const [selectedAdmin, setSelectedAdmin] = useState(null)
  const [mode, setMode] = useState(null)
  const [form, setForm] = useState({ name: '', email: '', password: '', repeat_password: '', chess_com_nickname: '' })
  const [error, setError] = useState('')

  useEffect(() => {
    let cancelled = false
    fetchAdmins()
      .then((payload) => {
        if (!cancelled) setAdmins(Array.isArray(payload.admins) ? payload.admins : [])
      })
      .catch(() => {
        if (!cancelled) onGateExpired()
      })
    return () => {
      cancelled = true
    }
  }, [onGateExpired])

  const updateForm = (key, value) => {
    setForm((current) => ({ ...current, [key]: value }))
  }

  const openLogin = (admin) => {
    setError('')
    setSelectedAdmin(admin)
    setMode('login')
    setForm({ name: admin.name, email: admin.email, password: '', repeat_password: '', chess_com_nickname: admin.chess_com_nickname || '' })
  }

  const openSignup = () => {
    setError('')
    setSelectedAdmin(null)
    setMode('signup')
    setForm({ name: '', email: '', password: '', repeat_password: '', chess_com_nickname: '' })
  }

  const handleSubmit = async (event) => {
    event.preventDefault()
    setError('')
    try {
      const payload = mode === 'signup'
        ? await submitAdminSignup(form)
        : await submitAdminLogin({ email: form.email, password: form.password })
      if (payload?.account) {
        onAuthenticated(payload.account)
      }
    } catch (submitError) {
      setError(submitError instanceof Error ? submitError.message : 'Request failed.')
    }
  }

  return (
    <main className="admins-gate-page">
      <section className="admins-panel">
        {!admins.length ? (
          <p className="admins-empty">No admins yet.</p>
        ) : (
          <div className="admins-list">
            {admins.map((admin) => (
              <button className="admin-name-button" type="button" key={admin.id} onClick={() => openLogin(admin)}>
                {admin.name}
              </button>
            ))}
          </div>
        )}

        {mode ? (
          <form className="admin-auth-form" onSubmit={handleSubmit}>
            {mode === 'signup' ? (
              <>
                <input className="admin-auth-input" type="text" value={form.name} onChange={(event) => updateForm('name', event.target.value)} autoComplete="name" placeholder="name" />
                <input className="admin-auth-input" type="email" value={form.email} onChange={(event) => updateForm('email', event.target.value)} autoComplete="email" placeholder="email" />
                <input className="admin-auth-input" type="text" value={form.chess_com_nickname} onChange={(event) => updateForm('chess_com_nickname', event.target.value)} autoComplete="off" placeholder="chess.com" />
              </>
            ) : (
              <input className="admin-auth-input" type="email" value={form.email} onChange={(event) => updateForm('email', event.target.value)} autoComplete="email" placeholder="email" />
            )}
            <input className="admin-auth-input" type="password" value={form.password} onChange={(event) => updateForm('password', event.target.value)} autoComplete={mode === 'signup' ? 'new-password' : 'current-password'} placeholder="password" />
            {mode === 'signup' ? (
              <input className="admin-auth-input" type="password" value={form.repeat_password} onChange={(event) => updateForm('repeat_password', event.target.value)} autoComplete="new-password" placeholder="repeat password" />
            ) : null}
            <button className="admin-auth-submit" type="submit">{mode === 'signup' ? 'Sign up' : 'Sign in'}</button>
            {error ? <p className="admin-auth-error">{error}</p> : null}
          </form>
        ) : null}
      </section>
      <button className="admin-signup-fab" type="button" onClick={openSignup}>Sign up</button>
    </main>
  )
}

function AdminOnlineStatus({ account }) {
  const displayName = account?.name || account?.email || 'Admin'

  return (
    <div className="admin-online-status" aria-label={`${displayName} online`}>
      <span className="admin-online-dot" aria-hidden="true" />
      <span>{displayName}</span>
    </div>
  )
}

function SuperadminSystem({ account, onLogout }) {
  const path = window.location.pathname.replace(/\/+$/, '') || '/'
  let page = <Home />

  if (path === '/games') {
    page = <Games />
  } else if (path === '/players') {
    page = <Players />
  } else if (path === '/add_games' || path === '/download_new_games') {
    page = <DownloadNewGames />
  } else if (path === '/main_characters') {
    page = <MainCharacters />
  } else if (path === '/secondary_character') {
    page = <SecondaryCharacter />
  } else if (path === '/analize_positions' || path === '/positions') {
    page = <Positions />
  } else if (path === '/live_analysis' || path === '/live_analyzis') {
    page = <LiveAnalysis />
  } else if (path === '/scored_positions') {
    page = <ScoredPositions />
  } else if (path === '/analyze_times') {
    page = <AnalyzeTimes />
  }

  return (
    <>
      {page}
      <AdminOnlineStatus account={account} />
      <button className="superadmin-logout" type="button" onClick={onLogout}>
        Log out
      </button>
    </>
  )
}

function App() {
  const [account, setAccount] = useState(null)
  const [checkedAuth, setCheckedAuth] = useState(false)
  const [path, setPath] = useState(window.location.pathname.replace(/\/+$/, '') || '/')

  useEffect(() => {
    let cancelled = false
    fetchCurrentAccount()
      .then((payload) => {
        if (cancelled) return
        setAccount(payload?.account || null)
      })
      .finally(() => {
        if (!cancelled) setCheckedAuth(true)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const enterSystem = (nextAccount) => {
    setAccount(nextAccount)
    window.history.pushState(null, '', '/')
    setPath('/')
  }

  const openAdmins = () => {
    window.history.pushState(null, '', '/admins')
    setPath('/admins')
  }

  const returnToGate = () => {
    window.history.pushState(null, '', '/')
    setPath('/')
  }

  const handleLogout = async () => {
    await logoutAdmin()
    setAccount(null)
    window.history.pushState(null, '', '/')
    setPath('/')
  }

  if (!checkedAuth) {
    return <SuperadminGate onUnlock={openAdmins} />
  }

  if (!account) {
    if (path === '/admins') {
      return <AdminsPage onAuthenticated={enterSystem} onGateExpired={returnToGate} />
    }
    return <SuperadminGate onUnlock={openAdmins} />
  }

  return <SuperadminSystem account={account} onLogout={handleLogout} />
}

export default App
