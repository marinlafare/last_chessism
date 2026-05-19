import { useEffect, useMemo, useRef, useState } from 'react'
import { Chess } from 'chess.js'
import { Chessboard } from 'react-chessboard'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import { API_BASE_URL } from '../config'

const START_FEN = new Chess().fen()
const LIVE_ANALYSIS_NODES = 250_000
const LIVE_ANALYSIS_MULTIPV = 3

const formatNumber = (value) => {
  const numeric = Number(value ?? 0)
  if (!Number.isFinite(numeric)) return '0'
  return numeric.toLocaleString('en-US')
}

const getAnalysisLines = (result) => {
  const analysis = result?.analysis
  if (Array.isArray(analysis)) return analysis
  return analysis && typeof analysis === 'object' ? [analysis] : []
}

const getScoreLabel = (score) => {
  const numeric = Number(score)
  if (!Number.isFinite(numeric)) return '--'

  if (Math.abs(numeric) >= 9000) {
    const mateDistance = Math.abs(Math.round(Math.abs(numeric) - 10000))
    return mateDistance > 0 ? `Mate ${mateDistance}` : 'Mate'
  }

  const pawns = numeric / 100
  return `${pawns > 0 ? '+' : ''}${pawns.toFixed(2)}`
}

const getWhiteScorePercent = (score) => {
  const numeric = Number(score)
  if (!Number.isFinite(numeric)) return 50
  if (numeric >= 9000) return 96
  if (numeric <= -9000) return 4
  return Math.max(4, Math.min(96, 50 + (numeric / 600) * 42))
}

const moveToSan = (fen, move) => {
  const text = String(move || '')
  if (text.length < 4) return text || '--'

  try {
    const game = new Chess(fen)
    const result = game.move({
      from: text.slice(0, 2),
      to: text.slice(2, 4),
      promotion: text.slice(4, 5) || undefined
    })
    return result?.san || text
  } catch {
    return text
  }
}

async function analyzeLiveFen(fen, signal) {
  const response = await fetch(`${API_BASE_URL}/analysis/fen`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    signal,
    body: JSON.stringify({
      fens: [fen],
      nodes_limit: LIVE_ANALYSIS_NODES,
      multipv: LIVE_ANALYSIS_MULTIPV
    })
  })
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return Array.isArray(payload) ? payload[0] : payload
}

function LiveAnalysis() {
  const [fen, setFen] = useState(START_FEN)
  const [engineOn, setEngineOn] = useState(false)
  const [analysis, setAnalysis] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [boardWidth, setBoardWidth] = useState(540)
  const [selectedSquare, setSelectedSquare] = useState('')
  const boardWrapRef = useRef(null)

  const game = useMemo(() => new Chess(fen), [fen])
  const analysisLines = useMemo(() => getAnalysisLines(analysis), [analysis])
  const bestLine = analysisLines[0]
  const whiteScorePercent = getWhiteScorePercent(bestLine?.score)
  const turnLabel = game.turn() === 'b' ? 'Black' : 'White'
  const legalTargets = useMemo(() => {
    if (!selectedSquare) return []
    try {
      return game.moves({ square: selectedSquare, verbose: true }).map((move) => move.to)
    } catch {
      return []
    }
  }, [game, selectedSquare])
  const customSquareStyles = useMemo(() => {
    const styles = {}
    if (selectedSquare) {
      styles[selectedSquare] = {
        boxShadow: 'inset 0 0 0 3px rgba(21, 199, 128, 0.85)'
      }
    }
    legalTargets.forEach((square) => {
      styles[square] = {
        ...(styles[square] || {}),
        background: 'radial-gradient(circle, rgba(21, 199, 128, 0.42) 0 18%, transparent 20%)'
      }
    })
    return styles
  }, [legalTargets, selectedSquare])

  useEffect(() => {
    const node = boardWrapRef.current
    if (!node) return undefined

    const updateWidth = () => {
      setBoardWidth(Math.max(280, Math.min(540, Math.floor(node.clientWidth))))
    }

    updateWidth()
    const observer = new ResizeObserver(updateWidth)
    observer.observe(node)
    return () => observer.disconnect()
  }, [])

  useEffect(() => {
    if (!engineOn) {
      setLoading(false)
      setError('')
      return undefined
    }

    const controller = new AbortController()
    const timer = window.setTimeout(async () => {
      setLoading(true)
      setError('')
      try {
        const payload = await analyzeLiveFen(fen, controller.signal)
        setAnalysis(payload)
      } catch (err) {
        if (err?.name !== 'AbortError') {
          setError(err instanceof Error ? err.message : 'Analysis failed')
        }
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false)
        }
      }
    }, 180)

    return () => {
      window.clearTimeout(timer)
      controller.abort()
    }
  }, [engineOn, fen])

  const makeMove = (sourceSquare, targetSquare, piece) => {
    if (!engineOn) return false

    const nextGame = new Chess(fen)
    const movingPiece = piece || nextGame.get(sourceSquare)
    const pieceType = typeof movingPiece === 'string' ? movingPiece[1]?.toLowerCase() : movingPiece?.type
    const isPromotion = pieceType === 'p' && ['1', '8'].includes(targetSquare?.[1])
    let move = null

    try {
      move = nextGame.move({
        from: sourceSquare,
        to: targetSquare,
        promotion: isPromotion ? 'q' : undefined
      })
    } catch {
      move = null
    }

    if (!move) return false
    setFen(nextGame.fen())
    setSelectedSquare('')
    return true
  }

  const handlePieceDrop = (sourceSquare, targetSquare, piece) => {
    return makeMove(sourceSquare, targetSquare, piece)
  }

  const handleSquareClick = (square) => {
    if (!engineOn) return

    const clickedPiece = game.get(square)
    if (selectedSquare && makeMove(selectedSquare, square)) return

    if (clickedPiece?.color === game.turn()) {
      setSelectedSquare(square)
      return
    }

    setSelectedSquare('')
  }

  const resetBoard = () => {
    setFen(START_FEN)
    setAnalysis(null)
    setError('')
    setSelectedSquare('')
  }

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="live-analysis-main">
          <section className="live-analysis-panel">
            <div className="live-analysis-toolbar">
              <p className="eyebrow">LIVE ANALYSIS</p>
              <button
                className={`btn btn-secondary btn-inline live-stockfish-toggle ${engineOn ? 'active' : ''}`}
                type="button"
                onClick={() => setEngineOn((current) => !current)}
              >
                Stockfish {engineOn ? 'On' : 'Off'}
              </button>
            </div>

            <div className="live-analysis-stage">
              <div
                className="live-score-bar"
                style={{
                  '--black-score-percent': `${100 - whiteScorePercent}%`,
                  '--white-score-percent': `${whiteScorePercent}%`
                }}
                aria-label="Engine score"
              >
                <div className="live-score-black" />
                <div className="live-score-white" />
                <span>{getScoreLabel(bestLine?.score)}</span>
              </div>

              <div className="live-board-shell" ref={boardWrapRef}>
                <Chessboard
                  id="live-analysis-board"
                  position={fen}
                  boardWidth={boardWidth}
                  animationDuration={0}
                  arePiecesDraggable={engineOn}
                  arePremovesAllowed={engineOn}
                  autoPromoteToQueen
                  customSquareStyles={customSquareStyles}
                  onPieceDrop={handlePieceDrop}
                  onSquareClick={handleSquareClick}
                />
              </div>
            </div>

            <div className="live-analysis-footer">
              <span className="stat-chip">{turnLabel} to move</span>
              <span className="stat-chip">{engineOn ? (loading ? 'Analyzing' : getScoreLabel(bestLine?.score)) : 'Stockfish off'}</span>
              <span className="stat-chip">Depth {formatNumber(bestLine?.depth)}</span>
              <span className="stat-chip">Best {moveToSan(fen, bestLine?.pv?.[0])}</span>
              <button className="btn btn-secondary btn-inline" type="button" onClick={resetBoard}>
                Reset
              </button>
            </div>

            {error ? <div className="status-banner warn">{error}</div> : null}
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default LiveAnalysis
