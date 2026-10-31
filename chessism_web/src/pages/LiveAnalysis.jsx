import { pageStyleTarget } from '../appearance/styleCore.js'
import { useEffect, useMemo, useRef, useState } from 'react'
import './live-analysis/liveAnalysis.css'
import { Chess } from 'chess.js'
import { Chessboard } from 'react-chessboard'
import SideRail from '../components/layout/SideRail'
import { formatNumber } from '../utils/formatters'
import { useBrowserAnalysis } from './live-analysis/useBrowserAnalysis'
import { ENGINE_ASSETS } from './live-analysis/browserStockfish'

const START_FEN = new Chess().fen()

const getAnalysisLines = (result) => {
  const analysis = result?.analysis
  if (Array.isArray(analysis)) return analysis
  return analysis && typeof analysis === 'object' ? [analysis] : []
}

const getScoreLabel = (score) => {
  if (score == null) return '--'
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

function LiveAnalysis() {
  const [fen, setFen] = useState(START_FEN)
  const [engineOn, setEngineOn] = useState(false)
  const { analysis, evaluation, status, error } = useBrowserAnalysis(fen, engineOn)
  const [boardWidth, setBoardWidth] = useState(280)
  const [selectedSquare, setSelectedSquare] = useState('')
  const stageRef = useRef(null)

  const game = useMemo(() => new Chess(fen), [fen])
  const analysisLines = useMemo(() => getAnalysisLines(analysis), [analysis])
  const bestLine = analysisLines[0]
  const whiteScorePercent = getWhiteScorePercent(evaluation?.score)
  const evaluationNote = !evaluation ? 'Enable the engine to evaluate this position'
    : !engineOn ? 'Last evaluation — engine off'
      : evaluation.fen !== fen ? 'Previous position evaluation — waiting for the new score' : 'Evaluation from White’s perspective'
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
    const node = stageRef.current
    if (!node) return undefined

    const updateWidth = () => {
      const narrow = window.matchMedia('(max-width: 720px)').matches
      const { width, height } = node.getBoundingClientRect()
      const availableWidth = width - (narrow ? 0 : 44) - 2
      const availableHeight = height - (narrow ? 40 : 0) - 2
      setBoardWidth(Math.max(1, Math.floor(Math.min(availableWidth, availableHeight))))
    }

    updateWidth()
    const observer = new ResizeObserver(updateWidth)
    observer.observe(node)
    return () => observer.disconnect()
  }, [])

  const makeMove = (sourceSquare, targetSquare, piece) => {
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
    setSelectedSquare('')
  }

  return (
    <div {...pageStyleTarget('page')} className="page-frame live-page">
      <SideRail />
        <main className="live-analysis-main">
          <section {...pageStyleTarget('analysis.panel')} className="live-analysis-panel">
            <div className="live-analysis-toolbar">
              <h1 {...pageStyleTarget('heading')}>Chessism</h1>
              <button {...pageStyleTarget('button')}
                className={`btn btn-secondary btn-inline live-stockfish-toggle ${engineOn ? 'active' : ''}`}
                type="button"
                aria-pressed={engineOn}
                aria-label={engineOn ? 'Stop engine' : 'Start engine'}
                title={engineOn ? 'Turn off Stockfish Lite' : 'Turn on Stockfish Lite — runs in your browser'}
                onClick={() => setEngineOn((current) => !current)}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round">
                  <rect x="6" y="6" width="12" height="12" rx="2" />
                  <path d="M9 3v3m6-3v3M9 18v3m6-3v3M3 9h3m-3 6h3M18 9h3m-3 6h3M13 9l-3 4h4l-3 3" />
                </svg>
              </button>
            </div>

            <div className="live-analysis-stage" ref={stageRef} style={{ '--live-board-size': `${boardWidth}px` }}>
              <div
                className="live-score-bar"
                style={{
                  '--white-score-fraction': whiteScorePercent / 100,
                }}
                title={evaluationNote}
                aria-label={`Engine score: ${getScoreLabel(evaluation?.score)}. ${evaluationNote}`}
              >
                <div className="live-score-black" />
                <div className="live-score-white" />
                <span>{getScoreLabel(evaluation?.score)}</span>
              </div>

              <div className="live-board-shell">
                <Chessboard
                  id="live-analysis-board"
                  position={fen}
                  boardWidth={boardWidth}
                  animationDuration={0}
                  arePiecesDraggable
                  arePremovesAllowed={false}
                  autoPromoteToQueen
                  customSquareStyles={customSquareStyles}
                  onPieceDrop={handlePieceDrop}
                  onSquareClick={handleSquareClick}
                />
              </div>
            </div>

            <div className="live-analysis-footer">
              <span className="stat-chip">{turnLabel} to move</span>
              <span className="stat-chip" role="status" aria-live="polite">{!engineOn ? 'Stockfish off' : ({ loading: 'Loading engine…', analyzing: 'Analyzing', paused: 'Paused', error: 'Engine unavailable' }[status] || getScoreLabel(bestLine?.score))}</span>
              <span className="stat-chip">Depth {formatNumber(bestLine?.depth)}</span>
              <span className="stat-chip">Best {moveToSan(fen, bestLine?.pv?.[0])}</span>
              <button {...pageStyleTarget('button')} className="btn btn-secondary btn-inline" type="button" onClick={resetBoard}>
                Reset
              </button>
              <a {...pageStyleTarget('link')} className="live-engine-credit" href={`${ENGINE_ASSETS}NOTICES.txt`} target="_blank" rel="noopener noreferrer" title="Engine license and source code">Stockfish Lite · Browser</a>
            </div>

            {error ? <div className="status-banner warn" role="alert">{error}</div> : null}
          </section>
        </main>
    </div>
  )
}

export default LiveAnalysis
