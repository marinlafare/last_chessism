import { useEffect, useMemo, useState } from 'react'
import PlayerAnalysisDisclosure from './PlayerAnalysisDisclosure'
import PlayerDailyEfficiencyChart from './PlayerDailyEfficiencyChart'
import PlayerGameExplorerModal from './PlayerGameExplorerModal'
import PlayerGameReviewModal from './PlayerGameReviewModal'
import { PLAYER_ANALYSIS_MODES } from './PlayerModeToggles'

export default function PlayerMeasuresPanel({ playerName, quality }) {
  const [explorerSelection, setExplorerSelection] = useState(null)
  const [reviewGameId, setReviewGameId] = useState(null)
  const availableModes = useMemo(() => new Set(PLAYER_ANALYSIS_MODES.filter((mode) => (
    !quality.by_mode || Number(quality.by_mode[mode]?.coverage?.eligible_games || 0) > 0
  ))), [quality])

  useEffect(() => {
    setExplorerSelection(null)
    setReviewGameId(null)
  }, [quality, playerName])

  return (
    <>
      <div className="player-analysis-chart-list">
        <PlayerAnalysisDisclosure label="Accuracy" buttonTarget="player.accuracy.toggle">
          <PlayerDailyEfficiencyChart
            availableModes={availableModes}
            playerName={playerName}
            onSelectPoint={setExplorerSelection}
          />
        </PlayerAnalysisDisclosure>
      </div>
      {!reviewGameId ? (
        <PlayerGameExplorerModal
          playerName={playerName}
          selection={explorerSelection}
          onClose={() => setExplorerSelection(null)}
          onOpenGame={setReviewGameId}
        />
      ) : null}
      <PlayerGameReviewModal
        gameId={reviewGameId}
        playerName={playerName}
        onBack={() => setReviewGameId(null)}
        onClose={() => {
          setReviewGameId(null)
          setExplorerSelection(null)
        }}
      />
    </>
  )
}
