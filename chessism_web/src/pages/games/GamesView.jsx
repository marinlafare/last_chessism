import { Chessboard } from 'react-chessboard'
import Header from '../../components/layout/Header'
import Footer from '../../components/layout/Footer'
import SideRail from '../../components/layout/SideRail'
import {
  ClusterSelect, CompactBarChart, HeatStrip, OPENINGS_BOARD_WIDTH,
  OPENING_N_MOVES_MAX, OPENING_N_MOVES_MIN, TimeControlRatingsScatterChart,
  formatNumber, formatPercent, formatSeconds, handleBadgeHoverLeave,
  handleBadgeHoverMove,
} from './gamesPageSupport'

export default function GamesView({ page }) {
  const {
    activeOpeningGames, activeOpeningMeanRating, activeOpeningRow, activeOpeningTop,
    activityTrendData, activityTrendError, activityTrendLoading, boardFen,
    closeRatingModal, countLoading, countPlayer, countResult, gameLengthData,
    gameLengthError, gameLengthLoading, generalities, generalitiesError,
    generalitiesLoading, handleActivityClusterChange, handleCount,
    handleLengthsClusterChange, handleLoadOpenings, handleMoveColorChange,
    handleMovesClusterChange, handleMovesNext, handleMovesPrevious, handleNext,
    handleOpeningStep, handleOpeningsClusterChange, handlePrevious,
    handleResultsClusterChange, handleSelectMode, handleSelectOpening, modeError,
    modeLoading, modePage, modeRows, modeTotalPages, openRatingModal,
    openingError, openingLoading, openingMoveAnimByTop, openingNMoves,
    openingNMovesInput, openingPlyByTop, openingRequested, openingRows, ratingChart,
    ratingError, ratingLoading, ratingModalOpen, recentData, recentError, recentLoading,
    recentPage, resultMatrix, resultMatrixError, resultMatrixLoading,
    selectedActivityCluster, selectedLengthsCluster, selectedMode, selectedMoveColor,
    selectedMovesCluster, selectedOpeningsCluster, selectedPlayer,
    selectedResultsCluster, setCountPlayer, setOpeningNMovesInput, summaryData,
    summaryError, summaryLoading, timeControlCounts, timeControlsError,
    timeControlsLoading,
  } = page

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="games-main">
          <div className="games-hero-wrap">
            <h1
              className="games-hero-badge games-main-title badge-hover"
              onMouseMove={handleBadgeHoverMove}
              onMouseLeave={handleBadgeHoverLeave}
            >
              GameS
            </h1>
            <section className="games-hero">
              <div className="games-generalities-grid">
              <article className="games-generality-card">
                <h3>Number of games</h3>
                <p>
                  {generalitiesLoading
                    ? 'Loading...'
                    : formatNumber(generalities?.n_games_in_db)}
                </p>
              </article>
              <a
                className="games-generality-card games-link-card"
                href="/main_characters"
                data-note="players with all their games in"
              >
                <h3>Main characters</h3>
                <p>
                  {generalitiesLoading
                    ? 'Loading...'
                    : formatNumber(generalities?.main_characters)}
                </p>
              </a>
              <a
                className="games-generality-card games-link-card"
                href="/secondary_character"
                data-note="the opponents of our main players"
              >
                <h3>Secondary characters</h3>
                <p>
                  {generalitiesLoading
                    ? 'Loading...'
                    : formatNumber(generalities?.secondary_characters)}
                </p>
              </a>
              <a
                className="games-generality-card games-link-card"
                href="/analize_positions"
                data-note="stats about the positions"
              >
                <h3>Number of positions</h3>
                <p>
                  {generalitiesLoading
                    ? 'Loading...'
                    : formatNumber(generalities?.n_positions)}
                </p>
              </a>
              <a
                className="games-generality-card games-link-card"
                href="/scored_positions"
                data-note="stats about the positions"
              >
                <h3>Scored fens</h3>
                <p>
                  {generalitiesLoading
                    ? 'Loading...'
                    : formatNumber(generalities?.scored_fens)}
                </p>
              </a>
              <a
                className="games-generality-card games-link-card"
                href="/analize_positions"
                data-note="stats about the positions"
              >
                <h3>Unscored fens</h3>
                <p>
                  {generalitiesLoading
                    ? 'Loading...'
                    : formatNumber(Math.max(0, (generalities?.n_positions ?? 0) - (generalities?.scored_fens ?? 0)))}
                </p>
              </a>
            </div>
            {generalitiesError ? <p className="result-line">{generalitiesError}</p> : null}
            </section>
          </div>

          <div className="games-time-wrap">
            <h2
              className="games-hero-badge time-control-badge badge-hover"
              onMouseMove={handleBadgeHoverMove}
              onMouseLeave={handleBadgeHoverLeave}
            >
              Time Control
            </h2>
            <section className="games-time-controls" aria-label="Time controls">
              <div className="games-generalities-grid games-time-controls-grid">
              <button
                className="games-generality-card games-mode-button games-link-card"
                type="button"
                onClick={() => handleSelectMode('bullet')}
              >
                <h3>bullet</h3>
                <span className="time-control-icon bullet-cartridge-icon" aria-hidden="true">
                  <span className="bullet-cartridge-shape" />
                </span>
                <p>{timeControlsLoading ? 'Loading...' : formatNumber(timeControlCounts.bullet)}</p>
              </button>
              <button
                className="games-generality-card games-mode-button games-link-card"
                type="button"
                onClick={() => handleSelectMode('blitz')}
              >
                <h3>blitz</h3>
                <span className="time-control-icon time-control-icon-blitz" aria-hidden="true">⚡</span>
                <p>{timeControlsLoading ? 'Loading...' : formatNumber(timeControlCounts.blitz)}</p>
              </button>
              <button
                className="games-generality-card games-mode-button games-link-card"
                type="button"
                onClick={() => handleSelectMode('rapid')}
              >
                <h3>rapid</h3>
                <span className="time-control-icon time-control-icon-rapid" aria-hidden="true">⏱</span>
                <p>{timeControlsLoading ? 'Loading...' : formatNumber(timeControlCounts.rapid)}</p>
              </button>
            </div>
            {timeControlsError ? <p className="result-line">{timeControlsError}</p> : null}
            </section>
          </div>

          {selectedMode ? (
            <>
              <section className="games-mode-detail rating-clickable" aria-label={`${selectedMode} ratings`} onClick={openRatingModal}>
                {ratingLoading ? <p className="result-line">Loading ratings chart...</p> : null}
                {ratingError ? <p className="result-line">{ratingError}</p> : null}
                {!ratingLoading && !ratingError ? (
                  <TimeControlRatingsScatterChart mode={selectedMode} chart={ratingChart} size="large" />
                ) : null}
                <p className="result-line rating-hint">Click to expand</p>
              </section>
              {ratingModalOpen ? (
                <div className="rating-modal-backdrop" onClick={closeRatingModal}>
                  <div className="rating-modal" role="dialog" aria-label={`${selectedMode} ratings enlarged`} onClick={(e) => e.stopPropagation()}>
                    <TimeControlRatingsScatterChart mode={selectedMode} chart={ratingChart} />
                  </div>
                </div>
              ) : null}
            </>
          ) : null}

          {selectedMode ? (
            <section className="games-mode-detail" aria-label={`${selectedMode} top moves`}>
              <div className="section-head mode-detail-head">
                <h2 className="games-action-title">{selectedMode}</h2>
                <div className="mode-detail-controls">
                  <div className="mode-color-switch">
                    <button
                      className={`mode-color-btn mode-color-white ${selectedMoveColor === 'white' ? 'active' : ''}`}
                      type="button"
                      onClick={() => handleMoveColorChange('white')}
                    >
                      white
                    </button>
                    <button
                      className={`mode-color-btn mode-color-black ${selectedMoveColor === 'black' ? 'active' : ''}`}
                      type="button"
                      onClick={() => handleMoveColorChange('black')}
                    >
                      black
                    </button>
                  </div>
                  <ClusterSelect
                    idPrefix="moves-cluster"
                    value={selectedMovesCluster}
                    onChange={handleMovesClusterChange}
                  />
                </div>
              </div>
              <div className="mode-moves-table">
                <div className="mode-moves-row mode-moves-header mode-moves-chart-header">
                  <span>top moves chart</span>
                  <span className={`mode-chart-side mode-chart-side-${selectedMoveColor}`}>{selectedMoveColor}</span>
                </div>
                {modeLoading ? (
                  <p className="result-line">Loading top moves...</p>
                ) : null}
                {modeError ? (
                  <p className="result-line">{modeError}</p>
                ) : null}
                {!modeLoading && !modeError ? (
                  <div className="mode-moves-chart">
                    {(() => {
                      const maxTimes = Math.max(...modeRows.map((row) => Number(row.times_played || 0)), 1)
                      return modeRows.map((row) => {
                        const y = Number(row.times_played || 0)
                        const percent = Math.max(6, Math.round((y / maxTimes) * 100))
                        return (
                          <div key={`mode-${selectedMode}-move-${row.move_number}`} className="mode-moves-chart-col">
                            <span className="mode-moves-chart-value">{formatNumber(y)}</span>
                            <div className="mode-moves-chart-bar-wrap">
                              <div className="mode-moves-chart-bar" style={{ height: `${percent}%` }} />
                            </div>
                            <span className="mode-moves-chart-x">#{row.move_number}</span>
                            <span className="mode-moves-chart-tick">{row.move}</span>
                          </div>
                        )
                      })
                    })()}
                  </div>
                ) : null}
                <div className="mode-moves-pagination">
                  <button
                    className="btn btn-secondary"
                    type="button"
                    onClick={handleMovesPrevious}
                    disabled={modeLoading || modePage <= 1}
                  >
                    previous 5
                  </button>
                  <span>page {modePage} / {modeTotalPages || 0}</span>
                  <button
                    className="btn btn-secondary"
                    type="button"
                    onClick={handleMovesNext}
                    disabled={modeLoading || modeTotalPages <= 0 || modePage >= modeTotalPages}
                  >
                    next 5
                  </button>
                </div>
              </div>
            </section>
          ) : null}

          {selectedMode ? (
            <section className="games-openings-detail" aria-label={`${selectedMode} top openings`}>
              <div className="section-head analytics-section-head">
                <h2
                  className="games-action-title badge-hover openings-title"
                  onMouseMove={handleBadgeHoverMove}
                  onMouseLeave={handleBadgeHoverLeave}
                >
                  {selectedMode} openings
                </h2>
                <ClusterSelect
                  idPrefix="openings-cluster"
                  value={selectedOpeningsCluster}
                  onChange={handleOpeningsClusterChange}
                />
              </div>
              <div className="openings-layout">
                <div className="openings-board-row">
                  <div
                    className="react-board-wrap"
                    style={{ width: `${Math.round(OPENINGS_BOARD_WIDTH * 1.05)}px` }}
                  >
                    <Chessboard
                      id="openings-board"
                      position={boardFen}
                      arePiecesDraggable={false}
                      boardWidth={OPENINGS_BOARD_WIDTH}
                    />
                  </div>
                </div>
                <div className="openings-query-row">
                  <label htmlFor="openings-n-moves">moves</label>
                  <select
                    id="openings-n-moves"
                    className="text-input openings-n-moves-select"
                    value={openingNMovesInput}
                    onChange={(event) => setOpeningNMovesInput(event.target.value)}
                  >
                    <option value="" disabled>
                      min: {OPENING_N_MOVES_MIN} max: {OPENING_N_MOVES_MAX}
                    </option>
                    {Array.from({ length: OPENING_N_MOVES_MAX - OPENING_N_MOVES_MIN + 1 }, (_, idx) => {
                      const val = OPENING_N_MOVES_MIN + idx
                      return (
                        <option key={`n-moves-${val}`} value={val}>
                          {val}
                        </option>
                      )
                    })}
                  </select>
                  <button
                    className="btn btn-secondary"
                    type="button"
                    onClick={() => handleLoadOpenings()}
                    disabled={openingLoading}
                  >
                    {openingLoading ? 'Loading...' : 'go'}
                  </button>
                </div>
                <div className="opening-meta-row">
                  <span className="opening-meta-chip">
                    mean rating: {activeOpeningMeanRating !== null ? formatNumber(activeOpeningMeanRating) : '--'}
                  </span>
                  <span className="opening-meta-chip">
                    number of games: {activeOpeningGames !== null ? formatNumber(activeOpeningGames) : '--'}
                  </span>
                </div>
                <div className="mode-moves-table openings-list-table">
                  {openingLoading ? <p className="result-line">Loading openings...</p> : null}
                  {openingError ? <p className="result-line">{openingError}</p> : null}
                  {!openingRequested && !openingLoading ? (
                    <p className="result-line">Select moves and click go.</p>
                  ) : null}
                  {openingRequested && !openingLoading && !openingError ? (
                    openingRows.length ? (
                      openingRows.map((row) => {
                        const totalHalfMoves = row.half_moves.length
                        const currentPly = Number(openingPlyByTop[row.top] || (totalHalfMoves > 0 ? 1 : 0))
                        const currentMove = currentPly > 0 ? row.half_moves[currentPly - 1] : '-'
                        const moveAnimState = openingMoveAnimByTop[row.top] || null
                        const moveAnimClass =
                          moveAnimState?.dir === 'from-right'
                            ? 'opening-move-enter-from-right'
                            : moveAnimState?.dir === 'from-left'
                              ? 'opening-move-enter-from-left'
                              : ''
                        return (
                          <div
                            key={`opening-${selectedMode}-${row.top}`}
                            className={`mode-moves-row opening-step-row ${activeOpeningTop === row.top ? 'active' : ''}`}
                          >
                            <button
                              className={`opening-top-btn ${activeOpeningTop === row.top ? 'active' : ''}`}
                              type="button"
                              onClick={() => handleSelectOpening(row.top)}
                            >
                              top_{row.top}
                            </button>
                            <button
                              className="btn btn-secondary opening-arrow-btn"
                              type="button"
                              onClick={() => handleOpeningStep(row.top, 'prev')}
                              disabled={currentPly <= 1}
                            >
                              {'<'}
                            </button>
                            <span
                              key={`opening-move-${row.top}-${currentPly}-${moveAnimState?.seq || 0}`}
                              className={`opening-move-text ${moveAnimClass}`}
                            >
                              {currentMove}
                            </span>
                            <button
                              className="btn btn-secondary opening-arrow-btn"
                              type="button"
                              onClick={() => handleOpeningStep(row.top, 'next')}
                              disabled={currentPly <= 0 || currentPly >= totalHalfMoves}
                            >
                              {'>'}
                            </button>
                          </div>
                        )
                      })
                    ) : (
                      <p className="result-line">No openings found.</p>
                    )
                  ) : null}
                </div>
              </div>
            </section>
          ) : null}

          {selectedMode ? (
            <section className="games-mode-detail" aria-label={`${selectedMode} result color matrix`}>
              <div className="section-head analytics-section-head">
                <h2 className="games-action-title">{selectedMode} results</h2>
                <ClusterSelect
                  idPrefix="results-cluster"
                  value={selectedResultsCluster}
                  onChange={handleResultsClusterChange}
                />
              </div>
              <div className="mode-moves-table analytics-table-wrap">
                {resultMatrixLoading ? <p className="result-line">Loading result matrix...</p> : null}
                {resultMatrixError ? <p className="result-line">{resultMatrixError}</p> : null}
                {!resultMatrixLoading && !resultMatrixError ? (
                  <>
                    <div className="matrix-color-grid">
                      <div className="matrix-color-card">
                        <div className="matrix-color-head">
                          <span className="result-matrix-color white">white</span>
                          <span className="matrix-color-meta">
                            score: {formatPercent(resultMatrix?.white?.score_rate || 0)}
                          </span>
                        </div>
                        <div className="wl-bar">
                          <div className="wl-row">
                            <span className="wl-label">Wins</span>
                            <div className="wl-track">
                              <div
                                className="wl-fill wl-fill-win"
                                style={{ width: `${((resultMatrix?.white?.wins || 0) / (resultMatrix?.white?.total || 1)) * 100}%` }}
                              />
                            </div>
                            <span className="wl-value">{formatNumber(resultMatrix?.white?.wins || 0)}</span>
                          </div>
                          <div className="wl-row">
                            <span className="wl-label">Losses</span>
                            <div className="wl-track">
                              <div
                                className="wl-fill wl-fill-loss"
                                style={{ width: `${((resultMatrix?.white?.losses || 0) / (resultMatrix?.white?.total || 1)) * 100}%` }}
                              />
                            </div>
                            <span className="wl-value">{formatNumber(resultMatrix?.white?.losses || 0)}</span>
                          </div>
                          <div className="wl-row">
                            <span className="wl-label">Draws</span>
                            <div className="wl-track">
                              <div
                                className="wl-fill wl-fill-draw"
                                style={{ width: `${((resultMatrix?.white?.draws || 0) / (resultMatrix?.white?.total || 1)) * 100}%` }}
                              />
                            </div>
                            <span className="wl-value">{formatNumber(resultMatrix?.white?.draws || 0)}</span>
                          </div>
                        </div>
                      </div>

                      <div className="matrix-color-card">
                        <div className="matrix-color-head">
                          <span className="result-matrix-color black">black</span>
                          <span className="matrix-color-meta">
                            score: {formatPercent(resultMatrix?.black?.score_rate || 0)}
                          </span>
                        </div>
                        <div className="wl-bar">
                          <div className="wl-row">
                            <span className="wl-label">Wins</span>
                            <div className="wl-track">
                              <div
                                className="wl-fill wl-fill-win"
                                style={{ width: `${((resultMatrix?.black?.wins || 0) / (resultMatrix?.black?.total || 1)) * 100}%` }}
                              />
                            </div>
                            <span className="wl-value">{formatNumber(resultMatrix?.black?.wins || 0)}</span>
                          </div>
                          <div className="wl-row">
                            <span className="wl-label">Losses</span>
                            <div className="wl-track">
                              <div
                                className="wl-fill wl-fill-loss"
                                style={{ width: `${((resultMatrix?.black?.losses || 0) / (resultMatrix?.black?.total || 1)) * 100}%` }}
                              />
                            </div>
                            <span className="wl-value">{formatNumber(resultMatrix?.black?.losses || 0)}</span>
                          </div>
                          <div className="wl-row">
                            <span className="wl-label">Draws</span>
                            <div className="wl-track">
                              <div
                                className="wl-fill wl-fill-draw"
                                style={{ width: `${((resultMatrix?.black?.draws || 0) / (resultMatrix?.black?.total || 1)) * 100}%` }}
                              />
                            </div>
                            <span className="wl-value">{formatNumber(resultMatrix?.black?.draws || 0)}</span>
                          </div>
                        </div>
                      </div>
                    </div>
                    <p className="result-line">total games: {formatNumber(resultMatrix?.total_games || 0)}</p>
                  </>
                ) : null}
              </div>
            </section>
          ) : null}

          {selectedMode ? (
            <section className="games-mode-detail" aria-label={`${selectedMode} game length analytics`}>
              <div className="mode-moves-table analytics-table-wrap">
                <div className="section-head analytics-section-head">
                  <h2 className="games-action-title">{selectedMode} lengths</h2>
                  <ClusterSelect
                    idPrefix="lengths-cluster"
                    value={selectedLengthsCluster}
                    onChange={handleLengthsClusterChange}
                  />
                </div>
                {gameLengthLoading ? <p className="result-line">Loading game length analytics...</p> : null}
                {gameLengthError ? <p className="result-line">{gameLengthError}</p> : null}
                {!gameLengthLoading && !gameLengthError ? (
                  <>
                    <div className="length-stats-grid">
                      <span className="opening-meta-chip">games: {formatNumber(gameLengthData?.total_games || 0)}</span>
                      <span className="opening-meta-chip length-meta-right">
                        avg moves: {formatNumber(Math.round(Number(gameLengthData?.summary?.avg_n_moves ?? 0)))}
                      </span>
                    </div>
                    <div className="length-chart-spacer">
                      <CompactBarChart
                        title=""
                        labels={gameLengthData?.n_moves_hist?.x || []}
                        values={gameLengthData?.n_moves_hist?.y || []}
                      />
                    </div>
                    <p className="result-line duration-label">moves per game</p>
                    <CompactBarChart
                      title=""
                      labels={gameLengthData?.time_elapsed_hist?.x || []}
                      values={gameLengthData?.time_elapsed_hist?.y || []}
                    />
                    <p className="result-line duration-label">Minutes</p>
                    <span className="opening-meta-chip length-meta-right">
                      avg elapsed: {formatSeconds(gameLengthData?.summary?.avg_time_elapsed_sec)}
                    </span>
                  </>
                ) : null}
              </div>
            </section>
          ) : null}

          {selectedMode ? (
            <section className="games-mode-detail" aria-label={`${selectedMode} activity trend`}>
              <div className="section-head analytics-section-head">
                <h2 className="games-action-title">{selectedMode} activity trend</h2>
                <ClusterSelect
                  idPrefix="activity-cluster"
                  value={selectedActivityCluster}
                  onChange={handleActivityClusterChange}
                />
              </div>
              <div className="mode-moves-table analytics-table-wrap">
                {activityTrendLoading ? <p className="result-line">Loading activity trend...</p> : null}
                {activityTrendError ? <p className="result-line">{activityTrendError}</p> : null}
                {!activityTrendLoading && !activityTrendError ? (
                  <div className="activity-heat-stack">
                    <HeatStrip
                      title="games by month"
                      labels={activityTrendData?.month_heat?.labels || []}
                      values={activityTrendData?.month_heat?.values || []}
                    />
                    <HeatStrip
                      title="games by weekday"
                      labels={activityTrendData?.weekday_heat?.labels || []}
                      values={activityTrendData?.weekday_heat?.values || []}
                    />
                    <HeatStrip
                      title="games by hour"
                      labels={activityTrendData?.hour_heat?.labels || []}
                      values={activityTrendData?.hour_heat?.values || []}
                      singleRow
                    />
                  </div>
                ) : null}
              </div>
            </section>
          ) : null}

          <section className="games-count" aria-label="Games explorer">
            <div className="section-head">
              <h2>Games Explorer</h2>
              <p>Check how many games exist, then view stats and recent games.</p>
            </div>
            <div className="count-row">
              <div className="count-controls">
                <label htmlFor="count-player">Player name</label>
                <input
                  id="count-player"
                  className="text-input"
                  type="text"
                  value={countPlayer}
                  onChange={(event) => setCountPlayer(event.target.value)}
                  onKeyDown={(event) => event.key === 'Enter' && handleCount()}
                  placeholder="e.g. gothamchess"
                />
                <button className="btn btn-secondary" type="button" onClick={handleCount} disabled={countLoading}>
                  {countLoading ? 'Checking...' : 'Check Count'}
                </button>
                <p className="result-line strong">{countResult || '-'}</p>
              </div>

              {summaryLoading ? <p className="result-line">Loading stats...</p> : null}
              {summaryError ? <p className="result-line">{summaryError}</p> : null}
              {summaryData ? (
                <div className="games-stats">
                  <div className="stat-chip stat-win">Wins: {summaryData.wins ?? 0}</div>
                  <div className="stat-chip stat-loss">Losses: {summaryData.losses ?? 0}</div>
                  <div className="stat-chip stat-draw">Draws: {summaryData.draws ?? 0}</div>
                  <div className="stat-chip">From: {summaryData.date_from ? summaryData.date_from.slice(0, 10) : '—'}</div>
                  <div className="stat-chip">To: {summaryData.date_to ? summaryData.date_to.slice(0, 10) : '—'}</div>
                  <div className="stat-chip">Modes: {summaryData.time_controls?.length || 0}</div>
                </div>
              ) : null}
            </div>
          </section>

          {selectedPlayer ? (
            <section className="games-recent" aria-label="Recent player games">
              <div className="section-head">
                <h2>Recent Games</h2>
                <p>Last 10 games for {selectedPlayer}. Use pagination to browse the next ten.</p>
              </div>

              <div className="recent-pagination">
                <button className="btn btn-secondary" type="button" onClick={handlePrevious} disabled={recentLoading || recentPage <= 1}>
                  Previous 10
                </button>
                <span>
                  Page {recentData?.page || recentPage} / {recentData?.total_pages || 0}
                </span>
                <button
                  className="btn btn-secondary"
                  type="button"
                  onClick={handleNext}
                  disabled={recentLoading || !recentData || recentPage >= (recentData.total_pages || 0)}
                >
                  Next 10
                </button>
              </div>

              {recentLoading ? <p className="result-line">Loading games...</p> : null}
              {recentError ? <p className="result-line">{recentError}</p> : null}

              {!recentLoading && !recentError ? (
                <div className="recent-list">
                  {recentData?.games?.length ? (
                    recentData.games.map((game, index) => {
                      const [datePart, timePart] = game.played_at.split(' ')
                      const [year, monthNum, day] = datePart.split('-')
                      const month = new Intl.DateTimeFormat('en', { month: 'short' }).format(
                        new Date(parseInt(year, 10), parseInt(monthNum, 10) - 1, 1)
                      ).toUpperCase()
                      const resultClass =
                        game.result === 'win' ? 'result-win' : game.result === 'loss' ? 'result-loss' : 'result-draw'

                      return (
                        <a
                          key={`${game.link || index}-${game.played_at}`}
                          className="recent-line"
                          href={`/games/${game.link || ''}`}
                          aria-label={`Open game ${game.link || ''}`}
                        >
                          <span className="recent-date date-chip">{year}</span>
                          <span className="recent-date date-chip">{month}</span>
                          <span className="recent-date date-chip">{day}</span>
                          <span className="time-chip">{timePart}</span>
                          <span className={`color-chip ${game.color === 'white' ? 'color-white' : 'color-black'}`}>
                            {game.color}
                          </span>
                          <span className={resultClass}>{game.result}</span>
                          <span className="recent-open">Open</span>
                        </a>
                      )
                    })
                  ) : (
                    <p className="recent-line">No games found.</p>
                  )}
                </div>
              ) : null}
            </section>
          ) : null}
        </main>
        <Footer />
      </div>
    </div>
  )
}
