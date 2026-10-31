import { useState } from 'react'
import SideRail from '../components/layout/SideRail'
import Header from '../components/layout/Header'
import { pageStyleTarget } from '../appearance/styleCore'
import { useBoardSoundPreferences } from '../components/chess/BoardSoundPreferences'
import { BOARD_SOUND_SETS, getBoardSoundSet } from '../components/chess/boardSoundSets'
import SoundPreviewBoard from './settings/SoundPreviewBoard'
import './settings/settings.css'

export default function Settings() {
  const { soundSet, updatePreferences, saveError } = useBoardSoundPreferences()
  // Choosing a candidate only affects the trial board, not the saved setting.
  const [draftSet, setDraftSet] = useState(null)
  const selectedSet = draftSet ?? soundSet
  const applySet = () => {
    updatePreferences({ soundSet: selectedSet })
    setDraftSet(null)
  }
  const base = `${import.meta.env.BASE_URL}sounds/chess/`

  return (
    <div {...pageStyleTarget('page')} className="page-frame settings-page">
      <SideRail />
      <div className="home-shell">
        <Header eyebrow="Settings" />
        <main className="settings-main">
          <section {...pageStyleTarget('panel')} className="settings-sounds" aria-labelledby="board-sounds-title">
            <div className="settings-sounds-heading">
              <div>
                <h2 {...pageStyleTarget('title')} id="board-sounds-title">Board sounds</h2>
                <p {...pageStyleTarget('text')} className="settings-current-set" role="status">
                  Current: <strong>{getBoardSoundSet(soundSet).name}</strong>
                </p>
              </div>
            </div>
            <p {...pageStyleTarget('text')} className="settings-hint">
              Choose a set, try it on the board, then press Set as current to use it in the game viewer.
            </p>
            <div className="settings-sound-workspace">
              <div className="settings-sound-trial">
                <SoundPreviewBoard soundSet={selectedSet} />
                <button {...pageStyleTarget('sounds.apply')} className="settings-control settings-apply" type="button"
                  disabled={selectedSet === soundSet} onClick={applySet}>
                  Set as current
                </button>
                <p {...pageStyleTarget('text')} className="settings-trial-note">Saved for your account in this browser.</p>
              </div>
              <fieldset className="settings-sound-sets">
                <legend className="settings-sr-only">Sound set to try</legend>
                {BOARD_SOUND_SETS.map(set => (
                  <div {...pageStyleTarget('card')} key={set.id}
                    className={`settings-sound-card${selectedSet === set.id ? ' is-selected' : ''}`}>
                    <label className="settings-sound-choice">
                      <input type="radio" name="board-sound-set" value={set.id}
                        checked={selectedSet === set.id} onChange={() => setDraftSet(set.id)} />
                      <span>
                        <strong {...pageStyleTarget('sounds.name')}>{set.name}</strong>
                        <span {...pageStyleTarget('text')} className="settings-sound-description">{set.description}</span>
                      </span>
                      {soundSet === set.id ? <span className="settings-current-badge">Current</span> : null}
                    </label>
                  </div>
                ))}
              </fieldset>
            </div>
            {saveError ? <p className="settings-error" role="alert">{saveError}</p> : null}
            <details className="settings-sound-credits">
              <summary {...pageStyleTarget('text')}>Sound credits</summary>
              <p {...pageStyleTarget('text')}>
                Piano, NES, SFX and Futuristic are Lichess themes by Enigmahack, distributed unmodified under AGPLv3+.
                {' '}<a {...pageStyleTarget('link')} href={`${base}lichess/SOURCES.txt`} target="_blank" rel="noreferrer">Sources</a>
                {' · '}<a {...pageStyleTarget('link')} href={`${base}lichess/LICENSE.txt`} target="_blank" rel="noreferrer">License</a>
                {' · '}<a {...pageStyleTarget('link')} href={`${base}lichess/COPYING.txt`} target="_blank" rel="noreferrer">Upstream credits</a>
              </p>
              <p {...pageStyleTarget('text')}>
                Wooden uses Kenney’s CC0 Impact Sounds.
                {' '}<a {...pageStyleTarget('link')} href={`${base}SOURCES.txt`} target="_blank" rel="noreferrer">Sources and license</a>
              </p>
              <p {...pageStyleTarget('text')}>
                Glass, Metal and Soft adapt Kenney’s CC0 Impact Sounds for the board.
                {' '}<a {...pageStyleTarget('link')} href={`${base}kenney/SOURCES.txt`} target="_blank" rel="noreferrer">Sources and adaptations</a>
                {' · '}<a {...pageStyleTarget('link')} href={`${base}License.txt`} target="_blank" rel="noreferrer">License</a>
              </p>
            </details>
          </section>
        </main>
      </div>
    </div>
  )
}
