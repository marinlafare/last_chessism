import { useId, useState } from 'react'
import { playerAvatarUrl } from './summaryData.js'

export default function BarPlayerAvatar({ avatar, x, y }) {
  const clipId = `home-player-photo-${useId().replace(/:/g, '')}`
  const [failedUrl, setFailedUrl] = useState(null)
  const src = playerAvatarUrl(avatar)
  const showPhoto = Boolean(src && failedUrl !== src)
  return (
    <g className="home-games-avatar" transform={`translate(${x} ${y})`} aria-hidden="true"
      data-avatar-state={showPhoto ? 'photo' : 'fallback'}>
      <defs><clipPath id={clipId}><rect width="40" height="40" rx="6" /></clipPath></defs>
      <rect className="home-games-avatar-background" width="40" height="40" rx="6" />
      <g className="home-games-avatar-fallback" stroke="none">
        <circle cx="20" cy="14" r="7" />
        <path d="M 6 36 C 6 20 34 20 34 36 Z" />
      </g>
      {showPhoto && <image className="home-games-avatar-photo" href={src} width="40" height="40"
        preserveAspectRatio="xMidYMid slice" clipPath={`url(#${clipId})`} onError={() => setFailedUrl(src)} />}
      <rect className="home-games-avatar-frame" width="40" height="40" rx="6" />
    </g>
  )
}
