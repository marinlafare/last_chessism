import { useId } from 'react'
import usePlayerAvatar from '../../components/players/usePlayerAvatar'

export default function BarPlayerAvatar({ avatar, x, y }) {
  const clipId = `home-player-photo-${useId().replace(/:/g, '')}`
  const photo = usePlayerAvatar(avatar)
  return (
    <g className="home-games-avatar" transform={`translate(${x} ${y})`} aria-hidden="true"
      data-avatar-state={photo.fallback ? 'fallback' : 'photo'}>
      {photo.title ? <title>{photo.title}</title> : null}
      <defs><clipPath id={clipId}><rect width="40" height="40" rx="6" /></clipPath></defs>
      <rect className="home-games-avatar-background" width="40" height="40" rx="6" />
      <image className="home-games-avatar-photo" href={photo.src} width="40" height="40"
        preserveAspectRatio="xMidYMid slice" clipPath={`url(#${clipId})`} onError={photo.onError} />
      <rect className="home-games-avatar-frame" width="40" height="40" rx="6" />
    </g>
  )
}
