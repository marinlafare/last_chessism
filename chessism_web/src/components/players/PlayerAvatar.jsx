import usePlayerAvatar from './usePlayerAvatar'

export default function PlayerAvatar({ src, name, className, ...props }) {
  const photo = usePlayerAvatar(src)
  return <img {...props} className={className} src={photo.src} onError={photo.onError}
    alt={photo.fallback ? `Default image for ${name || 'a player'} without a profile photo` : `${name || 'Player'} avatar`}
    title={photo.title} data-default-avatar={photo.fallback ? 'true' : undefined} />
}
