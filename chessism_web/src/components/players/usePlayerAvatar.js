import { useState } from 'react'

export const DEFAULT_PLAYER_AVATAR = `${import.meta.env.BASE_URL}images/player-no-photo.jpg`
export const DEFAULT_PLAYER_AVATAR_NOTE = 'This is our default image for a player without a profile photo.'

export function playerAvatarUrl(value) {
  if (typeof value !== 'string') return ''
  const url = value.trim()
  return /^https?:\/\//i.test(url) || /^\/(?![\/\\])/.test(url) ? url : ''
}

export default function usePlayerAvatar(value) {
  const [failedUrl, setFailedUrl] = useState(null)
  const url = playerAvatarUrl(value)
  const fallback = !url || failedUrl === url
  return {
    src: fallback ? DEFAULT_PLAYER_AVATAR : url,
    fallback,
    title: fallback ? DEFAULT_PLAYER_AVATAR_NOTE : undefined,
    onError: () => { if (!fallback) setFailedUrl(url) },
  }
}
