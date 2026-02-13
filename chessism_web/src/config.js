const envBase = import.meta.env.VITE_API_BASE_URL

export const API_BASE_URL = envBase || (import.meta.env.DEV ? 'http://localhost:8000' : '/api')
export const FRONTEND_VERSION = import.meta.env.VITE_FRONTEND_VERSION || '0.0.1'
export const SHOW_ADMIN_LINK = import.meta.env.VITE_SHOW_ADMIN_LINK === 'true'
