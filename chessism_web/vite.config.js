import { fileURLToPath } from 'node:url'
import { defineConfig, loadEnv, searchForWorkspaceRoot } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '')
  const proxyTarget = env.VITE_DEV_PROXY_TARGET || 'http://localhost:8000'
  const hegelDocument = fileURLToPath(new URL('../hegel_.md', import.meta.url))

  return {
    plugins: [react()],
    resolve: {
      alias: [{ find: /^@hegel-document(?=\?|$)/, replacement: hegelDocument }]
    },
    server: {
      host: true,
      port: 6789,
      strictPort: true,
      allowedHosts: ['chessism.blackcrowlabs.com'],
      // Permit this one root-level document, not the rest of the repository.
      fs: { allow: [searchForWorkspaceRoot(process.cwd()), hegelDocument] },
      headers: {
        'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
        'CDN-Cache-Control': 'no-store',
        'Cloudflare-CDN-Cache-Control': 'no-store'
      },
      proxy: {
        '/api': {
          target: proxyTarget,
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api/, '')
        },
        '/openapi.json': {
          target: proxyTarget,
          changeOrigin: true
        }
      }
    }
  }
})
