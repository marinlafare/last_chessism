import { access, readFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const WEB_ROOT = fileURLToPath(new URL('../', import.meta.url))
const SRC_ROOT = path.join(WEB_ROOT, 'src')

const routeFiles = [
  ['pages/Home.jsx', './home/home.css'],
  ['pages/Games.jsx', './games/games.css'],
  ['pages/Players.jsx', './players/players.css'],
  ['pages/DownloadNewGames.jsx', './download-new-games/downloadNewGames.css'],
  ['pages/MainCharacters.jsx', './main-characters/mainCharacters.css'],
  ['pages/SecondaryCharacter.jsx', './secondary-character/secondaryCharacter.css'],
  ['pages/Positions.jsx', './positions/positions.css'],
  ['pages/LiveAnalysis.jsx', './live-analysis/liveAnalysis.css'],
  ['pages/ScoredPositions.jsx', './scored-positions/scoredPositions.css'],
  ['pages/AnalyzeTimes.jsx', './analyze-times/analyzeTimes.css'],
]

const apiBoundaryFiles = [
  'pages/home/homeApi.js',
  'pages/games/gamesApi.js',
  'pages/players/playerApi.js',
  'pages/players/playerAnalysisApi.js',
  'pages/download-new-games/downloadNewGamesApi.js',
  'pages/main-characters/mainCharactersApi.js',
  'pages/positions/positionsApi.js',
  'pages/live-analysis/liveAnalysisApi.js',
  'pages/scored-positions/scoredPositionsApi.js',
  'pages/analyze-times/analyzeTimesApi.js',
]

const errors = []

for (const [routeFile, cssImport] of routeFiles) {
  const source = await readFile(path.join(SRC_ROOT, routeFile), 'utf8')
  if (!source.includes(`'${cssImport}'`) && !source.includes(`"${cssImport}"`)) {
    errors.push(`${routeFile} must import its page stylesheet: ${cssImport}`)
  }
}

for (const apiFile of apiBoundaryFiles) {
  await access(path.join(SRC_ROOT, apiFile)).catch(() => {
    errors.push(`Missing page API boundary: ${apiFile}`)
  })
}

async function collectSourceFiles(directory) {
  const { readdir } = await import('node:fs/promises')
  const entries = await readdir(directory, { withFileTypes: true })
  const files = await Promise.all(entries.map(async (entry) => {
    const entryPath = path.join(directory, entry.name)
    if (entry.isDirectory()) return collectSourceFiles(entryPath)
    return /\.(js|jsx)$/.test(entry.name) ? [entryPath] : []
  }))
  return files.flat()
}

for (const file of await collectSourceFiles(SRC_ROOT)) {
  const relative = path.relative(SRC_ROOT, file)
  const source = await readFile(file, 'utf8')
  const isPageApi = /Api\.js$/.test(relative)
  const isTransport = relative === 'services/apiClient.js'
  const isAuthApi = relative === 'services/authApi.js'

  if (source.includes('services/apiClient') && !isPageApi) {
    errors.push(`${relative} imports the HTTP client outside a page-owned *Api.js module`)
  }
  if (/\bfetch\s*\(/.test(source) && !isTransport && !isAuthApi) {
    errors.push(`${relative} calls fetch() outside an API boundary`)
  }
}

if (errors.length) {
  errors.forEach((error) => console.error(error))
  process.exitCode = 1
} else {
  console.log(`Page-boundary check passed: ${routeFiles.length} routes own CSS and ${apiBoundaryFiles.length} API modules are present.`)
}
