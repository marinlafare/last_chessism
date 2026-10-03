import { readdir, readFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const SOURCE_ROOT = fileURLToPath(new URL('../src/', import.meta.url))
const MAX_LINES = 1000
const SOURCE_EXTENSIONS = new Set(['.css', '.js', '.jsx', '.ts', '.tsx'])

async function listSourceFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true })
  const nestedFiles = await Promise.all(entries.map(async (entry) => {
    const entryPath = path.join(directory, entry.name)
    if (entry.isDirectory()) return listSourceFiles(entryPath)
    return SOURCE_EXTENSIONS.has(path.extname(entry.name)) ? [entryPath] : []
  }))
  return nestedFiles.flat()
}

const files = await listSourceFiles(SOURCE_ROOT)
const oversized = []

for (const file of files) {
  const source = await readFile(file, 'utf8')
  const lineCount = source.split(/\r?\n/).length
  if (lineCount > MAX_LINES) {
    oversized.push({ file: path.relative(process.cwd(), file), lineCount })
  }
}

if (oversized.length) {
  for (const { file, lineCount } of oversized) {
    console.error(`${file}: ${lineCount} lines (maximum ${MAX_LINES})`)
  }
  process.exitCode = 1
} else {
  console.log(`Source-size check passed: ${files.length} files, maximum ${MAX_LINES} lines each.`)
}
