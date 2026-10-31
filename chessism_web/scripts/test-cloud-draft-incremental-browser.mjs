// Real combined builder with every API call mocked; never starts a cloud job.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-incremental-draft-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9294'], { stdio: ['ignore', 'pipe', 'pipe'] })
let socket
try {
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error('Browser startup timeout')), 15000)
    const output = chunk => { const match = String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/); if (match) { clearTimeout(timer); resolve(match[1]) } }
    browser.stdout.on('data', output); browser.stderr.on('data', output)
    browser.once('error', error => { clearTimeout(timer); reject(error) })
  })
  socket = new WebSocket(endpoint.replace(/\/$/, '') + '/session')
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject })
  const pending = new Map(); let id = 0
  socket.onmessage = ({ data }) => {
    const response = JSON.parse(data), call = pending.get(response.id)
    if (!call) return
    pending.delete(response.id); clearTimeout(call.timer)
    response.type === 'error' ? call.reject(Error(JSON.stringify(response))) : call.resolve(response.result)
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const key = ++id, timer = setTimeout(() => { pending.delete(key); reject(Error('Timeout: ' + method)) }, 20000)
    pending.set(key, { resolve, reject, timer }); socket.send(JSON.stringify({ id: key, method, params }))
  })
  await send('session.new', { capabilities: {} })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  const evaluate = async expression => {
    const result = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (result.type === 'exception') throw Error(JSON.stringify(result))
    return result.result.value
  }
  const assert = async expression => { if (!await evaluate(expression)) throw Error('Failed: ' + expression) }
  const waitFor = async expression => {
    for (let i = 0; i < 100; i++) { if (await evaluate(`Boolean(${expression})`)) return; await new Promise(resolve => setTimeout(resolve, 100)) }
    throw Error('Timeout: ' + expression)
  }
  const click = label => evaluate(`[...document.querySelectorAll('button')].find(b=>b.textContent===${JSON.stringify(label)}).click()`)
  const idle = `document.querySelector('.cloud-game-draft').getAttribute('aria-busy')==='false'`
  const previews = `cloudTest.requests.filter(r=>r.path.includes('/drafts/') && r.path.endsWith('/preview')).length`
  const writes = `cloudTest.requests.filter(r=>r.method!=='GET').length`
  for (const backend of ['batch_spot', 'cloud_run']) {
    await send('browsingContext.navigate', { context, url: `http://localhost:6789/tests/fixtures/cloudAnalysis.html?draftOnly=1&backend=${backend}`, wait: 'complete' })
    await waitFor(`${idle} && !document.querySelector('input[aria-label="Draft player"]').matches(':disabled')`)
    await evaluate(`window.setPlayer = value => {
      const input = document.querySelector('input[aria-label="Draft player"]')
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(input,value)
      input.dispatchEvent(new Event('input',{bubbles:true}))
    }`)
    await evaluate(`setPlayer('hikaru')`)
    await click('Calculate')
    await waitFor(`[...document.querySelectorAll('button')].some(b=>b.textContent==='Add' && !b.matches(':disabled'))`)
    await evaluate(`cloudTest.holdSave=true`)
    await click('Add'); await click('Add')
    await waitFor('cloudTest.finishSave')
    await assert(`cloudTest.requests.filter(r=>r.path.endsWith('/entries')).length===1 && !(${idle})`)
    await evaluate('cloudTest.holdSave=false;cloudTest.finishSave()')
    await waitFor(`${idle} && document.querySelectorAll('.cloud-draft-step').length===1 && !document.querySelector('.cloud-draft-run').disabled`)
    await assert(`${previews}===0 && cloudTest.draft.preview.fens_to_analyze===2`)
    await evaluate(`setPlayer('magnuscarlsen')`)
    await click('Calculate')
    await waitFor(`[...document.querySelectorAll('button')].some(b=>b.textContent==='Add' && !b.matches(':disabled'))`)
    await click('Add')
    await waitFor(`${idle} && document.querySelectorAll('.cloud-draft-step').length===2`)
    await assert(`${previews}===0 && cloudTest.draft.preview.fens_to_analyze===3`)
    await evaluate(`document.querySelector('.cloud-draft-step').click()`)
    await click('Calculate')
    await waitFor(`[...document.querySelectorAll('button')].some(b=>b.textContent==='Save step' && !b.matches(':disabled'))`)
    await evaluate(`cloudTest.before=JSON.stringify(cloudTest.draft);cloudTest.entryError='Synthetic save failure'`)
    await click('Save step')
    await waitFor(`${idle} && document.querySelector('.cloud-game-draft > [role="alert"]')`)
    await assert(`JSON.stringify(cloudTest.draft)===cloudTest.before && ${previews}===0`)
    await evaluate('cloudTest.entryError=null')
    await click('Save step')
    await waitFor(`${idle} && !document.querySelector('.cloud-game-draft > [role="alert"]')`)
    await evaluate(`document.querySelectorAll('.cloud-draft-step')[1].click()`)
    await click('Remove step')
    await waitFor(`${idle} && document.querySelectorAll('.cloud-draft-step').length===1`)
    await assert(`${previews}===0 && cloudTest.draft.preview.fens_to_analyze===2`)
    await evaluate(`cloudTest.beforeWrites=${writes};cloudTest.remount()`)
    await waitFor(`${idle} && document.querySelectorAll('.cloud-draft-step').length===1`)
    await assert(`${writes}===cloudTest.beforeWrites && ${previews}===0`)
    await evaluate(`document.querySelector('[aria-label="Recalculate group data"]').click()`)
    await waitFor(`${idle} && ${previews}===1`)
    await assert(`!cloudTest.requests.some(r=>r.path.endsWith('/submit'))`)
    if (backend === 'cloud_run') {
      for (const cpus of [200, 201, 200]) {
        await evaluate(`{ const input=document.querySelector('input[max="200"]');
          Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(input,'${cpus}');
          input.dispatchEvent(new Event('input',{bubbles:true})); }`)
        await waitFor(`document.querySelector('.cloud-draft-run').disabled === ${cpus > 200}`)
      }
      for (const count of [3700000, 6000000, 6000001]) {
        await evaluate(`cloudTest.draft.preview.fens_to_analyze=${count};cloudTest.remount()`)
        await waitFor(`${idle} && document.querySelector('.cloud-draft-run')`)
        await assert(`document.querySelector('.cloud-draft-run').disabled === ${count > 6000000}`)
        if (count > 6000000) await assert(`document.body.textContent.includes('at most 6,000,000 FENs')`)
      }
      await assert(`!cloudTest.requests.some(r=>r.path.endsWith('/submit'))`)
    }
  }
  console.log('PASS: both backends; incremental draft totals, restore, guards, Cloud Run 200-CPU/6m-FEN acceptance and over-limit rejection; no launches.')
} finally {
  socket?.close()
  if (browser.exitCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM')
    await Promise.race([stopped, new Promise(resolve => setTimeout(resolve, 3000))])
    if (browser.exitCode === null) { browser.kill('SIGKILL'); await stopped }
  }
  await rm(temporary, { recursive: true, force: true })
}
