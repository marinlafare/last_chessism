// Local Vite fixtures must mock every API request before mounting components.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

export async function browserFixture(port, run, { reducedMotion } = {}) {
  const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-ui-check-')), profile = path.join(temporary, 'profile')
  await mkdir(profile)
  if (typeof reducedMotion === 'boolean') await writeFile(path.join(profile, 'user.js'), `user_pref("ui.prefersReducedMotion", ${reducedMotion ? 1 : 0});\n`)
  const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', String(port)], {stdio:['ignore','pipe','pipe']})
  let socket
  try {
    const endpoint = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(Error('Browser startup timeout')), 15000)
      const output = chunk => { const match=String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/); if(match){clearTimeout(timer);resolve(match[1])} }
      browser.stdout.on('data', output); browser.stderr.on('data', output)
      browser.once('error', error => {clearTimeout(timer);reject(error)})
    })
    socket = new WebSocket(endpoint.replace(/\/$/,'')+'/session')
    await new Promise((resolve,reject)=>{socket.onopen=resolve;socket.onerror=reject})
    const pending=new Map(); let id=0
    socket.onmessage=({data})=>{const msg=JSON.parse(data), call=pending.get(msg.id);if(!call)return;pending.delete(msg.id);clearTimeout(call.timer);msg.type==='error'?call.reject(Error(JSON.stringify(msg))):call.resolve(msg.result)}
    const send=(method,params)=>new Promise((resolve,reject)=>{const key=++id,timer=setTimeout(()=>{pending.delete(key);reject(Error('Timeout: '+method))},20000);pending.set(key,{resolve,reject,timer});socket.send(JSON.stringify({id:key,method,params}))})
    await send('session.new',{capabilities:{}})
    const {context}=await send('browsingContext.create',{type:'tab'})
    const evaluate=async expression=>{const result=await send('script.evaluate',{expression,target:{context},awaitPromise:true});if(result.type==='exception')throw Error(JSON.stringify(result));return result.result.value}
    const assert=async expression=>{if(!await evaluate(expression))throw Error('Failed: '+expression)}
    const waitFor=async expression=>{for(let i=0;i<100;i++){if(await evaluate(expression))return;await new Promise(resolve=>setTimeout(resolve,100))}throw Error('Timeout: '+expression)}
    await run({send,context,evaluate,assert,waitFor})
  } finally {
    socket?.close()
    if(browser.exitCode===null){const stopped=once(browser,'exit');browser.kill('SIGTERM');await Promise.race([stopped,new Promise(resolve=>setTimeout(resolve,3000))]);if(browser.exitCode===null){browser.kill('SIGKILL');await stopped}}
    await rm(temporary,{recursive:true,force:true})
  }
}
