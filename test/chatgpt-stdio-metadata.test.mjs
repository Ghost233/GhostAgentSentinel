import test from 'node:test'
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtemp,readFile,rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { fileURLToPath } from 'node:url'
import { createStdioMetadataClient } from '../chatgpt/stdio-metadata.mjs'
import { createNativeIO,NATIVE_EXECUTABLE } from '../chatgpt/native-io.mjs'
import { setTimeout as delay } from 'node:timers/promises'

const entry=fileURLToPath(new URL('./fixtures/chatgpt-stdio-metadata.mjs',import.meta.url))
async function fixture(t,mode='normal'){
  const dir=await mkdtemp(join(tmpdir(),'sentinel-stdio-metadata-')),log=join(dir,'calls.jsonl'),children=[]
  const client=createStdioMetadataClient({executable:process.execPath,args:[entry,mode,log],spawn(...args){const child=spawn(...args);child.ended=new Promise(resolve=>child.once('close',(code,signal)=>resolve({code,signal})));children.push(child);return child}})
  t.after(async()=>{try{await client.close().catch(error=>{assert.ok(['stubborn','bad-exit','crash'].includes(mode));assert.equal(error.code,'metadata-cleanup-failed')})}finally{for(const child of children)if(child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');await child.ended;assert.fail('fixture rescue cannot count as metadata cleanup success')}await rm(dir,{recursive:true,force:true})}})
  return{client,children,calls:async()=>JSON.parse('['+(await readFile(log,'utf8')).trim().split('\n').join(',')+']')}
}
test('readonly stdio metadata starts lazily, initializes once and naturally closes its own process',async t=>{
  const f=await fixture(t)
  assert.equal(f.children.length,0)
  for(const method of ['thread/resume','thread/start','turn/start','config/value/write','plugin/install','plugin/uninstall'])await assert.rejects(f.client.request(method,{}),/non-observation method rejected/)
  assert.equal(f.children.length,0)
  const [slow,fast]=await Promise.all([f.client.request('thread/read',{key:'slow'}),f.client.request('config/read',{key:'fast'})])
  assert.equal(slow.key,'slow');assert.equal(fast.key,'fast');assert.equal(slow.initialized,1);assert.equal(fast.ready,true)
  assert.equal(f.children.length,1)
  assert.deepEqual((await f.calls()).map(value=>value.method),['initialize','initialized','thread/read','config/read'])
  await f.client.close()
  assert.deepEqual(await f.children[0].ended,{code:0,signal:null})
  assert.deepEqual(f.client.diagnostics(),{closed:true,pendingRequests:0,deadlineTimers:0,connected:false})
  await assert.rejects(f.client.request('config/read',{}),{code:'cancelled',stage:'metadata'});assert.equal(f.children.length,1)
})
async function until(check){const deadline=Date.now()+2000;while(!await check()){assert.ok(Date.now()<deadline,'owned fixture condition must settle');await delay(5)}}
test('unused close never spawns, and the default executable and argv are fixed',async t=>{
  let count=0,child
  const spawnFixture=(executable,args,options)=>{count++;assert.equal(executable,NATIVE_EXECUTABLE);assert.deepEqual(args,['app-server']);child=spawn(process.execPath,[entry,'normal'],options);return child}
  const unused=createStdioMetadataClient({spawn:spawnFixture});await unused.close();assert.equal(count,0)
  const client=createStdioMetadataClient({spawn:spawnFixture});t.after(()=>client.close())
  await client.request('plugin/list',{});assert.equal(count,1);await client.close();assert.equal(child.exitCode,0)
})
test('NativeIO accepts the injected readonly client while retaining its host and plugin qualification',async t=>{
  const f=await fixture(t),profile={appVersion:'26.1013.31256',appBuild:'20586',cliVersion:'0.163.0-alpha.10.2'},started='Sun Oct 18 00:00:00 2026'
  const native=createNativeIO({pluginId:'sentinel@test',metadataClient:f.client,profileSample:async()=>profile,processSample:async()=>[{pid:10,ppid:1,command:'/Applications/ChatGPT.app/Contents/MacOS/ChatGPT',started},{pid:11,ppid:10,command:NATIVE_EXECUTABLE+' app-server',started}]})
  t.after(()=>native.close())
  assert.equal((await native.sampleHost()).available,true);assert.equal(f.children.length,0)
  assert.deepEqual(await native.readEnablement(null),{user:'enabled',effective:'enabled'})
  assert.equal((await native.readThread('fixture-thread')).anchorTurnId,'turn')
  assert.deepEqual((await native.listThreads()).data,[{id:'fixture-thread'}]);assert.equal(f.children.length,1)
})
test('disconnect settles bounded pending work and a later allowed request starts one new generation',async t=>{
  const f=await fixture(t,'pending'),work=Promise.allSettled(Array.from({length:32},(_,key)=>f.client.request('thread/read',{key})))
  await until(()=>f.client.diagnostics().pendingRequests===32)
  await assert.rejects(f.client.request('thread/list',{}),{code:'metadata-unavailable',stage:'metadata'})
  await f.client.disconnect();assert.ok((await work).every(value=>value.status==='rejected'&&value.reason.code==='cancelled'))
  assert.deepEqual(await f.children[0].ended,{code:0,signal:null});assert.equal(f.client.diagnostics().pendingRequests,0)
  const request=f.client.request('thread/list',{}).catch(error=>error);await until(()=>f.children.length===2&&f.client.diagnostics().pendingRequests===1);await f.client.disconnect();assert.equal((await request).code,'cancelled')
  const ids=(await f.calls()).filter(value=>value.id!==null).map(value=>value.id);assert.equal(new Set(ids).size,ids.length)
})
test('outbound size and cyclic params are rejected before any child starts',async t=>{
  const f=await fixture(t),cyclic={};cyclic.self=cyclic
  for(const params of [{key:'x'.repeat(1024*1024)},cyclic])await assert.rejects(f.client.request('config/read',params),{code:'metadata-send-failed',stage:'metadata'})
  assert.equal(f.children.length,0)
})
test('a later successful generation initializes again with new IDs after natural disconnect',async t=>{
  const f=await fixture(t)
  assert.equal((await f.client.request('thread/read',{key:'first'})).key,'first');await f.client.disconnect()
  assert.equal((await f.client.request('thread/read',{key:'second'})).key,'second');assert.equal(f.children.length,2)
  const calls=await f.calls(),ids=calls.filter(value=>value.id!==null).map(value=>value.id)
  assert.equal(calls.filter(value=>value.method==='initialize').length,2);assert.equal(new Set(ids).size,ids.length)
})
test('close cancels an initialization in flight and never starts a replacement',{timeout:4000},async t=>{
  const f=await fixture(t,'initialize-pending'),pending=f.client.request('config/read',{}).catch(error=>error)
  await until(()=>f.children.length===1&&f.client.diagnostics().pendingRequests===1);await f.client.close()
  assert.equal((await pending).code,'cancelled');await assert.rejects(f.client.request('config/read',{}),{code:'cancelled'});assert.equal(f.children.length,1)
})
test('a real child error exit settles every pending request and remains a cleanup failure',async t=>{
  const f=await fixture(t,'crash'),values=await Promise.allSettled([f.client.request('thread/read',{}),f.client.request('config/read',{})])
  assert.ok(values.every(value=>value.status==='rejected'&&value.reason.code==='metadata-unavailable'&&value.reason.stage==='metadata'))
  await assert.rejects(f.client.close(),{code:'metadata-cleanup-failed',stage:'metadata'});assert.deepEqual(await f.children[0].ended,{code:7,signal:null})
})
test('public plugin list and read remain observation-only catalog requests',async t=>{
  const f=await fixture(t)
  assert.equal((await f.client.request('plugin/list',{})).method,'plugin/list')
  assert.equal((await f.client.request('plugin/read',{marketplacePath:'/fixture/catalog',pluginName:'fixture'})).method,'plugin/read')
  assert.deepEqual((await f.calls()).map(value=>value.method),['initialize','initialized','plugin/list','plugin/read'])
})
test('ordinary stderr over a long-lived metadata connection cannot invalidate valid RPC or replace the process',async t=>{
  const f=await fixture(t,'stderr-chatter')
  for(let n=0;n<8;n++){
    const result=await f.client.request('config/read',{key:'stderr-burst'})
    assert.equal(result.ready,true)
  }
  assert.equal((await f.client.request('thread/read',{threadId:'still-readable'})).thread.id,'still-readable')
  assert.equal(f.children.length,1,'discarded diagnostic output must not restart metadata')
  await f.client.close()
  assert.deepEqual(await f.children[0].ended,{code:0,signal:null})
})
for(const [mode,code] of [['eof','metadata-unavailable'],['rpc','metadata-rpc-error'],['invalid','metadata-invalid-response'],['oversized','metadata-invalid-response']])test(mode+' settles pending requests with a safe metadata classification',async t=>{
  const f=await fixture(t,mode)
  await assert.rejects(f.client.request('thread/list',{}),error=>{assert.equal(error.code,code);assert.equal(error.stage,'metadata');assert.doesNotMatch(error.message,/PRIVATE|\/private\/source/);return true})
  await f.client.close();assert.equal(f.client.diagnostics().deadlineTimers,0)
})
test('a real request deadline never lets its late response satisfy a different request',{timeout:10000},async t=>{
  const f=await fixture(t,'late'),started=performance.now()
  await assert.rejects(f.client.request('thread/read',{key:'late'}),{code:'deadline',stage:'metadata'})
  assert.ok(performance.now()-started>=4900)
  assert.equal((await f.client.request('thread/read',{key:'next'})).key,'next')
  await until(async()=> (await f.calls()).some(value=>value.event==='reply'&&value.key==='late'))
  assert.equal((await f.client.request('thread/read',{key:'last'})).key,'last')
})
test('initialization has its own real three-second budget and closes the unused process',{timeout:6000},async t=>{
  const f=await fixture(t,'initialize-pending'),started=performance.now()
  await assert.rejects(f.client.request('config/read',{}),{code:'deadline',stage:'metadata'});assert.ok(performance.now()-started>=2900)
  await f.client.close();assert.deepEqual(await f.children[0].ended,{code:0,signal:null})
})
for(const mode of ['bad-exit','stubborn'])test(mode+' cleanup cannot become success even after owned rescue',{timeout:5000},async t=>{
  const f=await fixture(t,mode);await f.client.request('config/read',{})
  await assert.rejects(f.client.close(),error=>{assert.equal(error.code,'metadata-cleanup-failed');assert.equal(error.stage,'metadata');assert.equal(error.forcedSignals,mode==='stubborn'?1:0);assert.doesNotMatch(error.message,/PRIVATE|\/private\/source/);return true})
  const terminal=await f.children[0].ended;assert.equal(terminal.code,mode==='bad-exit'?7:null);assert.equal(terminal.signal,mode==='stubborn'?'SIGKILL':null)
  await assert.rejects(f.client.request('config/read',{}),{code:'cancelled'});assert.equal(f.children.length,1)
})
