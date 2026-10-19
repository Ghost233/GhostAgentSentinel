import { createInterface } from 'node:readline'
import { appendFileSync } from 'node:fs'

const [mode='normal',log]=process.argv.slice(2)
let initialized=0,ready=false
const input=createInterface({input:process.stdin})
process.stdout.on('error',()=>process.exit(0))
if(mode==='stubborn')setInterval(()=>{},1000)
input.on('line',line=>{
  const message=JSON.parse(line)
  if(log)appendFileSync(log,JSON.stringify({method:message.method,id:message.id??null})+'\n')
  const send=result=>{if(mode==='late'&&log)appendFileSync(log,JSON.stringify({event:'reply',id:message.id,key:result.key??null})+'\n');process.stdout.write(JSON.stringify({id:message.id,result})+'\n')}
  if(message.method==='initialize'){if(mode==='initialize-pending')return;initialized++;send({initialized});return}
  if(message.method==='initialized'){ready=true;return}
  if(!ready){process.exit(9);return}
  if(mode==='eof'){process.exit(0);return}
  if(mode==='crash'){process.stderr.write('PRIVATE-RAW-TOKEN /private/source\n');process.exit(7);return}
  if(mode==='invalid'){process.stdout.write('{invalid\n');return}
  if(mode==='oversized'){process.stdout.write('x'.repeat(1024*1024+1)+'\n');return}
  if(mode==='rpc'){process.stdout.write(JSON.stringify({id:message.id,error:{code:-1,message:'PRIVATE-RAW-TOKEN /private/source'}})+'\n');return}
  if(mode==='late'&&message.params?.key==='late'){setTimeout(()=>send({key:'late',initialized}),5200);return}
  if(mode==='pending')return
  const result={key:message.params?.key,method:message.method,initialized,ready}
  if(message.method==='config/read')Object.assign(result,{config:{plugins:{'sentinel@test':{enabled:true}}},layers:[{name:{type:'user'},config:{plugins:{'sentinel@test':{enabled:true}}}}]})
  if(message.method==='thread/read')result.thread={id:message.params.threadId,parentThreadId:null,source:'vscode',originator:'Codex Desktop'}
  if(message.method==='thread/turns/list')result.data=[{id:'turn'}]
  if(message.method==='thread/list')result.data=[{id:'fixture-thread'}]
  setTimeout(()=>send(result),message.params?.key==='slow'?25:0)
})
input.on('close',()=>{if(mode!=='stubborn'){if(mode==='bad-exit')process.stderr.write('PRIVATE-RAW-TOKEN /private/source\n');process.exit(mode==='bad-exit'?7:0)}})
