import { isAbsolute } from 'node:path'
import { sanitizeObservation } from '../src/privacy.mjs'

const shellNames = new Set(['bash', 'pwsh'])
const output = value => value && typeof value.text === 'string' && typeof value.truncated === 'boolean'
const foreground = value => value?.kind === 'foreground'
  && typeof value.cwd === 'string' && isAbsolute(value.cwd)
  && (Number.isSafeInteger(value.exitCode) || value.exitCode === null)
  && (typeof value.signal === 'string' || value.signal === null)
  && typeof value.timedOut === 'boolean' && typeof value.aborted === 'boolean'
  && Number.isFinite(value.timeoutMs) && value.timeoutMs > 0
  && output(value.stdout) && output(value.stderr)

// Only the committed native shell DTO is a process result. Model prose,
// durable text and an arbitrary run_code return value are not process facts.
export function extractVerificationEvidence({ type, exec, result, event } = {}) {
  const data = {}, gaps = []
  if (type === 'session/event') {
    const durable = event?.data
    if (event?.type === 'tool/ptc-dispatch' || event?.type === 'tool/ptc-dispatch-start' || event?.type === 'tool/call') {
      exec = { name: durable?.name, arguments: durable?.arguments }
    }
    result = event?.type === 'tool/result' ? { ...durable?.message, meta: durable?.meta } : durable
    type = ['tool/result', 'tool/ptc-dispatch'].includes(event?.type) ? 'session/result' : 'session/start'
  }
  if (!shellNames.has(exec?.name)) return { data, gaps }
  let args = exec.arguments
  if (typeof args === 'string' && args.length <= 65_536) {
    try { args = JSON.parse(args) } catch { args = undefined }
  }
  const command = sanitizeObservation(args?.command)
  if (typeof args?.command === 'string' && !command.redacted) {
    data.command = command.text.slice(0, 240)
    if (command.text.length > 240) data.truncated = true
  } else if (command.redacted) {
    data.redacted = true
    gaps.push('命令包含认证材料，验证命令证据已省略')
  }
  if (type === 'session/result') {
    const cwd = result?.meta?.cwd
    if (typeof cwd === 'string' && isAbsolute(cwd)) data.cwd = cwd.slice(0, 1024)
    gaps.push('持久工具事件未保留结构化进程退出码；文本或工具成功不能替代')
    return { data, gaps }
  }
  if (type !== 'tools/result') return { data, gaps }
  if (result?.isError !== false || !foreground(result.value)) {
    gaps.push('缺少原生前台进程结构化结果；工具成功或文本不能证明退出码')
    return { data, gaps }
  }
  const value = result.value
  data.resultSource = 'native-shell-canonical'
  data.exitCode = value.exitCode
  data.cwd = value.cwd.slice(0, 1024)
  data.signal = value.signal
  data.timedOut = value.timedOut
  data.aborted = value.aborted
  data.interrupted = value.timedOut || value.aborted || value.signal !== null || typeof value.stopped === 'string'
  if (value.sandbox?.denied === true || value.sandbox?.runnerFailed === true) {
    data.interrupted = true
    gaps.push('沙盒拒绝或执行器故障，进程结果不能作为完整验证通过证据')
  }
  gaps.push('未取得执行前后可比较的源码快照，验证与当前版本绑定未知')
  return { data, gaps }
}
