import { jsonParser } from 'stream-json/core/parser.js'
import { none, isMany } from 'stream-chain/defs.js'
import { sanitizeObservation } from '../src/privacy.mjs'

const values = value => value === none ? [] : isMany(value) ? value.values : [value]
const metadata = new Set(['type', 'timestamp', 'ordinal', 'payload.originator', 'payload.source', 'payload.cli_version', 'payload.cwd', 'payload.history_mode', 'payload.forked_from_id', 'payload.forked_from_ordinal_exclusive', 'payload.history_base.thread_id', 'payload.history_base.end_ordinal_exclusive', 'payload.history_base.end_byte_offset', 'payload.type', 'payload.id', 'payload.role', 'payload.turn_id', 'payload.root_turn_id', 'payload.thread_id', 'payload.call_id', 'payload.name', 'payload.item.type', 'payload.item.id', 'payload.item.exit_code', 'payload.turn_attribution.turn_id', 'payload.turn_attribution.turn_trigger', 'payload.internal_chat_message_metadata_passthrough.turn_id'])
const bodies = new Set(['payload.message', 'payload.arguments', 'payload.input', 'payload.output', 'payload.item.aggregated_output'])
const metadataContainers = new Set(['payload', 'payload.item', 'payload.history_base', 'payload.turn_attribution', 'payload.internal_chat_message_metadata_passthrough'])
// Literal dotted or empty keys never introduce structural path segments.
const fieldPath = (parent, key) => {
  const segment = typeof key === 'string' && (!key || key.includes('.')) ? '\0unselected-key' : key
  return parent ? parent + '.' + segment : String(segment)
}
const contentType = /^payload\.(?:item\.)?content\.\d+\.type$/
const contentText = /^payload\.(?:item\.)?content\.\d+\.text$/
const toolType = /^payload\.output\.\d+\.type$/
const toolText = /^payload\.output\.\d+\.text$/

function append(target, capture) {
  if (target.text && capture.text) target.text += '\n'
  const remaining = Math.max(0, 240 - target.text.length)
  target.truncated ||= capture.truncated || capture.text.length > remaining
  target.redacted ||= capture.redacted
  target.text = (target.text + capture.text).slice(0, 240)
}

// Fixed field projection: no complete response record or reasoning value is retained.
export class MaterialProjection {
  constructor() {
    this.decode = new TextDecoder('utf-8', { fatal: true })
    this.parse = jsonParser({ packValues: false, streamValues: true })
    this.stack = []
    this.fields = {}
    this.body = { text: '', truncated: false, redacted: false }
    this.agentBody = { text: '', truncated: false, redacted: false }
    this.toolBody = { text: '', truncated: false, redacted: false }
    this.toolArray = null
    this.outputValues = 0
    this.recordType = null; this.recordTypeCount = 0
    this.payloadType = null; this.payloadTypeCount = 0
    this.structureAmbiguous = false
    this.peakProjectionBytes = 0
  }

  path() {
    this.rootOutputValue = false
    this.kindField = null
    const parent = this.stack.at(-1)
    if (!parent) return ''
    const child = parent.type === 'array' ? parent.index++ : parent.key
    if (parent.type === 'object') parent.key = null
    const path = fieldPath(parent.path, child)
    if (parent.type === 'object' && child === 'type') {
      this.kindField = this.stack.length === 1 ? 'recordType' : this.stack.length === 2 && parent.path === 'payload' ? 'payloadType' : null
      if (this.kindField) { this[this.kindField] = null; this[this.kindField + 'Count']++ }
    }
    this.rootOutputValue = this.stack.length === 2 && parent.type === 'object' && parent.path === 'payload' && child === 'output'
    if (this.rootOutputValue) this.outputValues++
    return path
  }

  selected(path) { return metadata.has(path) || bodies.has(path) || contentType.test(path) || contentText.test(path) || toolType.test(path) || toolText.test(path) }

  accept(path, capture, toolField = null) {
    if (toolType.test(path) || toolText.test(path)) {
      const item = this.stack.at(-1)
      if (!item?.toolElement || !toolField) return
      if (toolField === 'type') item.toolType = capture.text
      else item.toolText = capture
    } else if (contentType.test(path) || contentText.test(path)) {
      const item = this.stack.at(-1)
      if (contentType.test(path)) item.contentType = capture.text
      else item.contentText = capture
    } else this.fields[path] = capture
    const bytes = Buffer.byteLength(JSON.stringify(this.fields)) + Buffer.byteLength(this.body.text) + Buffer.byteLength(this.agentBody.text) + Buffer.byteLength(this.toolBody.text) + Buffer.byteLength(this.stack.at(-1)?.contentText?.text ?? '') + Buffer.byteLength(this.stack.at(-1)?.toolText?.text ?? '')
    this.peakProjectionBytes = Math.max(this.peakProjectionBytes, bytes)
    if (bytes > 16384) throw new Error('material projection limit exceeded')
  }

  tokens(result) {
    for (const token of values(result)) {
      if (token.name === 'startKey') { this.key = ''; continue }
      if (token.name === 'endKey') {
        const item = this.stack.at(-1)
        const path = fieldPath(item.path, this.key)
        if (metadata.has(path) || bodies.has(path) || metadataContainers.has(path)) {
          item.projectedKeys ??= new Set()
          if (item.projectedKeys.has(this.key)) this.structureAmbiguous = true
          item.projectedKeys.add(this.key)
        }
        if (this.toolArray && item.toolElement) {
          if (!['type', 'text'].includes(this.key)) this.toolArray.valid = false
          else { const seen = this.key === 'type' ? 'toolTypeSeen' : 'toolTextSeen'; if (item[seen]) this.toolArray.valid = false; item[seen] = true }
        }
        item.key = this.key; this.key = null; continue
      }
      if (token.name === 'stringChunk' && this.key != null) { this.key = (this.key + token.value).slice(0, 129); continue }
      if (token.name === 'startObject' || token.name === 'startArray') {
        const parent = this.stack.at(-1), element = parent?.toolRoot === true, field = parent?.toolElement && ['type', 'text'].includes(parent.key)
        const path = this.path()
        if (this.rootOutputValue && token.name === 'startArray') this.toolArray = { valid: true, elements: 0 }
        if (this.toolArray && element) { this.toolArray.elements++; if (token.name !== 'startObject') this.toolArray.valid = false }
        if (this.toolArray && field) this.toolArray.valid = false
        this.stack.push({ path, type: token.name === 'startObject' ? 'object' : 'array', index: 0, key: null, toolRoot: this.rootOutputValue && token.name === 'startArray', toolElement: element })
        if (this.stack.length > 64) throw new Error('JSON depth limit exceeded')
      } else if (token.name === 'endObject' || token.name === 'endArray') {
        const item = this.stack.pop()
        if (this.toolArray && item.toolElement) {
          if (item.type !== 'object' || item.toolType !== 'input_text' || !item.toolText) this.toolArray.valid = false
          else append(this.toolBody, item.toolText)
        }
        if (/^payload\.(?:item\.)?content\.\d+$/.test(item.path)) {
          if (['input_text', 'output_text', 'text'].includes(item.contentType) && item.contentText) append(this.body, item.contentText)
          else this.body.truncated = true
          if (item.contentType === 'Text' && /^payload\.item\./.test(item.path) && item.contentText) append(this.agentBody, item.contentText)
          else { this.agentBody.truncated = true; this.agentBody.omitted = true }
        }
      } else if (token.name === 'startString' || token.name === 'startNumber') {
        const parent = this.stack.at(-1), field = parent?.toolElement && ['type', 'text'].includes(parent.key) ? parent.key : null
        const path = this.path()
        if (this.toolArray && (parent?.toolRoot || token.name !== 'startString' && field)) this.toolArray.valid = false
        this.scalar = { path, toolField: field, kindField: this.kindField, selected: this.selected(path) && (!(toolType.test(path) || toolText.test(path)) || field !== null), text: '', scan: '', length: 0, truncated: false, redacted: false }
      } else if (token.name === 'stringChunk' || token.name === 'numberChunk') {
        const scalar = this.scalar
        if (!scalar?.selected) continue
        const limit = scalar.path === 'payload.cwd' ? 1024 : scalar.path === 'payload.arguments' ? 4096 : metadata.has(scalar.path) || contentType.test(scalar.path) || toolType.test(scalar.path) ? 256 : 240
        scalar.length += token.value.length
        const scan = scalar.scan + token.value
        scalar.redacted ||= sanitizeObservation(scan).redacted
        scalar.scan = scan.slice(-4096)
        scalar.text = (scalar.text + token.value).slice(0, limit)
        scalar.truncated ||= scalar.length > limit
      } else if (token.name === 'endString' || token.name === 'endNumber') {
        if (this.scalar?.selected) {
          const { path, text, truncated, redacted } = this.scalar
          if ((metadata.has(path) || contentType.test(path) || toolType.test(path)) && truncated) throw new Error('material metadata field exceeds limit')
          if (this.scalar.kindField) this[this.scalar.kindField] = text
          this.accept(path, { text: redacted ? '认证材料已省略，当前证据不完整' : text, truncated, redacted }, this.scalar.toolField)
        }
        this.scalar = null
      } else if (['nullValue', 'trueValue', 'falseValue'].includes(token.name)) {
        const parent = this.stack.at(-1), field = parent?.toolElement && ['type', 'text'].includes(parent.key)
        this.path()
        if (this.toolArray && (parent?.toolRoot || field)) this.toolArray.valid = false
      }
    }
  }

  write(bytes) {
    for (let at = 0; at < bytes.length; at += 4096) this.tokens(this.parse(this.decode.decode(bytes.subarray(at, at + 4096), { stream: true })))
  }

  finish() {
    this.tokens(this.parse(this.decode.decode()))
    this.tokens(this.parse(none))
    if (this.stack.length) throw new Error('incomplete JSON record')
    if (this.structureAmbiguous) throw new Error('ambiguous material structure')
    const scalar = path => this.fields[path]?.text
    // Only the observed custom-tool array form is admitted. One streaming
    // element and one total 240-character excerpt are retained, never N bodies.
    if (this.toolArray) {
      delete this.fields['payload.output']
      if (this.outputValues === 1 && this.toolArray.valid && this.toolArray.elements > 0 && this.recordTypeCount === 1 && this.payloadTypeCount === 1 && this.recordType === 'response_item' && this.payloadType === 'custom_tool_call_output' && scalar('type') === this.recordType && scalar('payload.type') === this.payloadType) this.fields['payload.output'] = { ...this.toolBody, text: this.toolBody.redacted ? '认证材料已省略，当前证据不完整' : this.toolBody.text }
    }
    return { ordinal: scalar('ordinal') === undefined ? undefined : Number(scalar('ordinal')), type: scalar('type'), timestamp: scalar('timestamp'), payload: Object.fromEntries(['type', 'id', 'role', 'turn_id', 'root_turn_id', 'thread_id', 'call_id', 'name', 'originator', 'source', 'cli_version', 'cwd', 'history_mode', 'forked_from_id', 'forked_from_ordinal_exclusive'].map(key => [key, scalar('payload.' + key)]).concat([['history_base', scalar('payload.history_base.thread_id') ? { thread_id: scalar('payload.history_base.thread_id'), end_ordinal_exclusive: Number(scalar('payload.history_base.end_ordinal_exclusive')), end_byte_offset: Number(scalar('payload.history_base.end_byte_offset')) } : undefined]])), material: { fields: this.fields, body: this.body, agentBody: this.agentBody, turnId: scalar('payload.internal_chat_message_metadata_passthrough.turn_id'), turnTrigger: scalar('payload.turn_attribution.turn_trigger') } }
  }
}

export function materialRecord(row, turnId, recordStart, recordEnd, turnTrigger) {
  const itemType = row.material.fields['payload.item.type']?.text
  const completed = row.type === 'event_msg' && row.payload.type === 'item_completed'
  const producer = completed && itemType === 'UserMessage'
  const commandResult = completed && itemType === 'CommandExecution'
  const mcpResult = completed && itemType === 'McpToolCall'
  const agentOutput = completed && itemType === 'AgentMessage'
  if (row.type !== 'response_item' && !producer && !commandResult && !mcpResult && !agentOutput) return null
  const payload = row.payload
  const data = { nativeType: 'response_item/' + payload.type, sourceKind: 'chatgpt-native', sourceForm: 'response_item/' + payload.type, runId: row.material.turnId ?? turnId }
  let kind, capture
  if (agentOutput) {
    if (payload.turn_id !== turnId) return null
    kind = 'output'
    capture = row.material.agentBody
    Object.assign(data, { nativeType: 'item_completed/AgentMessage', sourceForm: 'event_msg/item_completed', runId: payload.turn_id, messageId: row.material.fields['payload.item.id']?.text })
    if (capture.omitted || !capture.text.trim()) {
      kind = 'gap'
      capture = { text: '原生代理输出含未识别或缺失的文字内容，当前材料不完整', truncated: true }
    }
  } else if (mcpResult) {
    if (payload.turn_id !== turnId) return null
    kind = 'gap'
    capture = { text: '原生 MCP 结果正文的固定字段尚未核实，当前材料已省略', truncated: true }
    Object.assign(data, { nativeType: 'item_completed/McpToolCall', sourceForm: 'event_msg/item_completed', runId: payload.turn_id })
  } else if (commandResult) {
    if (payload.turn_id !== turnId) return null
    kind = 'tool-result'
    capture = row.material.fields['payload.item.aggregated_output'] ?? { text: '', truncated: true }
    Object.assign(data, { nativeType: 'item_completed/CommandExecution', sourceForm: 'event_msg/item_completed', runId: payload.turn_id, toolName: 'CommandExecution' })
    const itemId = row.material.fields['payload.item.id']?.text
    if (itemId) data.callId = 'native-item:' + itemId
    const exit = row.material.fields['payload.item.exit_code']?.text
    if (exit !== undefined && Number.isSafeInteger(Number(exit))) data.exitCode = Number(exit)
  } else if (producer) {
    if (payload.turn_id !== turnId) return null
    kind = ['composer', 'edit_user_message'].includes(turnTrigger) ? 'input' : 'status'
    capture = row.material.body
    Object.assign(data, { nativeType: 'item_completed/UserMessage', sourceForm: 'event_msg/item_completed', runId: payload.turn_id, messageId: row.material.fields['payload.item.id']?.text, inputOrigin: kind === 'input' ? 'user-entry' : 'unknown', inputPhase: kind === 'input' ? 'consumed' : 'unknown', inherited: false })
  } else if (payload.type === 'message' && ['user', 'assistant'].includes(payload.role)) {
    kind = payload.role === 'user' ? 'status' : 'output'
    capture = row.material.body
    if (payload.id) data.messageId = payload.id
    if (payload.role === 'user') { data.inputOrigin = 'unknown'; data.inputPhase = 'unknown'; data.inherited = false }
  } else if (['function_call', 'custom_tool_call'].includes(payload.type)) {
    kind = 'tool-start'
    capture = row.material.fields[payload.type === 'function_call' ? 'payload.arguments' : 'payload.input'] ?? { text: '', truncated: true }
    if (payload.name) data.toolName = payload.name
    if (payload.call_id) data.callId = payload.call_id
    if (payload.type === 'function_call' && !capture.truncated && !capture.redacted) {
      try {
        const args = JSON.parse(capture.text)
        const command = sanitizeObservation(args.command ?? args.cmd)
        if (command.redacted) capture = { text: command.text, redacted: true, truncated: false }
        else if (command.text) { data.command = command.text.slice(0, 240); if (command.text.length > 240) data.truncated = true }
      } catch { /* The native arguments need not contain JSON or a command. */ }
    }
  } else if (['function_call_output', 'custom_tool_call_output'].includes(payload.type)) {
    kind = 'tool-result'
    capture = row.material.fields['payload.output'] ?? { text: '', truncated: true }
    if (payload.call_id) data.callId = payload.call_id
  } else if (['reasoning', 'agent_message'].includes(payload.type) || payload.type === 'message' && ['system', 'developer'].includes(payload.role)) return null
  else {
    kind = 'gap'
    capture = { text: '未识别原生响应材料类型：' + String(payload.type).slice(0, 80), truncated: true }
  }
  if (capture.truncated || capture.text.length > 240) data.truncated = true
  if (capture.redacted) { data.redacted = true; delete data.command }
  const at = Number.isFinite(Date.parse(row.timestamp)) ? Date.parse(row.timestamp) : null
  return { recordStart, recordEnd, turnId: data.runId, at, kind, text: capture.text.slice(0, 240), data }
}
