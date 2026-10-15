const authentication = [
  /\b(?:authorization|proxy-authorization)\s*["']?\s*[:=]/i,
  /\b[a-z0-9_]*(?:api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|secret[_-]?access[_-]?key|client[_-]?secret|password)\s*["']?\s*[:=]/i,
  /[?&](?:token|api_key|access_token)=/i,
  /(?:^|\s)--?(?:api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|password|token)(?:[=\s])/i,
  /\bhttps?:\/\/[^/\s:@"']+:[^/\s@"']+@/i,
  /\bcurl\b[^\n]*(?:^|\s)(?:-u|--user)(?:[=\s])/i,
  /-----BEGIN (?:[A-Z]+ )?PRIVATE KEY-----/,
  /\b(?:cat|head|tail|less|sed|awk|rg|grep|type|Get-Content)\b[^\n]*(?:\.codex\/auth\.json|\.aws\/credentials|\.npmrc|\.netrc|id_rsa|id_ed25519|(?:^|[\s/])\.env(?:[\s.'"/]|$))/i,
]

// Omit the whole field so neither quoted values nor long-token tails survive truncation.
export function sanitizeObservation(value) {
  const text = String(value ?? '')
  const redacted = authentication.some(pattern => pattern.test(text))
  return { text: redacted ? '认证材料已省略，当前证据不完整' : text, redacted }
}
