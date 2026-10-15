const NAMES = new Set(['Error', 'TypeError', 'DOMException', 'AbortError', 'TimeoutError', 'NetworkError', 'SocketError']);
const CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN', 'ETIMEDOUT', 'EPERM', 'EACCES', 'UND_ERR_SOCKET', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT', 'UND_ERR_ABORTED']);
const read = (value, key) => { try { return value?.[key]; } catch { return null; } };

export function inferenceFailureInfo(value, operation = read(value, 'operation')) {
  if (!['fetch', 'response-body'].includes(operation)) return null;
  const cause = read(value, 'cause');
  const name = read(value, 'name'), code = read(value, 'code');
  const causeName = read(cause, 'name'), causeCode = read(cause, 'code');
  return { operation, name: NAMES.has(name) ? name : null, code: CODES.has(code) ? code : null,
    cause: { name: NAMES.has(causeName) ? causeName : null, code: CODES.has(causeCode) ? causeCode : null } };
}
