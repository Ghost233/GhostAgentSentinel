import { createRequire } from 'node:module'
import { access, readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

export const workflowRoot = process.env.DSH_WORKFLOW_ROOT || fileURLToPath(new URL('../../DSH-Workflow/', import.meta.url))
const loader = resolve(workflowRoot, 'scripts/harness-test-loader.mjs')
try { await access(loader) }
catch { throw new Error(`DSH 原生测试依赖缺失：${loader}。设置 DSH_WORKFLOW_ROOT 指向已准备的 DSH-Workflow；本检查不会静默跳过。`) }
await import(pathToFileURL(loader).href)
export const req = createRequire(resolve(workflowRoot, 'deepseek-harness/apps/cli/package.json'))
export const clientRequire = createRequire(resolve(workflowRoot, 'deepseek-harness/packages/test-support/client-runtime/package.json'))

// Only VM fixtures need the actual browser bundle. Node-only native fixtures
// can import req while that separate SDK artifact is being rebuilt.
export async function resolveGatewayBrowserClientPath() {
  const packagePath = req.resolve('@deepseek-ai/dsh-api-gateway/package.json')
  const clientExport = JSON.parse(await readFile(packagePath, 'utf8')).exports?.['./client']
  const browserExport = typeof clientExport === 'string' ? clientExport : clientExport?.browser
  if (typeof browserExport !== 'string' || !browserExport.startsWith('./')) throw new Error('DSH gateway must export its actual browser client bundle')
  const path = resolve(dirname(packagePath), browserExport)
  await access(path)
  return path
}
