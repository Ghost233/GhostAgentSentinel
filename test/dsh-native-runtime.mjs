import { createRequire } from 'node:module'
import { access } from 'node:fs/promises'
import { resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

export const workflowRoot = process.env.DSH_WORKFLOW_ROOT || fileURLToPath(new URL('../../DSH-Workflow/', import.meta.url))
const loader = resolve(workflowRoot, 'scripts/harness-test-loader.mjs')
try { await access(loader) }
catch { throw new Error(`DSH 原生测试依赖缺失：${loader}。设置 DSH_WORKFLOW_ROOT 指向已准备的 DSH-Workflow；本检查不会静默跳过。`) }
await import(pathToFileURL(loader).href)
export const req = createRequire(resolve(workflowRoot, 'deepseek-harness/apps/cli/package.json'))
export const clientRequire = createRequire(resolve(workflowRoot, 'deepseek-harness/packages/test-support/client-runtime/package.json'))
