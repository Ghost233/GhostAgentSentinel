import assert from 'node:assert/strict'
import { readFile, stat } from 'node:fs/promises'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { join, resolve } from 'node:path'
import { runInNewContext } from 'node:vm'
import { req, clientRequire, workflowRoot as dshRoot } from '../test/dsh-native-runtime.mjs'

const root = resolve(process.argv[2] || fileURLToPath(new URL('../', import.meta.url)))
const { bundlePatchPaths, loadOverlayPatches, composeEntries } = await import(pathToFileURL(join(dshRoot, 'deepseek-harness/packages/boot/app-boot/lib/index.js')))
const manifest = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'))
assert.equal(manifest.dsh.client.platform, 'web')
assert.deepEqual(manifest.dsh.client.inject, ['@deepseek-ai/dsh-client-ui-sidebar-right', '@deepseek-ai/dsh-client-ui-conversation', '@deepseek-ai/dsh-client-ui-settings-general'])
const paths = bundlePatchPaths(root, manifest.dsh.bundle)
assert.equal(paths.length, 1)
const entries = composeEntries([paths.flatMap(path => loadOverlayPatches('dsh', path))])
assert.deepEqual(entries.map(({ id, name, config }) => ({ id, name, config })), [{ id: manifest.name, name: manifest.name, config: {} }])
const runtimeFiles = ['dsh/host.mjs', 'dsh/remote.cjs', 'dsh/observation.mjs', 'dsh/hierarchy-reader.mjs', 'dsh/verification-evidence.mjs',
  'src/hierarchy.mjs', 'src/hierarchy-projection.mjs', 'src/core-client.mjs', 'src/daemon.mjs', 'src/server.mjs', 'src/protocol.mjs', 'src/privacy.mjs', 'src/enrollment-config.mjs',
  'src/core/index.mjs', 'src/analysis/index.mjs', 'src/analysis/http.mjs']
for (const path of [...runtimeFiles, manifest.main, manifest.exports['.'], manifest.exports['./client'], manifest.dsh.bundle.patch, manifest.bin['ghost-agent-sentinel']]) {
  assert.ok((await stat(join(root, path))).isFile(), `Required package runtime file is not a regular file: ${path}`)
}
assert.equal(resolve(root, manifest.main), resolve(root, manifest.exports['.']), 'main and root export must load the same DSH plugin')
const host = await import(pathToFileURL(join(root, manifest.exports['.'])))
assert.equal(host.default.name, manifest.name, 'DSH package, patch, and native plugin namespace must agree')
assert.deepEqual(host.default.inject, ['agents'])
assert.equal(typeof host.default.apply, 'function')
assert.ok(!manifest.dependencies, 'runtime dependencies must use the DSH host, not a copied SDK')
assert.ok(!manifest.scripts.preinstall && !manifest.scripts.postinstall, 'no install scripts')
let bootstrap
runInNewContext(await readFile(req.resolve('@deepseek-ai/dsh-client-modules/client'), 'utf8'), {
  window: { __ModuleLoader__: { load(value) { bootstrap = value } } },
})
const exports = bootstrap.factory(req)
const loader = { mode: 'queue', pendingQueue: [], load(value) { this.pendingQueue.push(value) } }
const clientURL = 'dsh-app://package-check/client.js'
const modules = exports.createClientModuleSystem(loader, { id: bootstrap.id, exports }, {
  boot: { rev: manifest.version, entries: [{ id: manifest.name, url: clientURL, rev: manifest.version }],
    batches: [{ phase: 'application', url: clientURL, rev: manifest.version, entries: [manifest.name] }] },
  staticModules: { react: clientRequire('react') },
  loadBundle: async () => runInNewContext(await readFile(join(root, manifest.exports['./client']), 'utf8'), {
    window: { __ModuleLoader__: loader },
  }),
})
const register = loader.load
let registeredId
loader.load = value => { registeredId = value.id; return register(value) }
const client = await modules.import(manifest.name)
assert.equal(registeredId, manifest.name, 'delivered client registration must match the package namespace')
assert.ok(Array.isArray(client.inject))
assert.equal(typeof client.apply, 'function')
console.log('DSH native bundle composition, package entries and actual client loader checks passed')
