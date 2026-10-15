import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { createHash } from 'node:crypto'
import { readFile, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = resolve(fileURLToPath(new URL('../', import.meta.url)))
const manifest = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'))
const expected = JSON.parse(await readFile(join(root, '.github/release-manifest.json'), 'utf8'))
assert.equal(process.env.RELEASE_TAG || `v${manifest.version}`, `v${manifest.version}`, 'tag and package version must agree')
assert.equal(expected.version, manifest.version)
const destination = resolve(process.argv[2] || 'release-assets')
assert.notEqual(destination, root, 'release assets require a separate directory')
await mkdir(destination, { recursive: true })
const temporary = await mkdtemp(join(tmpdir(), 'sentinel-release-check-'))
const sha = value => createHash('sha256').update(value).digest('hex')
try {
  const [pack] = JSON.parse(execFileSync('npm', ['pack', '--json', '--ignore-scripts', '--offline', '--no-audit', '--no-fund', '--cache', join(temporary, 'cache'), '--pack-destination', destination], { cwd: root, encoding: 'utf8' }))
  assert.equal(pack.filename, `${manifest.name}-${manifest.version}.tgz`)
  assert.deepEqual(pack.files.map(file => file.path).sort(), Object.keys(expected.files).sort(), 'packed file list must match the release manifest')
  const archive = await readFile(join(destination, pack.filename))
  assert.equal(sha(archive), expected.sha256, 'archive must match the locally validated release')
  const extracted = join(temporary, 'extracted')
  await mkdir(extracted)
  execFileSync('tar', ['-xzf', join(destination, pack.filename), '-C', extracted])
  for (const [path, hash] of Object.entries(expected.files)) {
    assert.equal(sha(await readFile(join(root, path))), hash, `source changed: ${path}`)
    assert.equal(sha(await readFile(join(extracted, 'package', path))), hash, `packed content changed: ${path}`)
  }
  await writeFile(join(destination, 'SHA256SUMS'), `${expected.sha256}  ${pack.filename}\n`)
  console.log(JSON.stringify({ version: manifest.version, filename: pack.filename, bytes: archive.length, files: pack.files.length, sha256: expected.sha256, status: 'passed' }, null, 2))
} finally {
  await rm(temporary, { recursive: true, force: true })
}
