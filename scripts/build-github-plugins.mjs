import { mkdir, readFile, writeFile, copyFile, chmod, lstat } from 'node:fs/promises'
import { resolve, join, dirname } from 'node:path'
import { pathToFileURL } from 'node:url'
import { createHash } from 'node:crypto'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { inspectPluginPackage, PLUGIN_NODE } from '../chatgpt/plugin-package.mjs'

const execute = promisify(execFile)
const repository = 'https://github.com/Ghost233/GhostAgentSentinel'
const hash = bytes => createHash('sha256').update(bytes).digest('hex')

export async function buildGitHubPlugins({ packageRoot, outputRoot } = {}) {
  const payload = await inspectPluginPackage(resolve(packageRoot))
  for (const name of ['github-plugin-bridge.mjs', 'github-plugin-main.mjs', 'github-installation.mjs', 'github-cleanup.mjs', 'stdio-metadata.mjs']) if (!payload.manifest.files['chatgpt/' + name]) throw new Error('GitHub runtime entry missing: ' + name)
  outputRoot = resolve(outputRoot)
  if (await lstat(outputRoot).catch(error => { if (error.code === 'ENOENT') return null; throw error })) throw new Error('new distribution output required')
  await mkdir(outputRoot, { mode: 0o700 })
  const template = await readFile(new URL('./templates/github-launch.sh', import.meta.url), 'utf8')
  const nodeLicense = await readFile(join(dirname(dirname(PLUGIN_NODE)), 'LICENSE'))
  const archive = join(outputRoot, 'payload.tar.gz')
  await execute('/usr/bin/tar', ['-czf', archive, '-C', payload.root, '.'], { timeout: 60000, maxBuffer: 1048576, env: { ...process.env, COPYFILE_DISABLE: '1' } })
  const bytes = await readFile(archive), archiveSHA256 = hash(bytes)
  if (bytes.length >= 100 * 1024 * 1024) throw new Error('GitHub archive exceeds normal Git file budget')
  const plugins = []
  for (const channel of ['stable', 'test']) {
    const name = channel === 'test' ? 'ghost-sentinel-test' : 'ghost-sentinel'
    const displayName = channel === 'test' ? 'GhostSentinel test' : 'GhostSentinel'
    const pluginRoot = join(outputRoot, 'plugins', name)
    await mkdir(join(pluginRoot, '.codex-plugin'), { recursive: true })
    await mkdir(join(pluginRoot, 'bin')); await mkdir(join(pluginRoot, 'licenses'))
    const manifest = { name, version: payload.manifest.version, description: 'ChatGPT.app 本机主聊天及分叉的只读检查（开发预览）', author: { name: 'Ghost233', url: repository }, homepage: repository, repository, license: 'UNLICENSED', requires_local_executor: true, mcpServers: './.mcp.json', interface: { displayName, shortDescription: '本机主会话 · 只读检查 · macOS Apple Silicon' } }
    const serverName = channel === 'test' ? 'ghost_sentinel_test' : 'ghost_sentinel'
    const mcp = { mcpServers: { [serverName]: { command: './bin/launch-mcp', args: [], cwd: '.' } } }
    await writeFile(join(pluginRoot, '.codex-plugin/plugin.json'), JSON.stringify(manifest, null, 2) + '\n')
    await writeFile(join(pluginRoot, '.mcp.json'), JSON.stringify(mcp, null, 2) + '\n')
    await writeFile(join(pluginRoot, 'bin/launch-mcp'), template.replaceAll('@CHANNEL@', channel).replaceAll('@NODE_SHA256@', payload.manifest.node.sha256), { mode: 0o755 })
    await chmod(join(pluginRoot, 'bin/launch-mcp'), 0o755)
    await copyFile(archive, join(pluginRoot, 'payload.tar.gz'))
    await writeFile(join(pluginRoot, 'payload.sha256'), archiveSHA256 + '\n')
    await writeFile(join(pluginRoot, 'payload.manifest.sha256'), payload.digest + '\n')
    await writeFile(join(pluginRoot, 'licenses/node-LICENSE'), nodeLicense)
    await writeFile(join(pluginRoot, 'README.md'), '# ' + displayName + '\n\n从仓库市场安装并在 App 中启用。关闭面板继续观察；停用插件停止观察。首次启用会校验并解压包内运行环境，无需 npm 或独立常驻程序。\n\n当前为开发预览；App 冷启动和退出重开验收尚未完成。未配置模型时明确显示不可判断。仅支持已核实版本的 macOS Apple Silicon ChatGPT.app。\n\n安装、升级、资料位置和完整清理步骤：' + repository + '/blob/test/docs/chatgpt-github-installation.md\n')
    plugins.push({ name, channel, pluginRoot, displayName, serverName })
  }
  return { version: payload.manifest.version, archiveSHA256, manifestSHA256: payload.digest, archiveBytes: bytes.length, plugins }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const args = process.argv.slice(2)
  if (args.length !== 4 || args[0] !== '--package' || args[2] !== '--out') throw new Error('build-github-plugins --package <payload> --out <new-directory>')
  process.stdout.write(JSON.stringify(await buildGitHubPlugins({ packageRoot: args[1], outputRoot: args[3] })) + '\n')
}
