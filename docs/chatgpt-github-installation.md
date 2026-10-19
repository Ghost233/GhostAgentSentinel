# 从 GitHub 安装 GhostSentinel

## 添加仓库并安装

在 Codex 的插件页添加 GitHub 仓库：

```text
https://github.com/Ghost233/GhostAgentSentinel
```

选择 `GhostSentinel` 市场，安装需要的插件，再在 App 中启用。

| 安装项 | 用途 | 插件身份 |
| --- | --- | --- |
| GhostSentinel | 日常使用的独立安装 | `ghost-sentinel@ghostsentinel` |
| GhostSentinel test | 与日常版并行测试 | `ghost-sentinel-test@ghostsentinel` |

两项可以同时安装和启用。测试版名称以小写 `test` 结尾。它的会话面板入口为“GhostSentinel 会话检查 test”，通知标题也标明 `test`。

首次启用会校验并解压插件自带的 Node、依赖和观察代码。你无需安装 Node、运行 npm、复制开发目录，或启动另一个监控程序。运行期间只读访问宿主公开资料及已核实格式的本机执行记录。

若列表暂未刷新，先关闭再展开插件列表。不要为了刷新列表切换其它插件的开关。首次 App 激活须使用所安装插件自己的开关；后台登记成功与 App 已激活分别核对。

## 当前版本与范围

GitHub 安装包为 `0.1.2-preview.1`。`GhostSentinel` 是日常安装身份；该软件版本仍是开发预览，App 冷启动和退出／重开验收尚未完成。

仅支持 macOS Apple Silicon，以及下列已核实宿主组合：

| ChatGPT.app 版本 | 构建号 | 内置 Codex 执行版本 |
| --- | --- | --- |
| 26.1007.71326 | 20398 | 0.162.0-alpha.17.2 |
| 26.1013.31256 | 20586 | 0.163.0-alpha.10.2 |

观察范围是本机主聊天及分叉，排除子代理、远程和云端执行。只有聊天侧面板，没有全局入口。未知宿主版本或材料格式会显示覆盖缺口。

关闭面板后，观察实例继续工作。停用插件时停止采集、分析和通知，并回收本安装的观察进程。当前实现不安装登录启动器或停用后待机的监控服务。App 打开即启动的目标仍待最后一轮实机验证，不能由 stdio 配置推定。

没有配置可用的摘取模型和判断模型时，面板会显示不可判断。GitHub 安装不会下载模型或更改已有的共享核心模型设置。模型准确度和显卡拥堵不属于框架验收。

## 两版隔离与资料位置

每个安装项拥有独立的插件开关、观察实例、令牌、读取进度和聊天暂停状态。关闭或升级测试版不会控制日常版。

```text
~/Library/Application Support/GhostSentinel/github/stable/
~/Library/Application Support/GhostSentinel/github/test/
```

每个目录内的 `state/installation.json` 属于该安装，`payloads/` 保存校验后的运行包。配置和解压资料使用用户私有权限。两版复用 `~/.ghost-agent-sentinel` 中的共享核心，但按安装身份隔离观察来源和清理范围；DSH 资料保留。

已监控历史滚动保留 24 小时。停用插件会保留资料及手动暂停设置，供下次启用继续使用。

## 更新与清理

在 Codex 中刷新仓库市场，并更新对应安装项。启用新版时，启动入口先撤销旧观察实例的工作资格，等待它退出，再切换同一安装的版本。安装身份、读取进度和暂停设置保留；未知锁、未完成事务或回收失败会阻止切换。

日常版的市场条目固定到已检查的提交；测试版跟随 `test` 分支。两者的软件版本可以暂时相同，配置和运行归属仍独立。

App 的“卸载”移除指定插件及宿主缓存。私有监控资料的完整清理还须使用该安装的清理入口；不能把停用或删除宿主缓存当作资料已清除。清理器先核对插件已卸载、观察进程已退出，再撤销本安装待投递的通知，并按安装清理共享核心资料。另一版和 DSH 的资料保留。

先在 App 中卸载目标插件，保留 `GhostSentinel` 市场，再清理资料。以测试版为例，以下命令先预览；确认结果后在最后一行追加 `--apply`。日常版将 `test` 改为 `stable`。

```sh
sentinel_channel=test
sentinel_root="$HOME/Library/Application Support/GhostSentinel/github/$sentinel_channel"
sentinel_archive=$(/usr/bin/plutil -extract activeArchiveSHA256 raw -o - "$sentinel_root/github-installation.json")
sentinel_release="$sentinel_root/payloads/$sentinel_archive"
"$sentinel_release/runtime/node" "$sentinel_release/chatgpt/github-cleanup.mjs" \
  --state-root "$sentinel_root" --channel "$sentinel_channel"
```

无法确认归属、存在未完成升级或出现未知文件时，清理器保留现场并报告原因。它不自动删除整个共享核心目录。

## 开发者重建

先按锁文件准备依赖，用已有自包含构建器生成新运行包。再生成两个 GitHub 插件目录：

```sh
node scripts/build-github-plugins.mjs \
  --package /absolute/path/to/verified-payload \
  --out /absolute/path/to/new-distribution
```

生成器输出两份完整的插件目录和相同的压缩运行包。压缩包保留普通 Git 文件，不依赖 Git LFS。发布前须重新核对包摘要、源码对应关系及两个插件来源，运行仓库要求的检查。Node 与随附依赖的许可文件随安装包分发。

仓库市场及兼容插件格式依据 [OpenAI 插件打包说明](https://developers.openai.com/plugins/build/plugins)。这条 GitHub 市场路线与 OpenAI 公共插件目录审核分别管理。
