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

本轮安装版本为 `0.1.3`，从仓库 `0.1.2` 累加一个补丁版本。旧 `0.1.2-preview.2` 是开发预览，此前正式基线为 `0.1.1`。新版实际 App 面板、部分双端通知与 App 退出／重开仍有未验项，见 [发布验证范围](release-status.md)。

仅支持 macOS Apple Silicon，以及下列已核实宿主组合：

| ChatGPT.app 版本 | 构建号 | 内置 Codex 执行版本 |
| --- | --- | --- |
| 26.1007.71326 | 20398 | 0.162.0-alpha.17.2 |
| 26.1013.31256 | 20586 | 0.163.0-alpha.10.2 |

观察范围是本机主聊天及分叉，排除子代理、远程和云端执行。只有聊天侧面板，没有全局入口。未知宿主版本或材料格式会显示覆盖缺口。

关闭面板后，观察实例继续工作。停用插件时停止采集、分析和通知，并回收本安装的观察进程。当前实现不安装登录启动器或停用后待机的监控服务。App 打开即启动的目标仍待最后一轮实机验证，不能由 stdio 配置推定。

没有配置可用的总结模型和 JEV 判断模型时，面板会显示不可判断。GitHub 安装不下载、更换或启动模型，也不导入旧共享模型配置。模型准确度、显卡拥堵和推理耗时不属于框架验收。

## 模型设置与暂停

会话面板顶部固定提供“设置”。当前会话尚未确认或采集失败时，设置仍可打开；采集、两个模型和通知分别显示状态。

分别填写总结模型与 JEV 判断模型的回环 HTTP(S) 服务地址和模型 ID。点击“读取列表／检查连接”只请求 `GET /v1/models`，不会发送聊天材料或推理请求。目录可用时提供选择，目录不可用或角色未确认时仍可手填。服务在线、模型可见和真实分析成功分别表达，连接检查不证明 JEV 判断能力。

点击“保存模型设置”后，本安装的活动聊天按新配置重新排队。保存会取消本安装的旧分析并忽略晚到结果，采集及原任务继续，DSH 和另一安装保持自己的设置。同一安装内所有聊天共用两个模型；旧共享配置需要重新手填，不迁移、导入或兼容旧格式，其它观察资料保留。

总结模型整理证据并生成附依据的 1～2 句执行短总结，JEV 负责六项检查。本轮两者实际调用成功，且输出和引用有效后，才发布新总结。任一环节失败或未完成时，显示实际阶段，旧总结按历史显示。合法的 unknown 保持不可判断，不显示为正常。

服务离线时继续采集并保留配置，分析按默认 60、120、240、480 秒退避，最长 900 秒。同一服务与模型恢复后，分析按原队列恢复，不解除手动暂停，也不新增停用后的常驻重试进程。

本安装暂停与单聊天暂停分别保存。任一暂停生效时，该聊天不分析或投递对应告警；继续采集。恢复一项不会解除另一项暂停，也不影响其它安装。

## 两版隔离与资料位置

每个安装项拥有独立的插件开关、观察实例、令牌、读取进度、模型配置及暂停状态。关闭测试版只撤销测试版的工作资格；版本切换与清理也按所属安装执行。

```text
~/Library/Application Support/GhostSentinel/github/stable/
~/Library/Application Support/GhostSentinel/github/test/
```

每个目录内的 `state/installation.json` 属于该安装，`payloads/` 保存校验后的运行包。配置和解压资料使用用户私有权限。两版复用 `~/.ghost-agent-sentinel` 中的共享核心，但按安装身份隔离观察来源和清理范围；DSH 资料保留。

已监控历史滚动保留 24 小时。停用插件会保留资料及手动暂停设置，供下次启用继续使用。

## 更新与清理

在 Codex 中刷新仓库市场，并更新对应安装项。启用新版时，启动入口先撤销旧观察实例的工作资格，等待它退出，再切换同一安装的版本。安装身份、读取进度和暂停设置保留；未知锁、未完成事务或回收失败会阻止切换。

日常版的市场条目固定到已检查的提交；测试版跟随 `test` 分支。两者的软件版本可以暂时相同，配置和运行归属仍独立。

本轮按用户确认不执行升级专项验证。上述版本切换说明不代表 0.1.3 已通过旧版升级、新旧包切换或真实 App 退出重开验收，历史预览版的结果也不自动覆盖新候选。

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

开发代码位于 `test` 分支。使用该分支的干净副本，先按锁文件准备依赖，用已有自包含构建器生成新运行包。再生成两个 GitHub 插件目录：

```sh
node scripts/build-github-plugins.mjs \
  --package /absolute/path/to/verified-payload \
  --out /absolute/path/to/new-distribution
```

生成器输出两份完整的插件目录和相同的压缩运行包。压缩包保留普通 Git 文件，不依赖 Git LFS。发布前须重新核对包摘要、源码对应关系及两个插件来源，运行仓库要求的检查。Node 与随附依赖的许可文件随安装包分发。

仓库市场及兼容插件格式依据 [OpenAI 插件打包说明](https://developers.openai.com/plugins/build/plugins)。这条 GitHub 市场路线与 OpenAI 公共插件目录审核分别管理。
