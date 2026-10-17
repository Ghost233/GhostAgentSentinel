# ChatGPT 插件安装候选

## 适用范围

本页供开发者验证自包含安装包。ChatGPT 适配子包当前为 `0.1.0-dev.1`，独立于 DSH 插件版本。正式候选仍须完成安装、升级和真实 App 生命周期验收，不能把本页命令当作已发布版本。

安装包观察本机主聊天及分叉，排除子代理。ChatGPT 中只有会话侧面板。一个完整观察进程服务多个聊天，每个聊天可以有独立的轻量 MCP 连接。

源码采用两组精确宿主版本：

| ChatGPT 版本 | 构建号 | 本机执行版本 |
| --- | --- | --- |
| 26.1007.71326 | 20398 | 0.162.0-alpha.17.2 |
| 26.1013.31256 | 20586 | 0.163.0-alpha.10.2 |

版本相近也不能直接接纳。允许宿主版本后，每份记录仍核对自己的格式和来源；未知材料保留缺口。当前版本的工具结果数组只摘取已核实的文字形式，排除 reasoning 正文，不把工具文本当用户授权。

日常启停使用插件开关。关闭面板后继续监控；停用插件或退出 App 时回收观察进程和连接。App 打开即启动仍须实测，不用 stdio 配置或一次暖启用结果替代。当前阶段把退出重开留到最后。

## 构建自包含包

先按锁文件准备仓库和 `chatgpt` 子包依赖。构建器要求本机固定的 Node `v24.21.0`，并校验已锁定的二进制摘要。构建时取得这份二进制；安装后使用包内副本。

```sh
node scripts/build-chatgpt-plugin.mjs \
  --out /absolute/path/to/new-payload \
  --version 0.1.0-dev.1
```

输出目录必须是新目录。包内包含 Node、观察代码、共享核心、依赖、面板和管理入口。`plugin-package.json` 绑定文件内容、权限和软件版本。协议版本另记为 `runtimeVersion`，升级软件时不能随意改变协议版本。

若需要原生通知，构建时附加 `--notification-app /absolute/path/to/GhostSentinel\ Notifications.app`，提供已有的签名组件。构建器核验并复制组件，不请求通知许可，也不提交通知。开发签名不代表公证或公开发布。

安装包不包含观察令牌、安装配置、登录启动器或旧常驻辅助服务。构建和检查不会安装、登记或启动观察。

## 预览与安装

安装位置、核心资料目录和会话来源目录必须明确。共享核心和原始会话记录保留在安装目录之外。首次安装示例：

```sh
/absolute/path/to/new-payload/runtime/node \
  /absolute/path/to/new-payload/scripts/manage-chatgpt-plugin.mjs install \
  --root /absolute/path/to/private-installation \
  --package /absolute/path/to/new-payload \
  --plugin-id ghost-sentinel@sentinel-local \
  --plugin-name ghost-sentinel \
  --marketplace-name sentinel-local \
  --core-data-dir /absolute/path/to/shared-core \
  --sessions-root /absolute/path/to/sessions \
  --metadata-socket /absolute/path/to/app-server-control.sock
```

默认只输出预览。确认目标、阻塞项和预期启动结果后，在同一命令末尾添加 `--apply`。管理器重新核对输入，预览过期或配置发生并发修改时停止。

安装器生成私密配置与绝对路径的 stdio 定义，只登记本安装的市场和插件。启用位的条件写入会请求宿主将配置应用到已加载聊天，与 App 自身插件开关一致；这不是单插件激活或运行就绪的保证。安装器不调用全局 MCP 重载接口，不改其它插件的开关，也不操作聊天。

确认启用后，安装器先保存已确认的配置修订号，再通过 `mcpServerStatus/list` 仅发现 `sentinel` 服务，让宿主初始化本插件连接。发现结果须同时匹配服务名和插件身份。这个步骤不调用工具，也不依赖打开面板；同名静态 MCP 配置会在启用前阻断。未发现服务、来源不明、结构不合法或连接失联均不能记为成功，失败恢复仍使用已确认的修订号。单服务过滤的接口说明见 [App Server](https://learn.chatgpt.com/docs/app-server)。

宿主登记成功和观察启动分别验收。完成安装须确认正确的插件来源、启用资格、唯一观察实例、安装身份、认证状态及所选版本的实际进程路径。若宿主仍等待信任或激活，保留中间事务；不能把这一步记作安装通过，也不能手动启动观察程序来补造自动启动证据。

## 升级与恢复

升级使用同一安装根，传入新的包：

```sh
/absolute/path/to/new-payload/runtime/node \
  /absolute/path/to/new-payload/scripts/manage-chatgpt-plugin.mjs upgrade \
  --root /absolute/path/to/private-installation \
  --package /absolute/path/to/new-payload
```

仍先预览，再添加 `--apply`。升级先撤销旧版本的工作资格，停止本插件，并核对观察、连接和在途通知的退出。确认回收后切换版本，再核验新实例。

升级保留安装身份、令牌、首采进度、24 小时观察资料和手动暂停设置。失败恢复只处理本事务仍拥有的文件和字段，不回滚整份共享核心或用户配置。并发修改、未知进程和清理失败须保留原错误与残留。

CLI 的 `exitCode: 0` 表示本次操作完成。`2` 表示阻塞或需要处理的中间状态；`1` 表示失败。保留中的未完成事务按原操作 ID 使用 `--resume` 核对恢复，不删除事务后重新冒充首次安装。

若升级结果已明确 `rollback.status: complete`，旧版本已经恢复，本轮操作已归档。处理原失败后重新预览升级，不对已归档事务使用 `--resume`。

## 停用与完整卸载

停用只回收进程，保留资料和暂停设置。重新启用时继续核对来源，不刷新旧材料的首次采集时间。

完整卸载使用 `uninstall --root /absolute/path/to/private-installation`，同样默认预览，添加 `--apply` 后执行。卸载须核对进程自然退出、待投递通知撤销、按安装清理的逻辑及物理结果、插件和市场移除，以及安装文件回收。

DSH 使用中的共享核心和其它安装的资料保留。未知文件或无法确认归属的旧服务不自动接管或删除。测试插件用完应完整卸载并复查登记；仅停用不算测试清理完成。

旧常驻辅助后台的命令保留在历史开发记录中，不作为本页的安装或恢复路线。
