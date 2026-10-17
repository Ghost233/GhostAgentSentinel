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

安装器生成私密配置与绝对路径的 stdio 定义，只登记本安装的市场和插件。控制 socket 可能属于独立的配置 daemon。它和 App 的聊天后端分别核对，不能用配置写入或 daemon 的连接成功证明 App 已激活。安装器不调用全局 MCP 重载接口，不改其它插件的开关，也不操作聊天。

只有控制连接的进程与当前 App 后端确实一致时，安装器才通过 `mcpServerStatus/list` 初始化本插件。查询只发现 `sentinel`，并核对服务名和插件身份。独立 daemon 或来源未确认的连接不会代替 App 拉起监控。同名静态 MCP 配置仍会在启用前阻断。单服务过滤的接口说明见 [App Server](https://learn.chatgpt.com/docs/app-server)。

当前原生 App 需要自己完成最后激活。安装器返回 `needs-user-action`、`plugin_app_activation_required` 和 `action: enable-in-app`，保留已登记插件、版本文件和原操作 ID。此时显示“App 待激活”，退出码为 `2`；插件保持停用，后台不会因等待而常驻。安装事务不是失败清理，也不是安装完成。

在 App 插件设置中启用“Sentinel 会话检查”一次。如果 App 仍显示旧的启用状态，停用再启用一次。无需先打开面板，也不手动运行观察程序。随后恢复原操作：

```sh
/absolute/path/to/new-payload/runtime/node \
  /absolute/path/to/new-payload/scripts/manage-chatgpt-plugin.mjs install \
  --root /absolute/path/to/private-installation \
  --resume ORIGINAL_OPERATION_ID --apply
```

恢复只核对待激活版本，不重复登记或覆盖插件开关。须确认连接进程属于当前 App，核实认证观察实例和实际版本路径，才报告完成。只有观察进程、daemon 连接或用户启用位时，仍保持待激活。期间停用或退出 App，不会因恢复命令重新启用监控。

若 App 连接已出现、观察进程尚未就绪，恢复命令保留检查点和原错误，不再次停用、登记或切换版本。可稍后用同一操作 ID 重新核对。用户已在 App 卸载插件时，恢复命令也不会自动装回。

宿主登记成功和观察启动分别验收。完成安装须确认正确的插件来源、启用资格、唯一观察实例、安装身份、认证状态及所选版本的实际进程路径。若宿主仍等待信任或激活，保留中间事务；不能把这一步记作安装通过，也不能手动启动观察程序来补造自动启动证据。

## 升级与恢复

升级使用同一安装根，传入新的包：

```sh
/absolute/path/to/new-payload/runtime/node \
  /absolute/path/to/new-payload/scripts/manage-chatgpt-plugin.mjs upgrade \
  --root /absolute/path/to/private-installation \
  --package /absolute/path/to/new-payload
```

仍先预览，再添加 `--apply`。升级先撤销旧版本的工作资格，停止本插件，并核对观察、连接和在途通知的退出。确认回收后切换版本，再核验新实例。需要 App 原生更新或重新启用时，保留“App 待激活”事务，完成原生操作后使用 `upgrade --root ... --resume ORIGINAL_OPERATION_ID --apply` 核对。

升级保留安装身份、令牌、首采进度、24 小时观察资料和手动暂停设置。失败恢复只处理本事务仍拥有的文件和字段，不回滚整份共享核心或用户配置。并发修改、未知进程和清理失败须保留原错误与残留。

CLI 的 `exitCode: 0` 表示本次操作完成。`2` 表示阻塞或需要处理的中间状态；`1` 表示失败。保留中的未完成事务按原操作 ID 使用 `--resume` 核对恢复，不删除事务后重新冒充首次安装。

若升级结果已明确 `rollback.status: complete`，旧版本已经恢复，本轮操作已归档。处理原失败后重新预览升级，不对已归档事务使用 `--resume`。

若旧版本已切回，但仍等待 App 激活，结果会保留原失败原因和待激活事务。恢复只确认旧版本，不重新尝试失败的新版本。确认后返回 `rolled-back` 并归档原事务；原升级仍按失败记录，不能算升级成功。

## 停用与完整卸载

停用只回收进程，保留资料和暂停设置。重新启用时继续核对来源，不刷新旧材料的首次采集时间。

已经绑定同一观察实例时，单次健康读取的网络错误、超时或工具转发的 HTTP 失败，只让本次请求返回未确认。暂停或恢复可能已经提交，桥接不会自动重发；后续只读刷新重新核对实际状态。健康检查仍使用原时限；健康端点返回非 200、身份或配置不符、观察代次变化时仍回收连接，不能借恢复流程接管新实例。

完整卸载使用 `uninstall --root /absolute/path/to/private-installation`，同样默认预览，添加 `--apply` 后执行。卸载须核对进程自然退出、待投递通知撤销、按安装清理的逻辑及物理结果、插件和市场移除，以及安装文件回收。

DSH 使用中的共享核心和其它安装的资料保留。未知文件或无法确认归属的旧服务不自动接管或删除。测试插件用完应完整卸载并复查登记；仅停用不算测试清理完成。

旧常驻辅助后台的命令保留在历史开发记录中，不作为本页的安装或恢复路线。
