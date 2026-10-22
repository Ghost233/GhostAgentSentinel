# 开发接缝与验收

本次发布版本为 0.1.8，上一正式版本为 0.1.7。本文记录当前源码的接缝，不代表已运行的旧进程已经具备新增能力；实际验证范围及未验项目见 [发布验证范围](release-status.md)。用户在了解实机缺口后决定发布，产品规格尚未结项。

## 范围和分工

- 共享核心：持久化、来源与任务状态、调度、分析取消、摘要、告警合并和通知投递状态。
- 分析模块：六项检测、带来源的摘取、JEV 判断、引用/协议校验和未知/错误区分。
- DSH 适配器：原生事件与基线、非阻塞发送、当前来源登记、右侧总览和桌面通知。
- 主线程：公共协议、守护进程/客户端、运行依赖与打包、整体集成检查和文档。

发布使用当前本地 main，先完成最终输入的本地检查，再提交和推送；不创建额外分支或 worktree，不提交本地运行记录。发布前门禁与发布后验收按 [发布流程与完成条件](release-status.md#发布流程与完成条件) 执行。

测试与真实宿主验收只在本地 macOS arm64 执行。GitHub Actions 仅保留打包工作流，使用 `macos-15` arm64 运行器及固定 Node／npm，生成安装包、核对版本／文件清单／哈希并上传产物；不运行测试，也不为 Ubuntu 扩展适配。运行器规格见 [GitHub 官方说明](https://docs.github.com/en/actions/reference/runners/github-hosted-runners)。仅修改 workflow 时，只验证相关配置与打包，复用未受影响的本地证据，不重复完整测试、安装或界面操作。

来源采用用户已选择的逐后台接入：每个独立 DSH 配置启用一次适配器，之后自动连接同一核心。预期来源清单持久保存配置身份、名称和启用状态，显示已配置但未连接的来源及清单外来源；缺少完整发现证明时总览显示覆盖未确认。首版不实现跨后台自动发现和装配。本轮也不批准新的告警质量/负载指标。实现参数是可复核的开发默认值，不替代正式验收。

## 原版 DSH 的能力边界

2026-10-17，用户选择仓库当前检出的原版子模块作为目标，不修改客户端或 SDK，也不恢复旧扩展。该轮验收输入是上游 `deepseek-ai/deepseek-harness` 的 `1517a4dbca0a2789ff1d41ba439600f8547d23fa`，模块版本为 `0.2.1-alpha.5`。每次验收重新记录实际提交和构建产物；子模块记录不等于原版客户端界面已验收。

后续已核实的原版宿主证据使用提交 `687f282e8625c6b8d11ff58211a90524be2e4317`，版本为 `0.2.1-rc.2`。具体证据见仓库中的[兼容验证与发布准备](https://github.com/Ghost233/GhostAgentSentinel/blob/test/docs/dsh-0.1.2-compatibility.md)。各次结果保留原范围，不直接记为 0.1.8 的新界面或完整双端验收通过。

| 能力 | 当前范围 |
| --- | --- |
| 原生代理与工具活动、子代理起止和归属 | 按公开事件采集，缺失关系保留覆盖缺口。 |
| 已加载资料的逻辑切片、取消与关闭 | SDK 提供逻辑切片；本轮未将其当作冷读替代，不宣称具备冷读物理预算。 |
| 持久化冷日志的有界分页 | 当前公开接口不支持原物理／解码字节预算及可恢复游标。拒绝不受预算约束的补读，历史保持不可判断。 |
| 父代理等待的开始、目标变化与结束 | 当前公开接口不足以证明，也不能可靠区分 await 和 fire-and-forget。不生成对应等待豁免，不从子代理活动推断父代理正在等待。 |

旧扩展场景及原始证据保留，仅在真实公开能力存在时执行其正向验收。原版缺失能力时明确标为不支持，跳过不计通过；拒读、隐私、缺口表达、取消和资源回收断言继续执行。六项检查保留，材料不足的项目仍为不可判断，不混入正常结果。框架预算、只读范围及其它安全要求保持。

这段替代旧阶段中“必须恢复 SDK 分页或父等待补丁”的交付要求。ChatGPT 的本机主聊天、分叉和独立生命周期要求不受此调整影响；真实 App 生命周期仍在交付前集中验证。

## 测试接缝

沿用用户已确认的接缝：统一观察输入和监控结果读取、DSH 原生事件与客户端 Remote、公开 Chat/JEV HTTP、独立核心的启停和保存。业务断言跨这些公共接缝，不直接查询私有数据库或绕过模块接口。独立的持久化 IO 检查可在自有临时存储上核对实际删除或注入存储失败，并明确标注用途；它们不替代公开接口的业务断言，也不为测试增加私有资料导出接口。时钟、外部推理 HTTP 和系统通知为外部 IO 适配器，测试可以替换；业务状态、调度、告警和事件处理必须真实执行。

每个实现者先写一个失败用例，再实现相应纵向行为，保留命令与真实退出结果。测试覆盖隔离、重复、晚到结果、24 小时到期、真实重启、冷却、失败与非干扰；不得只验证字段常量。

## 固定接口

协议版本为 1。所有时间为 epoch 毫秒；原始时间不可获得时保留 null，capturedAt 是来源首次采集时间，重传不改它。身份字符串作为不透明值处理，不按名称或 PID 猜归属。

来源描述：`{registrationId, hostRunId, name, profile?, capabilities?, gaps?, enroll?}`。registrationId 标识接入配置，hostRunId 区分独立宿主运行上下文；一个配置可产生不同运行实例，重连不改变现存 hostRunId。enroll 仅稳定配置身份为 true，在用户尚未手动管理清单时登记该配置；临时身份不自动加入清单。任务描述：`{id, lifecycleId, sessionId?, title, status, parentId?, gaps?}`，status 为 running/idle/disposed/unknown。

事件：`{id, taskId, taskLifecycleId, kind, occurredAt, capturedAt, text, evidence?, data?}`。kind 为 input/output/tool-start/tool-result/status/gap。data 只放可获取的事实：callId/rootCallId/parentCallId、toolName、command、exitCode、isError、errorCode、sourceVersion、validationScope、artifactChanged、status、truncated、redacted。无来源的字段不补造。摘录默认最多 240 字符单元，截断标明缺口，不能读认证头、凭据配置或传输令牌。传输认证只用于核心连接，不进入监控结果。原任务内容中显式的认证头、常见凭据字段、私钥和已识别凭据读取命令，在适配器与 HTTP 接收处整字段省略，标 redacted；省略影响当前请求的覆盖。该规则识别显式格式，不是任意秘密文本的通用识别器。

核心模块导出 `createCore({dataDir, analyzer, analyzerFactory?, now?, cooldownMs?, repeatMs?, leaseMs?, idleGraceMs?, onIdle?})`，异步方法包括 registerSource(descriptor)、heartbeat(sourceKey)、ingest(sourceKey,{events?,tasks?,gaps?})、unregisterSource(sourceKey,reason)、snapshot()、diagnostics()、sourcePlan()、configureSources(entries)、claimNotifications(sourceKey)、ackNotification(id,result)、close()。安装模型配置与暂停使用下文的安装级接缝。register 返回来源 key；ingest 返回接受/重复/过期计数与 paused。snapshot 读取业务状态；diagnostics 只读资源计数，不含任务正文或令牌。configureSources 整体替换最多 100 条配置，不控制宿主插件开关。

用户已确认首版沿用聊天和工具现有的版本、验证范围与退出结果。缺失时显示不可判断，六项检测全部保留。额外读取工作区的版本指纹采集后置；适配器不主动读取工作区来补全证据。

分析模块导出 `createAnalyzer(config)`，对象提供 `analyze({task,source,events,coverage},{signal})`。返回 `{summary,checks,coverage,usage?,quality:'unvalidated'}`；summary 保留 currentTask/currentState/recentProgress/attention 字符串。执行短总结使用可选的 `summary.generated = {text,evidenceIds,evidenceRefs}`，只在本轮总结模型和 JEV 实际调用成功且输出、引用合法后赋值。text 为附依据的 1～2 句，最多 600 字符单元；任一环节失败或未完成均不发布新总结。

checks 固定六项 ID：stalled_progress/request_drift/failure_loop/validation_cost/validation_gap/long_wait，每项 `{id,status,reason,evidenceIds}`，status 是 attention/clear/unknown。evidenceIds 必须属于本轮对应任务，unknown 不算正常，confidence 不表示准确率。未配置推理时返回明确依赖状态，不能发语义成功告警。旧总结受原历史与保留期规则管理，不能充当本轮结果。

核心 snapshot 包含 protocol、generatedAt、monitoring、overview、sources、tasks。overview 显示 runningTasks/knownSources/connectedSources/attentionTasks/coverage。任务含 key/sourceKey/id/lifecycleId/title/status/currentRequest/updatedAt/lastActivity/events/analysis/alerts。analysis 含 state/through/completedAt/nextEligibleAt/error/summary/checks；来源含 key/descriptor/status/lastSeenAt/gaps。响应不暴露访问令牌。

分析模块的私有 LLM 接缝使用有限证据目录，每组原文只提供一次，可选项使用 `optionId`。模型必须严格返回 `{selectedOptionIds,gaps,summary:{text,evidenceOptionIds}}`；总结依据也使用目录选项，代码再映射为公开的事件引用。模型载荷不展示结构化的来源事件 ID 或当前请求 ID，以 `isCurrentInput` 标明已确认锚点；真实事件定位、原文和来源允许的事实分类仍由代码绑定。

目录项仅表示可选择资格，实际门禁、授权等语义仍需选择与判断。未知或重复选项、额外字段、旧键、无效总结或引用均报错，不自动修复，也不接受事件 ID 作为选项。还原后继续校验原文引用与来源权限。当前请求从保留范围内已确认的真实非空 input 确定，空、缺失或过期时保留未知，不以任务标题或元数据补造。完整 LLM 请求 JSON 受原 24,000 字符预算约束，输出上限仍为 1,024 token；JEV 的判断职责和既有适配器输入协议保持。

偏离检查的询问资格依据真实当前请求、完整覆盖以及本轮非空输出或工具开始活动，不要求模型再选出 action 分类。活动只说明有材料可问，偏离、合理调查或必要前置仍由 JEV 核对；不直接设置判断，也不补造分类。仅有工具结果、缺少请求或活动、覆盖不足时保持未知。引用保留本轮相关原文，便于展开核对权限和调查理由。

任务另有 `activity: {summary, observedAt, evidenceIds}`，由核心在读取快照时依据本任务保留观察事件临时生成。它描述已采集的输出、工具记录和未配对结束记录，不代表模型推断或完成证明。当前请求范围、来源失联及采集缺口须明确；等待时长只表示已观察到的时间下界，不能据此断言工具仍在执行或卡死。分析暂停、依赖未配置或旧轮次等待时，新采集活动仍能更新该字段。该字段不新增持久记录或专用计时器；旧核心未提供时，客户端明确显示活动摘要未提供。

守护进程 HTTP 全部绑定回环并校验私有连接令牌；路径由主线程固定。事件确认仅表示持久接收。适配器只使用 HTTP 接缝，不导入核心业务类。通知由所属来源领取，提交成功与用户看见分别表达；任务结束、来源失联或引用过期停止重复。认领开发默认 60 秒到期、结果未知；再次投递仍遵守 10 分钟间隔并使用新 ID，晚到旧回执不能改变新尝试。异步系统失败可修正已提交状态。

系统可见通知正文保留与面板一致的完整观察身份 `task.key` 和来源身份 `source.key`。身份在来源名称和告警原因之前，客户端前 240 字符也能区分同名跨来源任务；完整正文仍限 1200 字符。私有通知记录中的身份不替代用户可见定位。

同一主会话合并多个代理告警时，兼容字段 `taskKey` 和正文中的完整观察身份指向投递前仍有效的首成员，共同来源身份仍完整显示。首成员被过滤后，两处身份一起更新，原 3 秒合并截止不延后。其余成员在正文以名称、必要短标识和原因表示，公开 `members` 资料保留逐代理完整身份及问题关系；它不替代上述系统可见定位。该规则不改变单成员通知、前 240 字符定位和 1200 字符正文界限。

适配器接缝：`src/core-client.mjs` 导出 `connectCore({dataDir})`，提供上述业务方法对应的 HTTP 方法及 `close()`。close 只关闭客户端；适配器须先 unregisterSource。连接断开后适配器重试 connectCore 和登记／基线，不能依赖旧连接 token 或旧 sourceKey。公开事件验证由主线程完成，业务语义与去重由核心完成。核心默认租约 15 秒，适配器心跳 3 秒；开发默认长等待阈值 5 分钟明确标为待质量验收，不作为任务失败结论。

安装配置使用 `installationAnalysisConfig(registrationId)` 读取 `{registrationId,config,revision}`，使用 `configureInstallationAnalysis(registrationId,config,revision)` 保存。HTTP 为 `GET /v1/analysis/installations/config?registrationId=…` 与同路径 POST，写入正文为 `{registrationId,config,revision}`。配置为 `{llm:{baseUrl,model},jev:{baseUrl,model}}`，未设置的项可以省略，只允许回环 HTTP(S)，不包含密钥。同安装的聊天共用配置，不同 registrationId 各自持久保存；未配置安装返回空配置，不回落到旧全局设置。

同一安装以最后一次有效保存为准，不因另一窗口先保存而拒绝旧修订号；仍校验安装身份、修订号格式和整份配置。每次成功写入更换该安装的修订号，并将该安装受影响的配置、任务和通知变化一起持久提交。无效配置不部分生效。分析轮次和结果绑定 registrationId、配置修订号及执行材料；保存后取消旧轮次、封存晚到结果，按新配置公平排队。采集、原任务、其它安装与手动暂停保持。响应丢失时保留结果未知，调用方重新读取，不能仅凭值相同推定写入归属。安装暂停、会话暂停和旧全局配置各自的修订冲突规则保持。

双端的保存响应用 `saveOutcome` 区分明确拒写与结果未知。已确认的配置 HTTP 拒绝，以及请求前已确认能力不足、尚未发出请求，均属于明确拒绝。未确认、混合错误和响应中断保持未知。MCP 的 `isError` 或 Remote 的通用错误本身不能证明配置未写入。缺少结果标记时也按未知处理，保留输入，不自动重试；其它操作的错误语义保持。

安装暂停使用 `installationMonitoring(registrationId)` 和 `setInstallationMonitoring(registrationId,paused,revision)`，返回 `{registrationId,paused,revision}`。HTTP 为 `GET /v1/monitoring/installations?registrationId=…` 与同路径 POST，写入正文为 `{registrationId,paused,revision}`。相同目标状态幂等，过期的相反写入返回 409 和 `installation_monitoring_conflict`。DSH 界面只调用所属安装的控制；安装暂停与会话暂停取逻辑或，恢复其中一项不解除另一项，暂停期间仍采集。

旧 `configureAnalysis`、`analysisConfig`、`analysisConfigState`、`configureAnalysisIfCurrent`、全局 pause／resume 和 setAnalyzer 仅保留给内部管理或 CLI。对应 `/v1/analysis/config`、`/state`、`/compare` 不代表安装配置，也不为新安装导入或迁移旧设置。旧全局比较保存仍使用修订号；恢复无法确认时返回 503 和 `analysis_config_unconfirmed`，该状态不能当作安装配置已确认。

模型连接检查仅请求 `GET /v1/models`，核对在线与模型可见状态，不发送聊天材料或合成推理。服务在线和模型可见不证明实际分析成功，也不按名称猜测 JEV 能力。生产分析失败沿既有调度默认退避 60、120、240、480 秒，最长 900 秒；同服务及模型恢复后继续合法分析，不新增常驻恢复器，不解除暂停，不下载、更换或启动模型。

设置中的“读取列表”和“检查连接”分别发起只读请求。前者只更新候选，不选模或保存；后者独立核对所填地址和模型名称。更改输入后，旧检查结果不再代表当前内容。选择列表模型、手填后离开分组或按 Enter 才触发有效组保存，组内按钮不触发保存。

Codex 的 app-only `sentinel_installation_diagnostics({})` 与 DSH 的 `installationDiagnostics()` 返回 protocol 1 安装诊断。两端固定使用所属安装身份，组合既有安装配置和快照，返回采集连接、配置完整性、活动任务的分析计数及最近轮次的安全阶段／错误码。registrationId 保留原有不透明标识，用于核对本安装；不返回任务正文、标题、任务／来源身份、执行材料路径、令牌或其它安装资料。最近保留轮次和当前活动分开，旧轮次、过期和未知状态不冒充当前成功。来源连接只说明连接事实，当前宿主不支持或采集失败仍须显示。诊断仅在打开或显式刷新时读取，关闭及资格失效后取消请求并忽略晚到结果。

最后来源退出：保存、取消自己分析请求，短宽限后退出；模型驻留，晚到结果忽略。重启恢复只使用 24 小时保留资料，来源/运行状态先标未知，重新登记与基线确认后再分析，不恢复原任务或旧请求。

安装通知偏好由 `installationNotificationPreferences(registrationId)` 读取，`setInstallationNotificationPreference(registrationId, kind, enabled)` 只修改一类；返回 `{registrationId, preferences, revision}`，两类键为 `task-alert` 与 `monitoring-failure`，默认开启，revision 仅作状态标识，不做比较交换。HTTP 使用 `/v1/notifications/installations/preferences` 的 GET／POST。偏好、待发取消和证据抑制在既有存储事务中提交，关闭与重开不清除任务问题，也不补发关闭期间积压；正式版、test 和 DSH 按各自安装隔离。DSH 监控故障通知按用户决定后置，设置明确显示“暂不支持”。

Codex 设置使用 app-only 的 `sentinel_installation_notification_preferences` 与 `sentinel_set_installation_notification_preference`；明确申请系统许可使用 `sentinel_request_installation_notification_permission`，不依赖某条聊天或采集恢复，普通读取不申请许可。保存拒写与响应未知沿用模型设置的分类。

DSH 的每次通知认领附带不透明的 `attemptToken`。客户端建立该尝试的撤销等待，再经 `beginNotificationSubmission(sourceKey, id, attemptToken)` 取得提交资格；Host 固定来源身份，客户端不能指定另一来源。一次性的 `waitNotificationSubmissionRevocation(sourceKey, id, attemptToken, signal)` 使用 POST `/v1/notifications/revocation`，只随该尝试存活，不新增轮询或广播。收到撤销后，客户端先同步封存尝试，再提交带令牌的 `invalidated` 回执；旧的 true 回包不能恢复资格。真正调用浏览器通知与本地状态检查之间不插入异步操作，提交或失败回执也绑定同一令牌。实际核心须声明 `notification-submission-revocation/1`，不支持时不得以未受保护的资格继续投递。

新 DSH Remote 的认领请求必填 `{submissionProtocol: 1}`，避免旧页面忽略新资格协议。新版 Core HTTP 客户端也发送该标记。原版 DSH 0.1.1 的 HTTP 请求不带标记，核心保留其既有投递路径，但将返回认领结果视作旧客户端已经取得的在途许可。其原有 `submitted`／`failed` 回执只证明这次尝试已消费，不证明同步撤销，也不恢复已取消记录；没有回执时持续未确认。旧存储中已知认领或提交开始、却没有消费证明的事实同样保留有界围栏。这些兼容检查不能替代原版宿主的实际共享核心验收。

关闭通知先保存偏好并取消待发记录，再等待本安装该类已签发许可完成撤销或提交结算；从另一 Host 操作和重复保存 false 也不能跳过确认。断连、取消或超时不证明旧许可已失效，返回 `notification_revocation_unconfirmed`，读取偏好时也保留对应 `revocation` 未确认状态，不能只凭 false 显示关闭完成。未结算尝试只保留有界、无正文的撤销资料，历史到期、数量裁剪或核心重开不能将其视作已安全结算；晚到回执不能复活通知历史。此接缝的自动检查与实际宿主集中验收分别记录。

Codex 监控页只提供“当前问题／详情”，不再包含设置表单、系统许可按钮和旧的按执行通知记录界面。历史结果仅在详情呈现；`history.currentStatus` 与 `history.currentSignal` 取当前主聊天执行，不使用所选历史轮次的灯号。无已确认身份不补造运行节点，终止节点退出四灯计数。问题中的通知短标记只反映已有提交事实，不表示用户看见。

安装通知记录通过 `installationNotificationRecords(registrationId, {cursor?, limit?})` 和 GET `/v1/notifications/installations/records` 读取。Codex 的 app-only `sentinel_installation_notification_records({cursor?})` 与 DSH Remote 的同名安装查询固定所属安装，不依赖当前聊天。实际运行核心须声明 `installation-notification-records/1`；缺少能力时明确报不支持，不以健康状态或客户端同名方法代验。

Core 客户端从已核实实例的 health 取得能力列表。安装通知读取要求 `installation-notification-records/1`，偏好写入还要求 `notification-submission-revocation/1`。缺少能力时不发送对应请求，返回 `core_feature_unsupported` 及限定操作的 `coreCapabilityFailure`；这不属于传输失败，也不停止其它来源。更新插件文件不会给旧 Core 进程补接口，客户端不自动替换或终止 Core；需要切换时由用户安排维护窗口，等待所有来源停用后旧 Core 自然退出。

记录按创建时间与 ID 稳定倒序，回执更新不改变排序。默认每页 20 条、最多 100 条，游标绑定安装并可在核心重开后使用；到期或裁剪造成的缺口单独表达。两类通知在本安装内合计只保留最近 24 小时的最新 1000 条，另一安装的容量和缺口互不影响。仍可能需要取消系统待发请求的旧记录只保留有界、无正文的撤销信息，不进入可见历史，沿原投递 IO 确认取消后删除；晚到回执不恢复已裁剪历史。

两端设置页的通知记录 tab 仅在打开、显式刷新或翻页时读取，不轮询或自动遍历所有记录。每条显示可核实的聊天名称或归属未确认、时间及真实投递事实；DSH 浏览器记录没有原生通知中心事实时保留 `native: null`，`humanSeen` 不因系统接受而改成已看见。关闭、隐藏和断连后取消或封存旧读取，模型草稿与通知开关不受影响。实际宿主候选的集中界面验收仍独立保留。

## 交付界限

共享运行与资源回收覆盖任务／工具索引、客户端请求收尾、面板刷新与后台通知分工，保留至少 20 个并发客户端和 100 次循环的资源检查。Codex 共享 HTTP MCP 方案后置。

本版验收框架运行。模型准确度、GPU 拥堵及推理耗时不作为发布门槛；依赖失败、不可判断、排队和滞后仍须如实表达。真实宿主接口能力与实测边界见 [安装说明](installation.md) 和 [发布验证范围](release-status.md)。

## ChatGPT 只读观察增量

0.1.3 提供 source-observation/1 健康能力。来源登记可声明 mode 为 observation-only；省略该字段保持原有分析行为。未知模式拒绝整次登记，不静默回落。

该模式保留真实任务生命周期和观察事件。分析状态为 observation-only，不调度语义或时间分析，不从静默生成任务告警。活动节点以灰色“只读观察”说明尚无语义结论；任务结束仍保留真实结束状态。来源失联和任务未知单独表达。

只读来源的任务状态尚为 unknown 时，核心保持任务基线未确认；确认 running 或 idle 后恢复基线。冷读追赶、半行待续或尚未找到当前轮次属于当前观察进度，不写入历史采集缺口。真实历史缺口仍按原规则保存，空 gaps 不会将其清除。其它来源的 unknown 描述继续沿用原有基线语义。

同一来源从分析切为只读观察时，核心取消该来源在途轮次、使旧判断与未完成通知失效，并忽略晚到结果。其它来源继续调度。模式保存在来源描述中，重启后仍需来源及任务基线重新确认。

ChatGPT 适配器使用一个安装及 App 执行域作为来源，原生线程身份区分观察对象。单项目资格失效通过批量观察将对应任务置为未知，不注销其它聊天；全局插件停用或 App 执行域失效才注销整份来源。

chatgpt-main/1 健康能力及来源 policy 启用本机主聊天检查。未知 policy 拒绝登记；变更 policy 会封存原分析及请求，旧来源的角色标签不能转为原生用户证明。该策略只接受自己的用户消息生产者及开始引用，自动续跑的用户角色重放不建立新请求。

生产辅助进程启用有界材料采集。适配器的 metadata 默认入口保留为生命周期诊断接缝，材料模式使用独立游标命名空间。两种模式共享物理读取上限；材料模式每批最多 16 条事件，投递前持久保存首次采集时间，确认回执后再推进游标。恢复不把旧 metadata 游标当作已读正文。

目录发现与当前主记录资格共用固定存储根规则。配置根名称为 sessions 时，才额外允许同级 archived_sessions；其它相邻目录不接纳。qualify 为已核实路径选择对应 allowedRoot，读取器继续检查文件身份和只读范围。只有材料模式的 nativeOriginVersion 2 接纳当前 .jsonl.zst；v1 与 metadata 入口仍只接纳 .jsonl，不因目录分类升级协议。

已核实的原生 thread_goal_updated 仅作为目标元数据读取，不建立用户请求、授权、限制、执行活动、验证结果或执行开始／结束。目标内容不进入观察材料；未知原生事件仍返回明确缺口。

ChatGPT 的纯静默和未结束工具只显示“等待原因未确认”及观察时长，不生成对应红色停滞告警。核心接收分析结果时也执行此限制，其它有独立依据的告警保留。DSH 按实际公开等待证据判断；原版缺少父等待审计时，沿用上述能力缺口规则。

conversation-monitoring/1 提供独立聊天控制。GET /v1/conversation/monitoring 查询 registrationId、taskId；POST 同路径接收 registrationId、taskId、paused、revision。设置按 registrationId 与原生 task.id 持久保存，最多 1000 个已控制范围；观察保留期不会解除手动暂停。相同目标状态幂等，过期的相反写入返回 409 conversation_monitoring_conflict。面板写控制只向 app 暴露，宿主会话身份与预期身份须一致，不新增查看入口。

monitoring-failures/1 提供 GET /v1/monitoring/failures 补读及 POST /v1/monitoring/collection-outcome 采集结算。回执带 sourceKey、mainId、lifecycleId、channel、首次轮次、原时间和安全错误分类；初次身份未确认的采集使用显式 null 范围。目录与聊天读取的 channel 分开恢复。分析结算由核心持有，取消或未执行调用不制造失败。已断连但仍保留的 ChatGPT 来源可补报原时间的历史结算，不恢复来源或任务运行状态。

任务的 monitoringHealth 按自身范围投影，来源的同名字段只包含身份未确认的来源级故障。当前状态从故障状态索引取得，不能用第一页历史回执推断是否仍失败。 同一失败回执查询显式指定主聊天和执行范围时，另返回该范围的当前健康投影；它与历史页的数量、裁剪和游标独立。未锚定执行的已核实主聊天使用 mainId 与 null lifecycleId，不补造执行身份。回执和状态各最多 1024 条，固定保留 24 小时；查询支持 afterSequence 与最多 100 条分页，裁剪通过 historyGap 表达。适配器最多 256 条结算 FIFO 只保存安全元数据，失败和恢复按原顺序补报。面板单独显示监控故障，原生通知以同一故障回执建立独立投递记录。

monitoring-notifications/1 提供 GET /v1/notifications/native、POST /v1/notifications/native/eligibility 与 POST /v1/notifications/native/result。通知记录按 sourceKey 分页，包含通知、执行和尝试身份；授权、提交、通知中心快照和用户动作分别写入。结果协议拒绝额外字段、错误身份和无效状态迁移；晚到回执可保留事实，但不能恢复已取消的请求。记录最多 1024 条并保留 24 小时，分页最多 100 条，保留缺口单独表达。原有 DSH 通知认领和确认协议保持。

ChatGPT 的通知驱动在共享核心登记可信 App／插件资格，每次提交前再核对具体主聊天的项目资格。资格未确认时不发送；来源失效、停用或暂停按对应范围取消尚未投递的请求。执行结束取消旧任务告警；已经成立的首次监控故障按 App、插件、项目、本聊天暂停及保留期独立判断，不能因任务结束、首次锚定或故障迅速恢复而丢弃，通知保留故障发生时的身份，恢复不另发通知。原生 IO 只管理本安装前缀下的请求，已显示的通知不因清理而改写历史。权限仅由 app-only 的显式面板动作申请；普通刷新不会弹出许可。独立原生组件使用固定 GhostSentinel bundle 身份，实际系统许可和展示仍须实机验收。

读取进度恢复时，已经监控过的聊天也可能尚未重新确认执行。此时同安装、同宿主运行代次、同原因的 `cursor-*`、`record-*` 及 `native_origin_*` 核验故障共用一次通知尝试，不受三秒时间窗限制；首条立即提醒，其余通知记录标为已合并，不冒充系统接受。每条聊天仍保留独立故障回执，完整恢复后再发生可形成新一段提醒。不同安装、宿主代次、故障原因以及已锚定执行的故障保持独立；重启核心不重置已经消费的合并尝试。原 `cursor-*` 合并键保持，已有尝试不能因本轮扩展而重置。

`monitoring-recovery-rollup/1` 标明共享核心支持上述恢复故障合并。启用原生通知的适配器在采集前核对该能力，缺少时明确显示 `core-update-required`，不继续使用旧通知行为，也不替换或停止共享核心。旧核心自然退出、新核心可用后，后续检查可恢复接入；其他能力缺失仍按原边界拒绝。

撤销每批最多向原生 IO 提交 100 个请求 ID，一轮最多 11 批；只为实际提交的 ID 确认撤销结果。已确认不再待投递的撤销记录不占用后续批次，超过处理上限保留缺口，不能提前确认未提交的请求。

sentinel_current_chat 可携带 expectedThreadId、executionKey 和 historyCursor，按当前已核实的主聊天返回本安装 24 小时内已监控执行。历史选择和分页不调度已结束执行的分析；首次采集时间不因核心或适配器重启延长。旧 app-only 的 sentinel_notification_records 按所选执行查询通知，每页最多 20 条；该服务接口保留，但监控页面已移除旧入口。所有返回均核对聊天、来源和执行范围，切换或销毁页面后晚到结果失效。

history 或 inherited 材料保留为上下文，但不更新当前请求、任务状态、执行活动，也不作新验证。历史非用户材料的缺口保留在历史，不直接污染当前检查；缺少用户上下文所需的证明仍阻止完整判断。

chatgpt-fork-context/1 用固定原生回执传递分叉继承证明，所有材料模式适配器均要求此健康能力。当前 typed UserMessage 的 forkReceiptId／forkGeneration 只声明所需历史，不赋予用户角色；当前绑定仍仅引用自己的开始与输入。继承回执核对文件及头部代次、归属、原生范围、候选数和结构摘要，核心再验证实际保留的开始证据和用户摘录；不能从幸存材料反推预期候选集合。用户候选总数最多 1024，每段最多 4096 项结构证明。超额返回明确上下文缺口，正常采集仍成功；原有显式非法引用继续整批拒绝，100 项引用上限保持。

当前优先交付的输入使用 forkHistoryPending=true 和真实 forkGeneration，与 forkReceiptId 互斥。该标记只用于本机 ChatGPT 的真实 typed UserMessage，不适用于 DSH、历史、子代理或中继材料。指令绑定可用 forkContext 保存相同 generation 和可选 receiptId；没有真实回执时产生 fork_context_pending，不能把缺失标记解释成没有继承要求。补齐后仅更新 hierarchy revision 的绑定，原输入正文、来源与首次采集时间保持不变。

observation.currentCoverage 表示当前文件的采集进度，forkProvenance.complete 继续表示严格整链完成。分析的当前采集覆盖与 contextGaps 分开表达，依赖继承要求的检查在缺口存在时保持不可判断。currentFacts 只从当前公开任务及同轮非历史事件生成；历史容器不显示为当前事实。面板不得因当前片段未见问题而显示完整绿灯，仍保留独立有效的问题。

chatgpt-fork-storage/1 区分历史记录键与归属主会话，全部材料模式在读取正文前要求该能力。分叉边界的 sourceThreadId 保留原生 history_base 记录键，sourceOwnerThreadId 表示经独立核验的记录归属会话。只在锁定宿主格式、明确继承链接、精确文件名、主会话元数据、文件头及原生切点共同匹配时接受两种身份；当前主会话的身份核验仍要求精确匹配，不能用会话树、项目路径或创建来源替代。记录路径可以与规范主会话的当前路径不同，其余身份、项目、来源及创建关系字段须一致；两种角色各自的路径和元数据在读取前后都须稳定。归属进入回执摘要，字段缺失或不一致不能被旧核心静默忽略。

chatgpt-fork-lineage/1 提供平铺谱系恢复和跨来源的原生首采保护。所有材料模式在读取正文前要求该能力。谱系最多 16 个物理帧，活动读取器最多 4 个；每泵 Native 元数据方法最多 144 次，原生物理读取和投递仍分别限 256 KiB 与 16 条。v2 另有整棵谱系共享的 256 KiB 解码预算，校验、丢弃及重放产生的字节也计入，不能给每个帧重新分配额度。谱系游标固定限 256 KiB，其它游标默认 16 KiB；只保存元数据，不保存正文。核心沿全部回执检查到真实 ordinal 0，窗口结束或代数、字节预算耗尽不能记为完整。

正常耗尽读取或解码预算只保留追赶进度，不以零预算调用读取器，也不生成永久来源缺口。末尾守卫尚未完成时，最多暂存原有 16 条输出，恢复 checkpoint 与公开 captureThroughOffset 同时冻结；实际交付后才推进。等待期间不发布完整来源证明，不声明已追上。

分叉当前通道先完成自己的批前、批后守卫，祖先补齐继续共享原预算。分叉外层游标新写 format 3，必须保存 current／history 两个交付水位，区分当前先交付和低 ordinal 历史后补。普通外层游标仍为 format 2。新加载器可读取旧 format 2，并沿原顺序保证迁移两条水位；旧加载器须拒绝新格式。全局 captureThroughOffset 不表示尚未交付历史已经覆盖。发送前首采保存、核心接收后的推进和响应丢失重放继续保持去重。

chatgpt-fork-legacy/1 只支持精确 0.162.0-alpha.2 与 0.160.1 的 paginated 历史子集，不扩大当前 Host 的版本范围。两个旧生产者共用 version 2 回执，固定 userProofMode 为 legacy-unverified；既有 typed 回执仍为 version 1。核心验证来源链后保留 fork_user_context_unverified，即使该范围没有识别出 typed 用户也不声明上下文完整。缺少触发来源的用户材料不补写 composer，不把角色标签当作用户授权。重放与恢复从原始头确定同一模式；缺失或过期回执仍保留对应缺口。所有材料模式在正文读取前要求此能力，未知历史版本、非 paginated 或缺少 ordinal 的历史继续拒绝。

nativeRecordId 在历史虚拟位置映射前固定，只用于重复采集和首采时间核对，不赋予角色或授权。v1 保留原物理文件及头部身份算法；正式 v2 按规范存储文件名、解压后的原始字节位置和原生类型生成身份，仅移除已核实的 .zst 后缀，压缩、解压与归档移动不产生新身份。文件头摘要作为独立防变检查，不混入新身份刷新首采时间。同一原生记录在原会话和分叉中出现时，正文及 capturedAt 不刷新；任务、执行、当前与历史的引用范围仍各自独立。隐含历史证明复用既有每来源 10000 项账本，最多使用其中 8000 项。跨来源的首采进度另按 registrationId 与 nativeRecordId 保存，全核心最多 10000 项；未到期时仅在首份正文仍可核对时复用时间、正文和证据，正文缺失则保留缺口。24 小时后清除正文定位和精确时间，只保留必要的过期标记。来源正常回收不清除此围栏，按 registration 卸载才移除；容量满时拒绝此前未知身份，不静默淘汰旧身份。过期、裁剪或预算不足后不能通过新容器复活内容。

chatgpt-fork-compression/1 是 v2 材料路径读取前必须确认的能力。生产 startFromInstallation 固定使用 nativeOriginVersion 2；直接适配器调用默认 1 仅保留旧合同，没有用户切换选项。NativeJsonlSource 隐藏 plain／zstd 差异，限定进程级最多 4 个压缩 lease、8 MiB 解码窗口及 64 KiB 待消费解码尾块。源身份和生产者项目资格先核实，再完成单 frame 校验；字典、附加 frame、尾随数据、截断或坏 frame 拒绝。无 checksum 的 frame 只说明格式、长度及 EOF，不宣称 checksum 通过。完整校验后才向材料链重放所需范围，切点之后的内容只作完整性校验。plain 与 zstd 同时存在时，两份候选须独立证明头部与所需前缀等价才能合并。

元数据仍指向 `.jsonl`，且该原路径的 lstat 确认 ENOENT 时，NativeJsonlSource 才尝试同名 `.jsonl.zst`。已有 plain 文件保持优先；权限错误、悬空链接、越界路径和两份文件都缺失继续拒绝。回退保留逻辑存储身份，守卫绑定实际选中的物理文件，并继续核对头部、已确认前缀和完整压缩帧。恢复不重放已确认材料，不刷新首采时间，也不把归档历史算作新活动。

POST /v1/source/native-origin 按 sourceKey 与 key 读取同 registration 的无正文进度。v2 采集批次最多携带 16 份 nativeOrigins，包含头摘要、revision、旧前缀、重新验证的旧前缀及新前缀；前缀由固定 64 KiB block 的 root/count 和末块 length/hash 表达，不保存正文、decoder 或 crypto 内部状态。核心另限 10000 份存储代防变记录。每条 v2 原生事件必须携带 nativeGenerationKey、nativeRecordStart、nativeRecordEnd，身份及范围须与本批完整证明吻合。不同切点并发时，不回退已经确认的较长前缀；旧 revision 扩展明确冲突，适配器返回已确认游标重新核验。

plain 读取在接受材料前再次核对路径与打开文件的身份、大小和时间；同大小改写同时核对 mtime 与 ctime。追加后在共享物理及解码预算内重新核验最长已知前缀，验证完成前不认可新的物理签名或发布材料。重验中恢复游标仍保留最后可信签名。完成的历史读取器只在 plain 重验尚未完成时保留；已完成的压缩守卫及时归还解码名额。已确认的 compressed-data-invalid、native_origin_prefix_changed 和 native_origin_header_changed 进入固定采集失败分类；修订冲突、未知 frame 及正常预算等待不因此升级为失败。

旧执行冷读采用同一材料投影的有界预扫及原游标重放。预扫继承首次已核实的文件身份、头部和前缀，不推进材料 ACK、captureThrough 或首采时间；暂停的材料 IO 先关闭，全部读取仍共用原每泵预算。仅同一执行的完成或中断记录可形成不含正文的 terminalProof，绑定存储键、头摘要、前缀、执行 ID、结束状态和时间。重放前再次验证来源，只豁免该执行的旧开始缺口；新执行和其它缺口独立判断。尚未核实的旧执行仍不可判断；其历史材料不恢复该执行的运行状态，也不调度模型。

旧执行尚未确认结束而出现后继执行时，successorProof 只绑定已读取的新开始、当轮已核实 Native anchor、旧执行及同一存储头部和前缀；新开始不得早于 hostStartedAt 加一秒。重验后仍从原材料 checkpoint 重放，旧开始映射 unknown 并保留 historical-execution-unconfirmed，新执行使用自己的真实开始、输入及结束引用。预扫仅保留一个真实开始的小元数据对象，直到下一开始或关闭；接口身份迟到不会丢失这一事实，结束状态仍由真实材料决定。完成预扫后恢复的 IO 元数据保留已核实物理身份，不能接受未经重新资格核验的同路径文件替换。

版本记录只随实际 Native 首采进度或存储代进度确定；future-only 或全部忽略批次不产生单独版本行。新写入的版本记录由两份各 10000 项的固定进度推得最多 20000 项。旧原型已遗留、没有可追溯进度的版本行保守保留，单列 legacyModeOnly 数量作为迁移门禁，不自动删除或重计时。按 registration 卸载须同时清除版本、存储代与首采进度；停用和普通来源回收不等同于卸载。

带原生身份的 ChatGPT 采集批次，将首采、存储代、版本进度与本批任务、材料和时间更新一起提交。写入失败时回滚受影响的持久记录和内存投影，不能占用新增身份容量、留下部分正文或缩短旧记录时间；成功提交后才发布新进度和调度。已取消的计算保持封存，不因回滚恢复旧在途结果。

当前优先的谱系 reader 顶层新写 format 4，必须带双水位。kind 仍按原生协议使用 fork-lineage/1 或 fork-lineage/2，nativeOriginVersion 和 receiptVersion 不变。新 reader 可读旧 v1 format 2 及旧 v2 format 3；内部 BoundedReader 帧格式和普通 v2 format 3 保持。旧 reader 拒绝新顶层格式，降级不能静默忽略双水位。

游标保存有界元数据，重放回到上次已确认位置；完成的压缩游标恢复也核对 inode、size、mtime 与 ctime。跨 codec 必须重新核对唯一候选和已确认前缀，只重绑物理状态。非空 v1／v2 进度不能相互切换；无法映射的旧进度及不具备所需状态的游标明确拒绝，不能清空游标、变更 registration 或重计时间绕过。分析复用身份包含当前指令上下文成员与缺口，隐含开始证明过期后不复用旧完整结论。

v2 游标新增不含宿主运行代次的 `storageScope`，继续绑定存储身份、主聊天、宿主版本与读取模式；旧 `scope` 保留。跨运行代次恢复时，非空进度必须匹配同安装核心保留的原生存储键、头摘要及覆盖已读位置的前缀，再按原字节预算重验物理文件、头部和前缀。完成前显示 `cursor-revalidating`，冻结原游标且不发布新材料或完整判断。完全未读的旧游标可以开始正常读取；缺少必要原生账本、身份不符或文件已改写仍拒绝。分叉保留当前／历史交付水位，未完成的祖先元数据探针核对原绑定后续读。旧运行状态不因此变为新宿主中的活动，首采时间不刷新。

持久游标中的设备号变化不单独证明文件已被替换。同路径、同 inode 的恢复只有在原生存储键和同安装可信前缀存在、覆盖原读取位置时，才允许重新核验并绑定当前设备号。原大小、时间与压缩帧守卫继续生效，普通文件追加仍须重验旧前缀；重验期间冻结进度、保持不可判断。inode 变化、截断、同大小改写、头部或前缀不符仍拒绝。当前打开文件与路径的实时身份校验保持严格，不把这条恢复规则用于放行读取途中的文件替换。

自动发现和原生材料的覆盖边界见 [开发候选说明](chatgpt-observation.md)。候选须通过真实主聊天、小规模真实分叉、压缩可读和生命周期框架验收；十几层完整重放为非阻塞兼容性专项。来源串位、错误授权、首采刷新和资源泄漏仍是框架缺陷。最终构建、实机、完整本地门禁和交付复核按实际证据记录。本次按用户在获知未验项目后提出的递增发布要求交付；真实 App 退出重开及其它实机缺口继续保留。本轮不执行升级专项，未执行项不计通过；统一版本号不扩大已验证范围。
