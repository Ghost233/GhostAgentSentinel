# 首版开发接缝与验收

版本：0.1.0。本文记录核心开发接缝；最终验收范围见 [发布验证范围](release-status.md)。

## 范围和分工

- 共享核心：持久化、来源与任务状态、调度、分析取消、摘要、告警合并和通知投递状态。
- 分析模块：六项检测、带来源的摘取、JEV 判断、引用/协议校验和未知/错误区分。
- DSH 适配器：原生事件与基线、非阻塞发送、当前来源登记、右侧总览和桌面通知。
- 主线程：公共协议、守护进程/客户端、运行依赖与打包、整体集成检查和文档。

发布使用当前本地 main，先完成最终输入的本地检查，再提交和推送；不创建额外分支或 worktree，不提交本地运行记录。发布前门禁与发布后验收按 [发布流程与完成条件](release-status.md#发布流程与完成条件) 执行。

来源采用用户已选择的逐后台接入：每个独立 DSH 配置启用一次适配器，之后自动连接同一核心。预期来源清单持久保存配置身份、名称和启用状态，显示已配置但未连接的来源及清单外来源；缺少完整发现证明时总览显示覆盖未确认。首版不实现跨后台自动发现和装配。本轮也不批准新的告警质量/负载指标。实现参数是可复核的开发默认值，不替代正式验收。

## 测试接缝

沿用用户已确认的接缝：统一观察输入和监控结果读取、DSH 原生事件与客户端 Remote、公开 Chat/JEV HTTP、独立核心的启停和保存。测试跨这些公共接缝，不直接查询私有数据库或绕过模块接口。时钟、外部推理 HTTP 和系统通知为外部 IO 适配器，测试可以替换；业务状态、调度、告警和事件处理必须真实执行。

每个实现者先写一个失败用例，再实现相应纵向行为，保留命令与真实退出结果。测试覆盖隔离、重复、晚到结果、24 小时到期、真实重启、冷却、失败与非干扰；不得只验证字段常量。

## 固定接口

协议版本为 1。所有时间为 epoch 毫秒；原始时间不可获得时保留 null，capturedAt 是来源首次采集时间，重传不改它。身份字符串作为不透明值处理，不按名称或 PID 猜归属。

来源描述：`{registrationId, hostRunId, name, profile?, capabilities?, gaps?, enroll?}`。registrationId 标识接入配置，hostRunId 区分独立宿主运行上下文；一个配置可产生不同运行实例，重连不改变现存 hostRunId。enroll 仅稳定配置身份为 true，在用户尚未手动管理清单时登记该配置；临时身份不自动加入清单。任务描述：`{id, lifecycleId, sessionId?, title, status, parentId?, gaps?}`，status 为 running/idle/disposed/unknown。

事件：`{id, taskId, taskLifecycleId, kind, occurredAt, capturedAt, text, evidence?, data?}`。kind 为 input/output/tool-start/tool-result/status/gap。data 只放可获取的事实：callId/rootCallId/parentCallId、toolName、command、exitCode、isError、errorCode、sourceVersion、validationScope、artifactChanged、status、truncated、redacted。无来源的字段不补造。摘录默认最多 240 字符单元，截断标明缺口，不能读认证头、凭据配置或传输令牌。传输认证只用于核心连接，不进入监控结果。原任务内容中显式的认证头、常见凭据字段、私钥和已识别凭据读取命令，在适配器与 HTTP 接收处整字段省略，标 redacted；省略影响当前请求的覆盖。该规则识别显式格式，不是任意秘密文本的通用识别器。

核心模块导出 `createCore({dataDir, analyzer, now?, cooldownMs?, repeatMs?, leaseMs?, idleGraceMs?, onIdle?})`，异步方法：registerSource(descriptor)、heartbeat(sourceKey)、ingest(sourceKey,{events?,tasks?,gaps?})、unregisterSource(sourceKey,reason)、snapshot()、diagnostics()、sourcePlan()、configureSources(entries)、pause()、resume()、setAnalyzer(analyzer)、claimNotifications(sourceKey)、ackNotification(id,result)、close()。register 返回来源 key；ingest 返回接受/重复/过期计数与 paused。snapshot 读取业务状态；diagnostics 只读资源计数，不含任务正文或令牌。configureSources 整体替换最多 100 条配置，不控制宿主插件开关。

用户已确认首版沿用聊天和工具现有的版本、验证范围与退出结果。缺失时显示不可判断，六项检测全部保留。额外读取工作区的版本指纹采集后置；适配器不主动读取工作区来补全证据。

分析模块导出 `createAnalyzer(config)`，对象提供 `analyze({task,source,events,coverage},{signal})`。返回 `{summary,checks,coverage,usage?,quality:'unvalidated'}`；summary 包含 currentTask/currentState/recentProgress/attention 字符串。checks 固定六项 ID：stalled_progress/request_drift/failure_loop/validation_cost/validation_gap/long_wait，每项 `{id,status,reason,evidenceIds}`，status 是 attention/clear/unknown。evidenceIds 必须属于本轮对应任务，unknown 不算正常，confidence 不表示准确率。未配置推理时返回明确依赖状态，不能发语义成功告警。

核心 snapshot 包含 protocol、generatedAt、monitoring、overview、sources、tasks。overview 显示 runningTasks/knownSources/connectedSources/attentionTasks/coverage。任务含 key/sourceKey/id/lifecycleId/title/status/currentRequest/updatedAt/lastActivity/events/analysis/alerts。analysis 含 state/through/completedAt/nextEligibleAt/error/summary/checks；来源含 key/descriptor/status/lastSeenAt/gaps。响应不暴露访问令牌。

分析模块的私有 LLM 接缝使用有限证据目录。每组原文只提供一次，可选项明确使用 `optionId`；模型只返回 `selectedOptionIds` 与 `gaps`。模型载荷不展示结构化的来源事件 ID 或当前请求 ID，以 `isCurrentInput` 标明已确认锚点；真实事件定位、原文和来源允许的事实分类仍由代码绑定。目录项仅表示可选择资格，实际门禁、授权等语义仍需选择与判断。未知或重复选项、额外字段、旧键和自由事实格式均报错，不自动修复或接受事件 ID 作为选项。还原后继续校验原文引用与来源权限。当前请求从保留范围内已确认的真实非空 input 确定，空、缺失或过期时保留未知，不以任务标题或元数据补造。完整 LLM 请求 JSON 受原 24,000 字符预算约束，输出上限仍为 1,024 token；公共分析结果、JEV 和适配器协议保持不变。

偏离检查的询问资格依据真实当前请求、完整覆盖以及本轮非空输出或工具开始活动，不要求模型再选出 action 分类。活动只说明有材料可问，偏离、合理调查或必要前置仍由 JEV 核对；不直接设置判断，也不补造分类。仅有工具结果、缺少请求或活动、覆盖不足时保持未知。引用保留本轮相关原文，便于展开核对权限和调查理由。

任务另有 `activity: {summary, observedAt, evidenceIds}`，由核心在读取快照时依据本任务保留观察事件临时生成。它描述已采集的输出、工具记录和未配对结束记录，不代表模型推断或完成证明。当前请求范围、来源失联及采集缺口须明确；等待时长只表示已观察到的时间下界，不能据此断言工具仍在执行或卡死。分析暂停、依赖未配置或旧轮次等待时，新采集活动仍能更新该字段。该字段不新增持久记录或专用计时器；旧核心未提供时，客户端明确显示活动摘要未提供。

守护进程 HTTP 全部绑定回环并校验私有连接令牌；路径由主线程固定。事件确认仅表示持久接收。适配器只使用 HTTP 接缝，不导入核心业务类。通知由所属来源领取，提交成功与用户看见分别表达；任务结束、来源失联或引用过期停止重复。认领开发默认 60 秒到期、结果未知；再次投递仍遵守 10 分钟间隔并使用新 ID，晚到旧回执不能改变新尝试。异步系统失败可修正已提交状态。

系统可见通知正文保留与面板一致的完整观察身份 `task.key` 和来源身份 `source.key`。身份在来源名称和告警原因之前，客户端前 240 字符也能区分同名跨来源任务；完整正文仍限 1200 字符。私有通知记录中的身份不替代用户可见定位。

同一主会话合并多个代理告警时，兼容字段 `taskKey` 和正文中的完整观察身份指向投递前仍有效的首成员，共同来源身份仍完整显示。首成员被过滤后，两处身份一起更新，原 3 秒合并截止不延后。其余成员在正文以名称、必要短标识和原因表示，公开 `members` 资料保留逐代理完整身份及问题关系；它不替代上述系统可见定位。该规则不改变单成员通知、前 240 字符定位和 1200 字符正文界限。

适配器接缝：`src/core-client.mjs` 导出 `connectCore({dataDir})`，返回与上述核心方法同名的 HTTP 方法（不含 setAnalyzer），以及 `configureAnalysis(config)`、`analysisConfig()` 和 `close()`。close 只关闭客户端；适配器须先 unregisterSource。连接断开后适配器重试 connectCore 和登记/基线，不能依赖旧连接 token 或旧 sourceKey。配置为 `{llm:{baseUrl,model},jev:{baseUrl,model}}`，未设置的项可以省略；只允许回环 HTTP(S)，不包含密钥。配置读取仅返回这两个公开地址/模型 ID。公开事件验证由主线程完成，业务语义与去重由核心完成。核心默认租约 15 秒，适配器心跳 3 秒；开发默认长等待阈值 5 分钟明确标为待质量验收，不作为任务失败结论。

第二十轮新增条件配置接缝，用于避免临时测试覆盖并发修改。`analysisConfigState()` 返回 `{config,revision}`。`configureAnalysisIfCurrent(config,revision)` 在共享写入队列内核对修订号，成功后返回新的状态。原 `configureAnalysis(config)` 也进入同一队列。每次成功写入均换修订号，包括值未变化的写入；核心重启也换号。修订号只在当前运行中有效，不改变持久配置格式。

HTTP 状态读取为 `GET /v1/analysis/config/state`，条件写入为 `POST /v1/analysis/config/compare`，正文是 `{config,revision}`。修订冲突返回 409 和 `analysis_config_conflict`，不修改配置。应用失败先尝试逐项恢复旧文件和旧分析器，保留原错误和恢复错误。恢复无法确认时，两个配置读取入口及条件写入返回 503 和 `analysis_config_unconfirmed`。成功的原无条件写入可以重新确认配置。响应丢失时，调用方保留结果未知，不能仅凭值相同推定写入归属。具体实现与故障验收状态见 [发布验证范围](release-status.md)。

最后来源退出：保存、取消自己分析请求，短宽限后退出；模型驻留，晚到结果忽略。重启恢复只使用 24 小时保留资料，来源/运行状态先标未知，重新登记与基线确认后再分析，不恢复原任务或旧请求。

## 交付界限

共享运行与资源回收覆盖任务／工具索引、客户端请求收尾、面板刷新与后台通知分工，保留至少 20 个并发客户端和 100 次循环的资源检查。Codex 共享 HTTP MCP 方案后置。

本版验收框架运行。模型准确度、GPU 拥堵及推理耗时不作为发布门槛；依赖失败、不可判断、排队和滞后仍须如实表达。真实宿主接口能力与实测边界见 [安装说明](installation.md) 和 [发布验证范围](release-status.md)。
