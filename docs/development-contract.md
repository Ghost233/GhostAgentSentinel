# 首版开发接缝与验收

源码候选：0.1.2-dev.0。本文记录核心开发接缝；已发布 0.1.1 的验收范围见 [发布验证范围](release-status.md)。ChatGPT 增量仍在开发，不扩大旧版本的交付承诺。

## 范围和分工

- 共享核心：持久化、来源与任务状态、调度、分析取消、摘要、告警合并和通知投递状态。
- 分析模块：六项检测、带来源的摘取、JEV 判断、引用/协议校验和未知/错误区分。
- DSH 适配器：原生事件与基线、非阻塞发送、当前来源登记、右侧总览和桌面通知。
- 主线程：公共协议、守护进程/客户端、运行依赖与打包、整体集成检查和文档。

发布使用当前本地 main，先完成最终输入的本地检查，再提交和推送；不创建额外分支或 worktree，不提交本地运行记录。发布前门禁与发布后验收按 [发布流程与完成条件](release-status.md#发布流程与完成条件) 执行。

来源采用用户已选择的逐后台接入：每个独立 DSH 配置启用一次适配器，之后自动连接同一核心。预期来源清单持久保存配置身份、名称和启用状态，显示已配置但未连接的来源及清单外来源；缺少完整发现证明时总览显示覆盖未确认。首版不实现跨后台自动发现和装配。本轮也不批准新的告警质量/负载指标。实现参数是可复核的开发默认值，不替代正式验收。

## 测试接缝

沿用用户已确认的接缝：统一观察输入和监控结果读取、DSH 原生事件与客户端 Remote、公开 Chat/JEV HTTP、独立核心的启停和保存。业务断言跨这些公共接缝，不直接查询私有数据库或绕过模块接口。独立的持久化 IO 检查可在自有临时存储上核对实际删除或注入存储失败，并明确标注用途；它们不替代公开接口的业务断言，也不为测试增加私有资料导出接口。时钟、外部推理 HTTP 和系统通知为外部 IO 适配器，测试可以替换；业务状态、调度、告警和事件处理必须真实执行。

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

## ChatGPT 只读观察增量

0.1.2-dev.0 新增 source-observation/1 健康能力。来源登记可声明 mode 为 observation-only；省略该字段保持原有分析行为。未知模式拒绝整次登记，不静默回落。

该模式保留真实任务生命周期和观察事件。分析状态为 observation-only，不调度语义或时间分析，不从静默生成任务告警。活动节点以灰色“只读观察”说明尚无语义结论；任务结束仍保留真实结束状态。来源失联和任务未知单独表达。

只读来源的任务状态尚为 unknown 时，核心保持任务基线未确认；确认 running 或 idle 后恢复基线。冷读追赶、半行待续或尚未找到当前轮次属于当前观察进度，不写入历史采集缺口。真实历史缺口仍按原规则保存，空 gaps 不会将其清除。其它来源的 unknown 描述继续沿用原有基线语义。

同一来源从分析切为只读观察时，核心取消该来源在途轮次、使旧判断与未完成通知失效，并忽略晚到结果。其它来源继续调度。模式保存在来源描述中，重启后仍需来源及任务基线重新确认。

ChatGPT 适配器使用一个安装及 App 执行域作为来源，原生线程身份区分观察对象。单项目资格失效通过批量观察将对应任务置为未知，不注销其它聊天；全局插件停用或 App 执行域失效才注销整份来源。

chatgpt-main/1 健康能力及来源 policy 启用本机主聊天检查。未知 policy 拒绝登记；变更 policy 会封存原分析及请求，旧来源的角色标签不能转为原生用户证明。该策略只接受自己的用户消息生产者及开始引用，自动续跑的用户角色重放不建立新请求。

生产辅助进程启用有界材料采集。适配器的 metadata 默认入口保留为生命周期诊断接缝，材料模式使用独立游标命名空间。两种模式共享物理读取上限；材料模式每批最多 16 条事件，投递前持久保存首次采集时间，确认回执后再推进游标。恢复不把旧 metadata 游标当作已读正文。

目录发现与当前主记录资格共用固定存储根规则。配置根名称为 sessions 时，才额外允许同级 archived_sessions；其它相邻目录不接纳。qualify 为已核实路径选择对应 allowedRoot，读取器继续检查文件身份和只读范围。只有材料模式的 nativeOriginVersion 2 接纳当前 .jsonl.zst；v1 与 metadata 入口仍只接纳 .jsonl，不因目录分类升级协议。

已核实的原生 thread_goal_updated 仅作为目标元数据读取，不建立用户请求、授权、限制、执行活动、验证结果或执行开始／结束。目标内容不进入观察材料；未知原生事件仍返回明确缺口。

ChatGPT 的纯静默和未结束工具只显示“等待原因未确认”及观察时长，不生成对应红色停滞告警。核心接收分析结果时也执行此限制，其它有独立依据的告警保留。DSH 的原等待规则不变。

conversation-monitoring/1 提供独立聊天控制。GET /v1/conversation/monitoring 查询 registrationId、taskId；POST 同路径接收 registrationId、taskId、paused、revision。设置按 registrationId 与原生 task.id 持久保存，最多 1000 个已控制范围；观察保留期不会解除手动暂停。相同目标状态幂等，过期的相反写入返回 409 conversation_monitoring_conflict。面板写控制只向 app 暴露，宿主会话身份与预期身份须一致，不新增查看入口。

monitoring-failures/1 提供 GET /v1/monitoring/failures 补读及 POST /v1/monitoring/collection-outcome 采集结算。回执带 sourceKey、mainId、lifecycleId、channel、首次轮次、原时间和安全错误分类；初次身份未确认的采集使用显式 null 范围。目录与聊天读取的 channel 分开恢复。分析结算由核心持有，取消或未执行调用不制造失败。已断连但仍保留的 ChatGPT 来源可补报原时间的历史结算，不恢复来源或任务运行状态。

任务的 monitoringHealth 按自身范围投影，来源的同名字段只包含身份未确认的来源级故障。当前状态从故障状态索引取得，不能用第一页历史回执推断是否仍失败。 同一失败回执查询显式指定主聊天和执行范围时，另返回该范围的当前健康投影；它与历史页的数量、裁剪和游标独立。未锚定执行的已核实主聊天使用 mainId 与 null lifecycleId，不补造执行身份。回执和状态各最多 1024 条，固定保留 24 小时；查询支持 afterSequence 与最多 100 条分页，裁剪通过 historyGap 表达。适配器最多 256 条结算 FIFO 只保存安全元数据，失败和恢复按原顺序补报。面板单独显示监控故障，原生通知以同一故障回执建立独立投递记录。

monitoring-notifications/1 提供 GET /v1/notifications/native、POST /v1/notifications/native/eligibility 与 POST /v1/notifications/native/result。通知记录按 sourceKey 分页，包含通知、执行和尝试身份；授权、提交、通知中心快照和用户动作分别写入。结果协议拒绝额外字段、错误身份和无效状态迁移；晚到回执可保留事实，但不能恢复已取消的请求。记录最多 1024 条并保留 24 小时，分页最多 100 条，保留缺口单独表达。原有 DSH 通知认领和确认协议保持。

ChatGPT 的通知驱动在共享核心登记可信 App／插件资格，每次提交前再核对具体主聊天的项目资格。资格未确认时不发送；来源失效、停用或暂停按对应范围取消尚未投递的请求。执行结束取消旧任务告警；已经成立的首次监控故障按 App、插件、项目、本聊天暂停及保留期独立判断，不能因任务结束、首次锚定或故障迅速恢复而丢弃，通知保留故障发生时的身份，恢复不另发通知。原生 IO 只管理本安装前缀下的请求，已显示的通知不因清理而改写历史。权限仅由 app-only 的显式面板动作申请；普通刷新不会弹出许可。独立原生组件使用固定 GhostSentinel bundle 身份，实际系统许可和展示仍须实机验收。

撤销每批最多向原生 IO 提交 100 个请求 ID，一轮最多 11 批；只为实际提交的 ID 确认撤销结果。已确认不再待投递的撤销记录不占用后续批次，超过处理上限保留缺口，不能提前确认未提交的请求。

sentinel_current_chat 可携带 expectedThreadId、executionKey 和 historyCursor，按当前已核实的主聊天返回本安装 24 小时内已监控执行。历史选择和分页不调度已结束执行的分析；首次采集时间不因核心或适配器重启延长。app-only 的 sentinel_notification_records 另按所选执行查询通知，每页最多 20 条，页面仅在用户打开、刷新或翻页时读取。所有返回均核对聊天、来源和执行范围，切换或销毁页面后晚到结果失效。

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

POST /v1/source/native-origin 按 sourceKey 与 key 读取同 registration 的无正文进度。v2 采集批次最多携带 16 份 nativeOrigins，包含头摘要、revision、旧前缀、重新验证的旧前缀及新前缀；前缀由固定 64 KiB block 的 root/count 和末块 length/hash 表达，不保存正文、decoder 或 crypto 内部状态。核心另限 10000 份存储代防变记录。每条 v2 原生事件必须携带 nativeGenerationKey、nativeRecordStart、nativeRecordEnd，身份及范围须与本批完整证明吻合。不同切点并发时，不回退已经确认的较长前缀；旧 revision 扩展明确冲突，适配器返回已确认游标重新核验。

plain 读取在接受材料前再次核对路径与打开文件的身份、大小和时间；同大小改写同时核对 mtime 与 ctime。追加后在共享物理及解码预算内重新核验最长已知前缀，验证完成前不认可新的物理签名或发布材料。重验中恢复游标仍保留最后可信签名。完成的历史读取器只在 plain 重验尚未完成时保留；已完成的压缩守卫及时归还解码名额。已确认的 compressed-data-invalid、native_origin_prefix_changed 和 native_origin_header_changed 进入固定采集失败分类；修订冲突、未知 frame 及正常预算等待不因此升级为失败。

旧执行冷读采用同一材料投影的有界预扫及原游标重放。预扫继承首次已核实的文件身份、头部和前缀，不推进材料 ACK、captureThrough 或首采时间；暂停的材料 IO 先关闭，全部读取仍共用原每泵预算。仅同一执行的完成或中断记录可形成不含正文的 terminalProof，绑定存储键、头摘要、前缀、执行 ID、结束状态和时间。重放前再次验证来源，只豁免该执行的旧开始缺口；新执行和其它缺口独立判断。尚未核实的旧执行仍不可判断；其历史材料不恢复该执行的运行状态，也不调度模型。

旧执行尚未确认结束而出现后继执行时，successorProof 只绑定已读取的新开始、当轮已核实 Native anchor、旧执行及同一存储头部和前缀；新开始不得早于 hostStartedAt 加一秒。重验后仍从原材料 checkpoint 重放，旧开始映射 unknown 并保留 historical-execution-unconfirmed，新执行使用自己的真实开始、输入及结束引用。预扫仅保留一个真实开始的小元数据对象，直到下一开始或关闭；接口身份迟到不会丢失这一事实，结束状态仍由真实材料决定。完成预扫后恢复的 IO 元数据保留已核实物理身份，不能接受未经重新资格核验的同路径文件替换。

版本记录只随实际 Native 首采进度或存储代进度确定；future-only 或全部忽略批次不产生单独版本行。新写入的版本记录由两份各 10000 项的固定进度推得最多 20000 项。旧原型已遗留、没有可追溯进度的版本行保守保留，单列 legacyModeOnly 数量作为迁移门禁，不自动删除或重计时。按 registration 卸载须同时清除版本、存储代与首采进度；停用和普通来源回收不等同于卸载。

带原生身份的 ChatGPT 采集批次，将首采、存储代、版本进度与本批任务、材料和时间更新一起提交。写入失败时回滚受影响的持久记录和内存投影，不能占用新增身份容量、留下部分正文或缩短旧记录时间；成功提交后才发布新进度和调度。已取消的计算保持封存，不因回滚恢复旧在途结果。

当前优先的谱系 reader 顶层新写 format 4，必须带双水位。kind 仍按原生协议使用 fork-lineage/1 或 fork-lineage/2，nativeOriginVersion 和 receiptVersion 不变。新 reader 可读旧 v1 format 2 及旧 v2 format 3；内部 BoundedReader 帧格式和普通 v2 format 3 保持。旧 reader 拒绝新顶层格式，降级不能静默忽略双水位。

游标保存有界元数据，重放回到上次已确认位置；完成的压缩游标恢复也核对 inode、size、mtime 与 ctime。跨 codec 必须重新核对唯一候选和已确认前缀，只重绑物理状态。非空 v1／v2 进度不能相互切换；无法映射的旧进度及不具备所需状态的游标明确拒绝，不能清空游标、变更 registration 或重计时间绕过。分析复用身份包含当前指令上下文成员与缺口，隐含开始证明过期后不复用旧完整结论。

自动发现和原生材料的覆盖边界见 [开发候选说明](chatgpt-observation.md)。首版须通过真实主聊天、小规模真实分叉、压缩可读和生命周期框架验收；十几层完整重放改为非阻塞兼容性专项。来源串位、错误授权、首采刷新和资源泄漏仍是框架缺陷。正式后台、稳定连接、升级、按来源卸载及新候选实机验收由后续阶段完成。
