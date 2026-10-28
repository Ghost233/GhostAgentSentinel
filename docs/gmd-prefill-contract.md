# GMD 真实预填接口契约（gmd-prefill/1）

本文供 GhostSplash、Ghost Model Deck（GMD）和 Sentinel 的实现者交接。2026-10-28，用户要求本轮同时完成 CPU／调度优化和真实预填（prefill），并先写清接口，再由用户转交 GMD 项目。

**本文记录已对齐的接口与剩余验证边界，不代表现有运行服务已经升级。** 初次交接仅包含文档；后续授权与实施边界见下段。新增字段、能力声明和请求管理路径由三方按同一协议实施；变更仍须先对齐再接入。

2026-10-28 后续授权更新：用户要求两边沟通协作开发，并将 Sentinel 生产分析链接到正常预填流程。Sentinel 已实施 Core 维护优化、客户端及生产分析接线，并运行受控 HTTP 与隔离 Core 回归；随后使用 GMD 的隔离 PublicGateway 完成了跨仓文本、JEV 与身份级拒绝证明互验。未提交证明已按控制面回归冻结并接入客户端。用户进一步要求入口开启后发布并自行下载测试：GMD 0.3.1 已按实际运行实例能力启用入口，GhostSplash 提供路径能力声明；不支持或旧协议组合继续关闭。实际模型缓存与受管引擎联合验收仍未关闭，当前运行中的应用、Core 和模型服务没有改变。

## 1. 范围与现状

| 项目 | 本轮要求 |
| --- | --- |
| 文本路径 | `/v1/chat/completions`：零输出预算预填累计 `messages`，正数预算生成。 |
| JEV 路径 | `/v1/systemone`：`prefill_only: true` 预填累计 `state`，随后提交问题。仅限明确支持的组合。 |
| Sentinel 自动预填 | 持续后台预填先只用于总结模型。JEV 消费整理后的本轮证据；接口与联合验收覆盖 JEV 预填，不默认启用 JEV 持续后台预填。 |
| `/v1/completions` | Sentinel 当前不调用，本轮不要求 GMD 为它增加公开入口。GhostSplash 内部若使用该接口，由 GMD 适配，不改变 Sentinel 的调用入口。 |
| 交付顺序 | GhostSplash 的增量预填实现先适配并合入约定的当前版本，再由 GMD 公开能力，最后接入 Sentinel 并联合验收。历史分支或旧引擎的单独成功不能替代这一步。 |

本次源码核对基准：Sentinel 基于 `1a2fc28c088f9c514cf55bb5a0536c0f523eb711` 的本地工作区；GMD 为 `dcfe6995d872a3a947adad7dc93c6478148f98e1`；GhostSplash 为 `b9cfcabea4e4269b8db50cbc9cdacf6200ac2be2`。源码基准不代表运行服务的实际版本。

已核实的差距：

- Sentinel 在 `src/analysis/index.mjs` 仅调用 Chat Completions 与 System One。当前生成请求为非流式，并拒收 SSE（服务器发送事件）。
- GMD 的 `GenerationConfiguration` 当前要求输出预算为正整数或 `null`；GhostSplash 已核对版本也拒绝零预算。显式 `0` 需要新增真实预填语义。
- GMD 当前 JEV 顶层仅接受 `model/state/questions/stream/debug/images`。`prefill_only` 和 JEV 顶层 `cache_salt` 尚未公开。普通 JEV 的题目指令位于 `questions.<id>.instructions`，没有通用顶层 `instructions`。
- 已有 Splash 专用 `cache_salt` 只说明缓存隔离参数可传递，不能证明零输出预填、追加、完成或取消后缓存状态。GMD 的 JEV 适配还需透传这个参数。
- `stream: true` 的生成输出片段和结束帧不能充当预填完成证明。`/v1/models` 可见、`type: llm/decision` 或 `source: native/council` 也不能证明支持预填。

源码入口：`GhostModelDeck/lib/public_gateway.dart`、`lib/generation_configuration.dart`、`lib/jev_models.dart`；`GhostSplash/server/frontend.py`、`server/judgments.py`；Sentinel 的 [分析调用](../src/analysis/index.mjs)和 [HTTP 守卫](../src/analysis/http.mjs)。

### 协作核对：哪些证据仍缺失

以下为两边继续核对后的边界，尚未将提案字段定稿为已支持能力。GMD 的[协作记录](/Users/ghost233/Ghost233Code/GhostModelDeck/docs/requirements/incremental-prefill-gmd-review.md)保留其源码依据。

| 字段或行为 | 当前可核对的来源与限制 |
| --- | --- |
| 目标前缀命中 | GhostSplash 原生 `StartEvent.matched_prompt_tokens` 经 `NativeResult.cache.matched_tokens` 进入 Chat 的 `usage.prompt_tokens_details.cached_tokens`。这是目标侧计数候选来源，不证明未来可复用边界或草稿侧命中。 |
| 新处理量 | 当前 `prefill_tokens` 由 prompt 数减去 matched 数计算；另有 `deduplicated_tokens`。不能直接将这个派生量改名为实际新计算量，须保留来源及去重含义。 |
| 输入摘要与可复用边界 | 尚未证明引擎公开实际 token 输入摘要或 `reusable_prefix_tokens`。GMD 自算请求摘要只能证明请求绑定，不能冒充引擎 token 摘要；未知字段不填推算值。 |
| JEV 计数 | 历史状态预填仅有空 `answers`、`usage` 和 `prefill_only`；最终判断的逐题计数须保留题目与原生请求映射，不能把重复状态的合计量当作单一前缀。 |
| 取消释放 | 基线原生 runtime 的 Done 早于释放；GMD 关闭 HTTP 客户端后释放 permit 也不构成原生释放证明。GhostSplash 当前工作区原型改用原生协议 12，须在匹配的新二进制上验证释放后终态。 |
| 响应外形 | 历史 Chat 空内容可能为 `null`，历史 JEV 使用空 `answers`。外形可以在 GMD 适配；不得靠外形或零输出数字单独判断完成。 |

最低共同语义不能省略：真实零生成且输入处理完成；固定材料、盐、模型与运行代次绑定；同缓存串行及取消后的真实终态与释放；命中和最终结果可关联的原生证据。具体 JSON 外形，以及无法证明的前缀边界／token 摘要字段，可以继续协商精简，须在双方实施前更新本契约。下文 v1 示例中的未证明字段仍是提案要求，不是已有引擎事实。

初次对接时 GMD 仅声明 `supported: false` 和 `reason: gmd_prefill_unavailable`，该阶段不启用预填。后续 GMD 0.3.1 改为核对实际受管 Splash 的服务身份、就绪代次、`splash-prefill/1` 路径能力与完整操作协议，才声明对应模式支持。Sentinel 将缺字段、旧协议和明确不支持视为预填不可用，不因此禁用已有普通生成或判断。

GhostSplash 的 `/v1/splash/runtime` 同次响应包含 `service_instance`、就绪的 `runtime_generation` 与 `prefill_capabilities: {schema: 'splash-prefill/1', operation_tracking, chat_zero_budget, systemone_state}`。未就绪、关闭、致命错误或探测中代次变化时不声明支持。文本和 JEV 路径分别核对，状态前缀探针不证明所有输入都命中，也不证明草稿模型活动或模型精度。Sentinel 不直接读取原生入口，仍只消费 GMD 的公开绑定声明。

Sentinel 与 GMD 已对齐五项最小行为：操作绑定与权限／幂等；预填和普通结果分流；已知原生计数按子请求透传；断连保留、取消等待真实安全接续；新阶段使用独立操作 ID 并保留缓存绑定。GMD 已接受本文三个公开请求头、两条管理路径、在途重试 202 和完成后原结果重放作为实施目标。下文样例据此收敛为已知原生字段，不再把无法证明的 token 摘要和未来复用数量作为必填值。真实公开实现及联合验收尚未完成，不能据此宣称服务已经可用。

## 2. 按模型与运行代次声明能力

在现有 `GET /api/v1/models` 的对应模型行增加以下声明。一个名称同时存在 `llm` 和 `decision` 行时，分别声明两条路径，不把一条路径的支持推给另一条。

```json
{
  "key": "tiel-coder-35b-a3b-apex-i-miniplus-v2-1",
  "type": "llm",
  "source": "ghost-model-deck",
  "capabilities": {
    "prefill": {
      "protocol": "gmd-prefill/1",
      "supported": true,
      "mode": "chat_max_tokens_zero",
      "runtime_generation": "tiel-target-dflash2-run-01"
    }
  }
}
```

JEV 对应行使用 `type: decision` 和 `mode: systemone_state`。不支持时返回 `supported: false` 及稳定的 `reason`，例如 `engine_prefill_unavailable`。能力字段缺失表示未声明，不能推定支持。

`supported: true` 承诺本契约的零输出、真实缓存证据、终态查询与取消协调全部可用。仅支持缓存盐或普通推理时不能声明为真。声明须绑定实际模型、引擎及目标／草稿配置，不按模型名称建立硬编码黑白名单。

`runtime_generation` 是不透明、非空、可用于 HTTP 头的 ASCII 标识，符合 `[A-Za-z0-9._~-]{1,128}`。模型重新加载、路由绑定、tokenizer、聊天模板或草稿配置改变，导致旧缓存不能继续被信任时，必须更换该标识。模型别名相同不能绕过代次变化。未具备整体预填实现的 council 路由不能借成员能力宣布自身支持。

GhostSplash 的 HTTP Job ID 与 RuntimeClient 生成的原生请求 ID 分开映射。若原生代次计数在服务重开后会重置，须再绑定不可复用的服务启动身份；单独的整数代次、PID 或同名模型不足以核验跨重启归属。

所有参与本契约的预填、最终生成和判断请求增加三个公开请求头：

| 请求头 | 含义 |
| --- | --- |
| `Idempotency-Key` | 格式为 `gmd1.<运行代次 SHA-256 的 64 位小写十六进制摘要>.<issued_ms>.<从本操作随机凭证派生的 32 位小写十六进制认证标识>`，总长不超过 128。同一操作重试复用；新材料版本或新阶段使用新 ID。 |
| `X-GMD-Runtime-Generation` | 期望的运行代次，必须与能力声明一致。执行前不一致时返回明确错误，不静默切换引擎。 |
| `X-GMD-Operation-Token` | 调用方为本操作生成并保密的 256 位随机凭证，以 64 个十六进制字符传输。相同操作的提交、查询和取消使用同一凭证，不同操作分别生成。 |

这些请求头是本提案的新增公开契约。随机凭证让调用方在成功响应尚未到达时，也能安全查询或取消自己的请求。GMD 须在首次登记时原子绑定操作 ID、凭证摘要、模型、代次及缓存盐；已登记 ID 的不同凭证不得接管原操作。

认证标识的固定字节编码为：UTF-8 ASCII `gmd-prefill/1:operation-token`（无 BOM、无换行），接单字节 `00`，再接十六进制解码后的 32 字节随机 token；取 SHA-256 输出前 16 字节，再编码为小写十六进制。每个新操作先生成独立的 256 位安全随机凭证，再派生认证标识。凭证不能由 ID 反推；同一操作的重试仍使用原凭证。公开测试向量：`aa` 重复 32 次对应 `faa591af733fea97bd8c09da5bdd24b5`；`bb` 重复 32 次对应 `4146148b74e124330f8b80e7a7fd192c`。这两组固定值只用于测试，真实调用不得使用。

查询和取消须验证凭证及服务原有权限。操作 ID、模型名称和缓存盐均不能单独授予权限。凭证不能由这些值推导，也不进入输入材料、普通日志、诊断、通知或验收证据；服务端保存摘要并使用安全比较。凭证丢失时不能宣称旧请求已完成，仍由真实占用守卫阻止并发接续。

公开凭证不替代服务认证，也不等于 GMD 进程内的 `x-gmd-owned-jev-request` lease（请求占用资格）。GMD 仍须自行创建内部占用资格，再映射公开操作；客户端不能构造私有 lease ID 来取消另一调用。

首次登记的有界默认是过去 10 分钟、未来 1 分钟。GMD 提供安全时间基准和窗口，客户端按该基准加本地单调 elapsed 生成 ID，不依赖可能跳变的本地墙钟。已登记操作先鉴权及核对绑定，再查询或重放，不因年龄变成一次新执行。

公开运行代次另包含每次 GMD 启动不可复用的身份。终态结果至少保留到 ID 接受窗结束与终态后 10 分钟两者的较晚时点；安全回收后，同 ID 只能因过期或代次不符被拒，不能重新登记。运行中和 unknown 不按 TTL 回收；进程内接受时间不回退，时钟失配明确报错。结果只承诺当前进程内有界重放，重开丢失则 unknown，不自动重执行；旧引擎的未知占用守卫独立保留。

## 3. 文本：零预算预填，再生成

以下为缩略合成示例。Sentinel 接入时使用现有分析输入生成器的完整正文，保留角色、来源、引用和预算守卫。

第一次预填提交完整累计输入：

```http
POST /v1/chat/completions
Idempotency-Key: gmd1.392247563fea725bc2c11062880c07021fff8597b744bc2e434c430df58da666.1793157880000.faa591af733fea97bd8c09da5bdd24b5
X-GMD-Runtime-Generation: tiel-target-dflash2-run-01
X-GMD-Operation-Token: <64-hex-random-operation-token>
Content-Type: application/json
```

```json
{
  "model": "tiel-coder-35b-a3b-apex-i-miniplus-v2-1",
  "messages": [
    {"role": "system", "content": "整理执行证据并生成附依据的简短总结。"},
    {"role": "user", "content": "本轮当前有效的完整观察材料 A。"}
  ],
  "max_tokens": 0,
  "stream": false,
  "cache_salt": "sentinel:opaque-scope-7ac1"
}
```

规范要求：

- 显式 `max_tokens: 0` 必须执行输入处理。不能直接返回空成功，也不能继承实例默认正数、改成 `1` 或偷偷发起正常生成。
- 零预算涵盖全部生成 token，包括隐藏推理输出。预填过程中不生成总结、assistant 文本或 reasoning 内容。
- `null`、省略预算和 `0` 保持不同语义。涉及 `max_completion_tokens` 的歧义组合应明确拒绝，不能让别名的正数覆盖显式零预算。本契约请求统一使用 `max_tokens`。
- 预填使用 `stream: false`。零预算配合 `stream: true` 返回明确输入错误；本轮不定义流式预填响应。
- 新材料加入后仍完整提交累计 `messages`，使用同一缓存盐和新的操作 ID。由引擎核验真实 token 公共前缀，不能假定只是尾部追加。

预填完成响应采用以下结构。`choices` 为空，明确区分普通生成。

```json
{
  "id": "chatcmpl-prefill-01",
  "object": "chat.completion",
  "model": "tiel-coder-35b-a3b-apex-i-miniplus-v2-1",
  "choices": [],
  "usage": {"prompt_tokens": 1200, "completion_tokens": 0, "total_tokens": 1200},
  "prefill": {
    "protocol": "gmd-prefill/1",
    "request_id": "gmd1.392247563fea725bc2c11062880c07021fff8597b744bc2e434c430df58da666.1793157880000.faa591af733fea97bd8c09da5bdd24b5",
    "state": "completed",
    "terminal": true,
    "lease_released": true,
    "runtime_generation": "tiel-target-dflash2-run-01",
    "cache_salt_sha256": "<实际缓存盐摘要>",
    "children": [{
      "question_id": null,
      "native_request_id": 42,
      "runtime_generation": 1,
      "state": "length",
      "prompt_tokens": 1200,
      "completion_tokens": 0,
      "matched_prompt_tokens": 900,
      "deduplicated_tokens": 0,
      "lease_released": true
    }],
    "generated_tokens": 0,
    "evidence_source": "engine"
  }
}
```

数值、签发时间和随机字段仅为结构示例，真实调用须按当前时间基准重新生成随机 ID，不能照抄。预填完成必须有至少一个对应原生子请求；最终请求成功、原生已安全释放、所有子请求生成数为零，才可确认预填完成。纯确定性 JEV 没有原生子请求，不能冒充状态预填成功。

`prompt_tokens` 是原生输入长度；`matched_prompt_tokens` 来自 Start，`deduplicated_tokens` 来自 Done，`completion_tokens` 是原生生成数。未知字段省略，不按字符数估算或在未知时填零，也不从 prompt 减 matched 推算实际新计算量。原生整数代次与请求 ID 必须能经公开运行代次关联到不可复用的服务启动身份。

这些计数是目标侧原义。内部重复计算量与草稿侧计算分开记录，不能混加后冒充输入或命中。尚未证明的实际 token 摘要、未来可复用边界和草稿命中不填写；计时或耗时改善不能换算成复用 token 数。

HTTP 200 的完成响应必须等待实际预填完成和缓存提交。收到请求、进入队列、收到响应头或预估可命中，都不满足这个条件。

最终生成使用新的 `Idempotency-Key`，保持相同模型、运行代次、缓存盐和冻结后的完整 `messages`，把 `max_tokens` 改成正数。其余影响输入模板与 KV（键值缓存）的有效配置须一致。输出预算的变化本身不能让预填缓存失效。

正常分析不要求为最终版本再额外发一次零预算请求。后台 A 已预填并安全释放后，可以冻结完整 A＋B，直接以正预算 consume 提交；引擎核对实际前缀，处理未复用输入，再生成。若没有可恢复缓存则处理完整输入，不能保证 A 必然全部命中。这是普通生成内部的输入处理，不是绕过模型资源限制的回退分支。

引擎聊天已报告精确顺序的隔离模拟通过：`testPrefillOnlyThenGenerateWithAdditionalInput` 仅发送 A 零预算及 A＋B 正预算两个请求；2113 个模拟 A token 中恢复 2112 个，只处理剩余 1 个 A token 和 64 个 B token，然后 decode，两个请求均释放 lane/cache request。该项是原生 Engine＋模拟 Executor，直接输入 token；不覆盖 HTTP、真实 tokenizer／模板、Metal、草稿活动或 Tiel＋DFlash2。本项目未把它记作真实模型验收。

最终响应保留现有 Chat Completions 结构，并在本契约请求中附带第 5 节的 `cache` 证据。Sentinel 仍要求正常生成完整结束、内容非空，并通过现有结构与引用校验；零预算响应不能进入总结发布路径。

GMD 与 Sentinel 须分别校验预填和普通生成。不能为了接受零输出预填，放松正数预算请求的非空与完整结束校验。

## 4. JEV：预填累计 state，再提交问题

预填使用同样的三个请求头，模型必须明确声明支持 `systemone_state`：

```json
{
  "model": "supported-jev-model",
  "state": {
    "request": "只修改按钮文案",
    "events": [{"id": "result-1", "text": "已更新按钮文字"}]
  },
  "prefill_only": true,
  "stream": false,
  "cache_salt": "sentinel:opaque-jev-scope-91b2"
}
```

`prefill_only: true` 时允许缺少 `questions` 或使用空对象；不解析或执行判断题。若同时提供非空问题，明确拒绝，避免悄悄做判断。`state` 仍须满足模型与原生引擎实际支持的类型，不能为了接入把不支持的类型擅自转成字符串。

响应为 `{"object":"systemone.prefill","model":"supported-jev-model","prefill":{...}}`。`prefill` 使用第 3 节的公共完成字段，`generated_tokens` 为零。响应不包含伪造的 `answers`、`choice` 或概率；预填不算 JEV 判断完成。

累计 `state` 更新时完整重发新版本，使用同盐、新操作 ID。最终判断使用相同冻结状态，省略 `prefill_only` 或设为 `false`，再加入正常问题：

```json
{
  "model": "supported-jev-model",
  "state": {
    "request": "只修改按钮文案",
    "events": [{"id": "result-1", "text": "已更新按钮文字"}]
  },
  "questions": {
    "request_drift": {
      "type": "choice",
      "instructions": "依据 state 核对执行是否偏离当前请求。",
      "criteria": {"clear": "符合请求", "attention": "有依据的偏离", "unknown": "依据不足"}
    }
  },
  "stream": false,
  "cache_salt": "sentinel:opaque-jev-scope-91b2"
}
```

普通问题继续沿用现有题型、数量和校验规则，指令保留在题内。最终响应保留原有 `answers`，并附带本契约的缓存证据。

GhostSplash 必须保证状态预填得到的可复用前缀，能用于后续“同一状态＋问题”的判断。若实际模板把问题插在状态之前，或末尾标记不稳定，须在适配中解决并以真实计数证明；不能把重复处理全部状态称为复用。

## 5. 缓存绑定与输入变化

`cache_salt` 是隔离命名空间，不升级为隐式可变会话或服务端追加游标。每次请求的完整输入才是事实来源，本轮不要求新增上下文句柄。

Sentinel 按安装、角色、任务、执行代次、当前请求锚点及模型／渲染配置生成不透明缓存盐。正式版、test、Codex、DSH 和不同代理分别隔离。盐中不写聊天正文。普通材料追加保持同盐；材料版本通过操作 ID 与请求摘要单独绑定，不能每次新增输出就换盐而失去复用机会。

每个操作 ID 在 Sentinel 本地绑定一份固定材料版本。GMD 对同 ID 校验模型、代次、盐和有效参数摘要；同 ID 不同输入返回冲突。不得把旧版本响应归给新的材料版本。

当前 Sentinel 的事件重排、过期、引用编号和时间渲染都可能修改旧输入。因此引擎必须对实际模板和 tokenizer 结果核对公共前缀，只复用匹配的 KV，并正确裁去旧尾部。失配时安全重算或返回可识别错误，不盲目追加，也不保留本轮已移除的旧材料作为隐藏上下文。

最终生成／判断为本契约调用时，响应附带：

```json
{
  "cache": {
    "protocol": "gmd-prefill/1",
    "request_id": "gmd1.392247563fea725bc2c11062880c07021fff8597b744bc2e434c430df58da666.1793157880100.4146148b74e124330f8b80e7a7fd192c",
    "runtime_generation": "tiel-target-dflash2-run-01",
    "cache_salt_sha256": "<实际缓存盐摘要>",
    "terminal": true,
    "lease_released": true,
    "children": [{
      "question_id": null,
      "native_request_id": 43,
      "runtime_generation": 1,
      "state": "stop",
      "prompt_tokens": 1200,
      "completion_tokens": 80,
      "matched_prompt_tokens": 1200,
      "deduplicated_tokens": 0,
      "lease_released": true
    }],
    "evidence_source": "engine"
  }
}
```

缓存驱逐、代次变化或前缀改写可能造成 `miss`，须返回真实原因。安全全量处理后，普通结果仍可合法完成；不能为了追求命中反复重跑并无限延后结果。但 `miss` 不能充当联合验收中的命中正例。

客户端可以把真实目标匹配归纳为全部命中、部分命中、未命中或未确认，不能把这种归纳当作新的原生计数。验收核对实际复用范围，不强求所有请求都全部命中。JEV 的各个原生子请求按题目分别保留，不相加冒充单份状态前缀。

Tiel Coder 配合 DFlash2 时，目标模型与草稿模型的缓存、位置和回退必须保持一致。只在关闭 DFlash2 时成功，不能算该组合通过。记录须说明缓存计数属于哪一层；未知的草稿侧信息不能写成已命中。

## 6. 在途、取消与终态查询

同一模型运行代次、同一缓存盐上的预填和消费该缓存的生成／判断必须串行。前一计算未释放占用资格时，不接受会修改同一缓存的新操作。其他命名空间仍受引擎真实容量限制，不能靠换盐绕过占用限制。

协作中的最小边界：释放须证明当前操作不再修改该缓存及模型状态，允许下一操作安全顺序接续；不要求等待无关的全引擎后台工作或 SSD 持久化落盘。缓存提交指后续请求可复用的有效状态，不承诺跨重启持久化。已核对的 GhostSplash 基线在 `Engine::finish/finishFailure` 中先发送终态、再执行 `release`；收到旧终态不能直接映射为 `lease_released: true`。

GhostSplash 当前工作区原型把 Python／C++ 原生协议一同提升到 12，规定终态晚于当前请求的 lane/cache release，旧二进制不能与新服务握手。内部原型见[接口说明](/Users/ghost233/Ghost233Code/GhostSplash/docs/incremental-prefill-internal.md)：`/v1/splash/runtime` 提供服务启动身份及已就绪代次，带凭证的操作查询／取消提供原生子请求映射；未知状态保持未释放。这是未提交、未部署的原型，不是 GMD 已公开支持；真实二进制、Tiel＋DFlash2 和联合验收仍待验证。

公开操作 ID、权限凭证和幂等注册由 GMD 层负责。原生引擎提供可关联的原生请求、运行代次及真实证据，不要求再复制一套公开操作注册系统。

本提案要求 GMD 增加以下公开管理操作，覆盖上述两条路径及最终生成／判断：

| 操作 | 语义 |
| --- | --- |
| `GET /api/v1/inference/requests/{request_id}` | 只读查询公开操作 ID 的状态、绑定、终态及占用是否释放；不返回原始输入正文。 |
| `POST /api/v1/inference/requests/{request_id}/cancel` | 幂等请求取消。可立即结束，也可返回取消协调中；不能先报已释放、让旧计算继续占用缓存。 |

两处的 `request_id` 对应 `Idempotency-Key`，不使用进程内私有 lease ID。查询、取消和幂等重放均须携带并校验 `X-GMD-Operation-Token`，再沿用服务原有认证与权限。未授权时不返回原操作的绑定或结果；操作 ID 不是取消凭证。

管理请求使用同一回环基地址与上表固定路径，不跟随外部查询地址或重定向。查询和取消按原操作记录的模型、运行代次及占用处理，不因当前别名已切换就操作新的模型实例。

公开状态区分运行中、取消协调中、完成、取消、失败和未知，运行中可细分排队、预填、生成与判断。状态名称与具体响应样例须由两端实现一致；是否可接续仍同时核对 `terminal`、`lease_released`、模型、代次及盐摘要，不能只看一个状态名称。

规范要求：

- 首次调用注册操作，再交给引擎。相同 ID、同凭证、同绑定与输入的在途重试返回 HTTP 202 及状态，不创建第二次执行；已完成的相同 POST 重放所保存的原结果。最终生成或判断断连后也须按此恢复，不能重新执行来代替结果重放。
- HTTP 断开、客户端超时、收到取消响应，都不单独证明引擎已经结束。未确认时返回非终态，`lease_released: false`，继续保留真实占用。
- 只有确认终态且占用释放后，Sentinel 才向同一缓存提交下一请求。等待采用异步完成事件或有界退避查询，不阻塞采集、诊断、界面和必要的时效检查。
- 取消中可能已经处理部分输入。GMD 必须说明最终提交边界；未完整处理本次输入时，不返回 `prefill.state: completed`。部分缓存仅在重新核对实际前缀后复用。
- 客户端不因失联盲目重放生成或判断。先查原操作；晚到结果仍绑定原材料与配置版本，不能恢复已取消的分析或通知资格。
- 结果记录可有界留存，但在途操作不能仅因查询记录到期就当作结束。HTTP 404 不能证明没有执行；HTTP 410 若无法确认原计算已结束，也不能允许客户端直接接续。
- 留存期限和登记容量须同时覆盖持续预填与最终生成／判断。当前 Splash 原型的 4096 条、结束后至少保留 1 小时，是待评估的容量边界，不能当作长期负载已通过；预填不得填满登记表后挤掉最终生成，unknown 不能为腾位置而被视为释放。
- Splash 内部原型已增加带原凭证的 `DELETE /v1/splash/operations/{id}`：GMD 先保管完整安全终态与原生证据，再确认回收内部记录。进行中或 unknown 拒绝回收。GMD 自己的公开幂等结果仍须有界保存，永不重用已回收操作 ID；内部回收后的 404 不覆盖先前已经保存的终态证据。此内部回收仍待定点结果，不新增 Sentinel 的公开删除操作。
- 内部原型使用 `X-Splash-Operation-Kind: prefill|consume` 预留容量，并在正文解析后、原生提交前核对实际类型。当前预填最多 3584 条、总计 4096 条，消费请求可使用其余容量；伪称消费不能绕过预填上限。消费自身的未确认记录耗尽剩余容量时仍明确背压，不伪造释放。来源聊天已报告相关受控测试通过；这不等于本项目长期负载或真实模型验收通过。
- GMD 或引擎重启后，旧结果可能无法恢复。必须明确代次变化及计算是否仍未知；不能仅凭 GMD 重开就认定旧引擎空闲。旧引擎已确实退出时，才可确认对应占用已释放，并让客户端重建缓存绑定。

当前尚无跨 GMD 崩溃验证旧 Python、native 及后代全部退出的持久证明，也没有自动解除这种未知的实现。代次变更、PID／PGID 不存在、EOF、404 或锁可获取均不单独证明释放。未来“结果仍未知但占用已证实解除”的独立恢复语义尚在设计；当前客户端不接受 `terminal: false, lease_released: true` 作为放行依据。

失败查询的可选 `error` 字段须使用稳定 code、stage 和脱敏通用信息。客户端容忍未识别的错误字段，不据此改写终态或推造原因；404／unknown 不伪造失败原因。

GMD 已用隔离控制面 HTTP 回归冻结两类未提交证明，Sentinel 已接入解析。共同必填字段如下；它们只证明本次操作未进入原生执行，不释放同盐的其它操作，也不触发冷回退：

```json
{
  "request_id": "<本次操作 ID>",
  "runtime_generation": "<本次绑定代次>",
  "state": "rejected",
  "terminal": true,
  "lease_released": true,
  "native_dispatch": "not_started",
  "proof_scope": "identity_only",
  "error": { "code": "not_admitted", "stage": "admission" }
}
```

| 证明范围 | 绑定要求 |
| --- | --- |
| `identity_only` | 核对本次 ID、凭证对应的 ID 和运行代次；必须省略 `model` 与 `cache_salt_sha256`，不能填空值后宣称完整绑定。 |
| `bound` | 在上述身份外，必须提供并匹配 `model` 和 `cache_salt_sha256`；`children` 可省略或为空数组，不能同时带原生子请求。 |

直接容量拒绝使用 HTTP 503 和 `capacity_unavailable`；无效输入使用 HTTP 400 和 `invalid_prefill_request`；同实例同盐正忙使用 HTTP 409 和 `prefill_context_busy`。水位恢复的 GET 或登记前取消可能只知道 `not_admitted`，不能据此还原原来的具体拒绝原因。客户端不复制任意提供方错误正文，保留稳定代码；有效拒绝不是通信故障，之前确有断连的记录仍独立保留。

已有登记操作先按原凭证和绑定处理；代次、凭证、保留窗口或持久化状态不可信时，GMD 不得构造此证明。任意 HTTP 错误、404、缺字段或矛盾字段都不单独解除占用。登记后、原生派发前的取消可返回 `cancelled`、完整绑定、`native_dispatch: not_started`、`proof_scope: bound` 与空 `children`；不能冒充原生子请求。迟到的查询失败不能覆盖已经核实的终态与释放。

## 7. 错误与旧版本

新契约错误须提供稳定 `error.code` 和可读 `message`。Chat 路径可保留原有 `error.type`；不要假定现有 Chat 与 JEV 的错误对象已经相同。

| HTTP | 建议错误码 | 客户端处理 |
| --- | --- | --- |
| 400 | `invalid_prefill_request` | 输入、标志或预算组合不合法；修正输入，不重试成正预算。 |
| 404 | `model_not_found` | 模型或路由不可见；不当作能力已验证。 |
| 403 | `operation_forbidden` | 操作凭证或服务权限不匹配；不泄露另一操作，不接管或取消它。 |
| 409 | `prefill_context_busy` | 同一缓存仍被占用，查原操作或按退避等待。 |
| 409 | `idempotency_conflict` | 同一操作 ID 对应不同输入，拒绝覆盖。 |
| 409 | `runtime_generation_changed` | 原绑定失效，重新发现能力与建立上下文。 |
| 409／499 | `cancelled` | 保留路径原有取消 HTTP 状态；是否终态与是否释放仍看操作状态。 |
| 429 | `capacity_unavailable` | 模型容量不足，按服务给出的重试提示排队；不忙等。 |
| 501 | `prefill_not_supported` | 当前模型／引擎／草稿组合明确不支持。 |
| 503 | `prefill_capability_unconfirmed` | 服务还不能确认真实预填和缓存证据。 |
| 504 | `timed_out`／`upstream_timeout` | 请求超时；计算可能尚未结束，先协调取消和查询终态。 |

未升级的 GMD 可能仍返回原有的零预算校验错误或未知字段错误。Sentinel 应显示旧协议未确认；不能把它改写成缓存命中。升级后的 GMD 面对不支持的旧引擎，须明确返回 501，不能静默忽略参数、改发普通生成或伪造零输出。

D3 lite／mini 当前报告的不支持情况保留在能力与验收矩阵中，不能用其普通判断可用来替代预填支持。未来某个新组合若支持，须记录新版本与独立证据，再更新矩阵。

## 8. Sentinel 接入边界

- 请求仍受现有材料来源、授权、输入预算和引用校验约束；预填不能多读工作区，也不扩充采集范围。
- 模型处理中继续采集，待处理请求合并为每代理最新完整版本。后台预填 A 结束并确认释放后，真正取得完整分析机会时冻结 A＋B，直接提交正预算生成；原生引擎处理未命中的输入，再生成和判断，不增加一次 A＋B 零预算请求。后续材料留待后续轮次。
- 正常完整分析完成后冷却 60 秒；条件未变时复用有效结果。预填完成不启动新的冷却，也不代表六项检查通过。
- 完整分析优先于尚未开始的预填，各代理的完整分析仍公平轮流；仅在没有完整分析待处理时预填。已经开始的预填自然结束，不为切换而强行中断。
- 本轮总结与 JEV 实际调用成功、输出及引用合法后，才发布完整新总结。服务在线、模型可见、能力声明和缓存命中都不能替代它。
- Core 的资料维护、过期、租约和通知资格检查独立推进。无模型配置也须降低空转开销；不以降低扫描频率掩盖重复全量扫描，也不等待模型结束才响应控制请求。
- 持续预填的容量和缓存数量上限继续明确；优先级按已确认的完整分析优先执行。不得无限保留所有历史版本，或让后台预填长期挤占已经符合条件的生成／判断。

Sentinel 的[客户端](../src/analysis/gmd-prefill-client.mjs)与[生产分析器](../src/analysis/index.mjs)已完成受控接线：按安装、任务、执行、当前请求、角色、服务、模型及指令版本隔离缓存；请求字节及路由绑定固定；未知与取消继续异步核对，状态回调不释放资格。取消接口的 HTTP 202 仅表示已接受取消，仍须取得终态与释放证据。正常生成保留完整结束、结构和引用校验。缺少当前请求依据时不借用历史输入，不发布完整新总结。预填只更新自己的进度及已核对回执，不重置完整分析冷却。

未声明支持协议的服务继续使用原普通分析路径；已开始的受管操作失败时不换 ID、不改发非受管请求。Core 关闭可终止自身网络和定时器，但不证明服务的原生占用已释放。当前 MCP 初始化不依赖此客户端联网。

[生产链回归](../test/analysis-gmd-production.test.mjs)使用本地合成 HTTP 服务和隔离 Core，覆盖 A→A＋B 正常顺序、在途采集、取消封存、安装／角色隔离、双模型校验和回执来源。它不证明真实缓存命中，也不替代 GMD 公开 POST、同实例同盐协调、终态重放及真实模型联合验收；当前服务尚未因此启用真实预填。

2026-10-28 跨仓受控互验使用 GMD 的 `test/support/prefill_gateway_fixture_test.dart`：实际运行 GMD PublicGateway、登记及响应转换，替代原生进程与结果。Sentinel 的生产 `createAnalyzer` 完成 A 零预算预填→A＋B 正预算生成→JEV→附依据总结，三次模型请求使用独立操作 ID，文本预填与生成同盐，JEV 另盐；没有额外的 A＋B 零预算请求。随后通过同盐、新操作 ID 的 JEV 状态预填与问题判断。Node 与 Flutter fixture 均退出 0，服务通过其关闭接口正常收尾。证据保存在本地 `.scratch/prefill-maintenance-20261028/gmd-cross-project-validation.json`。模型、原生事件及命中计数均为模拟；此次通过不证明真实缓存复用、GPU、草稿模型或生产主应用装配，当前运行服务保持不变。

未提交证明解析还对同一 GMD fixture 做了一次合成无效消息请求：实际收到 HTTP 400 的 `identity_only` 证明，客户端结束本次请求并保留 `invalid_prefill_request`，未改发生成；Node 与 fixture 均退出 0 并正常关闭。证据在本地 `.scratch/prefill-maintenance-20261028/gmd-no-admission-cross-project-validation.json`。绑定证明、断连后恢复、矛盾字段及取消竞态另有客户端受控回归；不以这条输入拒绝记录冒充真实原生取消验收。

## 9. 联合验收与证据

联合验收在 GhostSplash 适配合入、GMD 和 Sentinel 接入后安排。本次文档交接不执行以下请求，不启动额外服务或更换用户模型。

| 用例 | 必需证据 |
| --- | --- |
| Tiel Coder＋DFlash2 冷预填 A | 零输出请求、引擎实际输入处理、完整完成响应、`generated_tokens: 0` 与释放证明。 |
| 同盐累计输入 A＋B 再预填 | 新操作 ID、原生 `matched_prompt_tokens > 0` 且范围进入 A 的材料；输入长度、命中与去重分别保留，不推算实际新计算量。 |
| 同一冻结输入最终生成 | 正数预算、完整非空生成结果、复用已提交前缀的引擎证据；DFlash2 保持启用。 |
| 支持的 JEV 状态预填 | 先预填状态 A，再以同盐、新操作 ID 预填累计状态 A＋B；两次均零输出、无答案或概率、明确完成，第二次的原生命中范围进入实际状态材料。 |
| 同状态提交 JEV 问题 | 对冻结的 A＋B 提交问题，验证实际前缀复用、正常 `answers`、支持的题型与结构合法；保留原始判断响应。 |
| 前缀改写与过期 | 输入不再是纯追加时安全裁剪或重算，不继续使用已经移除的上下文。 |
| 隔离 | 同输入不同安装／代理／角色盐不混用可变上下文；运行代次、模型或草稿变更不能沿用旧绑定。 |
| 取消、断连与晚到 | 控制面持续响应；同缓存不重入；计算终态、占用释放和旧结果封存均可追溯。 |
| 幂等重试 | 同 ID 不重复执行，不同输入复用 ID 被拒。 |
| 操作所有权 | 缺少或使用错误凭证时，不能查询、重放或取消另一调用；凭证不得出现在证据记录。 |
| D3 lite／mini 与旧引擎 | 保留真实不支持响应；无隐藏生成、无虚构命中，不影响它们原有的普通调用能力。 |
| 无模型与无新材料 | Core 不持续重扫全部历史；读取诊断、采集、暂停与必要的时效更新仍能响应。 |

命中正例在同一实际运行代次、未驱逐缓存的受控窗口内执行。累计输入后的最终生成／判断，须复用已经提交且声明可复用的前缀。驱逐造成的安全 miss 单独记录，不算该条命中正例通过。

每项记录至少包括：三项目源码／运行版本、GhostSplash 适配合入提交、模型和量化、目标与草稿实际绑定、脱敏后的请求与响应、操作 ID、材料版本映射、盐摘要、运行代次、输入摘要、引擎实际缓存计数、终态与释放、最终结果及结构校验。

命中须来自引擎真实缓存计数，或可直接证明复用范围的等价证据。若使用 `cached_tokens`，须说明原生字段与本契约的映射。单靠内部计时、请求较快或 GMD 自报 `hit` 都不足以通过。还须能把公开操作 ID 对应到原生请求或日志，不泄露私有 lease 能力或公开操作凭证。

累计 A→A＋B 的命中正例还须证明复用范围进入 A 的实际材料；JEV 须进入 `state`。仅命中 BOS 或固定系统模板，不算累计材料预填复用通过。可通过离线模板边界和原生 trace 核对，不要求公开聊天 token 或填造输入摘要字段。

最终结果须能审阅，不以“接口返回 200”替代。沿用已有的完整响应、合法结构和引用校验，不把模型准确率、GPU 拥堵或固定推理速度新增为本轮门槛。已有合成输入和未受影响的证据优先复用；必要的新路径在一个有界联合窗口内集中验证。

## 10. 交接完成条件

GMD 完成已对齐的公开提交、管理与重放链，并提供最终错误与拒绝证明格式、逐组合能力及受控接口证据；GhostSplash 提供适配合入的当前版本及能力证据。Sentinel 已按已确认部分实施生产接线，剩余格式确定后补齐相应解析，再进行三方公开链与真实模型联合验证。

产品验收仍需关闭真实预填和 Core CPU／响应问题。用户本次要求入口接通、开启后发布，由其下载进行真实测试；发布公开保留这些未验项，不将其改记为通过。普通生成可用、模型列表可见、单边模拟测试或历史引擎成功，都不能替代对应产品验收。
