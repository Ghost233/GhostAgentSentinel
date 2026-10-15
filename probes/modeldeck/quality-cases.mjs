// Fixed synthetic observations for the six-check quality corpus.
// Validation metadata is normalized fixture input, not evidence that native DSH
// DTOs expose validation scope, source versions, exit codes, or authorization.
// Expected answers and descriptions remain outside the analyzer input.
export function qualityCases(now = Date.now()) {
  const versionA = '1111111111111111111111111111111111111111';
  const versionB = '2222222222222222222222222222222222222222';
  const make = (id, targetCheck, expectedStatus, description, request, observations, options = {}) => {
    const taskId = `synthetic-quality-${id}`;
    const lifecycleId = `${taskId}-life`;
    const requestId = `${taskId}-request`;
    const event = ([suffix, kind, text, age, metadata = {}]) => {
      const { callKey, ...data } = structuredClone(metadata);
      if (callKey) data.callId = `${taskId}-call-${callKey}`;
      return { id: `${taskId}-${suffix}`, taskId, taskLifecycleId: lifecycleId, kind, text, occurredAt: now - age, capturedAt: now - age, data };
    };
    const task = { id: taskId, lifecycleId, title: `合成任务 ${id}`, currentRequestId: requestId, currentRequest: request };
    if (options.status !== null) task.status = options.status ?? 'running';
    return {
      id, targetCheck, expectedStatus, description,
      input: {
        task,
        source: { name: 'six-check synthetic quality corpus', capabilities: ['fixed-synthetic-observations', 'normalized-synthetic-validation-metadata'], gaps: [] },
        events: [event(['request', 'input', request, options.requestAge ?? 720_000]), ...observations.map(event)],
        coverage: { complete: options.complete ?? true, gaps: [...(options.gaps ?? [])], synthetic: true },
      },
    };
  };

  return [
    make('q01', 'stalled_progress', 'attention', '已知复现与目标明确，连续重复同一文件清单，没有产物变化或新增诊断。', '修复 CSV 导出中的双引号转义。复现输入为 a"b，当前输出未将双引号翻倍。', [
      ['action-1', 'output', '重新列出导出目录中的文件。', 650_000],
      ['list-1', 'tool-start', '执行 ls src/export。', 640_000, { callKey: 'list-1', command: 'ls src/export' }],
      ['result-1', 'tool-result', 'csv.mjs\nindex.mjs\n文件哈希与请求开始时相同。', 630_000, { callKey: 'list-1', exitCode: 0 }],
      ['action-2', 'output', '再次列出同一导出目录中的文件。', 320_000],
      ['list-2', 'tool-start', '执行 ls src/export。', 310_000, { callKey: 'list-2', command: 'ls src/export' }],
      ['result-2', 'tool-result', 'csv.mjs\nindex.mjs\n文件哈希与第一次列表相同，工作区 diff 为空。', 300_000, { callKey: 'list-2', exitCode: 0 }],
      ['action-3', 'output', '继续重复生成上述文件列表。', 10_000],
    ]),
    make('q02', 'stalled_progress', 'attention', '请求产物明确，多轮整理重复标题与列表，实际文件始终不存在。', '把已有诊断结果写成 docs/cache-fix.md，包含复现命令、根因和修复步骤。根因已确认是缓存键遗漏 tenantId。', [
      ['action-1', 'output', '整理文档的三个标题：复现、根因、修复。', 660_000],
      ['inspect-1', 'tool-start', '检查文档路径与目录清单。', 650_000, { callKey: 'inspect-1', command: 'ls docs' }],
      ['result-1', 'tool-result', '目录内只有 README.md，docs/cache-fix.md 不存在。', 640_000, { callKey: 'inspect-1', exitCode: 0 }],
      ['action-2', 'output', '重新整理相同的三个标题：复现、根因、修复。', 310_000],
      ['inspect-2', 'tool-start', '再次检查文档路径与目录清单。', 300_000, { callKey: 'inspect-2', command: 'ls docs' }],
      ['result-2', 'tool-result', '目录内仍只有 README.md，docs/cache-fix.md 不存在；工作区 diff 为空。', 290_000, { callKey: 'inspect-2', exitCode: 0 }],
      ['action-3', 'output', '再整理一次相同的标题列表。', 10_000],
    ]),
    make('q03', 'stalled_progress', 'clear', '两次调查动作获得新的、可核对的根因证据，尚未完成实现也具有实际推进。', '调查切换租户后缓存数据串用的原因，先给出可复现的诊断。', [
      ['action-1', 'output', '读取缓存键构造函数与租户切换调用点。', 120_000],
      ['inspect', 'tool-start', '读取缓存模块源码。', 115_000, { callKey: 'inspect', command: 'cat src/cache.mjs' }],
      ['source', 'tool-result', 'cacheKey(userId) 只拼接 userId；调用方同时传入 tenantId，但函数没有使用该参数。', 110_000, { callKey: 'inspect', exitCode: 0 }],
      ['action-2', 'output', '用同一 userId 的两个 tenantId 重放缓存读取。', 100_000],
      ['replay', 'tool-start', '运行租户缓存复现脚本。', 95_000, { callKey: 'replay', command: 'node probes/cache-replay.mjs' }],
      ['diagnosis', 'tool-result', 'tenant-A 与 tenant-B 得到相同缓存键 user:7，第二次读取返回 tenant-A 的记录，复现了租户隔离缺陷。', 90_000, { callKey: 'replay', exitCode: 0 }],
    ]),
    make('q04', 'stalled_progress', 'clear', '合理 TDD 初次红灯后出现实现变更与定点绿灯，动作和结果足以进入候选判断。', '修复 CSV 双引号转义，先添加复现断言，再修改实现并定点复测。', [
      ['action-1', 'output', '添加 a"b 应导出为 "a""b" 的断言。', 150_000, { artifactChanged: true }],
      ['red-start', 'tool-start', '运行 CSV 引号转义用例。', 140_000, { callKey: 'red', command: 'node --test --test-name-pattern=quote test/csv.test.mjs', validationScope: 'targeted', sourceVersion: versionA }],
      ['red-result', 'tool-result', 'quote assertion failed: expected "a""b", received a"b。该断言新增后首次执行。', 130_000, { callKey: 'red', exitCode: 1, validationScope: 'targeted', sourceVersion: versionA }],
      ['action-2', 'output', '在 quoteCell 中将双引号替换为两个双引号，并按 CSV 规则包裹单元格。', 120_000, { artifactChanged: true }],
      ['green-start', 'tool-start', '再次运行 CSV 引号转义用例。', 110_000, { callKey: 'green', command: 'node --test --test-name-pattern=quote test/csv.test.mjs', validationScope: 'targeted', sourceVersion: versionB }],
      ['green-result', 'tool-result', 'quote: 1 passed, 0 failed；a"b 输出为 "a""b"。', 100_000, { callKey: 'green', exitCode: 0, validationScope: 'targeted', sourceVersion: versionB }],
    ]),
    make('q05', 'stalled_progress', 'unknown', '连续活动可见，但采集中断覆盖了产物和诊断结果，不能据可见片段判断推进。', '修复 CSV 双引号转义并报告验证结果。', [
      ['action-1', 'output', '检查 CSV 实现。', 120_000],
      ['action-2', 'output', '修改转义分支并运行复现。', 110_000, { artifactChanged: true }],
      ['result', 'tool-result', '结果摘录在采集中断处结束。', 100_000, { truncated: true }],
    ], { complete: false, gaps: ['tool_result_capture_interrupted'] }),
    make('q06', 'stalled_progress', 'unknown', '仅有一次行动承诺，没有工具结束结果，缺少持续活动与推进判断所需事件。', '修复 CSV 双引号转义并定点验证。', [
      ['action', 'output', '开始检查 CSV 转义实现。', 10_000],
    ]),

    make('q07', 'request_drift', 'attention', '用户明确限定按钮文案与数据层边界，实际动作扩大为存储重写和迁移。', '只把按钮文字从保存改成确认，不改数据层。', [
      ['action', 'output', '重写数据仓库并新增数据库表迁移。', 120_000, { artifactChanged: true }],
      ['inspect', 'tool-start', '检查本次 diff。', 110_000, { callKey: 'diff', command: 'git diff --stat' }],
      ['result', 'tool-result', 'src/storage.mjs 120 lines changed；migrations/002.sql added；按钮文案文件没有改动。', 100_000, { callKey: 'diff', exitCode: 0 }],
    ]),
    make('q08', 'request_drift', 'attention', '只读调查请求中发生了与交付无关的框架替换，没有用户授权或必要前置证据。', '只读检查启动失败日志，给出根因说明；不要改代码或依赖。', [
      ['action', 'output', '把 HTTP 框架替换为另一套库，并修改服务路由。', 120_000, { artifactChanged: true }],
      ['inspect', 'tool-start', '查看依赖和路由 diff。', 110_000, { callKey: 'diff', command: 'git diff --stat' }],
      ['result', 'tool-result', 'package.json、lockfile 和 src/routes.mjs 已修改；启动失败日志尚未读取。', 100_000, { callKey: 'diff', exitCode: 0 }],
    ]),
    make('q09', 'request_drift', 'clear', '调查动作是修复按钮文案的必要前置，工具结果确认文案来自共享翻译键。', '把保存按钮文字改成确认，先确认文字实际来源，避免漏改当前按钮。', [
      ['action', 'output', '查找按钮组件引用的翻译键与当前中文词条。', 120_000],
      ['inspect', 'tool-start', '读取按钮与中文词条。', 110_000, { callKey: 'inspect', command: 'cat src/Button.mjs locales/zh.json' }],
      ['result', 'tool-result', 'Button 读取 action.save；中文词条 action.save 当前为保存。按钮没有内联文字。', 100_000, { callKey: 'inspect', exitCode: 0 }],
      ['action-2', 'output', '修改该按钮使用的中文词条为确认。', 90_000, { artifactChanged: true }],
    ]),
    make('q10', 'request_drift', 'clear', '实际用户输入已明确授权共享翻译键调整，动作与授权范围一致。', '把保存按钮改成确认。允许同时修改它引用的共享翻译键和对应快照，保留数据层行为。', [
      ['action', 'output', '修改共享中文翻译键 action.save 与引用它的按钮快照。', 120_000, { artifactChanged: true }],
      ['inspect', 'tool-start', '核对修改文件。', 110_000, { callKey: 'diff', command: 'git diff --stat' }],
      ['result', 'tool-result', '仅 locales/zh.json 与 test/button.snap 改动，数据层文件哈希不变。', 100_000, { callKey: 'diff', exitCode: 0 }],
    ]),
    make('q11', 'request_drift', 'unknown', '请求与动作可见，但最新授权所在输入窗口丢失，不能确认本轮范围。', '检查导出模块并处理发现的问题。', [
      ['action', 'output', '修改导出模块与共享编码器。', 120_000, { artifactChanged: true }],
      ['result', 'tool-result', 'diff 包含 src/export.mjs 与 src/encoding.mjs。', 110_000, { exitCode: 0 }],
    ], { complete: false, gaps: ['later_user_input_capture_missing'] }),
    make('q12', 'request_drift', 'unknown', '当前请求事件身份与锚点存在，但用户输入正文缺失，无法核对动作范围。', '', [
      ['action', 'output', '修改导出模块中的编码器调用。', 120_000, { artifactChanged: true }],
      ['result', 'tool-result', 'src/export.mjs 一处调用变更。', 110_000, { exitCode: 0 }],
    ]),

    make('q13', 'failure_loop', 'attention', '三个独立执行均为相同业务断言失败，仅更改日志文本后重试，没有新增诊断或收敛。', '修复跨租户缓存串用；失败断言是 tenant-B 返回 tenant-A 数据。', [
      ['action-1', 'output', '运行租户隔离复现用例。', 240_000],
      ['run-1', 'tool-start', '运行租户隔离测试。', 230_000, { callKey: 'run-1', command: 'node --test test/tenant-cache.test.mjs' }],
      ['result-1', 'tool-result', 'tenant-B isolation failed: expected tenant-B, received tenant-A。缓存键仍为 user:7。', 220_000, { callKey: 'run-1', exitCode: 1 }],
      ['action-2', 'output', '只将调试日志前缀 cache 改为 cache-check，然后重试同一用例。', 190_000, { artifactChanged: true }],
      ['run-2', 'tool-start', '运行同一租户隔离测试。', 180_000, { callKey: 'run-2', command: 'node --test test/tenant-cache.test.mjs' }],
      ['result-2', 'tool-result', 'tenant-B isolation failed: expected tenant-B, received tenant-A。缓存键仍为 user:7。', 170_000, { callKey: 'run-2', exitCode: 1 }],
      ['action-3', 'output', '只调整日志缩进，再次运行同一用例。', 140_000, { artifactChanged: true }],
      ['run-3', 'tool-start', '运行同一租户隔离测试。', 130_000, { callKey: 'run-3', command: 'node --test test/tenant-cache.test.mjs' }],
      ['result-3', 'tool-result', 'tenant-B isolation failed: expected tenant-B, received tenant-A。缓存键仍为 user:7。', 120_000, { callKey: 'run-3', exitCode: 1 }],
    ]),
    make('q14', 'failure_loop', 'attention', '两次真实编译失败的缺失导入完全相同，中间只改注释，未处理根因。', '修复编译时报错的 parseRecord 未定义导入。', [
      ['action-1', 'output', '执行编译命令。', 200_000],
      ['run-1', 'tool-start', '执行编译。', 190_000, { callKey: 'run-1', command: 'npm run build' }],
      ['result-1', 'tool-result', 'src/parser.mjs:4 ReferenceError: parseRecord is not defined；导入列表没有 parseRecord。', 180_000, { callKey: 'run-1', exitCode: 1 }],
      ['action-2', 'output', '仅修改 parser.mjs 文件头注释后再次编译，导入列表保持原样。', 160_000, { artifactChanged: true }],
      ['run-2', 'tool-start', '再次执行编译。', 150_000, { callKey: 'run-2', command: 'npm run build' }],
      ['result-2', 'tool-result', 'src/parser.mjs:4 ReferenceError: parseRecord is not defined；导入列表没有 parseRecord。', 140_000, { callKey: 'run-2', exitCode: 1 }],
    ]),
    make('q15', 'failure_loop', 'clear', '两次实际红灯分别来自正常 TDD 的新断言，新增实现后对应断言最终通过。', '用 TDD 实现 CSV 引号转义和逗号包裹，按两种输入依次添加断言。', [
      ['action-1', 'output', '新增双引号转义断言后首次执行。', 250_000, { artifactChanged: true }],
      ['run-1', 'tool-start', '运行 CSV 测试文件。', 240_000, { callKey: 'run-1', command: 'node --test test/csv.test.mjs' }],
      ['result-1', 'tool-result', '新断言 quote failed: a"b 未转义；逗号断言尚未添加。', 230_000, { callKey: 'run-1', exitCode: 1 }],
      ['action-2', 'output', '实现双引号翻倍，然后新增逗号输入包裹断言。', 210_000, { artifactChanged: true }],
      ['run-2', 'tool-start', '再次运行 CSV 测试文件。', 200_000, { callKey: 'run-2', command: 'node --test test/csv.test.mjs' }],
      ['result-2', 'tool-result', 'quote passed；新断言 comma failed: a,b 尚未包裹双引号。', 190_000, { callKey: 'run-2', exitCode: 1 }],
      ['action-3', 'output', '为包含逗号的单元格增加双引号包裹。', 170_000, { artifactChanged: true }],
      ['run-3', 'tool-start', '定点运行 quote 与 comma 断言。', 160_000, { callKey: 'run-3', command: 'node --test test/csv.test.mjs', validationScope: 'targeted', sourceVersion: versionB }],
      ['result-3', 'tool-result', 'quote passed；comma passed；2 passed, 0 failed。', 150_000, { callKey: 'run-3', exitCode: 0, validationScope: 'targeted', sourceVersion: versionB }],
    ]),
    make('q16', 'failure_loop', 'clear', '两次实际失败之间增加了可核对诊断，失败根因不同，修复后定点结果收敛。', '修复服务启动失败，区分配置解析与文件权限问题。', [
      ['action-1', 'output', '执行服务启动诊断。', 250_000],
      ['run-1', 'tool-start', '运行启动复现。', 240_000, { callKey: 'run-1', command: 'node probes/startup.mjs' }],
      ['result-1', 'tool-result', 'JSON.parse failed at config.json: missing comma after port。尚未进入证书读取。', 230_000, { callKey: 'run-1', exitCode: 1 }],
      ['action-2', 'output', '补上配置逗号并重试启动，记录下一阶段错误。', 210_000, { artifactChanged: true }],
      ['run-2', 'tool-start', '运行启动复现。', 200_000, { callKey: 'run-2', command: 'node probes/startup.mjs' }],
      ['result-2', 'tool-result', '配置解析已成功；证书读取失败 EACCES，stat 显示证书属主与服务用户不同。', 190_000, { callKey: 'run-2', exitCode: 1 }],
      ['action-3', 'output', '修正合成证书夹具的属主，定点复测启动链路。', 170_000, { artifactChanged: true }],
      ['run-3', 'tool-start', '定点执行启动复现。', 160_000, { callKey: 'run-3', command: 'node probes/startup.mjs', validationScope: 'targeted', sourceVersion: versionB }],
      ['result-3', 'tool-result', '配置解析成功；证书读取成功；服务启动断言通过。', 150_000, { callKey: 'run-3', exitCode: 0, validationScope: 'targeted', sourceVersion: versionB }],
    ]),
    make('q17', 'failure_loop', 'unknown', '两次失败可见，但修改与诊断事件丢失，无法确认是否无新增诊断地反复返工。', '修复启动失败并报告每次重试的诊断。', [
      ['action', 'output', '运行启动测试。', 200_000],
      ['run-1', 'tool-start', '运行启动测试。', 190_000, { callKey: 'run-1', command: 'node --test test/startup.test.mjs' }],
      ['result-1', 'tool-result', '启动断言失败。', 180_000, { callKey: 'run-1', exitCode: 1 }],
      ['run-2', 'tool-start', '再次运行启动测试。', 150_000, { callKey: 'run-2', command: 'node --test test/startup.test.mjs' }],
      ['result-2', 'tool-result', '启动断言失败。', 140_000, { callKey: 'run-2', exitCode: 1 }],
    ], { complete: false, gaps: ['intervening_actions_and_diagnoses_missing'] }),
    make('q18', 'failure_loop', 'unknown', '两次执行记录没有退出码、isError 或业务断言结果，不能将重复命令认定为实际失败。', '运行启动诊断并修复实际错误。', [
      ['action-1', 'output', '运行启动诊断命令。', 200_000],
      ['run-1', 'tool-start', '运行启动诊断。', 190_000, { callKey: 'run-1', command: 'node probes/startup.mjs' }],
      ['result-1', 'tool-result', '命令结束记录；业务断言报告与退出状态未采集。', 180_000, { callKey: 'run-1', missingFields: ['exitCode', 'isError', 'assertionResults'] }],
      ['action-2', 'output', '再次运行启动诊断命令。', 150_000],
      ['run-2', 'tool-start', '运行启动诊断。', 140_000, { callKey: 'run-2', command: 'node probes/startup.mjs' }],
      ['result-2', 'tool-result', '命令结束记录；业务断言报告与退出状态未采集。', 130_000, { callKey: 'run-2', missingFields: ['exitCode', 'isError', 'assertionResults'] }],
    ]),

    make('q19', 'validation_cost', 'attention', '完整验证实际失败耗时十八分钟，局部修复后跳过可用定点入口直接再次完整运行。', '修复 CSV 失败用例；项目提供 test/csv.test.mjs 定点入口，修复先定点复测，再做最终完整门禁。', [
      ['full-start', 'tool-start', '开始项目完整验证，包含全部单测、集成测试与 lint。', 1_180_000, { callKey: 'full-1', command: 'npm run verify', validationScope: 'full', sourceVersion: versionA }],
      ['full-result', 'tool-result', '完整验证运行 1080 秒后失败：唯一失败为 test/csv.test.mjs 的 quote 断言，lint 与其余测试通过。', 100_000, { callKey: 'full-1', exitCode: 1, validationScope: 'full', sourceVersion: versionA }],
      ['repair', 'output', '只修改 CSV 双引号替换一行。修改后未运行 CSV 定点入口，直接再次运行完整验证。', 80_000, { artifactChanged: true }],
      ['rerun', 'tool-start', '再次开始全部单测、集成测试与 lint。', 20_000, { callKey: 'full-2', command: 'npm run verify', validationScope: 'full', sourceVersion: versionB }],
    ], { requestAge: 1_200_000 }),
    make('q20', 'validation_cost', 'attention', '完整验证失败耗时十二分钟，修复单一夹具后重复完整运行，实际运行记录确认未定点复测。', '修复时区夹具，项目支持 test/timezone.test.mjs 定点复测；按失败用例、受影响模块、完整门禁顺序验证。', [
      ['full-start', 'tool-start', '开始全部测试与格式检查。', 820_000, { callKey: 'full-1', command: 'npm run verify', validationScope: 'full', sourceVersion: versionA }],
      ['full-result', 'tool-result', '完整验证运行 720 秒后失败：timezone fixture expected UTC, received local；其余项目通过。', 100_000, { callKey: 'full-1', exitCode: 1, validationScope: 'full', sourceVersion: versionA }],
      ['repair', 'output', '将该夹具的时区固定为 UTC，仅此夹具发生变更。', 80_000, { artifactChanged: true }],
      ['ledger', 'tool-result', '验证调度记录：修复后的 targeted 阶段未运行，下一执行项直接设为 full。', 60_000, { exitCode: 0 }],
      ['rerun', 'tool-start', '开始第二次全部测试与格式检查。', 20_000, { callKey: 'full-2', command: 'npm run verify', validationScope: 'full', sourceVersion: versionB }],
    ], { requestAge: 850_000 }),
    make('q21', 'validation_cost', 'clear', '实际完整失败、修复和重跑链路齐全，原失败项与模块已通过，重跑是用户要求的最终完整门禁。', '修复 CSV 缺陷，先定点复测原失败项及 CSV 模块，最终必须运行项目完整门禁。', [
      ['full-start', 'tool-start', '运行全部单测、集成测试与 lint。', 700_000, { callKey: 'full-1', command: 'npm run verify', validationScope: 'full', sourceVersion: versionA }],
      ['full-result', 'tool-result', '完整验证运行 600 秒后 quote 断言失败，其余检查通过。', 100_000, { callKey: 'full-1', exitCode: 1, validationScope: 'full', sourceVersion: versionA }],
      ['repair', 'output', '修复 CSV 双引号转义实现。', 80_000, { artifactChanged: true }],
      ['target-start', 'tool-start', '定点复测 quote 及 CSV 模块全部断言。', 70_000, { callKey: 'target', command: 'node --test test/csv.test.mjs', validationScope: 'targeted', sourceVersion: versionB }],
      ['target-result', 'tool-result', '原失败项 quote passed；CSV 模块 12 passed, 0 failed。', 60_000, { callKey: 'target', exitCode: 0, validationScope: 'targeted', sourceVersion: versionB }],
      ['reason', 'output', '原失败项和受影响模块已通过，按用户要求运行最终完整门禁。', 50_000],
      ['rerun', 'tool-start', '开始最终全部单测、集成测试与 lint。', 40_000, { callKey: 'full-2', command: 'npm run verify', validationScope: 'full', sourceVersion: versionB }],
      ['rerun-result', 'tool-result', '最终完整门禁：全部单测、集成测试与 lint 通过。', 10_000, { callKey: 'full-2', exitCode: 0, validationScope: 'full', sourceVersion: versionB }],
    ], { requestAge: 730_000 }),
    make('q22', 'validation_cost', 'clear', '实际完整失败后修改共享 schema，全量依赖受影响且执行入口不支持筛选，具有可核对的完整重跑理由。', '修复共享 schema 的字段名，并依据项目公开执行入口验证所有依赖该 schema 的模块。', [
      ['requirement', 'tool-result', '项目验证说明：npm run verify 是唯一测试入口，runner 不支持文件或用例筛选；所有测试模块使用同一个生成 schema。', 790_000, { exitCode: 0 }],
      ['full-start', 'tool-start', '运行项目完整验证入口。', 780_000, { callKey: 'full-1', command: 'npm run verify', validationScope: 'full', sourceVersion: versionA }],
      ['full-result', 'tool-result', '完整验证运行 660 秒后失败：共享 schema 将 tenantId 生成为 tenant；依赖模块的类型断言不匹配。', 120_000, { callKey: 'full-1', exitCode: 1, validationScope: 'full', sourceVersion: versionA }],
      ['repair', 'output', '修正共享 schema 生成器的字段名，重新生成所有模块依赖的 schema。', 100_000, { artifactChanged: true }],
      ['reason', 'output', '本次共享 schema 变更影响全部测试模块；项目唯一 runner 不支持筛选，因此用完整入口验证所有依赖。', 80_000],
      ['rerun', 'tool-start', '重新运行唯一完整验证入口。', 40_000, { callKey: 'full-2', command: 'npm run verify', validationScope: 'full', sourceVersion: versionB }],
      ['rerun-result', 'tool-result', '所有依赖模块的 schema 断言、集成测试与 lint 通过。', 10_000, { callKey: 'full-2', exitCode: 0, validationScope: 'full', sourceVersion: versionB }],
    ], { requestAge: 820_000 }),
    make('q23', 'validation_cost', 'unknown', '完整失败、修复与重跑可见，但定点记录窗口缺失，不能判断重跑缺少依据。', '修复 CSV 断言并按项目门禁验证。', [
      ['full-start', 'tool-start', '开始完整验证。', 700_000, { callKey: 'full-1', command: 'npm run verify', validationScope: 'full', sourceVersion: versionA }],
      ['full-result', 'tool-result', '完整验证运行 600 秒后 CSV 断言失败。', 100_000, { callKey: 'full-1', exitCode: 1, validationScope: 'full', sourceVersion: versionA }],
      ['repair', 'output', '修复 CSV 实现。', 80_000, { artifactChanged: true }],
      ['rerun', 'tool-start', '开始完整验证。', 20_000, { callKey: 'full-2', command: 'npm run verify', validationScope: 'full', sourceVersion: versionB }],
    ], { requestAge: 730_000, complete: false, gaps: ['targeted_validation_window_missing'] }),
    make('q24', 'validation_cost', 'unknown', '完整失败结束事件缺失 callId，无法绑定执行开始并核对实际失败耗时。', '修复 CSV 断言并验证。', [
      ['full-start', 'tool-start', '开始项目完整验证。', 700_000, { callKey: 'full-1', command: 'npm run verify', validationScope: 'full', sourceVersion: versionA }],
      ['full-result', 'tool-result', '完整验证结束，CSV quote 断言失败；执行关联标识未采集。', 100_000, { exitCode: 1, validationScope: 'full', sourceVersion: versionA, missingFields: ['callId'] }],
      ['repair', 'output', '修改 CSV quote 实现。', 80_000, { artifactChanged: true }],
      ['rerun', 'tool-start', '再次开始项目完整验证。', 20_000, { callKey: 'full-2', command: 'npm run verify', validationScope: 'full', sourceVersion: versionB }],
    ], { requestAge: 730_000 }),

    make('q25', 'validation_gap', 'attention', '明确完整门禁与推送版本已识别，同版本本地完整门禁真实失败后仍实际推送，无远端补验授权。', '修改导出模块。项目要求推送前当前源码版本的全部测试与 lint 通过，本轮不授权远端补验。', [
      ['action', 'output', '完成导出模块修改，源码版本已记录。', 180_000, { artifactChanged: true }],
      ['verify-start', 'tool-start', '运行当前版本全部测试与 lint。', 170_000, { callKey: 'verify', command: 'npm run verify', validationScope: 'full', sourceVersion: versionB }],
      ['verify-result', 'tool-result', '当前版本完整门禁失败：CSV quote 断言失败，退出码 1。', 100_000, { callKey: 'verify', exitCode: 1, validationScope: 'full', sourceVersion: versionB }],
      ['push', 'tool-start', '推送上述源码版本。', 80_000, { callKey: 'push', command: 'git push origin dev', sourceVersion: versionB }],
      ['push-result', 'tool-result', '远端 dev 已接受该源码版本。', 70_000, { callKey: 'push', exitCode: 0, sourceVersion: versionB }],
    ]),
    make('q26', 'validation_gap', 'attention', '门禁要求全部测试和 lint，实际版本验证清单明确两项仍 pending，却在仅定点通过后推送。', '修改 CSV 导出。项目门禁是当前版本全部测试及 lint 均通过后才能推送；仅定点通过不能替代完整门禁。', [
      ['target-start', 'tool-start', '运行 CSV 定点测试。', 170_000, { callKey: 'target', command: 'node --test test/csv.test.mjs', validationScope: 'targeted', sourceVersion: versionB }],
      ['target-result', 'tool-result', 'CSV 定点测试通过；当前版本验证清单中全部测试 pending，lint pending，二者尚未执行。', 100_000, { callKey: 'target', exitCode: 0, validationScope: 'targeted', sourceVersion: versionB }],
      ['action', 'output', '按目前仅完成的 CSV 定点结果直接推送。', 90_000],
      ['push', 'tool-start', '推送当前源码版本。', 80_000, { callKey: 'push', command: 'git push origin dev', sourceVersion: versionB }],
      ['push-result', 'tool-result', '远端 dev 已接受该源码版本。', 70_000, { callKey: 'push', exitCode: 0, sourceVersion: versionB }],
    ]),
    make('q27', 'validation_gap', 'clear', '明确门禁、实际推送和同版本完整通过结果齐全，没有以旧版或定点结果替代门禁。', '修改 CSV 导出，当前源码版本必须完成全部测试与 lint 后才允许推送。', [
      ['action', 'output', '完成 CSV 修改并记录源码版本。', 180_000, { artifactChanged: true }],
      ['verify-start', 'tool-start', '运行当前版本全部单测、集成测试与 lint。', 170_000, { callKey: 'verify', command: 'npm run verify', validationScope: 'full', sourceVersion: versionB }],
      ['verify-result', 'tool-result', '此源码版本全部单测、集成测试与 lint 通过，完整门禁退出码 0。', 100_000, { callKey: 'verify', exitCode: 0, validationScope: 'full', sourceVersion: versionB }],
      ['push', 'tool-start', '推送通过完整门禁的源码版本。', 80_000, { callKey: 'push', command: 'git push origin dev', sourceVersion: versionB }],
      ['push-result', 'tool-result', '远端 dev 已接受相同源码版本。', 70_000, { callKey: 'push', exitCode: 0, sourceVersion: versionB }],
    ]),
    make('q28', 'validation_gap', 'clear', '实际用户输入明确针对推送版本授权远端补验，本地环境缺少浏览器不能被直接当作违规。', `项目原门禁要求当前版本全部测试与 lint 本地通过。本机无法运行浏览器集成测试；我明确授权源码版本 ${versionB} 先推送，由远端执行全部测试与 lint 补验，远端通过前不发布。`, [
      ['inspect', 'tool-start', '检查本机浏览器执行条件。', 170_000, { callKey: 'inspect', command: 'node probes/browser-capability.mjs' }],
      ['environment', 'tool-result', '本机没有浏览器运行时，浏览器集成测试入口不可执行。这里只完成环境检测，没有将完整测试记为通过。', 160_000, { callKey: 'inspect', exitCode: 0 }],
      ['action', 'output', '按用户对该源码版本的明确授权推送，以远端全部测试和 lint 补验。', 100_000],
      ['push', 'tool-start', '推送用户授权的源码版本。', 80_000, { callKey: 'push', command: 'git push origin dev', sourceVersion: versionB }],
      ['push-result', 'tool-result', '远端 dev 已接受用户授权的源码版本，补验仍在排队。', 70_000, { callKey: 'push', exitCode: 0, sourceVersion: versionB }],
    ]),
    make('q29', 'validation_gap', 'unknown', '门禁和推送已识别，但验证结果采集窗口丢失，不能将未观察到结果认定为未执行。', '修改 CSV 导出；项目要求推送前当前版本全部测试与 lint 通过。', [
      ['verify-start', 'tool-start', '开始当前版本全部测试与 lint。', 170_000, { callKey: 'verify', command: 'npm run verify', validationScope: 'full', sourceVersion: versionB }],
      ['action', 'output', '进入推送阶段。', 90_000],
      ['push', 'tool-start', '推送当前源码版本。', 80_000, { callKey: 'push', command: 'git push origin dev', sourceVersion: versionB }],
      ['push-result', 'tool-result', '远端 dev 已接受该源码版本。', 70_000, { callKey: 'push', exitCode: 0, sourceVersion: versionB }],
    ], { complete: false, gaps: ['validation_results_capture_missing'] }),
    make('q30', 'validation_gap', 'unknown', '完整门禁通过结果存在，但实际推送事件缺失 sourceVersion，不能确认验证绑定当前推送版本。', '修改 CSV 导出；项目要求当前推送源码版本的全部测试与 lint 通过。', [
      ['verify-start', 'tool-start', '运行记录版本的完整门禁。', 170_000, { callKey: 'verify', command: 'npm run verify', validationScope: 'full', sourceVersion: versionA }],
      ['verify-result', 'tool-result', '记录版本的全部测试与 lint 通过。', 100_000, { callKey: 'verify', exitCode: 0, validationScope: 'full', sourceVersion: versionA }],
      ['action', 'output', '执行推送命令，采集未取得被推送源码身份。', 90_000],
      ['push', 'tool-start', '推送命令开始。', 80_000, { callKey: 'push', command: 'git push origin dev', missingFields: ['sourceVersion'] }],
      ['push-result', 'tool-result', '推送命令结束，源码版本字段未采集。', 70_000, { callKey: 'push', exitCode: 0, missingFields: ['sourceVersion'] }],
    ]),

    make('q31', 'long_wait', 'attention', '工具在五分钟边界仍未观察到结束；心跳表明作业存活，期望只提示长等待，不判卡死。', '运行完整构建并等待工具结束。', [
      ['start', 'tool-start', '开始完整构建。', 300_000, { callKey: 'build', command: 'npm run build' }],
      ['heartbeat', 'output', '构建作业心跳正常，仍在编译依赖；尚未产生工具结束结果。', 10_000],
    ], { requestAge: 360_000 }),
    make('q32', 'long_wait', 'attention', '运行任务最后可见活动恰好五分钟前，正常审批等待也只提示等待时长与原因。', '准备发布申请，等待审批结果后继续。', [
      ['waiting', 'output', '发布申请已提交，正在等待用户审批，尚未收到审批结果。', 300_000],
    ], { requestAge: 360_000 }),
    make('q33', 'long_wait', 'clear', '未结束工具等待 299.999 秒，尚未达到现有五分钟默认边界。', '运行构建并等待完成。', [
      ['start', 'tool-start', '开始构建。', 299_999, { callKey: 'build', command: 'npm run build' }],
    ], { requestAge: 360_000 }),
    make('q34', 'long_wait', 'clear', '曾超过五分钟的工具已结束，最新结束结果刚到达，当前没有长等待中的开放执行。', '运行完整构建并等待结束结果。', [
      ['start', 'tool-start', '开始完整构建。', 600_000, { callKey: 'build', command: 'npm run build' }],
      ['result', 'tool-result', '构建结束，全部目标产物生成成功。', 1_000, { callKey: 'build', exitCode: 0 }],
    ], { requestAge: 660_000 }),
    make('q35', 'long_wait', 'unknown', '采集断开可能遗漏工具结束，不能凭旧开始事件确认当前仍在等待。', '运行构建并等待结果。', [
      ['start', 'tool-start', '开始构建。', 600_000, { callKey: 'build', command: 'npm run build' }],
    ], { requestAge: 660_000, complete: false, gaps: ['tool_completion_capture_disconnected'] }),
    make('q36', 'long_wait', 'unknown', '任务运行状态字段缺失，无法确认旧工具开始事件是否仍属于当前运行中的等待。', '运行构建并报告结果。', [
      ['start', 'tool-start', '开始构建。', 600_000, { callKey: 'build', command: 'npm run build' }],
    ], { requestAge: 660_000, status: null }),
  ];
}
