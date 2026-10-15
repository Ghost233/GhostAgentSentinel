---
name: deploy-release
description: 发布 GhostAgentSentinel 正式版本或核验发布结果时使用。仅编辑本 skill 或发布文档不会启动发布。
---

# 正式发布

执行前读取仓库根目录下的 [开发契约](../../../docs/development-contract.md) 和 [发布流程与完成条件](../../../docs/release-status.md#发布流程与完成条件)。按后者执行发布前门禁、发布后远端验收及本地分支与 tag 同步；发布完成条件以该文档为准。

全部条件满足后，给出 Actions、Release 链接和本地同步结果并结束发布。缺少证据或同步受阻时，明确报告未完成项。
