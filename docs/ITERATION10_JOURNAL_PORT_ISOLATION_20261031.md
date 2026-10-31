# Execution journal 端口隔离修复记录（2026-10-31）

## 基线与范围

- 仓库：D:\bt_api_py\bt_api\bt_api_execution
- 分支与基线：dev，4f27a01f198635709f47f205332025671305dc25
- 本轮修改：src/bt_api_execution/journal.py、src/bt_api_execution/journal_archive.py，以及本记录。
- 工作区原有未跟踪文件 service_recovery.py、service_recovery_flow.py、service_recovery_ports.py 均保留；本轮未暂存、未提交。
- 本轮不新增持久测试文件。此前约定暂缓新增 execution 测试，故使用一次性纯本地合成数据验证。

## 修复内容

_validate_fee_pair_flat_release_evidence_in_transaction 保持静态方法和旧调用兼容，增加可选 ports 参数。Journal 自有的 RELEASED 验证、读取投影、兼容迁移及 schema 重开/升级路径显式传入实例捕获的 self._ports；未传参数的独立旧调用仍走 process-global 配置。

archive_snapshot() 将同一个 JournalPorts 传给 archive 创建 API。create_verified_execution_journal_archive() 和 verify_execution_journal_archive() 新增可选 journal_ports 参数，并将它贯穿临时副本校验、发布后校验及 schema validator。省略参数的既有 standalone 调用仍兼容原 global fallback；显式传入错误类型会 fail closed。

## 调用链审查

在子仓库内搜索 archive API 调用点，生产调用方只有 ExecutionDecisionJournal.archive_snapshot()；外部消费者仍可直接调用两个公开 archive 函数，因此新增参数保持可选。实例归档路径会在两次 archive verification 中使用实例的同一 port set。

RELEASED 证据 validator 是静态方法，因此不直接引用 self。其全部 Journal 内部调用点均显式传端口；schema validator 的重开和升级调用也传递端口。独立 legacy 调用仍保留默认行为。

另评估了 _create_schema() 的事务顺序：position_attribution.ensure_schema(cursor) 当前发生在 BEGIN IMMEDIATE 之前，与 JournalPositionAttributionPort 文档要求“在调用方事务内写入”不一致。本轮按任务边界只记录并上报，未改动该处。

## 验证结果

- pytest tests/test_journal_ports.py tests/test_journal_reader.py -q：28 passed。
- python -m compileall -q src/bt_api_execution/journal.py src/bt_api_execution/journal_archive.py：通过。
- git diff --check：通过。
- 一次性 RELEASED 端到端合成验证：通过。完整构造两条已终态 OPEN intent、各自的 reconciliation/resolution、两条 FLAT checkpoint、五类完整 query provenance、不可变 RESERVED/RELEASED 事件及已删除的 active hold。随后验证 schema 重开、读取和 archive_snapshot()。
  - 实例注入 facts port 调用数：重开 2、读取 1、临时/发布 archive 校验共 2。
  - process-global marker port 调用数：0。
  - 读取状态：RELEASED。
  - 最终状态：PASS_SYNTHETIC_RELEASED_PORT_ISOLATION。
- 合成数据只使用随机测试目录和固定合成标识，不读取账户、凭据或私密输入。
- 合成验证为隔离本轮目标，patch 了 reservation-record、workflow、scope、capacity validators；真实 RELEASED evidence validator 与 lifecycle schema validator 未 patch。故它证明显式端口贯穿完整 RELEASED 证据校验路径，但不等同于完整交易 workflow 验收或真实账户验收。

## 交付状态

当前仅有上述两处源码和本记录作为本轮 tracked 修改；未暂存、未提交。验证属于本地源码/合成数据层，不代表 provider、账户或真实交易验收。