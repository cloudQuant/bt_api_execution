# Iteration 10 journal schema 事务边界修复记录（2026-10-31）

## 结论

独立源码审查结果为 GO。本轮代码修改、真实 Backtrader position port 的临时 SQLite 事务验证和现存窄套件均通过。当前仅完成源码与本地合成验收，未暂存、未提交，也不代表真实账户或生产数据库验收。

## 基线与审查

- bt_api_execution 仓库：D:\bt_api_py\bt_api\bt_api_execution
- 本轮基线：分支 dev，HEAD 72271ef5b1426e9e431c27ea9daf5785c7c2a1fb。
- 被验证的 Backtrader 仓库：D:\source_code\backtrader，分支 dev，HEAD 0faef85514995a29a42343b4b20be44e44a76c5e。
- Backtrader position port 源文件 SHA-256：_journal_ports.py 为 F349455A23F6BCB3C49232F643D4ABFDDCDB98E94AF5ECE63BA7EF192669838C；factor_position_book.py 为 BD6EAF348D5CF4A4D0959641FB638D9CF2B102D088A4DEC9F10290CE51AC8677。
- 独立源码审查：GO（2026-10-31，上级 review）。
- bt_api_execution 的 index 保持空；原有未跟踪 service_recovery.py、service_recovery_flow.py、service_recovery_ports.py 均保留。

## 修改范围

- src/bt_api_execution/journal.py：将 BEGIN IMMEDIATE 放为 schema executescript 的首条语句，使基础表 DDL、实际 position port 建表和 schema migration 处于同一事务；在锁内读取 user_version。schema 失败时执行安全 rollback；通用异常原样抛出，不覆盖现有错误映射。构造失败时显式关闭 SQLite 连接。
- src/bt_api_execution/journal_ports.py：明确 port 在 Journal 已开启的事务内运行，不得自行 begin、commit、rollback，也不得调用可能隐式提交的 executescript。
- 未新增持久测试，也未修改 Backtrader 源码。

## 验证

- 现存窄套件：python -m pytest tests/test_journal_ports.py -q，23 passed。
- 语法检查：python -m compileall -q src/bt_api_execution/journal.py src/bt_api_execution/journal_ports.py，通过。
- 空白检查：git diff --check，通过。
- 真实 Backtrader port 临时数据库验证：由 ExecutionDecisionJournal 调用当前 Backtrader BacktraderPositionAttributionJournalPort.ensure_schema。观察 wrapper 只负责确认事务/第二连接状态，并在回滚分支于真实 port 完成后注入 RuntimeError。
  - port 返回时 writer 仍处于 outer transaction。
  - schema 尚未提交时，第二连接看不到 journal 或 position book 的 SQLite schema 对象。
  - 注入失败后，原 RuntimeError 对象原样传播；回滚后第二连接看不到任何 schema 对象，user_version 为 0。
  - 正常构造提交后，第二连接可见完整 journal 与 position schema；user_version 等于 Journal 当前 schema version，position book version 为 6。
- 一次性临时库补充验证：port 先写 DDL 再抛 RuntimeError、真实 v12 容量 migration 中途抛 RuntimeError、以及两个线程并发打开同一 schema。失败场景均完整回滚，健康重开成功；并发打开均完成并观察到相同版本。

第一次直接调用 position port 的小型夹具没有复现 Journal 的 sqlite3.Row 配置和基础 execution_decision_intents 表，因此被真实 port 正确拒绝。随后改为经完整 ExecutionDecisionJournal 调用真实 port 并按 Journal 配置临时库，事务可见性、回滚和成功提交三项均通过。该初次失败属于夹具缺少 Journal 前置条件，不是生产调用失败。

## 证据边界与交付状态

所有数据库均为临时合成 SQLite；未连接真实账户、生产数据库、provider 或私密输入。并发补充验证是单进程线程与 SQLite 连接，不覆盖跨进程/多平台故障矩阵。现存 23 项窄套件不等同于完整执行生命周期或账户验收。

本轮变更文件为 journal.py、journal_ports.py 与本记录。没有暂存或提交；交付状态为等待后续 review/提交流程。
