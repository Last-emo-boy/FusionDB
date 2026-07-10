---
title: "Learnings"
readMode: optional
priority: medium
category: learning
keywords:
  - bug
  - lesson
  - gotcha
  - learning
---

# Learnings

Add entries with: `/spec-add learning <description>`

## Entries



<spec-entry category="learning" keywords="git,checkpoint,flaky,maestro-explore" date="2026-07-10" title="检查点入库 2026-07-10:发现 44K 行未提交工作" source="main@6c727b4">

### 检查点入库 2026-07-10:发现 44K 行未提交工作

6 月 29 日 BENCHPROD-462 之后的多会话成果(manifest v2、SSTable 前沿、StatsEstimator、security.rs、benchmark parts 11-31、1061 测试)全部滞留工作树未提交,已按 6 个主题提交检查点入库 main(b0bd059..6c727b4)。教训:每个会话收尾必须核对 git status 并 dual-commit;.workflow/specs 现已入库,派生缓存 gitignore。另:cargo test --all-targets 首跑在高负载下偶发 1 例 lib flaky(556/557),复跑两次全绿,失败名未捕获,再现时需记录。maestro explore 本机无 endpoint(api-explore.json 缺失),检索用 delegate codex 或自有工具。

</spec-entry>