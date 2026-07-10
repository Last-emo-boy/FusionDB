---
title: "Review Standards"
readMode: required
priority: medium
category: review
keywords:
  - review
  - checklist
  - gate
  - approval
  - standard
---

# Review Standards

## Entries



<spec-entry category="review" keywords="lazy-reverse,review,topk,sstable" date="2026-07-09" title="Lazy reverse subagent review 2026-07-09" description="Review confirms lazy reverse correctness, with frontier precision follow-ups" source="main@8a12c0f">

### Lazy reverse subagent review 2026-07-09

Subagent cdx-131641-1ad3 found no correctness blocker in lazy SSTable reverse activation after reviewing ReverseSource, PendingReverseSstable, activation, reverse MVCC merge, and metrics. Findings: file-level frontier can over-activate mixed-keyspace SSTables; equal-frontier tombstone regression was required and has now been added; fusion_reverse_source_open_count includes write-buffer/memtable/SSTable and should not be treated as SSTable-only; raw work can still be high for tombstone/future-version-heavy top ranges. Follow-ups: block/index-derived in-range frontier, source_order tie test, dedicated SSTable activation metric.

</spec-entry>