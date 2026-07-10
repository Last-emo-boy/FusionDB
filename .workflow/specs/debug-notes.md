---
title: "Debug Notes"
readMode: optional
priority: medium
category: debug
keywords:
  - debug
  - issue
  - workaround
  - root-cause
  - gotcha
---

# Debug Notes

## Entries



<spec-entry category="debug" keywords="startup,benchmark,server,data" date="2026-07-09" title="Benchmark server clean cwd startup" description="Avoid stale repo-root data recovery when starting benchmark server" source="main@8a12c0f">

### Benchmark server clean cwd startup

The fusiondb binary always loads fusiondb.toml from the current working directory and uses storage.data_dir relative to that cwd. Starting from repo root on 2026-07-09 printed only the early config lines and did not listen within 20s because the repo-root data directory contained about 698 MB of previous SSTables. Starting /root/FusionDB/target/debug/fusiondb from an empty /tmp/fusiondb_part21_smoke cwd used default empty data/ and immediately printed FusionDB HTTP Server running on http://127.0.0.1:8091. For short benchmark smoke runs, use a clean cwd or an isolated config/data dir.

</spec-entry>

<spec-entry category="debug" keywords="fusion,shutdown,flush,race,sstable" date="2026-07-09" title="Fusion shutdown flush double-writer race" description="Prevents background and synchronous flush from writing the same SSTable" source="main@2026-07-09">

### Fusion shutdown flush double-writer race

A parallel test run exposed an early EOF failure opening a freshly flushed SSTable during snapshot/shutdown paths. Root cause: rotate_memtable notifies the background flush loop, while create_snapshot_now/shutdown immediately call flush_all_immutable_memtables; both paths could write the same memtable id to the same SSTable file concurrently. FusionStorage now owns flush_lock: Arc<AsyncMutex<()>>. The background flush_loop and flush_all_immutable_memtables share this lock, preventing double writers while still allowing the loser to observe that the memtable was already marked flushed.

</spec-entry>

<spec-entry category="debug" keywords="unique,phantom,occ,sentinel,benchprod-464" date="2026-07-10" title="单列 UNIQUE 并发幻读洞已核实,复合 PK 无洞(BENCHPROD-464 范围修正)" source="main@47449f4">

### 单列 UNIQUE 并发幻读洞已核实,复合 PK 无洞(BENCHPROD-464 范围修正)

调研 review 声称复合 UNIQUE 有 OCC 幻读洞——核实后修正:复合 PK 安全(row_id_for_insert 对 _pkey 返回 value_key 作 row_id,并发同值写相同 data key,exact-key OCC 碰撞);单列 PK 同理安全。真洞:①单列 UNIQUE 非 PK(insert.rs:480 scan 检查 + index:t:col:value:row_id 后缀键)并发同值双提交;②非 PK 复合 UNIQUE 索引根本未校验(composite_index.rs:514 loader 只认 _pkey)——独立功能缺口;③UPDATE 可能完全不检查单列 UNIQUE(待核)。464=sentinel 键堵①;②③单独开票。

</spec-entry>

<spec-entry category="debug" keywords="flaky,sstable,frsk,reverse-seek" date="2026-07-10" title="flaky 测试定名:reverse_iterator_uses_persisted_reverse_seek_sidecar" source="main@47449f4">

### flaky 测试定名:reverse_iterator_uses_persisted_reverse_seek_sidecar

多次 --all-targets 高负载运行中偶发失败的 lib 测试终于捕获名字:storage::sstable::tests::reverse_iterator_uses_persisted_reverse_seek_sidecar(FRSK 反向 seek sidecar)。单独运行稳定通过,仅在全量并行+高系统负载下偶发,疑似时序/文件系统竞态。与 BENCHPROD-463/464 改动无关(未触 sstable)。待专项修复:审查该测试的 sidecar 持久化等待逻辑。

</spec-entry>