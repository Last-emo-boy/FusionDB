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