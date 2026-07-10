# BENCHPROD-466d: Raft 快照安装重建 trigram 索引

465 review 确认的缺口:`replace_visible_entries_for_snapshot` 安装快照后只重建
vector 索引,trigram 保留快照前 postings(新 follower 则为空)→ trigram 加速的
通配 LIKE 用旧状态作答。

改动:`Column::is_trigram_text_column()` 上移 catalog 做单源;
`numeric_row_id_for_str` 下沉 trigram.rs;fusion 新增 `rebuild_trigram_index`
严格镜像 vector 重建,并在快照安装尾部以全新 TrigramIndex 重建。

测试:导出可见状态→清空内存 trigram(模拟新 follower)→安装→断言 postings
与 LIKE 结果。披露:与 vector 重建共享 legacy `data:` 前缀扫描的既有局限
(分片前缀不扫,两者以后一并修)。466 系列其余(确定性 KvBatch 复制、
pgwire 接入 Raft、vote/log 持久化)保持开放。
