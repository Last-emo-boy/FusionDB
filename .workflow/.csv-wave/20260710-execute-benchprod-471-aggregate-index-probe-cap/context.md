# BENCHPROD-471: 列聚合索引扫描的无界逐行点查

## 解剖(两次路径修正,均记 maestro)

`AVG(total) WHERE status='delivered'` xlarge warm 44s,10 万次点查。先疑
try_index_scan(其 cap/弃用机制完备,default 1024,无辜);再疑 stats gate
(压根不是这个家族)。真凶:纯聚合投影路由到
`simple_column_aggregate_index_scan`——扫全部索引条目后**无上限**逐行
txn.get(status 选择率 ~50% → 10 万随机点查 × ~440µs)。

**教训:同一逻辑查询随投影形态走不同执行家族,闸门必须逐家族审计。**

## 修复

`COLUMN_AGGREGATE_INDEX_PROBE_CAP=4096`:索引条目扫描限 cap+1,超限
`Ok(None)` 退回批式全扫聚合(盈亏平衡 ≈ 点查成本×cap vs 顺序批扫)。

## 验证

xlarge:44,087ms→**808ms warm(55×)**,聚合值逐位一致;同型 COUNT 711ms;
小命中索引路径保留。门禁全绿。

## 后续

Revenue by category 10s 疑为同家族 group_by 变体(同 cap 模式,先测);
有统计时可在扫条目前即拒绝。
