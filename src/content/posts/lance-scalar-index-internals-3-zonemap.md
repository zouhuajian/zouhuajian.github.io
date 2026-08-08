---
author: "Jay H. Zou"
pubDatetime: 2026-08-08T11:34:01+08:00
title: "Lance Scalar Index 原理与源码分析（三）：Zone Map 的统计结构与扫描剪枝"
lang: zh-CN
tags:
  - Lance
  - Scalar Index
  - Zone Map
  - 源码分析
description: "基于 Lance 9.0 源码，分析 Zone Map 如何按原始行序生成分区统计、将谓词转换为候选行范围，并通过 AtMost 与 NULL bitmap 保证扫描剪枝的正确性。"
---

> 本文源码基于 Lance `release/v9.0` 分支的提交 [`d293630df`](https://github.com/lance-format/lance/tree/d293630dff7a0393702e01a88a65da1a6591e867)。文中的代码片段保留关键控制流，并在注释中标出阅读重点。

[上一篇](/posts/lance-scalar-index-internals-2-btree/) 介绍的 BTree 会排序 `<value, row identifier>`，读取候选 leaf pages，再在页内过滤出精确的 row identifiers。Zone Map 采用的是另一条路径：**不排序、不保存逐行值，只为原始行序中的连续区域保存统计信息**。

它不能回答“具体哪几行满足条件”，但可以证明“哪些区域一定不满足条件”。这让 Zone Map 更像一层轻量的 scan pruning 结构，而不是 BTree 的低成本替代品。

## 1\. Zone Map 的剪枝模型

先看一段按时间大致有序的日志数据。为了便于观察，示例将 `rows_per_zone` 设为 4；Lance 9.0 的默认值是 8192。

| Row offset | `event_time` | Zone |
| ---------- | ------------ | ---- |
| 0          | 00:01        | Z0   |
| 1          | 00:04        | Z0   |
| 2          | 00:07        | Z0   |
| 3          | 00:09        | Z0   |
| 4          | 00:10        | Z1   |
| 5          | 00:12        | Z1   |
| 6          | 00:17        | Z1   |
| 7          | 00:19        | Z1   |
| 8          | 00:21        | Z2   |
| 9          | 00:24        | Z2   |
| 10         | 00:27        | Z2   |
| 11         | 00:29        | Z2   |

Zone Map 不改变这些行的位置，只为每 4 行生成一条统计记录：

```text
Z0: offsets [0, 4), min=00:01, max=00:09
Z1: offsets [4, 8), min=00:10, max=00:19
Z2: offsets [8,12), min=00:21, max=00:29
```

查询：

```sql
WHERE event_time >= '00:12' AND event_time < '00:18'
```

只需要比较三组 `min/max`：

- Z0 的 `max < 00:12`，整区跳过；
- Z1 与查询范围相交，读取 offsets `[4, 8)`；
- Z2 的 `min >= 00:18`，整区跳过。

Zone Map 将需要读取的范围从 12 行缩小到 4 行，但 Z1 中只有 offsets 5、6 真正匹配。offsets 4、7 是 false positives，读取数据后还要重新执行原谓词。

```text
Zone Map result: AtMost { offsets [4, 8) }
Recheck result:                  { offsets 5, 6 }
```

这就是全文最重要的边界：**Zone Map 能排除不可能命中的 zone，却不能仅凭统计值确认 zone 内的具体行。**

## 2\. 构建：保留 Row Address 顺序

BTree 的训练请求是 `TrainingOrdering::Values`；Zone Map 明确要求 `TrainingOrdering::Addresses`：

```rust
pub fn new(params: ZoneMapIndexBuilderParams) -> Self {
    Self {
        params,
        criteria: TrainingCriteria::new(TrainingOrdering::Addresses)
            .with_row_addr(), // [1] 按物理 Row Address 扫描，并保留 _rowaddr
    }
}
```

通用训练扫描只会为 `TrainingOrdering::Values` 增加 `ORDER BY`。对于 Zone Map，Dataset 的默认扫描顺序已经是 Row Address 顺序，因此不会按索引列重排：

```rust
// Addresses 模式不排序：保留 fragment 与 local row offset 的物理顺序
if TrainingOrdering::Values == criteria.ordering {
    scan.order_by(Some(vec![
        ColumnOrdering::asc_nulls_first(column.to_string()),
    ]))?;
}

if criteria.needs_row_addrs {
    scan.with_row_address(); // [2] 输出 _rowaddr
}
```

完整入口见 [`ZoneMapIndexTrainingRequest`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zonemap.rs#L1058-L1078) 和 [`scan_training_data`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance/src/index/scalar.rs#L126-L164)。

训练流随后进入共享的 `ZoneTrainer`。它累计最多 `rows_per_zone` 行，但还有一个优先级更高的边界：**zone 不能跨 fragment**。

```rust
while let Some(batch) = batches.try_next().await? {
    let row_addr_col = batch.column_by_name(ROW_ADDR)?;

    while batch_offset < batch.num_rows() {
        let fragment_id = row_addr_col.value(batch_offset) >> 32;

        if current_fragment_id != Some(fragment_id) {
            flush_current_zone()?; // [1] fragment 变化时，即使未满也立即结束
        }

        let take = same_fragment_rows.min(zone_capacity_left);
        processor.process_chunk(&values.slice(batch_offset, take))?;

        if current_zone_len == rows_per_zone {
            flush_current_zone()?; // [2] 达到容量时结束
        }
    }
}
```

上面是 [`ZoneTrainer::train`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zoned.rs#L84-L215) 的控制流摘要。假设两个 fragment 各有 6 行、`rows_per_zone=4`，结果不是三个跨 fragment 的满 zone，而是：

```text
F0 -> [0,4), [4,6)
F1 -> [0,4), [4,6)
```

这样每个 zone 都能用 `(fragment_id, zone_start, zone_length)` 表示为一个 fragment 内的连续 Row Address 范围。

![Zone Map 从原始 Row Address 顺序生成统计记录](/images/lance-scalar-index-internals/zonemap-build-layout.svg)

可编辑图源：[zonemap-build-layout.excalidraw](/images/lance-scalar-index-internals/zonemap-build-layout.excalidraw)

### `zone_length` 表示 offset span

如果训练扫描已经过滤了删除行，Row Address 可能存在空洞。例如一个 zone 收到 offsets `2, 3, 7`，它的范围必须覆盖 `[2, 8)`，因此 `zone_length=6`，而不是 3。

```rust
let first_offset = row_addr(first_row).row_offset();
let last_offset = row_addr(last_row).row_offset();

let bound = ZoneBound {
    fragment_id,
    start: first_offset,
    length: (last_offset - first_offset + 1) as usize,
};
```

这个范围可能重新包含已删除的 offset，但不会漏掉参与统计的行。多读是性能成本；少读则会造成 false negative。完整实现见 [`ZoneBound`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zoned.rs#L32-L44) 和 zone flush 的 [`flush_zone`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zoned.rs#L217-L244)。

## 3\. `zonemap.lance` 的统计结构

一个 zone 在内存中的核心状态很小：

```rust
struct ZoneMapStatistics {
    min: ScalarValue,
    max: ScalarValue,
    null_count: u32,
    nan_count: u32,
    bound: ZoneBound, // fragment_id + start + length
}
```

`ZoneMapProcessor` 使用 Arrow 统计累加器处理每个 value chunk；zone 结束时才把累计结果固化：

```rust
// 控制流摘要：辅助函数名经过简化
fn process_chunk(&mut self, array: &ArrayRef) -> Result<()> {
    self.statistics.update(array)?;
    Ok(())
}

fn finish_zone(&mut self, bound: ZoneBound) -> Result<ZoneMapStatistics> {
    let stats = self.statistics.statistics();
    Ok(ZoneMapStatistics {
        min: scalar(stats.min)?,
        max: conservative_max(stats.max, stats.nan_count)?,
        null_count: to_u32(stats.null_count)?,
        nan_count: to_u32(stats.nan_count.unwrap_or(0))?,
        bound,
    })
}
```

完整实现见 [`ZoneMapProcessor`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zonemap.rs#L946-L1037)。

所有 zone 最终写入一个 `zonemap.lance` 文件，每个 zone 对应一行：

| Column        | Lance 9.0 实现类型 | 作用                                       |
| ------------- | ------------------ | ------------------------------------------ |
| `min`         | indexed type       | zone 中最小的非 NULL 值                    |
| `max`         | indexed type       | zone 中最大的非 NULL 值；浮点 NaN 单独处理 |
| `null_count`  | `UInt32`           | zone 中 NULL 数量                          |
| `nan_count`   | `UInt32`           | 浮点 zone 中 NaN 数量                      |
| `fragment_id` | `UInt64`           | zone 所属 fragment                         |
| `zone_start`  | `UInt64`           | fragment 内的起始 row offset               |
| `zone_length` | `UInt64`           | 从首个到末个 row offset 的覆盖跨度         |

`min/max` 字段本身允许为 NULL：如果整个 zone 都是 NULL，就没有可用于普通值比较的边界。

文件 schema metadata 还保存：

- `rows_per_zone`：字符串形式的目标 zone 容量，默认 8192；
- `null_bitmap`：可选的 global buffer 编号，指向精确 NULL Row Address 集合。

固定提交的 format 文档将 `zone_length` 表格类型写成 `UInt32`，但 9.0 的写入和读取实现都使用并校验 `UInt64`。本文以实际 wire schema 为准。相关代码见 [`zonemap_stats_as_batch`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zonemap.rs#L863-L910) 和 [`try_from_serialized`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zonemap.rs#L490-L591)。

与 BTree 的 `page_data.lance` 不同，`zonemap.lance` 没有逐行 `values` 和 `ids`。打开索引时，Lance 会整体读取这些统计记录；真正的列值只在候选 zone 回表时读取。

## 4\. 谓词如何选择 Candidate Zones

Zone Map 支持的核心判断可以归纳为区间相交：

```rust
match query {
    Equals(value) => Ok(zone.min <= value && value <= zone.max),

    Range(start, end) => {
        // 省略 NULL、NaN 与 Bound 类型分支
        let reaches_lower = zone.max >= start;
        let starts_before_upper = zone.min < end;
        Ok(reaches_lower && starts_before_upper)
    }

    IsIn(values) => Ok(values.iter().any(|value|
        zone.min <= value && value <= zone.max
    )),

    IsNull => Ok(zone.null_count > 0),
}
```

实际实现还区分 `Included`、`Excluded`、无界范围、字符串前缀与 NaN，完整代码见 [`evaluate_zone_against_query`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zonemap.rs#L201-L447)。

对贯穿示例的半开区间 `[00:12, 00:18)`，一个普通 zone 被保留的条件是：

```text
zone.max >= 00:12 AND zone.min < 00:18
```

| Zone | `[min,max]`     | 剪枝结果          |
| ---- | --------------- | ----------------- |
| Z0   | `[00:01,00:09]` | skip              |
| Z1   | `[00:10,00:19]` | candidate `[4,8)` |
| Z2   | `[00:21,00:29]` | skip              |

选中的 zone 不会再访问任何索引 leaf。`search_zones` 直接把 zone bound 转成 Row Address 范围并合并：

```rust
for zone in zones {
    if zone_matches(zone)? {
        let start = (zone.fragment_id << 32) + zone.start;
        let end = start + zone.length as u64;
        candidate_rows.insert_range(start..end); // [1] 整个 zone 都进入候选集
    }
}

Ok(SearchResult::at_most(candidate_rows)) // [2] 明确要求读取后 recheck
```

完整实现见 [`search_zones`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zoned.rs#L247-L277) 和 [`ZoneMapIndex::search`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zonemap.rs#L633-L650)。

![Zone Map 从谓词相交到 AtMost 候选范围](/images/lance-scalar-index-internals/zonemap-scan-pruning.svg)

可编辑图源：[zonemap-scan-pruning.excalidraw](/images/lance-scalar-index-internals/zonemap-scan-pruning.excalidraw)

这里的 `AtMost` 不是“不太确定”的注释，而是执行器必须遵守的集合契约：返回集合是实际匹配行的上界。它可以包含 offsets 4、7 和删除行，却不能漏掉 offsets 5、6。Dataset 层据此保留原始 filter，对候选数据重新计算谓词。

## 5\. 数据局部性与剪枝率

Zone Map 的收益不由字段基数单独决定，而由 **相邻 Row Address 的值域是否集中** 决定。

把贯穿示例的 12 个时间值随机打散，但仍然每 4 行生成一个 zone，可能得到：

```text
Z0: [00:01, 00:29]
Z1: [00:04, 00:27]
Z2: [00:07, 00:24]
```

查询 `[00:12, 00:18)` 会与三个统计范围全部相交。即使真实匹配行仍然只有两行，Zone Map 也必须返回全部 12 行作为候选；这时索引在正确性上没有问题，只是失去了剪枝价值。

| 数据布局       | Candidate zones | Candidate rows | Pruned rows |
| -------------- | --------------- | -------------- | ----------- |
| 按时间大致有序 | 1 / 3           | 4 / 12         | 8 / 12      |
| 相同值随机打散 | 3 / 3           | 12 / 12        | 0 / 12      |

`rows_per_zone` 决定统计粒度：

| 较小的 zone                         | 较大的 zone                      |
| ----------------------------------- | -------------------------------- |
| `min/max` 更紧，false positive 较少 | 统计行更少，索引文件更小         |
| zone 数量和比较次数更多             | 一次命中会带入更多无关 rows      |
| 更容易形成较短的候选读取范围        | 更容易把宽值域压进同一条统计记录 |

因此默认 8192 只是通用折中。真正需要观察的是典型谓词命中了多少 zones、候选行占比，以及数据写入或 compaction 后是否仍然保持局部性。

## 6\. NULL Bitmap 与结果精度

`null_count` 只能判断一个 zone 是否含 NULL，不能指出 NULL 的具体位置。只靠它执行 `IS NULL`，结果仍然只能是整个 zone 的 `AtMost`。

Lance 9.0 在训练 zone 的同时收集每个 NULL 的 Row Address，并将序列化后的 `RowAddrTreeMap` 写入 `zonemap.lance` 的 global buffer：

```rust
// 控制流摘要：省略 RowAddrTreeMap 的序列化细节
if values.is_null(i) {
    null_rows.insert(row_addr_col.value(i)); // [1] 精确记录 NULL 行
}

let null_bitmap_idx = index_file
    .add_global_buffer(serialize(null_rows))
    .await?;

finish_with_metadata({ "null_bitmap": null_bitmap_idx }) // [2] 发布 buffer 位置
```

构建与写入实现见 [`ZoneTrainer` 的 NULL 收集](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zoned.rs#L159-L167) 和 [`write_index`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zonemap.rs#L912-L940)。

打开索引后，`IS NULL` 会优先使用完整 bitmap：

```rust
if let SargableQuery::IsNull() = query {
    if let Some(null_rows) = &self.null_rows {
        return Ok(SearchResult::exact(null_rows.clone())); // [1] 不返回整个 zone
    }
}

search_zones(...) // [2] 无完整 bitmap 时，按 null_count 返回 AtMost zones
```

这使 Zone Map 出现一个有意设计的例外：

| 查询      | 有完整 NULL bitmap | 没有完整 NULL bitmap |
| --------- | ------------------ | -------------------- |
| `IS NULL` | `Exact` rows       | `AtMost` zones       |
| 其他谓词  | `AtMost` zones     | `AtMost` zones       |

这里的 `Exact` 仍然只是被该 index segment 覆盖的数据中、针对 `IS NULL` 谓词的精确答案。Dataset 层仍要处理 deletion、未覆盖 fragments 和 Row Address remap；这与上一篇中 BTree 的 `Exact` 边界相同。

旧格式可能没有 `null_bitmap`。更新旧 segment 时，新数据的 NULL 位置并不能补回旧数据缺失的信息；合并多个 segments 时，只要任意来源缺少 bitmap，合并结果也不能声称自己拥有完整 bitmap。源码会保留 `None` 并退回 `AtMost`，避免把部分 NULL 集合误报成 `Exact`。完整逻辑见 [`load`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zonemap.rs#L449-L487)、[`update`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zonemap.rs#L671-L702) 和 [`merge_zonemap_indices`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/zonemap.rs#L730-L785)。

## 7\. NaN 的保守处理

浮点数的 NaN 不能只靠普通 `min/max` 处理。Lance 为每个 zone 额外保存 `nan_count`：

- 查询值是 NaN 时，通过 `nan_count > 0` 判断 zone 是否可能命中；
- zone 同时包含有限值和 NaN 时，统计 `max` 可能表现为 NaN；
- 此时有限值范围的上界信息不够完整，源码会保守保留 zone，而不是根据一个不可靠的 `max` 剪掉它。

这可能增加 false positives，但仍保持 Zone Map 的核心不变量：**统计不充分时可以少剪枝，不能漏掉可能匹配的行。**

常见谓词的最终语义如下：

- `x = v`：`v` 位于 `[min,max]`，NaN 看 `nan_count`，返回 `AtMost`；
- `a <= x < b`：zone 与查询范围相交，返回 `AtMost`；
- `x IN (...)`：任意目标值可能落入 zone，返回 `AtMost`；
- `starts_with(x, prefix)`：zone 与 `[prefix,next_prefix)` 相交，返回 `AtMost`；
- `x IS NULL`：优先读取精确 NULL bitmap；bitmap 完整时返回 `Exact`，否则返回 `AtMost`。

## 8\. 与 BTree 的结构差异

BTree 与 Zone Map 都使用 `min/max`，但相似之处到这里就结束了：

| 维度           | BTree                                  | Zone Map                                 |
| -------------- | -------------------------------------- | ---------------------------------------- |
| 构建顺序       | 按 value 排序                          | 保留 Row Address 顺序                    |
| 索引内容       | 每行的 `value + id`，另有 page summary | 每个 zone 一条统计，不保存逐行 value/id  |
| 查询第二阶段   | 读取 leaf page 并在索引内精确过滤      | 读取原始数据并执行原谓词                 |
| 普通查询结果   | `Exact` row identifiers                | `AtMost` row ranges                      |
| 收益的关键条件 | 查询选择性与 leaf page IO              | 相邻 rows 的值域局部性                   |
| 主要成本       | 排序、索引存储、candidate leaf reads   | 统计存储、zone 比较、candidate data scan |

Zone Map 的优势是构建直接、索引紧凑，并且候选范围天然连续；代价是它把精确判断留给数据扫描。BTree 的索引更大、构建更重，但能在索引内部把 candidate pages 细化为准确行集合。

这也解释了为什么不能只按字段类型选索引。时间列如果按时间写入，Zone Map 的范围通常很紧；同一列完全乱序时，每个 zone 都可能横跨整个时间域，此时相同索引结构几乎无法剪枝。

## 9\. 总结

Lance Zone Map 的关键不是 `min/max` 两个字段本身，而是统计值与物理行范围之间的对应关系：

1. 训练扫描按 Row Address 顺序读取，不按索引列排序。
2. `ZoneTrainer` 按 `rows_per_zone` 切分，并保证 zone 不跨 fragment。
3. `zonemap.lance` 每个 zone 保存统计值和 `(fragment_id, start, length)`，不保存逐行 value/id。
4. 查询只把可能相交的 zone 转为 Row Address ranges，因此普通谓词返回 `AtMost`，必须回表 recheck。
5. false positive 只影响性能；所有 NaN、删除空洞和旧格式分支都优先保证不会 false negative。
6. 完整 NULL bitmap 能直接定位 NULL rows，所以 `IS NULL` 可以返回 `Exact`；信息不完整时则退回 `AtMost`。
7. 剪枝效果最终取决于数据局部性和 zone 粒度，而不是字段基数或索引名称。

下一篇将离开单个索引的文件结构，比较 BTree、Zone Map 与 Bitmap 在不同 predicate、selectivity、locality 和维护成本下的选择方法。
