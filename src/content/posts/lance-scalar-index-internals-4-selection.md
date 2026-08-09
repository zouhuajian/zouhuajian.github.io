---
author: "Jay H. Zou"
pubDatetime: 2026-08-08T18:39:08+08:00
title: "Lance Scalar Index 原理与源码分析（四）"
lang: zh-CN
tags:
  - Lance
  - Scalar Index
  - BTree
  - Zone Map
  - Bitmap
  - 源码分析
description: "BTree、Zone Map 与 Bitmap 的设计取舍"
---

> 本文源码基于 Lance `release/v9.0` 分支的提交 [`d293630df`](https://github.com/lance-format/lance/tree/d293630dff7a0393702e01a88a65da1a6591e867)。

前三篇分别介绍了 Scalar Index 的[公共查询框架](/posts/lance-scalar-index-internals-1/)、[BTree 的两层页结构](/posts/lance-scalar-index-internals-2-btree/)和 [Zone Map 的扫描剪枝](/posts/lance-scalar-index-internals-3-zonemap/)。还剩下一个更接近实际工程的问题：

> 面对一列真实数据，应该选择 BTree、Zone Map，还是 Bitmap？

最常见的回答是：

```text
高基数选 BTree
低基数选 Bitmap
有序数据选 Zone Map
```

这个口诀可以作为起点，却不能直接用于决策。`status` 只有 8 个值，如果查询总是命中 80% 的数据，Bitmap 即使能快速得到精确 row identifiers，后面的数据读取仍然接近全表扫描；`event_time` 基数很高，如果数据按时间聚簇，Zone Map 可能只用很小的统计结构就排除绝大多数 zones。

**字段类型和基数只描述了数据的一部分。真正决定索引收益的是：查询谓词、命中比例、数据局部性以及得到候选行之后还要付出的读取成本。**

## 1\. 先建立统一的成本模型

Lance 的 Python API 会解释不同索引的大致适用场景：BTree 适合唯一值多、每个值对应行数少的列；Bitmap 适合唯一值少、每个值对应行数多的列；Zone Map 很小，但要求数据至少近似有序。完整说明见 [`create_scalar_index`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/python/python/lance/dataset.py#L3363-L3386)。

需要注意，Rust 的 `ScalarIndexParams::default()` 选择 BTree，只是 API 默认值，不是 Lance 根据查询负载和数据分布自动完成了选择：

```rust
impl Default for ScalarIndexParams {
    fn default() -> Self {
        Self {
            index_type: BuiltinIndexType::BTree.as_str().to_string(), // API 默认值
            params: None,
        }
    }
}

pub fn for_builtin(index_type: BuiltinIndexType) -> Self {
    Self {
        index_type: index_type.as_str().to_string(), // 调用方明确指定类型
        params: None,
    }
}
```

完整实现见 [`ScalarIndexParams`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index-core/src/scalar.rs#L93-L120)。

选择索引时，可以把一次过滤查询粗略拆成四段成本：

```text
T_query
  ≈ T_index_lookup      找 page、zone 或 bitmap
  + T_rowset            生成、合并 row identifier 集合
  + T_data_read         根据结果读取实际数据列
  + T_recheck           对候选行重新执行原始谓词
```

- BTree 用页目录控制 `T_index_lookup`，再通过 `Exact` 结果减少 `T_data_read`，通常没有索引语义上的 residual recheck。
- Bitmap 可以很快找到并合并精确 row sets，但结果集很大时，`T_rowset` 和 `T_data_read` 仍然很高。
- Zone Map 的索引本身很小，却主动接受 false positives；它是否有效取决于能否显著减少 `T_data_read + T_recheck`。

![Scalar Index 选择时需要比较的端到端成本](/images/lance-scalar-index-internals/scalar-index-selection-cost.svg)

可编辑图源：[scalar-index-selection-cost.excalidraw](/images/lance-scalar-index-internals/scalar-index-selection-cost.excalidraw)

这带来一个重要结论：

> `Exact` 描述的是结果集合的正确性边界，不代表端到端查询一定快；`AtMost` 表示需要重新检查，也不代表它一定慢。

如果 Zone Map 用几十 KiB 的统计数据排除了 99% 的原始行，即使剩余 1% 需要 recheck，它也可能比读取大量 BTree pages、物化大量 row identifiers 更便宜。

## 2\. 三种索引分别省掉了什么

把前三篇的实现压缩到同一个视角，可以看到三种索引优化的不是同一个环节：

| 维度               | BTree                                    | Zone Map                            | Bitmap                          |
| ------------------ | ---------------------------------------- | ----------------------------------- | ------------------------------- |
| 物理模型           | 页目录 + 排序的 `<value, id>` leaf pages | 原始行序中每个 zone 的统计值        | 每个唯一值对应一个压缩 row set  |
| 查询粒度           | leaf page，再精确到行                    | 连续 row range                      | value posting                   |
| 常规结果           | `Exact`                                  | `AtMost`                            | `Exact`                         |
| 主要跳过对象       | 不相关 leaf pages 和不匹配行             | 不可能命中的 zones                  | 不相关 values                   |
| 对值的物理顺序     | 不敏感                                   | 高度敏感                            | 不敏感                          |
| 冷查询主要 IO      | 候选 leaf pages                          | 加载较小的 zone statistics 后读数据 | 命中 value 的 bitmaps           |
| 最容易被忽略的成本 | 匹配 pages、结果行数与离散回表           | false positives 与 residual scan    | bitmap 数量、集合合并与结果行数 |

### BTree：先缩小到 Page，再精确到 Row

BTree 的 `page_lookup.lance` 用 `min/max` 找到候选 leaf pages，`page_data.lance` 再对边界页中的 `values` 做精确过滤。它保存了逐行值和 id，所以等值、范围与 `IN` 都可以返回精确 row identifiers。

高基数点查通常只触碰少量 pages，这是 BTree 最自然的场景。反过来，如果一个低基数值横跨很多 pages，哪怕结果仍然是 `Exact`，索引也必须处理大量匹配 ids。完整结构见 [`BTree format`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/docs/src/format/index/scalar/btree.md#L1-L58)。

### Zone Map：证明一段数据不可能命中

Zone Map 不保存逐行值，只保存 zone 的 `min/max/null_count/nan_count` 和物理范围。普通等值与范围查询返回候选 row ranges，也就是 `AtMost`；当前格式具备完整 null bitmap 时，`IS NULL` 可以返回 `Exact`。

它的价值不是“统计值比 BTree 更快”，而是使用很小的统计结构跳过成片数据。若每个 zone 的 `min/max` 都覆盖查询值，Zone Map 就无法剪枝。完整语义见 [`Zone Map format`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/docs/src/format/index/scalar/zonemap.md#L1-L61)。

### Bitmap：直接取出某个 Value 的 Row Set

Bitmap 将“列值到行”的关系预先倒排：

```text
status = "new"      -> {r0, r3, r8, r11}
status = "running"  -> {r1, r4, r5}
status = "failed"   -> {r2, r9}
status = NULL       -> {r6, r7, r10}
```

查询 `status = 'failed'` 时直接读取一个 row set；查询 `status IN ('new', 'failed')` 时读取两个 row sets 并取并集。这里没有候选 page，也没有 zone 内 false positives。

Bitmap 的优势因此来自两个条件同时成立：

1. 查询只涉及少量 values；
2. 这些 values 对应的 row set 足够小，或者足够容易压缩、合并和消费。

第二个条件经常被“低基数适合 Bitmap”掩盖。

## 3\. Bitmap 的源码模型

Bitmap 没有单独占用一篇文章，这里补齐理解选择边界所需的实现。

### 一个文件、一行一个 Value

Lance 9.0 的 Bitmap segment 只有一个主要文件 `bitmap_page_lookup.lance`：

| Column    | 含义                                      |
| --------- | ----------------------------------------- |
| `keys`    | 索引列中的一个唯一值，NULL 也有自己的记录 |
| `bitmaps` | 该值对应的序列化 `RowAddrTreeMap`         |

格式定义见 [`Bitmap format`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/docs/src/format/index/scalar/bitmap.md#L1-L30)。文件中的第 N 行把一个 key 和它的压缩 row set 放在一起。

加载索引时，Lance 先读取所有 `keys`，建立 `value -> row offset` 的小目录；真正的 bitmap 仍然留在文件中，只有查询命中该 key 时才读取并放入 cache：

```rust
// [1] 打开 segment 中唯一的 lookup 文件
let file = store.open_index_file("bitmap_page_lookup.lance").await?;

// [2] 只扫描 keys，记录每个 value 位于文件的哪一行
let mut keys_stream = file
    .read_range_stream(0..file.num_rows(), Some(&["keys"]))
    .await?;

while let Some(batch) = keys_stream.try_next().await? {
    for idx in 0..batch.num_rows() {
        let key = ScalarValue::try_from_array(batch.column(0), idx)?;
        index_map.insert(key, row_offset);
        row_offset += 1;
    }
}

// [3] 查询命中后，只读取这个 value 对应的一行 bitmap
let row_offset = index_map.get(&key)?;
let batch = file
    .read_range(row_offset..row_offset + 1, Some(&["bitmaps"]))
    .await?;
```

代码经过裁剪；完整实现见 [`BitmapIndex::load` 与 `load_bitmap`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/bitmap.rs#L364-L486)。

![Bitmap 从 value 分组到精确 row set 查询](/images/lance-scalar-index-internals/bitmap-value-postings.svg)

可编辑图源：[bitmap-value-postings.excalidraw](/images/lance-scalar-index-internals/bitmap-value-postings.excalidraw)

### Equals 读取一个，Range 可能读取很多个

Bitmap 搜索路径直接揭示了它的成本边界：

```rust
match query {
    SargableQuery::Equals(value) => {
        // 一个值 -> 读取一个 bitmap
        row_ids = self.load_bitmap(value).await?;
    }
    SargableQuery::Range(start, end) => {
        // 先枚举范围内所有唯一值
        let keys = self.index_map.range((start, end));

        // 每个值读取一个 bitmap，再把所有 row sets 合并
        let bitmaps = load_in_parallel(keys).await?;
        row_ids = RowAddrTreeMap::union_all(&bitmaps);
    }
    SargableQuery::IsIn(values) => {
        // IN 中每个已存在的值读取一个 bitmap，再取并集
        let bitmaps = load_in_parallel(values).await?;
        row_ids = RowAddrTreeMap::union_all(&bitmaps);
    }
    SargableQuery::IsNull() => {
        row_ids = self.null_map.clone();
    }
}

SearchResult::Exact(row_ids)
```

完整控制流见 [`BitmapIndex::search`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/bitmap.rs#L636-L790)。

设一列共有 `D` 个唯一值，查询覆盖其中 `M` 个值，最终命中 `H` 行：

```text
Equals:  M ≈ 1
IN:      M ≈ IN 列表中实际存在的值数
Range:   M ≈ 范围内的 distinct values 数

索引读取成本随 M 增长
结果集合与回表成本随 H 增长
```

因此：

- `country = 'SG'` 可能只读取一个 bitmap；
- `country IN ('SG', 'JP')` 可能读取两个；
- 对高基数时间戳执行一个宽范围查询，可能枚举并合并大量 bitmaps，此时 BTree 更自然；
- 一个值若命中 80% 的行，bitmap lookup 仍然可以很快，但后续读取 80% 数据的成本没有消失。

### 构建也需要考虑 Cardinality

当前 Bitmap 构建请求要求输入按 value 排序。builder 一次只累积当前 value 的 row set，value 变化时就写出上一组，而不是把所有 values 的 postings 同时留在内存：

```rust
criteria: TrainingCriteria::new(TrainingOrdering::Values)
    .with_row_id();

for (value, row_id) in sorted_input {
    if value == current_value {
        current_bitmap.insert(row_id);
    } else {
        writer.emit(current_value, current_bitmap).await?;
        current_value = value;
        current_bitmap = RowAddrTreeMap::default();
        current_bitmap.insert(row_id);
    }
}
```

完整实现见 [`streaming_build_and_write`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/bitmap.rs#L1368-L1435)。这种流式实现控制了 postings 构建阶段的峰值内存，但不会消除排序成本，也不会改变“一种 value 对应一份 bitmap”的存储模型。

## 4\. 选择索引的七个维度

### 4.1 Predicate：查询到底在问什么

先统计真实过滤条件，而不是先看 schema：

| Predicate                   | 首先考虑     | 原因                                                 |
| --------------------------- | ------------ | ---------------------------------------------------- |
| 高基数 `Equals`             | BTree        | 通常只读取少量 leaf pages，精确定位少量行            |
| 低基数 `Equals` / 小型 `IN` | Bitmap       | 直接读取少量 value postings                          |
| 小范围 `Range`              | BTree        | 排序值域可以直接定位边界 pages                       |
| 有明显物理局部性的 `Range`  | Zone Map     | 用 min/max 排除连续 zones，不必保存逐行值            |
| 宽范围 `Range`              | scan/layout  | 三种索引都可能产生大量结果，先评估是否还值得走索引   |
| `IS NULL`                   | 结合其他查询 | 三种索引都能处理；不要只为一个高命中 NULL 条件套口诀 |

同一列也可能同时存在点查和范围查询。例如 `event_time` 既用于“查一个精确时间”，又用于“读取最近 24 小时”。不能只按其中一个低频查询选择索引。

### 4.2 Cardinality：有多少个不同值

Cardinality 决定索引内部需要维护多少个 key 或多大的排序值域：

- BTree 保存排序后的逐行 values 与 ids，高基数不会产生“一种 value 一个独立对象”的额外问题。
- Bitmap 为每个唯一值维护一个 posting，`D` 越大，keys 目录和小 postings 越多。
- Zone Map 每个 zone 只保存统计值，索引大小主要由总行数和 `rows_per_zone` 决定，而不是由 `D` 直接决定。

但 Cardinality 只回答“总共有多少种值”，不回答“查询命中多少行”。

### 4.3 Selectivity：一次查询命中多少行

本文用 `s = matching_rows / total_rows` 表示命中比例，避免“高选择性”这个容易产生歧义的说法。

假设两个字段都只有 8 个唯一值：

```text
status = 'failed'  -> s = 0.2%
status = 'active'  -> s = 85%
```

它们 Cardinality 相同，索引价值却完全不同：

- `failed` 的 Bitmap 只读取一个 posting，最终也只需要读取少量行；
- `active` 的 Bitmap 同样只读取一个 posting，却要物化并读取绝大多数行；
- 如果查询还需要大部分列，直接顺序 scan 可能更简单、更快。

所以“低基数选 Bitmap”必须补全为：**低基数、查询覆盖少量 values，并且这些 values 的实际命中比例仍然足够低。**

### 4.4 Locality：相邻 Rows 的值域是否集中

Locality 是 Zone Map 的决定性变量。考虑同一批 `event_time`：

```text
按时间追加：每个 zone 只覆盖几分钟
随机打散：  每个 zone 都可能覆盖一整天
```

两份数据的类型、行数、Cardinality 和查询完全相同，但范围查询的 Zone Map 行为相反：

- 前者可以只保留与目标时间段重叠的少量 zones；
- 后者的每个 `min/max` 都可能与查询相交，最终接近全量 recheck。

BTree 和 Bitmap 的索引 lookup 不依赖这种值局部性，但它们返回的 row identifiers 若在数据文件中高度离散，回表 IO 仍然可能变贵。因此 Locality 不只影响 Zone Map，也影响精确索引结果被消费时的成本。

### 4.5 Skew：热点值会不会吞掉大部分数据

低基数列经常伴随倾斜。不要只计算 `count(distinct col)`，还要看 Top-N values 的行数占比：

```text
active  85%
failed   0.2%
其他     14.8%
```

同一个 Bitmap 对冷值很有效，对热点值却可能没有端到端收益。`IN` 查询如果包含热点值，也会迅速变成大结果集。

NULL 也应当作为一个真实 value 分布来观察。Lance 9.0 的 Bitmap 保存 null posting；Zone Map 在具备完整 null bitmap 时也能为 `IS NULL` 返回 `Exact`。但如果 NULL 占 90%，结果精确仍然不代表读取便宜。

### 4.6 Maintenance：数据如何变化

索引不是创建一次就永远没有成本：

- BTree 和 Bitmap 的训练输入都按 value 排序；新 segment 仍要为新增数据完成对应构建。
- Zone Map 保留 Row Address 顺序，构建主要是顺序统计，但效果会随新数据的写入顺序变化。
- append、update 和 compaction 会改变 segment 与 fragment 的关系，仍要纳入索引更新和优化流程。

这里不应只比较首次构建时间。持续写入的数据集还要比较：增量构建频率、索引 segment 数量、合并/更新成本，以及索引维护与 compaction 的并发影响。

### 4.7 Cost：对象存储、Cache 与最终数据读取

三种结构在冷查询和热查询下的表现可能完全不同：

- BTree 冷查询读取候选 leaf pages；热查询可能直接命中 page cache。
- Bitmap 按 value 独立缓存 postings；重复查询同一 value 很容易受益，范围查询却可能触碰许多 postings。
- Zone Map 的 statistics 较小，但候选 zones 对应的数据读取和 residual filter 仍是主要成本。

因此评估时必须同时观察 index cache 冷、热两种状态。只跑第二次查询，容易把缓存收益误认为索引结构本身的收益。

## 5\. 六个典型场景

### 场景一：高基数 ID 点查

```sql
WHERE user_id = 184467
```

如果 `user_id` 接近唯一，单次查询只命中几行，优先考虑 BTree。Bitmap 会为大量唯一值维护大量小 postings；Zone Map 即使数据大致有序，也只能先返回所在 zone 的候选范围。

### 场景二：近似有序的时间范围

```sql
WHERE event_time >= A AND event_time < B
```

如果数据按时间追加，目标范围通常只与少量 zones 相交，Zone Map 可以用很小的索引换取很高的 scan pruning。若查询需要返回该时间段内的大部分行，BTree 的精确 row identifiers 未必比连续读取候选 zones 更划算。

### 场景三：乱序时间戳上的小范围

谓词与场景二完全相同，但数据被随机打散。此时 Zone Map 的 `min/max` 可能大量重叠，小范围查询更适合 BTree。**选择变化来自 Locality，不是字段从 timestamp 变成了别的类型。**

### 场景四：低基数状态列的冷值

```sql
WHERE status IN ('failed', 'cancelled')
```

如果这两个值合计只占少量数据，Bitmap 能读取两个 postings、取并集并返回精确结果。这是 Bitmap 最典型的优势场景。

### 场景五：低基数状态列的热点值

```sql
WHERE status = 'active'
```

如果 `active` 占 85%，Bitmap lookup 本身可能很快，但最终仍需读取 85% 的数据。此时应优先比较无索引 scan，甚至重新考虑数据布局和查询是否需要读取这么多列，而不是继续调 Bitmap 参数。

### 场景六：多个低基数字段组合过滤

```sql
WHERE dt = '2026-08-08'
  AND hour = 10
  AND cluster = 'c1'
  AND namespace = 'ns1'
```

这是一个容易误判的场景。每个单列谓词都可能命中大量数据，但它们的交集可能很小。仅看单列 Cardinality，无法推导组合过滤的收益。

## 6\. 一个反例：单列 BTree 不是多级分区裁剪

我曾在 audilog 数据上尝试为 `dt/hour/cluster/ns` 分别构建单列 BTree，希望获得类似 Iceberg 多级分区裁剪的效果。这个类比的问题不在结果正确性，而在物理工作方式不同：

```text
多级分区裁剪
  dt -> hour -> cluster -> namespace
  逐级排除目录或文件，前一层会缩小后一层的搜索空间

多个单列 Scalar Index
  dt        -> 一组 row identifiers
  hour      -> 一组 row identifiers
  cluster   -> 一组 row identifiers
  namespace -> 一组 row identifiers
  最后再对多个集合求交集
```

Lance 可以组合多个索引结果，所以这不是正确性缺陷。但如果每个单列 BTree 都先产生很大的精确集合，查询仍要付出多次 index lookup、row-set 物化与集合运算成本；求交集后留下的 rows 也不一定在物理文件中连续。

这个场景还有两个常见误区：

1. `dt/hour/cluster/ns` 的单列基数都不高，BTree 本身未必是合适的 posting 表达。
2. 即使换成 Bitmap，集合求交可能更自然，也不会自动得到分区层级、文件级裁剪和连续读取的物理效果。

正确的结论不是“日志表不能用 BTree”，也不是“把 BTree 全部换成 Bitmap”，而是：

> 先测量真实组合谓词的交集大小、各单列 row-set 成本和最终数据局部性，再判断应该使用 Scalar Index、调整数据布局，还是保留 scan。

## 7\. 决策矩阵

下面的表适合作为候选生成器，不应替代实际基准测试：

| 场景                               | 候选与主要风险                                                           |
| ---------------------------------- | ------------------------------------------------------------------------ |
| 高基数，点查或很小范围，命中比例低 | **BTree**：检查 leaf page IO 与离散回表                                  |
| 低基数，Equals/小型 IN，命中比例低 | **Bitmap**：检查热点值、posting 大小与集合合并                           |
| 范围查询，数据有明显物理局部性     | **Zone Map**：检查 candidate zone 比例与 false positives                 |
| 范围查询，数据乱序且目标范围较小   | **BTree**：检查匹配 pages 与结果行数                                     |
| 低基数，但目标值命中大部分数据     | **Scan / 调整 layout**：索引可能只加速 lookup，无法减少数据读取          |
| `IS NULL` 且 NULL 很少             | **结合主查询选型**：Bitmap、BTree、完整 null bitmap 的 Zone Map 均可精确 |
| 多个低基数字段组合，单列命中率都高 | **实测 Bitmap、scan 或 layout**：检查大 row sets、交集成本与物理离散度   |
| 持续 append，范围列保持近似有序    | **Zone Map**：检查后续写入是否持续保留 locality                          |
| 频繁 update/compaction             | **按端到端维护成本决定**：检查 segment 更新、索引优化与并发冲突          |

一个更短的决策顺序是：

```text
1. 先看 Predicate
2. 再看命中比例，而不是只看 Cardinality
3. 范围查询额外检查 Locality
4. 估算 index result 的大小和物理离散度
5. 与无索引 scan 做冷、热两组端到端对比
```

## 8\. 如何做一次可信的 A/B 评估

不要只比较“索引 lookup 用了多少毫秒”。建议至少记录下面这组数据：

| 类别     | 指标                                                         |
| -------- | ------------------------------------------------------------ |
| 数据分布 | 总行数、distinct values、Top-N 占比、NULL 占比、zone overlap |
| 查询负载 | Predicate 类型、典型参数、命中行数/比例、组合条件            |
| 索引成本 | 构建时间、索引大小、增量更新时间、segment 数量               |
| 查询成本 | 冷/热 p50、p95、`parts_loaded`、候选/精确 row-set 大小       |
| 数据读取 | 实际读取 rows/bytes、fragment 分布、residual filter 成本     |

评估步骤可以保持很直接：

1. 从真实 workload 选出 Top-N 过滤模板，不使用人为构造的“漂亮查询”。
2. 为每个模板固定一组代表性参数，包括冷值、热点值和边界范围。
3. 分别测试无索引、BTree、Zone Map、Bitmap 中合理的候选，不要求三种都建。
4. 每种候选同时测试冷 cache 和热 cache，并重复到延迟分布稳定。
5. 加入一次 append/update 后重新测试，确认维护后的索引覆盖和性能仍然成立。
6. 最后比较端到端读取，而不是只比较 index operator。

Lance 的执行指标中，`parts_loaded` 可以帮助观察冷查询实际加载了多少 BTree pages 或 Bitmap postings；但它不能单独代表总成本。一个查询加载 1 个 bitmap，却返回 8000 万行，仍然可能比 scan 更慢。

## 9\. 总结

BTree、Zone Map 与 Bitmap 的核心取舍可以归纳为三句话：

1. **BTree 用排序的逐行值换取精确、通用的点查和范围查找。**
2. **Zone Map 用数据局部性换取轻量 scan pruning，普通查询接受 false positives。**
3. **Bitmap 用每个唯一值的压缩 row set 换取低基数 Equals/IN 的快速精确集合运算。**

真正的选择顺序不是“字段类型 → 索引类型”，而是：

```text
真实 Predicate
  -> 命中比例与数据倾斜
  -> 物理 Locality
  -> index result 的大小与离散度
  -> 冷/热查询和维护成本
  -> 与无索引 scan 的端到端对比
```

如果只能保留一个判断原则，那就是：

> 先判断索引能否显著减少最终数据读取，再讨论它查找 row identifiers 的速度。

至此，这组文章完成了从公共索引框架、BTree、Zone Map 到选择方法的完整路径：

1. [从 Manifest 到查询执行](/posts/lance-scalar-index-internals-1/)
2. [BTree 的两层页结构](/posts/lance-scalar-index-internals-2-btree/)
3. [Zone Map 的统计结构与扫描剪枝](/posts/lance-scalar-index-internals-3-zonemap/)
4. BTree、Zone Map 与 Bitmap 的设计取舍
