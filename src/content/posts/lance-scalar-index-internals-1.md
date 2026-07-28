---
author: "Jay H. Zou"
pubDatetime: 2026-07-28T17:12:31+08:00
title: "Lance Scalar Index 原理与源码分析（一）：从 Manifest 到查询执行"
tags:
  - Lance
  - Scalar Index
  - 源码分析
description: "基于 Lance 9.0 源码，从索引的基本模型出发，分析 Scalar Index 如何通过 Manifest、Index Segment、谓词规划和 Fragment 级读取加速查询。"
---

> 本文源码基于 Lance `release/v9.0` 分支的提交 [`d293630df`](https://github.com/lance-format/lance/tree/d293630dff7a0393702e01a88a65da1a6591e867)。文中的代码片段保留了关键控制流，省略了日志、错误处理和无关分支。

## 1\. Lance Index 的基本模型

Lance [Index Format](https://lance.org/format/index/) 对索引的定义很重要：**索引是建立在 table row identifier 之上的独立、冗余数据结构**。它不是数据文件内部必须存在的一层，也不是 Dataset 的事实来源。

这一定义带来三个直接结果：

1. Dataset 没有索引也能正常读取；索引只负责加速。
2. 索引查询主要返回 row identifier，而不是用户最终需要的数据。
3. 索引失效或覆盖不完整时，reader 必须回退到数据文件，保证查询结果不丢失。

Lance 将索引分为三类：

| 类型         | 输入                   | 输出                       | 典型用途                     |
| ------------ | ---------------------- | -------------------------- | ---------------------------- |
| Scalar Index | 等值、范围、集合等谓词 | row identifiers 或候选集合 | BTree、Bitmap、Zone Map 等   |
| Vector Index | query vector           | row identifiers 与距离     | ANN 检索                     |
| System Index | Dataset 内部状态       | 内部映射或维护信息         | compaction 后的 row id remap |

本文只讨论 Scalar Index。对于下面的过滤条件：

```sql
event_time >= TIMESTAMP '2026-07-28 10:00:00'
AND event_time < TIMESTAMP '2026-07-28 11:00:00'
```

Scalar Index 的职责不是返回完整记录，而是回答：

```text
哪些 row identifier 一定匹配？
哪些 row identifier 可能匹配？
这份判断覆盖了哪些 fragments？
```

Dataset reader 再结合当前版本的数据、删除记录以及未覆盖 fragments，生成最终结果。

![Lance Index 的独立索引层与 Scalar Index 查询边界](/images/lance-scalar-index-internals/lance-index-overview.svg)

可编辑图源：[lance-index-overview.excalidraw](/images/lance-scalar-index-internals/lance-index-overview.excalidraw)

官网列出的四个设计特征，正好解释了后面的实现：

| 设计特征                  | 对查询路径的影响                                                           |
| ------------------------- | -------------------------------------------------------------------------- |
| 按需加载                  | 打开 Dataset 时不必加载索引；只有 planner 选中索引后才读取对应文件         |
| 渐进加载                  | 查询只加载需要的索引页或索引结构，而不是把完整索引读入内存                 |
| 一个 segment 覆盖多个碎片 | 索引文件可以比 fragment 粒度更大，减少需要打开和查询的索引文件数量         |
| segment 写入后不可变      | 新数据或更新通过新文件和新 Manifest 发布，旧文件可以安全地被缓存和并发读取 |

### 贯穿示例

下面这个 Dataset 会贯穿全文：

| Fragment | 数据                             | `event_time_idx` |
| -------- | -------------------------------- | ---------------- |
| F0       | `r0=09:30`，`r1=10:15`           | segment A        |
| F1       | `r0=10:40 (deleted)`，`r1=12:10` | segment A        |
| F2       | `r0=10:55`                       | segment B        |
| F3       | `r0=10:20`，追加后尚未更新索引   | 未覆盖           |

文中用 `F0:r1` 表示 fragment 0 中的第 1 行，便于直接观察索引结果如何落到具体 fragment。执行层仍会通过 `RowIdSequence` 将通用的 row identifier mask 转换成实际读取范围。

上述查询的正确结果是：

```text
F0:r1 = 10:15
F2:r0 = 10:55
F3:r0 = 10:20
```

F1:r0 在索引创建后被删除，所以可能仍被索引命中；F3 没有被索引覆盖，却包含匹配数据。这两个边界决定了 Scalar Index 不能单独保证查询正确性。

## 2\. Manifest 中的索引入口

索引文件位于 Dataset 的 `_indices/{uuid}/` 目录，但 reader 不会扫描这个目录来判断当前版本可以使用哪些索引。原因很直接：目录里可能同时存在旧版本索引、未提交文件和当前版本索引，仅凭文件列表无法确定可见性。

当前 Dataset version 通过 Manifest 找到自己的索引元数据：

```text
Manifest file
├── Manifest
│   └── index_section: byte offset
└── IndexSection
    └── IndexMetadata[]

_indices/{uuid}/
└── index-specific files
```

这里先看经过裁剪的 protobuf 定义。注释中的 `[1]` 到 `[5]` 是阅读这段代码时真正需要关注的字段：

```protobuf
message Manifest {
  optional uint64 index_section = 6; // [1] Byte offset of IndexSection
}

message IndexMetadata {
  UUID uuid = 1;                     // [2] One physical index segment
  repeated int32 fields = 2;         // [3] Indexed field IDs
  string name = 3;                   // [4] Logical index name
  uint64 dataset_version = 4;        // Version used to build this segment
  bytes fragment_bitmap = 5;         // [5] Covered fragment IDs
  google.protobuf.Any index_details = 6;
}

message IndexSection {
  repeated IndexMetadata indices = 1;
}
```

- `[1]` 不是索引目录，而是 `IndexSection` 在 Manifest 文件中的位置。索引元数据因此可以按需读取。
- `[2]` 的 `uuid` 定位 `_indices/{uuid}/`，对应一个已经写入的物理 segment。
- `[3]` 和 `index_details` 告诉 reader 索引列、索引类型及其格式参数。
- `[4]` 是用户和 planner 看到的逻辑索引名。多个物理 segment 可以属于同一个逻辑索引。
- `[5]` 是正确性的关键：它明确这份 segment 对哪些 fragments 有效。

完整定义见 [`Manifest`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/protos/table.proto#L36-L99) 和 [`IndexMetadata`、`IndexSection`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/protos/table.proto#L237-L307)。

写入 Manifest 时，Lance 先写 `IndexSection`，再把它的位置写进 Manifest：

```rust
if let Some(indices) = indices.as_ref() {
    let section = pb::IndexSection {
        indices: indices.iter().map(|index| index.into()).collect(),
    };

    let pos = writer.write_protobuf(&section).await?; // [1] Persist metadata
    manifest.index_section = Some(pos);               // [2] Publish the offset
}

writer.write_struct(manifest).await                   // [3] Commit the manifest
```

这三步建立了索引可见性的边界：

- `[1]` 只表示索引元数据已经写入。
- `[2]` 将这批元数据接入即将发布的 Dataset version。
- `[3]` 对应的新 Manifest 成功提交后，reader 才能从该版本发现索引。

读取过程正好相反：从 Manifest 取出 offset，再读取对应的 `IndexSection`。

```rust
if let Some(pos) = manifest.index_section.as_ref() {
    let section =
        read_index_section(object_store, &location.path, location.size, *pos).await?;

    section
        .indices
        .into_iter()
        .map(IndexMetadata::try_from)
        .collect()
} else {
    Ok(vec![])
}
```

完整实现见 [`read_manifest_indexes` 与 `do_write_manifest`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-table/src/io/manifest.rs#L111-L177)。

因此，`_indices/{uuid}` 中存在文件并不代表它已经对查询可见。**Manifest 才是当前 Dataset version 使用哪些索引 segment 的权威入口。**

## 3\. 逻辑索引、物理 Segment 与覆盖范围

`event_time_idx` 对外是一个索引名，但在示例的 Manifest 中，它由两个物理 segment 组成：

```text
name = event_time_idx, uuid = A, fragment_bitmap = {F0, F1}
name = event_time_idx, uuid = B, fragment_bitmap = {F2}
```

F3 不在任何 `fragment_bitmap` 中。索引没有返回 F3 的 row identifier，只能说明索引不知道 F3 的结果，不能说明 F3 没有匹配行。

![Manifest、Index Segment 与 Fragment Coverage](/images/lance-scalar-index-internals/manifest-index-segments.svg)

可编辑图源：[manifest-index-segments.excalidraw](/images/lance-scalar-index-internals/manifest-index-segments.excalidraw)

源码将 segment 加载分成两个动作：先找出当前 Dataset 仍可使用的元数据，再决定打开一个物理索引还是组装一个逻辑索引。

```rust
let usable_indices = dataset
    .load_indices_by_name(index_name)
    .await?
    .into_iter()
    .filter(|index| index_intersects_dataset(index, dataset)) // [1]
    .collect::<Vec<_>>();

let indices = load_named_scalar_segments(dataset, column, index_name).await?;
match indices.len() {
    0 => return Err(/* no usable segment */),
    1 => dataset.open_scalar_index(column, &indices[0].uuid, metrics).await, // [2]
    _ => {
        let segments = try_join_all(indices.iter().map(|index| {
            dataset.open_scalar_index(column, &index.uuid, metrics)
        }))
        .await?;

        Ok(Arc::new(LogicalScalarIndex::try_new(
            index_name.to_string(),
            column.to_string(),
            segments,
        )?)) // [3]
    }
}
```

- `[1]` 排除与当前 Dataset fragment 集合完全没有交集的旧 segment。
- `[2]` 只有一个 segment 时直接使用，避免增加额外包装。
- `[3]` 有多个 segment 时才构造 `LogicalScalarIndex`。这个类型的目的不是定义新的物理格式，而是给上层提供一个统一查询入口。

`LogicalScalarIndex` 收到查询后，会并行搜索所有 segment，再合并 row identifier 集合：

```rust
let results = try_join_all(
    self.segments
        .iter()
        .map(|segment| segment.search(query, metrics)), // [1] Fan out
)
.await?;

combine_search_results(results)                        // [2] Union results
```

对 planner 来说，最终看到的仍然是：

```text
event_time_idx
├── segment A -> F0, F1
└── segment B -> F2

effective coverage = {F0, F1, F2}
```

这种设计让新写入的数据可以进入新的不可变 segment，而不必原地修改旧索引。代价是查询阶段必须同时维护“命中结果”和“有效覆盖范围”。

完整实现见 [`LogicalScalarIndex、segment 加载与打开`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance/src/index/scalar_logical.rs#L138-L355)。

## 4\. 谓词规划与索引查询

用户提交的是普通 SQL 过滤条件，索引实现需要的却是 BTree range、Bitmap equality 或 Zone Map interval 等具体查询。两者之间由 planner 完成转换。

在阅读源码前，先明确三个名称：

- `ScalarIndexInfo`：规划阶段的索引目录。它保存每个字段可用的 query parser，以及每个 `(column, index_name)` 的 fragment coverage。
- `index_query`：能够交给一个或多个 Scalar Index 执行的部分。
- `refine_expr`：索引无法完整判断、读取数据后仍需计算的部分。`full_expr` 则保留原始完整谓词，用于候选结果或未覆盖 fragment 的 recheck。

`create_filter_plan` 的核心逻辑只有几步：

```rust
let logical_expr = self.optimize_expr(filter)?; // [1] Normal filter expression

let indexed_expr =
    apply_scalar_indices(logical_expr.clone(), index_info)?; // [2] Split the filter

let mut skip_recheck = false;
if let Some(scalar_query) = indexed_expr.scalar_query.as_ref() {
    skip_recheck = !scalar_query.needs_recheck(); // [3]
}

FilterPlan {
    index_query: indexed_expr.scalar_query, // Search the index first
    refine_expr: indexed_expr.refine_expr,  // Evaluate remaining predicates
    full_expr: Some(logical_expr),          // Recheck candidates or fallback rows
    skip_recheck,
}
```

- `[1]` 得到与索引无关的逻辑表达式。
- `[2]` 让已注册的 query parser 尝试识别可以索引化的部分。planner 不需要把 BTree、Bitmap、Zone Map 的查询规则硬编码在公共流程中。
- `[3]` 只有索引能够精确回答整条谓词时，reader 才可能跳过数据级 recheck。

完整规划入口见 [`Scanner::create_filter_plan`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance/src/dataset/scanner.rs#L2445-L2502) 和 [`PlannerIndexExt::create_filter_plan`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/expression.rs#L2337-L2378)。

为了组合多个索引查询，Lance 将它们表示成一棵很小的布尔树：

```rust
pub enum ScalarIndexExpr {
    Not(Box<Self>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Query(ScalarIndexSearch), // One query against one named index
}
```

示例中的两个比较条件最初是：

```text
AND
├── Query(event_time_idx, event_time >= 10:00)
└── Query(event_time_idx, event_time < 11:00)
```

`ScalarIndexExpr::optimize` 会把同一索引上的相邻范围合并，避免对同一个索引做两次独立搜索：

```text
Query(event_time_idx, range = [10:00, 11:00))
```

这棵树不是为了抽象索引文件格式，而是为了让 `AND`、`OR`、`NOT` 能直接作用在不同索引返回的 row identifier 集合上。完整实现见 [`ScalarIndexExpr 与 range optimize`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/expression.rs#L1455-L1570)。

## 5\. 精确结果与候选结果

不同索引的粒度不同。BTree 可能精确定位满足范围的 row identifiers；Zone Map 只能判断某个区域“可能包含”匹配值。公共执行层不能把这两种结果当成同一种保证。

单个 Scalar Index 通过 `SearchResult` 明确表达自己的结果精度：

```rust
pub enum SearchResult {
    Exact(NullableRowAddrSet),   // [1] This set is the complete answer
    AtMost(NullableRowAddrSet),  // [2] The answer is a subset of this set
    AtLeast(NullableRowAddrSet), // [3] This set is part of the answer
}
```

- `[1] Exact`：集合内的行全部匹配，集合外的行全部不匹配。
- `[2] AtMost`：真实结果一定包含在候选集合中，但候选集合可能有假阳性，必须 recheck。
- `[3] AtLeast`：集合内一定匹配，但集合外也可能存在匹配行。当前普通 Scalar Index 不直接返回它，但 `NOT(AtMost(...))` 等布尔运算可能产生这种结果。

假设精确索引返回：

```text
Exact { F0:r1, F1:r0, F2:r0 }
```

这表示三个 row addresses 满足索引中的 `[10:00, 11:00)` 范围。它并不表示三行在当前 Dataset version 中都有效：F1:r0 仍要经过 deletion vector。

粗粒度索引则可能返回：

```text
AtMost {
  F0:r0,  # 09:30, false positive
  F0:r1,
  F1:r0,
  F2:r0
}
```

reader 可以安全跳过候选集合之外的行，但必须对集合内的行重新计算 `full_expr`，才能移除 F0:r0。

执行层把结果统一成一个集合区间 `[lower, upper]`：

```text
Exact(M)   -> lower = M, upper = M
AtMost(M)  -> lower = ∅, upper = M
AtLeast(M) -> lower = M, upper = ALL
```

`lower` 中的行一定匹配，`upper` 外的行一定不匹配。`lower` 与 `upper` 之间才是需要读取数据重新判断的区域。`AND`、`OR`、`NOT` 只需对这两个边界执行集合运算，就能保留结果精度。

完整实现见 [`SearchResult`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index-core/src/scalar.rs#L322-L370) 和 [`IndexExprResult 的 lower / upper 模型`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-select/src/result.rs#L1-L160)。

## 6\. Fragment 级查询执行

完成规划后，v2 storage 的执行路径分为两个职责清晰的阶段：

```text
ScalarIndexExec  -> 搜索索引，输出 row mask 与 coverage
FilteredReadExec -> 结合当前 fragments、deletion vector 和 filter 读取数据
```

`ScalarIndexExec` 的核心代码说明了它的边界：

```rust
let query_result =
    expr.evaluate(dataset.as_ref(), &metrics).await?; // [1] Search indices

let covered = Self::fragments_covered_by_index_query(
    &expr,
    dataset.as_ref(),
)
.await?;                                               // [2] Keep coverage

query_result.serialize(&covered, result_format)        // [3] No user data read
```

- `[1]` 执行前一节生成的 `ScalarIndexExpr`。
- `[2]` 单独计算查询中所有索引共同适用的 fragments。
- `[3]` 输出序列化后的 mask 和 coverage。这里不会读取 `event_time` 或用户投影列。

完整实现见 [`ScalarIndexExec`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance/src/io/exec/scalar_index.rs#L178-L284)。

`FilteredReadExec` 才负责把 mask 变成物理读取范围。进入下面的代码前，`to_read` 已经根据当前 fragment 的 deletion vector 排除了删除行：

```rust
if evaluated_index.applicable_fragments.contains(fragment_id) { // [1]
    let index_result = &evaluated_index.index_result;

    if index_result.is_exact() {
        let indexed_ranges =
            row_id_sequence.mask_to_offset_ranges(&index_result.upper);
        let matched = intersect_ranges(&to_read, &indexed_ranges); // [2]
        fragments_to_read.insert(fragment_id, matched);
    } else if !index_result.is_at_least() {
        let candidate_ranges =
            row_id_sequence.mask_to_offset_ranges(&index_result.upper);
        let matched = intersect_ranges(&to_read, &candidate_ranges); // [3]
        fragments_to_read.insert(fragment_id, matched);
    }
    // The AtLeast / limit-pushdown branch is omitted here.
} else if !only_indexed_fragments {
    fragments_to_read.insert(fragment_id, to_read); // [4] Fallback read
}
```

- `[1]` 先判断索引是否对当前 fragment 有效，而不是直接使用全局 row mask。
- `[2]` 精确结果只读取 index mask 与当前 live rows 的交集。
- `[3]` `AtMost` 或 refined result 按 `upper` 读取候选范围，并在后续执行完整过滤条件。
- `[4]` 未覆盖 fragment 保留完整 live ranges。默认查询必须读取它们，否则会漏掉新追加数据。

完整实现见 [`FilteredReadExec::apply_index_to_fragment`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance/src/io/exec/filtered_read.rs#L872-L940)。

回到贯穿示例，最终的 fragment 级读取计划是：

| Fragment | Coverage | 实际处理                                   | 输出  |
| -------- | -------- | ------------------------------------------ | ----- |
| F0       | 已覆盖   | index mask 只保留 r1                       | 10:15 |
| F1       | 已覆盖   | index 命中 r0，但 deletion vector 将其排除 | 无    |
| F2       | 已覆盖   | index mask 只保留 r0                       | 10:55 |
| F3       | 未覆盖   | 读取完整 live ranges，再执行 `full_expr`   | 10:20 |

![Scalar Index Fragment 级查询执行](/images/lance-scalar-index-internals/query-execution.svg)

可编辑图源：[query-execution.excalidraw](/images/lance-scalar-index-internals/query-execution.excalidraw)

最终返回：

```text
F0:r1 = 10:15
F2:r0 = 10:55
F3:r0 = 10:20
```

这里可以看到完整的正确性边界：**Scalar Index 只缩小可能需要读取的数据范围；当前 Dataset version、deletion vector、coverage 和 residual filter 共同决定最终结果。**

> Legacy storage 使用 `MaterializeIndexExec -> Take` 读取索引命中行，并通过 `UnionExec` 合并未覆盖 fragments。算子结构不同，但 coverage、deletion 和 recheck 的语义一致。

## 7\. 总结

Lance Scalar Index 的公共框架可以归纳为五步：

1. Manifest 的 `IndexSection` 决定当前 Dataset version 可以看到哪些 index segments。
2. 同名的多个物理 segment 在查询时组成一个 `LogicalScalarIndex`。
3. `fragment_bitmap` 明确索引覆盖范围，未覆盖 fragments 必须 fallback read。
4. planner 将普通谓词拆成 `index_query`、`refine_expr` 和用于 recheck 的 `full_expr`。
5. `FilteredReadExec` 将索引 mask、当前 live rows 和 filter 合并成最终读取计划。

这套框架不依赖具体索引格式。BTree、Zone Map 和 Bitmap 的主要差异，是索引文件如何组织、哪些谓词可以下推，以及返回精确 row set 还是候选范围。

下一篇将进入 BTree 的物理实现：键值与 row identifier 如何写入 page，`page_lookup` 和 `page_data` 如何配合，以及等值和范围查询实际读取哪些索引数据。
