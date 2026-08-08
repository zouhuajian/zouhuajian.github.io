---
author: "Jay H. Zou"
pubDatetime: 2026-07-29T21:23:23+08:00
title: "Lance Scalar Index 原理与源码分析（二）"
lang: zh-CN
tags:
  - Lance
  - Scalar Index
  - BTree
  - 源码分析
description: "BTree 的两层页结构"
---

> 本文源码基于 Lance `release/v9.0` 分支的提交 [`d293630df`](https://github.com/lance-format/lance/tree/d293630dff7a0393702e01a88a65da1a6591e867)。文中的代码片段保留关键控制流，并在注释中标出阅读重点。

[上一篇](/posts/lance-scalar-index-internals-1/) 介绍了 Scalar Index 的公共框架：Manifest 发现索引、多个 immutable segments 组成逻辑索引，`fragment_bitmap` 划定覆盖范围，执行器再把索引结果转换成数据读取。本篇只进入一个 BTree segment，回答两个问题：

1. 排序后的 `<value, id>` 如何变成 `page_lookup.lance` 和 `page_data.lance`？
2. 一次范围查询如何只读取少量 leaf pages，并返回精确的 row identifiers？

## 1\. BTree 的设计目标

Lance 官方将 [BTree Index](https://lance.org/format/index/scalar/btree/) 定义为一种 **two-level structure**。这个名称容易让人联想到传统数据库的 B+Tree，但 Lance 9.0 的实际结构更直接：

```text
page_lookup.lance
  小型页目录，记录每个 leaf page 的 min / max / null_count / page_idx
  查询时整体加载，通过二分和范围判断选择候选页

page_data.lance
  保存所有 leaf pages
  每页包含排序后的 values 和对应 ids
  只按需读取命中的页
```

因此，它更接近“**内存可缓存的页目录 + 磁盘上的 FlatIndex pages**”，而不是逐层读取多个树节点。

这种设计在两个极端之间取平衡：

- 把全部 `<value, id>` 常驻内存，查询很快，但索引越大，内存成本越难控制。
- 把完整多层树放在对象存储，每次查找都可能引入多次串行随机 IO。

Lance 只把每个 leaf page 的边界保留在上层目录中。查询先用小目录将搜索范围缩小到少量 pages，再读取其中的完整值。

### 贯穿示例

为了看清页边界，下面将每页缩小为 4 行；Lance 的默认值是 4096 行。

构建前，`score` 分散在原始数据的不同位置。BTree 按 `score` 排序后得到：

| Page | 排序后的 `(score, id)`                | 页范围     |
| ---- | ------------------------------------- | ---------- |
| P0   | `(10,r7) (12,r1) (15,r9) (18,r3)`     | `[10, 18]` |
| P1   | `(20,r4) (22,r11) (25,r0) (28,r8)`    | `[20, 28]` |
| P2   | `(30,r10) (32,r2) (35,r5) (40,r6)`    | `[30, 40]` |
| P3   | `(45,r12) (48,r13) (50,r14) (55,r15)` | `[45, 55]` |

本文跟踪这个查询：

```sql
WHERE score >= 17 AND score < 33
```

上层目录会把 P0、P1、P2 选出来，跳过 P3；leaf search 再得到：

```text
P0 -> r3
P1 -> r4, r11, r0, r8
P2 -> r10, r2

Exact {r0, r2, r3, r4, r8, r10, r11}
```

这里的关键不是页目录能直接给出答案，而是它能用很小的内存结构决定“哪些 leaf pages 值得读取”。

## 2\. 构建：从排序数据到固定大小的 Page

BTree 的输入不是原始 fragment 顺序，而是按索引列排序的 `<value, row identifier>` 流。

索引插件先声明训练数据必须满足两个条件：

```rust
impl BTreeTrainingRequest {
    pub fn new(parameters: BTreeParameters) -> Self {
        Self {
            parameters,
            criteria: TrainingCriteria::new(TrainingOrdering::Values)
                .with_row_id(), // [1] 每个 value 必须带回原始行的 id
        }
    }
}
```

通用训练流程看到 `TrainingOrdering::Values` 后，会在扫描阶段完成排序：

```rust
if TrainingOrdering::Values == criteria.ordering {
    scan.order_by(Some(vec![
        ColumnOrdering::asc_nulls_first(column.to_string()), // [2] value 升序，NULL 在前
    ]))?;
}

if criteria.needs_row_ids {
    scan.with_row_id(); // [3] 排序不能丢失 value 与原始行的对应关系
}
```

`[1]` 到 `[3]` 共同保证训练器收到的是：

```text
(NULL, id_x), ..., (10, r7), (12, r1), ..., (55, r15)
```

而不是按 fragment 或原始写入顺序排列的数据。完整入口见 [`BTreeTrainingRequest`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/btree.rs#L3162-L3207) 和 [`scan_training_data`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance/src/index/scalar.rs#L126-L164)。

排序流进入 `train_btree_index` 后，被重新切成固定行数的 batches。每个 batch 就是一个 leaf page：

```rust
let mut source = chunk_concat_stream(
    batches_source,
    batch_size as usize, // [1] 默认 4096 行/page
);

while let Some(batch) = source.try_next().await? {
    encoded_batches.push(
        train_btree_page(
            batch,
            batch_idx,              // [2] page_idx
            page_data_writer,
            flat_schema.clone(),
        )
        .await?,
    );
    batch_idx += 1;
}

let lookup = btree_stats_as_batch(encoded_batches, &value_type)?; // [3]
page_lookup_writer.write_record_batch(lookup).await?;
```

- `[1]` 决定一个 leaf page 包含多少个排序值。
- `[2]` 同时是这个 page 在 `page_data.lance` 中的逻辑编号。
- `[3]` 将每页的统计信息组装成上层目录。

构建参数在 API 中叫 `zone_size`，写入 `page_lookup.lance` schema metadata 后叫 `batch_size`。两者在这里表达同一件事：**每个 BTree leaf page 的目标行数**，默认是 4096。

![BTree 从排序数据到两层页结构](/images/lance-scalar-index-internals/btree-build-layout.svg)

可编辑图源：[btree-build-layout.excalidraw](/images/lance-scalar-index-internals/btree-build-layout.excalidraw)

## 3\. 两个文件分别保存什么

`train_btree_page` 同时产生 leaf data 和 page summary：

```rust
async fn train_btree_page(
    batch: RecordBatch,
    page_idx: u32,
    writer: &mut dyn IndexWriter,
    schema: Arc<Schema>,
) -> Result<EncodedBatch> {
    let stats = analyze_batch(&batch)?; // [1] 第一个 value、最后一个 value、NULL 数量

    let trained = RecordBatch::try_new(
        schema,
        vec![
            batch.column_by_name(VALUE_COLUMN_NAME).expect_ok()?.clone(), // [2] 完整排序值
            batch.column_by_name(ROW_ID).expect_ok()?.clone(),            // [3] 对应的行标识
        ],
    )?;
    writer.write_record_batch(trained).await?; // [4] 立即写入 page_data.lance

    // [5] 这里只返回摘要；所有 page 完成后才统一写入 page_lookup.lance
    Ok(EncodedBatch { stats, page_number: page_idx })
}
```

代码省略了少量 schema 处理，完整实现见 [`analyze_batch`、`train_btree_page` 与 `btree_stats_as_batch`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/btree.rs#L2351-L2463)。

最终两个文件的 schema 是：

### `page_lookup.lance`

| Column       | Type         | 作用                                 |
| ------------ | ------------ | ------------------------------------ |
| `min`        | indexed type | page 中的最小值                      |
| `max`        | indexed type | page 中的最大值                      |
| `null_count` | `UInt32`     | page 中 NULL 的数量                  |
| `page_idx`   | `UInt32`     | 指向 `page_data.lance` 中的逻辑 page |

贯穿示例对应的目录是：

```text
min  max  null_count  page_idx
10   18   0           0
20   28   0           1
30   40   0           2
45   55   0           3
```

### `page_data.lance`

| Column   | Type         | 作用                             |
| -------- | ------------ | -------------------------------- |
| `values` | indexed type | 这个 page 内的索引列值           |
| `ids`    | `UInt64`     | 每个 value 对应的 row identifier |

`page_lookup.lance` 只保存每页一行；`page_data.lance` 保存每个值。因此前者适合整体加载和缓存，后者只在查询命中相应 page 时读取。

这里还有一个容易混淆的细节：`page_data.lance` 在构建时按 value 排序，但当前 leaf 实现不是在页内再做一次二分查找。page 被加载成 `FlatIndex` 后，会使用 Arrow 表达式对最多 `batch_size` 个值做向量化过滤。排序的主要作用是形成边界紧凑、可以被上层目录裁剪的 pages。

## 4\. 第一层：`BTreeLookup` 如何选择 Page

打开 BTree segment 时，Lance 会读取整个 `page_lookup.lance`，并保留为一个 `RecordBatch`：

```rust
pub struct BTreeLookup {
    batch: RecordBatch,        // min | max | null_count | page_idx，按 min 排序
    null_pages: Vec<u32>,      // 含部分 NULL 的 pages
    all_null_pages: Vec<u32>,  // 全部为 NULL 的 pages
    search_start: usize,       // 第一个可参与普通 value 查询的 page
}
```

这就是“两层结构”的第一层。它不是包含 child pointers 的磁盘树节点集合，而是一个按 `min` 排序的页目录。等值和范围查询都先在 `min` 列上二分找到起始窗口，再使用 `max` 排除不可能命中的 pages。

范围查询的核心判断可以压缩为：

```rust
// [1] 先在按 min 排序的目录中找到可能重叠的连续窗口
let start = first_page_that_may_reach(lower_bound);
let end = first_page_whose_min_is_after(upper_bound);

for page in start..end {
    if page.max < lower_bound {
        continue; // [2] 与查询范围不相交
    }

    if page.min >= lower_bound && page.max <= upper_bound {
        matches.push(Matches::All(page.page_idx));  // [3] 整页都满足
    } else {
        matches.push(Matches::Some(page.page_idx)); // [4] 只有部分行可能满足
    }
}
```

这段代码是 [`pages_between`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/btree.rs#L1056-L1187) 的控制流摘要。实际实现还要精确处理 `Included`、`Excluded`、无界范围和 NULL。

对查询 `[17, 33)`：

| Page | Page range | 上层判断  | 原因                     |
| ---- | ---------- | --------- | ------------------------ |
| P0   | `[10,18]`  | `Some(0)` | 只有 18 可能命中         |
| P1   | `[20,28]`  | `All(1)`  | 整个 page 位于查询范围内 |
| P2   | `[30,40]`  | `Some(2)` | 只有 30、32 可能命中     |
| P3   | `[45,55]`  | 跳过      | `min >= 33`              |

![BTree 范围查询的 Page Lookup 与 Leaf Search](/images/lance-scalar-index-internals/btree-range-query.svg)

可编辑图源：[btree-range-query.excalidraw](/images/lance-scalar-index-internals/btree-range-query.excalidraw)

`min/max` 选择的是 **candidate pages**，不是最终 rows。尤其当相同值跨越 page 边界时，多个 pages 可能具有相同或重叠的边界。等值查询必须返回所有满足 `min <= value <= max` 的 pages，不能假设一个值只存在于一个 page。

## 5\. 第二层：FlatIndex 如何得到精确结果

选出 pages 后，BTree 为每个 page 创建查询任务。leaf page 先查缓存，cache miss 才读取 `page_data.lance`：

```rust
async fn lookup_page(&self, page_number: u32, reader: LazyIndexReader)
    -> Result<Arc<FlatIndex>>
{
    self.index_cache
        .get_or_insert_with_key(BTreePageKey { page_number }, || async {
            let batch = reader
                .get()
                .await?
                .read_record_batch(page_number as u64, self.batch_size)
                .await?; // [1] 只读取选中的 leaf page

            FlatIndex::try_new(batch) // [2] 构造可复用的页内索引
        })
        .await
}
```

完整实现见 [`lookup_page` 与 `read_page`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/btree.rs#L1632-L1663)。

加载 page 后，`Matches` 决定是否还需要计算谓词：

```rust
match matches {
    Matches::Some(_) => {
        subindex.search(query, metrics) // [1] 边界页：对 values 做精确过滤
    }
    Matches::All(_) => {
        subindex.all()                  // [2] 内部页：直接取整页 ids
    }
}
```

P0 和 P2 进入 `[1]`。`FlatIndex` 将范围谓词交给 Arrow/DataFusion 表达式执行，然后只保留匹配位置上的 `ids`：

```rust
let predicate = expr.evaluate(&self.data)?.into_array(self.data.num_rows())?;
let matching_ids = arrow_select::filter::filter(self.ids(), &predicate)?;
let selected = RowAddrTreeMap::from_sorted_iter(matching_ids.values().iter().copied())?;
```

P1 已经被上层证明整页位于 `[17, 33)`，因此走 `[2]`，不再逐行比较。`FlatIndex` 的完整实现见 [`flat.rs`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/btree/flat.rs#L33-L82) 和 [`FlatIndex::search`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/btree/flat.rs#L176-L275)。

所有 page 任务完成后，BTree 合并 row identifier 集合：

```rust
let results = stream::iter(page_tasks)
    .buffered(get_num_compute_intensive_cpus()) // 候选 pages 并发读取/计算
    .try_collect::<Vec<_>>()
    .await?;

let selection = NullableRowAddrSet::union_all(&results);
Ok(SearchResult::Exact(selection))
```

完整查询入口见 [`BTreeIndex::search`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/btree.rs#L2101-L2225)。

这解释了一个重要区别：

```text
page_lookup 返回 candidate pages
        ≠
BTree 返回 candidate rows
```

第一层可以保守地多选 page；第二层会过滤边界页，所以 BTree 最终仍返回 `Exact`。Dataset 层之后仍要处理 deletion、fragment coverage 和 row-id remap，这属于上一篇讨论的公共执行边界，并不改变 BTree 对索引谓词的精确性。

## 6\. 等值、范围、IN、NULL 与前缀查询

两层路径在不同谓词下只改变选页方式：

| Query                   | `page_lookup`                               | Leaf page                      |
| ----------------------- | ------------------------------------------- | ------------------------------ |
| `x = v`                 | 找出所有满足 `min <= v <= max` 的 pages     | 精确过滤 `values = v`          |
| `x BETWEEN a AND b`     | 找出与范围重叠的 pages，并区分 `Some / All` | 过滤边界页，整页接收内部页     |
| `x IN (v1, v2, ...)`    | 批量找出候选 pages 并去重                   | 复用一份编译后的 IN predicate  |
| `x IS NULL`             | 只选择 `null_count > 0` 的 pages            | 混合页过滤，全 NULL 页直接接收 |
| `starts_with(x, 'app')` | 源码转换成 `[app, next_prefix)` 的范围      | 按范围精确过滤                 |

前四项是 format 文档列出的核心查询。当前源码还支持简单字符串前缀查询；复杂 `LIKE` 即使可以利用固定前缀缩小范围，剩余模式仍可能需要 Dataset filter 做 residual recheck。

### NULL 不是普通的最小值

训练数据按 `NULLS FIRST` 排序，因此开头可能出现两类 page：

```text
all-null page:  min=NULL, max=NULL, null_count=page_size
mixed page:     min=NULL, max=first_non_null_range_max, null_count>0
```

`IS NULL` 可以直接跳过 `null_count = 0` 的 pages。全 NULL page 被标为 `Matches::All`，混合页则必须加载后过滤。

更容易忽略的是：普通比较查询也要保留 NULL rows 的信息。SQL 中 `NULL = 7` 的结果不是 `false`，而是 `NULL`；表达式 `NOT(x = 7)` 也不能把 NULL rows 变成 `true`。因此 BTree 会在普通 value pages 之外补充包含 NULL 的 pages，让 `NullableRowAddrSet` 同时记录 true rows 和 null rows：

```rust
if !matches!(query, SargableQuery::IsNull()) {
    for page_id in null_pages {
        pages.push(Matches::Some(page_id)); // 读取后把 NULL 记入 null-row mask
    }
}
```

这是为了保证 `NOT`、`OR` 等组合谓词的三值逻辑正确，不是多余的 page read。完整逻辑见 [`BTreeIndex::search` 的 NULL page 处理](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/btree.rs#L2160-L2186)。

## 7\. 缓存与 IO 边界

一次冷查询包含两类加载：

```text
1. load page_lookup.lance
   -> 构造 BTreeLookup
   -> 缓存可序列化的 BTreeIndexState

2. search page_lookup
   -> 得到 candidate page numbers
   -> cache miss 才打开 page_data.lance
   -> 按 page number 读取并缓存 FlatIndex
```

`LazyIndexReader` 的设计很具体：如果本次查询所需 pages 全部命中缓存，就不会打开 `page_data.lance`；只要出现 cache miss，同一次查询中的 page tasks 会共享一个延迟初始化的 reader。相同 page 的并发 cold lookup 也通过 cache 的 `get_or_insert` 合并为一次加载。

热查询则可能只剩：

```text
cached BTreeLookup
  -> candidate page numbers
  -> cached FlatIndex pages
  -> row identifier union
```

当前 BTree 层会并发执行多个 page tasks，但不会在这一层把相邻 cache-miss pages 显式合并成一次连续 range read。范围越大，命中的 leaf pages 越多，page IO、页内比较和最终回表读取都会增加。

这也说明 `zone_size` 的取舍：

| 较小的 leaf page                  | 较大的 leaf page                    |
| --------------------------------- | ----------------------------------- |
| 边界更精细，页内比较更少          | 页目录更小，page 数量和 IO 次数更少 |
| `page_lookup` 行数更多            | 单次读取和页内过滤的数据更多        |
| 大范围查询可能触发更多 page reads | 小范围查询可能读入更多无关值        |

默认 4096 是一个通用折中，不代表所有数据类型、对象存储和查询负载的最优值。BTree 最有利的场景仍是高选择性的等值、IN 和较小范围查询；当谓词命中大量 rows 时，即使选页很快，读取大量 leaf pages 和原始数据的成本也不会消失。

## 8\. Segment 更新与旧布局

本篇主线使用一个 canonical BTree segment：

```text
_indices/{uuid}/
├── page_lookup.lance
└── page_data.lance
```

segment 写入后不可变。追加数据或合并 segments 时，BTree 将已有的有序 page streams 与新数据做有序合并，再训练出新的文件，而不是原地修改某个 leaf page。这样 `min/max` 目录与 leaf data 始终作为一组发布。

源码中还能看到 `part_*_page_lookup.lance`、`part_*_page_data.lance` 以及 global page 到 `(file, local_page)` 的转换。这是 range-partitioned 与历史分布式构建布局的兼容路径。固定提交中的通用 `BTreeIndexPlugin` 已将 `range_id` 标记为 deprecated 并忽略；新的分布式流程以独立 segments 为边界，再由 segment API 提交或合并。因此这些 `part_*` 文件不应当作为当前两层结构的主心智模型。

相关实现见 [`BTreeParameters` 与 `train_index`](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/btree.rs#L3162-L3271) 和 [`BTreeIndex::load` 的兼容读取](https://github.com/lance-format/lance/blob/d293630dff7a0393702e01a88a65da1a6591e867/rust/lance-index/src/scalar/btree.rs#L1731-L1811)。

## 9\. 总结

Lance BTree 的核心不是复杂的磁盘树，而是两层页结构：

1. 构建阶段按 value 排序 `<value, id>`，再按 `zone_size` 切成固定大小的 leaf pages。
2. `page_lookup.lance` 每页保存一条 `min / max / null_count / page_idx`，查询时整体加载并选择候选页。
3. `page_data.lance` 保存完整 `values / ids`，只有候选页被按需读取和缓存。
4. 上层目录可以保守选页，`FlatIndex` 会精确过滤边界页，因此 BTree 最终返回 `Exact`。
5. page size、缓存命中率、查询选择性与对象存储 IO 共同决定实际收益。

下一篇将分析 Zone Map。它也使用 `min/max`，但不会排序数据，也不会在索引中保存每一行的 `<value, id>`。这两个差异决定了 Zone Map 只能负责 scan pruning，而不是像 BTree 一样返回精确 row identifiers。
