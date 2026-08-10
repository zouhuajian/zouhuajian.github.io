---
author: "Jay H. Zou"
pubDatetime: 2026-08-10T10:40:00+08:00
title: "Lance 向量索引原理与源码分析（五）"
lang: zh-CN
tags:
  - Lance
  - Vector Index
  - ANN
  - 查询执行
  - 源码分析
description: "过滤、精排与未索引数据回退"
---

> 本文源码基于 Lance `v10.0.0` 的提交 [`95f2f36b`](https://github.com/lance-format/lance/tree/95f2f36b22043c3face00afe088c34e0742d01df)。文中的代码片段保留关键控制流，省略日志、错误处理与本文无关的分支。

前四篇依次介绍了[向量数据与 Flat KNN](/posts/lance-vector-index-internals-1-data-flat/)、[Vector Index V3 与 IVF](/posts/lance-vector-index-internals-2-ivf/)、[PQ、SQ 与 RabitQ](/posts/lance-vector-index-internals-3-quantization/)以及 [HNSW 图搜索](/posts/lance-vector-index-internals-4-hnsw/)。这些结构最终都要回答同一个工程问题：

> 一条带过滤条件的向量查询，如何从 Python 参数变成一个既快、又不漏掉未索引新数据的执行计划？

“创建了 IVF_HNSW_PQ”并不能直接回答这个问题。一次实际查询还要处理五个彼此独立的边界：

1. 查询距离是否与索引距离一致；
2. IVF 和 HNSW 要产生多少 ANN 候选；
3. 过滤发生在候选生成前还是之后；
4. 量化距离是否需要用原始向量精排；
5. 索引创建后追加的 fragments 是否仍参与 Top-K。

本文用一条混合查询贯穿完整路径。

```python
result = dataset.scanner(
    nearest={
        "column": "vector",
        "q": query_vector,
        "k": 3,
        "metric": "cosine",
        "minimum_nprobes": 2,
        "maximum_nprobes": 8,
        "refine_factor": 4,
        "ef": 64,
    },
    filter="tenant_id = 42",
    prefilter=True,
).to_table()
```

假设 `vector_idx` 覆盖 F0、F1，F2 是创建索引后新追加的 fragment；`tenant_id` 上还有一个覆盖 F0、F1 的精确 BTree。查询的目标不是“从索引里取 3 行”，而是从**当前 Dataset 的有效行**中寻找满足 `tenant_id = 42` 的最近 3 行。

## 1\. Python 参数不是彼此独立的开关

`nearest` 字典先经过 `_build_vector_search_query`。它不仅转发参数，还在 API 边界检查向量维度和参数组合：

```python
q, q_dim = _coerce_query_vector(q)
if q_dim != dim:
    raise ValueError("query dimension does not match vector column")

if nprobes is not None:
    if minimum_nprobes is not None or maximum_nprobes is not None:
        raise ValueError("nprobes cannot be combined with min/max")
    minimum_nprobes = nprobes
    maximum_nprobes = nprobes

if minimum_nprobes > maximum_nprobes:
    raise ValueError("minimum_nprobes must be <= maximum_nprobes")
if refine_factor is not None and refine_factor < 1:
    raise ValueError("refine_factor must be >= 1")
```

完整实现见 [`_build_vector_search_query`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L7807-L7968)。

这里有一个容易忽略的区别：

| 写法                                   | 执行含义                                                     |
| -------------------------------------- | ------------------------------------------------------------ |
| `nprobes=8`                            | 将 minimum 和 maximum 都设为 8，固定搜索 8 个 IVF partitions |
| `minimum_nprobes=2`                    | 至少搜索 2 个；如果过滤后候选不足，可以继续搜索              |
| `minimum_nprobes=2, maximum_nprobes=8` | 在 2 到 8 之间按结果数量继续扩展                             |
| 只设置 `maximum_nprobes=8`             | minimum 使用 Rust 绑定的默认值，maximum 限制上界             |

Python 字典随后在 Rust 绑定中变成 `Query`。`minimum_nprobes`、`maximum_nprobes`、`ef`、`refine_factor` 和 `metric_type` 因而进入同一个查询对象，而不是分别控制互不相关的执行节点。绑定还明确说明：设置 `refine_factor` 会增加一次原始向量读取和 Flat 精排；默认不做，是为了避免额外 I/O 和随机访问。完整转换见 [`vector_query_params_from_dict`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/src/dataset.rs#L5415-L5535) 与 [`Query`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector.rs#L104-L169)。

因此，调参时应先写出候选预算：

```text
requested top-k = k
ANN candidates  = k × refine_factor
IVF search      = minimum_nprobes .. maximum_nprobes
HNSW frontier   = ef
```

这些值共同决定召回、CPU、索引页读取和原始向量随机读取，而不是四个独立的“越大越准”旋钮。

## 2\. 距离不匹配时不能继续使用索引

向量索引在构建时已经固定距离类型。Cosine 的归一化、Dot 的排序方向以及 L2 的距离计算不能在查询时随意互换。

Scanner 找到目标列上的索引后，会比较查询距离与索引距离：

```rust
let index_metric = metric_type_from_index_metadata(index)
    .unwrap_or(open_index(index.uuid).await?.metric_type());

let use_this_index = match query.metric_type {
    Some(user_metric) if user_metric != index_metric => false,
    _ => true,
};

if use_this_index {
    query.metric_type = Some(index_metric);
    plan_ann(query, index_segments).await
} else {
    plan_flat_knn(query).await
}
```

完整控制流见 [`Scanner::vector_search`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/scanner.rs#L3803-L4054)。

普通查询显式请求 `cosine`，而列上只有 L2 索引时，Lance 会记录警告并退回 Flat KNN；它不会用 L2 索引生成候选，再把结果伪装成 Cosine Top-K。若没有显式指定 metric，则采用索引的 metric。

这条回退保证了**距离语义正确**，代价可能是一次全量向量扫描。排查“创建了索引但查询仍慢”时，第一步应比较创建与查询的 metric，再查看 `explain_plan()` 中是否真的出现 `ANNIvfPartition` / `ANNSubIndex`。

## 3\. IVF 先保证最小探测数，再按需扩大

匹配到索引后，ANN 路径分为两个执行节点：

```text
ANNSubIndex: k = k × refine_factor
  ANNIvfPartition:
    minimum_nprobes = 2
    maximum_nprobes = 8
```

`ANNIvfPartitionExec` 为每个物理 index segment 计算 query 到所有 centroids 的距离，返回按距离从近到远的 partitions；`ANNIvfSubIndexExec` 再进入这些 partitions 搜索实际向量候选。对应实现见 [`new_knn_exec` 与 ANNIvfPartitionExec](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/io/exec/knn.rs#L1104-L1344)。

v10.0.0 的 `minimum_nprobes` / `maximum_nprobes` 不是简单地取一个中间值：

```rust
search_partitions(0, minimum_nprobes).await?;

if found_so_far < query.k {
    search_partitions(
        minimum_nprobes,
        maximum_nprobes,
        stop_when_enough_results,
    )
    .await?;
}
```

执行层先搜索 minimum 个 partitions。若 prefilter 后已经得到足够结果，就不继续打开更远的 partitions；不足时才继续，直到结果数满足目标或达到 maximum。完整逻辑见 [`initial_search` 与 `late_search`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/io/exec/knn.rs#L1627-L1848)。

这里的“足够”只是候选数量条件，不是召回证明。某个未探测 partition 仍可能包含真实近邻。因此：

- minimum 决定无论如何都支付的基础搜索成本；
- maximum 限制高选择性过滤下的最坏搜索范围；
- 固定 `nprobes` 更容易做可重复基准；
- 自适应 min/max 更适合候选数随过滤条件大幅变化的在线查询。

## 4\. HNSW 的 `ef` 必须容纳精排候选

对于 IVF_HNSW 系列，每个 IVF partition 内不是线性扫描，而是用 HNSW 找候选。查询真正交给 HNSW 的 `k` 已经乘上 `refine_factor`：

```rust
let candidate_k = query.k * query.refine_factor.unwrap_or(1) as usize;
let params = HnswQueryParams {
    ef: query.ef.unwrap_or(candidate_k + candidate_k / 2),
    // ...
};

if params.ef < candidate_k {
    return Err("ef must be greater than or equal to k");
}
```

完整实现见 [`HnswQueryParams::from`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L1098-L1117) 和 [`HNSW::search`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L1266-L1280)。

例如本文 `k=3, refine_factor=4`，HNSW 至少要容纳 12 个候选；`ef=8` 会在执行时失败，而不是被静默放大。未指定 `ef` 时，当前默认值是 `candidate_k + candidate_k / 2`，即本例中的 18。

`ef` 增大通常会搜索更多图节点，但它只影响 partition 内的图遍历；它不能补偿 `nprobes` 太小而完全没有进入正确 IVF partition 的问题。

## 5\. `refine_factor` 是候选内精排，不是全局精确搜索

量化索引返回的是近似距离。`refine_factor=4` 会让 ANN 阶段先保留 `3 × 4 = 12` 个候选，再读取这 12 行的原始 `vector`，用同一种 metric 重算距离并取最终 3 行：

```rust
let ann = new_knn_exec(dataset, segments, query, prefilter)?;
let candidates = SortExec::new(by_distance, ann)
    .with_fetch(Some(query.k * refine_factor));

let candidates_with_vector = take(candidates, vector_column)?;
let reranked = flat_knn(candidates_with_vector, query)?;
```

源码见 [`Scanner::vector_search` 的 refine 分支](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/scanner.rs#L3951-L4012) 和 [`Scanner::ann`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/scanner.rs#L5131-L5166)。

“重算原始向量距离”是精确的，但**精确范围只有 ANN 已经找出的 12 个候选**。如果真实最近邻因为 IVF partition 未被探测，或 HNSW 图搜索没有访问到而未进入候选集，精排无法把它找回来。

因此必须区分：

| 性质                                 | `refine_factor` 能否改善                          |
| ------------------------------------ | ------------------------------------------------- |
| PQ/SQ/RQ 近似距离导致的候选内部错序  | 能                                                |
| IVF 没有探测到真实近邻所在 partition | 不能                                              |
| HNSW 没有把真实近邻放入候选集        | 不能                                              |
| 索引未覆盖追加 fragment              | 由正常模式的 Flat fallback 处理，不由 refine 处理 |

## 6\. Prefilter 与 Postfilter 回答的是不同问题

### Prefilter：先限定合格行，再寻找 Top-K

`prefilter=True` 时，过滤结果成为 ANN 的输入 mask。Lance 会优先利用 Scalar Index：如果 scalar 查询结果精确，而且覆盖范围满足向量索引所需 fragments，`ScalarIndexExec` 可以直接提供 row ids；否则执行只读取 row id 的 filtered scan，得到 mask 后再交给 ANN。完整选择逻辑见 [`Scanner::prefilter_source`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/scanner.rs#L5255-L5328)。

本文的 `tenant_id = 42` 有完整 BTree 覆盖，因此索引侧计划可以是：

```text
ANNSubIndex
├── ANNIvfPartition
└── ScalarIndexQuery: tenant_id = 42
```

ANN 只接受 mask 中的行。这样查询语义是：

```text
TopK(distance(vector, q) among rows where tenant_id = 42)
```

### Postfilter：先取近邻，再从中删除不合格行

`prefilter=False` 是默认值。此时先完成 Top-K，再对这些行执行普通内存过滤；Scalar Index 不参与这个过滤阶段。执行入口会把 filter plan 改成 refine-only，源码见 [`vector_search_source`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/scanner.rs#L3198-L3240)。

如果全局最近的 3 行只有 1 行属于 tenant 42，postfilter 只返回 1 行，不会自动继续向下补足到 3 行。这不是执行错误，而是两种算子顺序本来就不等价：

```text
prefilter : Filter → ANN → TopK(3)  => 最多 3 个合格近邻
postfilter: ANN → TopK(3) → Filter  => 可能少于 3 行
```

高选择性 tenant、category、时间范围过滤通常应从 prefilter 开始验证；过滤命中率很高且允许少于 k 行时，postfilter 才可能省掉构建 mask 的成本。无论选择哪种，都应分别测量返回行数、Recall@K 与延迟，不能只看延迟。

## 7\. 未索引 Fragment 必须进入另一条 Flat KNN 支路

索引 segment 的 `fragment_bitmap` 只表示它覆盖哪些 fragments。F2 在索引创建后追加，不会原地写入旧 segment。正常查询不能把“索引没有 F2”解释成“F2 没有近邻”。

v10.0.0 的 `knn_combined` 会找到逻辑索引未覆盖的 fragments，对它们执行独立 Flat KNN：

```rust
let fallback_fragments = dataset.unindexed_fragments(index_name).await?;

let flat_topk = if !fallback_fragments.is_empty() {
    let scan = scan_fragments(fallback_fragments);
    let filtered = apply_full_filter(scan);
    Some(flat_knn(filtered, query)?)
} else {
    None
};

let all_candidates = union(flat_topk, ann_candidates);
flat_knn(all_candidates, query) // global exact distance + Top-K
```

完整实现见 [`Scanner::knn_combined`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/scanner.rs#L4137-L4250)。

最后一层 `flat_knn` 很重要：它不是简单拼接两个已经排序的结果，而是让 F2 的精确距离与索引候选使用同一 metric 比较，再产生全局 Top-K。对于本文示例，执行顺序如下：

1. BTree 产生 F0、F1 中 `tenant_id = 42` 的 prefilter mask；
2. IVF 先探测 2 个 partitions，必要时增加到最多 8 个；
3. 每个 partition 内的 HNSW 以 `ef=64` 生成 ANN 候选；
4. 索引支路保留 12 个候选并读取原始向量精排；
5. F2 先执行完整 tenant filter，再做 Flat Top-3；
6. 两条支路合并，以原始向量距离选出 Dataset 当前版本的 Top-3。

![带过滤的 Lance 向量查询执行路径](/images/lance-vector-index-internals/vector-query-execution.svg)

可编辑图源：[vector-query-execution.excalidraw](/images/lance-vector-index-internals/vector-query-execution.excalidraw)

Fallback 修复的是**覆盖完整性**，不是 ANN 算法召回率。F0、F1 内仍然可能因为有限 nprobes / ef 而漏掉近邻；F2 则是精确 Flat 搜索。

## 8\. `fast_search` 明确放弃当前 Dataset 的完整覆盖

`fast_search=True` 会跳过 `knn_combined`，只搜索已经进入索引的 fragments。若根本没有可用向量索引，v10.0.0 返回具有 KNN schema 的空结果，而不是退回全表 Flat。源码将它定义为 weak consistency search：[`Scanner::fast_search`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/scanner.rs#L1701-L1712)；Python API 同样明确说明会跳过最近追加的未索引数据：[`fast_search` 参数说明](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L1324-L1327)。

在本文示例中：

```text
normal search = ANN(F0, F1) ∪ Flat(F2) → global Top-K
fast_search   = ANN(F0, F1)            → indexed-subset Top-K
```

因此 `fast_search` 不是一个无条件的性能开关。只有调用方接受以下契约时才应使用：

> 结果是“已索引子集上的近似 Top-K”，不保证包含当前 Dataset 中最近追加数据里的近邻。

如果业务要求 read-after-append，不能开启它来掩盖索引维护延迟；应缩短 `optimize_indices` 周期，或让调用方显式区分“低延迟旧快照检索”和“当前版本完整覆盖检索”。

## 9\. 用三个维度判断查询是否正确

向量检索里“正确”经常混在一个 Recall 数字里。更可靠的验收方式是拆成三层：

| 层次       | 要验证的问题                       | 主要控制项                                |
| ---------- | ---------------------------------- | ----------------------------------------- |
| 语义正确性 | metric、过滤顺序、删除行是否正确   | metric matching、prefilter、deletion mask |
| 覆盖完整性 | 未索引 fragments 是否参与          | normal fallback；不要误用 fast_search     |
| ANN 召回率 | 已索引数据中的真实近邻是否进入候选 | nprobes、ef、refine_factor、索引类型      |

Flat KNN 是三者共同的基线。对同一批 query vectors，应使用 `use_index=False` 得到精确 Top-K，再分别比较：无过滤、prefilter、高选择性过滤、刚 append 后和 `fast_search=True` 的结果。

一条查询变慢，可能是 metric 不匹配后退回 Flat；返回不足 k，可能是 postfilter；append 后延迟上升，可能是 unindexed fallback 扫描扩大；Recall 下降，则要继续拆分 nprobes、ef 和精排候选数。只有先定位到这三层中的一层，调参才不会变成盲目放大所有参数。

下一篇将继续讨论这些状态如何积累：append、update、delete 与 compaction 分别怎样改变索引覆盖，何时应创建 delta segment、合并或重新训练，以及如何用一套可重复实验选择实际索引。
