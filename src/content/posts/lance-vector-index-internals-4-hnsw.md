---
author: "Jay H. Zou"
pubDatetime: 2026-08-10T10:30:00+08:00
title: "Lance 向量索引原理与源码分析（四）"
lang: zh-CN
tags:
  - Lance
  - Vector Index
  - HNSW
  - IVF
  - 源码分析
description: "HNSW 与 Lance 组合索引"
---

> 本文源码基于 Lance `v10.0.0` 的提交 [`95f2f36b2`](https://github.com/lance-format/lance/tree/95f2f36b22043c3face00afe088c34e0742d01df)。代码片段保留 HNSW 构建、查询和 partition 装载的关键控制流，省略日志、filter 与错误处理分支。

[上一篇](/posts/lance-vector-index-internals-3-quantization/) 将 PQ、SQ 与 RabitQ 放回 quantizer 边界。量化减少的是向量 storage 与单次距离计算成本，却没有回答 partition 内应该比较多少个节点。

如果一个 IVF partition 中仍有十万条向量，`IVF_FLAT`、`IVF_PQ` 或 `IVF_SQ` 都要扫描该 partition 的全部 codes。Lance 引入 HNSW，是为了把 partition 内的组织方式从“逐条扫描”换成“沿邻接图导航”。

这里有一个贯穿全文的边界：

> Lance 10.0.0 的公开 HNSW 索引不是覆盖整个 Dataset 的一张全局图，而是 **IVF 先选 partitions，再搜索每个 partition 自己的 HNSW 图**。

因此一次查询同时有两层候选控制：外层是 IVF 的 `nprobes`，内层是 HNSW 的 `ef`。只调其中一个参数，不能消除另一个阶段漏掉近邻的可能。

## 1\. `IVF_HNSW_FLAT` 的 FLAT 不是穷举搜索

Vector Index V3 把索引拆成三个正交部分：

```text
clustering       sub-index       quantization
IVF          ×   FLAT/HNSW   ×   FLAT/PQ/SQ/RQ
```

名称中的最后一段表示 quantization。于是：

- `IVF_FLAT`：IVF + FLAT sub-index + FLAT quantization，partition 内穷举原始向量；
- `IVF_HNSW_FLAT`：IVF + HNSW sub-index + FLAT quantization，保留原始精度向量，但通过图近似导航；
- `IVF_HNSW_PQ`：图结构不变，节点距离改由 PQ storage 估算；
- `IVF_HNSW_SQ`：图结构不变，节点距离改由 SQ8 storage 估算。

也就是说，`IVF_HNSW_FLAT` 仍然是 ANN。它的 `FLAT` 只表示“不量化”，不表示 partition 内 exhaustive scan。源码把这两个维度分别建模为 `IvfSubIndex` 与 `Quantization`，格式文档也明确记录了三段式结构，见 [`IvfSubIndex`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/v3/subindex.rs#L18-L49) 与 [`Vector Index V3 concepts`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/docs/src/format/index/vector/index.md#L9-L48)。

![Lance 的 IVF 分区与 partition-local HNSW 查询路径](/images/lance-vector-index-internals/ivf-hnsw-query.svg)

可编辑图源：[ivf-hnsw-query.excalidraw](/images/lance-vector-index-internals/ivf-hnsw-query.excalidraw)

## 2\. HNSW 图在 Lance 中保存什么

每个 HNSW node 对应 quantization storage 中的一个本地 vector id。图本身保存三列：vector id、邻居列表与到邻居的距离；每个 partition 另有 metadata，记录 `entry_point`、构建参数和各 level 的 offset。实际向量或量化 code 在 `auxiliary.idx`，不重复嵌进每条边。

```text
index.idx                           auxiliary.idx
partition 0 HNSW levels            partition 0 vector storage
partition 1 HNSW levels            partition 1 vector storage
...                                ...

HnswMetadata {
  entry_point,
  params,
  level_offsets,
}
```

完整 schema 与 metadata 见 [`HNSW on-disk layout`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/docs/src/format/index/vector/index.md#L81-L140)。

HNSW 的层级承担两种不同工作：

1. 高层节点少，负责从固定 entry point 快速移动到 query 附近；
2. 最底层节点最全，负责保留多个候选并做更细的邻域扩展。

Lance 构建器将 node 0 设为 entry point，并让它拥有 `max_level` 个 level slots；其余节点按指数分布抽取 level，再截断到 `max_level - 1`：

```rust
nodes.push(GraphBuilderNode::new(0, max_level as usize));

let mut level_rng = SmallRng::seed_from_u64(HNSW_LEVEL_RNG_SEED);
for i in 1..len {
    let level = self.random_level(&mut level_rng) as usize + 1;
    nodes.push(GraphBuilderNode::new(i as u32, level));
}

fn random_level<R: Rng + ?Sized>(&self, rng: &mut R) -> u16 {
    let ml = 1.0 / (self.params.m as f32).ln();
    min(
        (-rng.random::<f32>().ln() * ml) as u16,
        self.params.max_level - 1,
    )
}
```

完整实现见 [`HNSWBuilder::with_params` 与 `random_level`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L674-L728)。

### 可复现的 Level Assignment

10.0.0 将 level RNG seed 固定为 `42`。源码注释给出的意图是：相同数据和参数应构造相同的图，使构建可复现、测试稳定。这个结论有明确边界：固定 seed 直接保证的是随机 level 序列不漂移，不应外推成“跨 Lance 版本、不同实现或不同浮点环境得到字节完全相同的索引”。常量与设计说明见 [`HNSW_LEVEL_RNG_SEED`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L52-L59)。

## 3\. 构建：先导航，再连边，最后双向裁剪

插入一个新节点时，Lance 从 entry point 开始：高于新节点自身最高层的部分只做 greedy search；进入新节点存在的 level 后，用 `ef_construction` 大小的候选集搜索邻居，建立出边，再处理反向边。

下面是裁剪后的控制流：

```rust
// Descend through levels where the new node has no edges.
for level in (target_level + 1..self.params.max_level).rev() {
    ep = greedy_search(level_view(level), ep, &dist_calc, prefetch_distance);
}

// Search and connect at every level owned by the new node.
for level in (0..=target_level).rev() {
    let neighbors = self.search_level(
        &ep,
        level,
        &dist_calc,
        nodes,
        visited,
    ); // search_level uses ef_construction

    for neighbor in &neighbors {
        current_node.add_neighbor(neighbor.id, neighbor.dist, level);
    }
    self.prune(storage, &mut current_node, level);
    ep = neighbors[0].clone();
}

// Add backlinks and prune the neighbor nodes too.
for neighbor in selected_neighbors {
    chosen_node.add_neighbor(node, neighbor.dist, level);
    self.prune(storage, &mut chosen_node, level);
}
```

完整插入流程见 [`HNSWBuilder::insert`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L730-L795)，构建候选集如何传入 `ef_construction` 见 [`search_level`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L797-L822)。

### 三个构建参数分别约束什么

10.0.0 的实际 `Default` 实现是：

```rust
HnswBuildParams {
    max_level: 7,
    m: 20,
    ef_construction: 150,
    prefetch_distance: Some(2),
}
```

这里必须以 [`Default implementation`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L87-L96) 为准。紧邻的 setter 文档注释仍写着旧值 `8/30/100`，见 [`stale setter comments`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L98-L120)；复制那些注释会得到错误的 v10 默认配置。

| 参数              | 直接控制的结构/过程                                                          | 增大后的主要成本                         |
| ----------------- | ---------------------------------------------------------------------------- | ---------------------------------------- |
| `max_level`       | level slots 上限；随机层级被截断在这个范围内                                 | 更多稀疏层 metadata 与导航工作           |
| `m`               | 上层最多 `m` 条边、底层最多 `2*m` 条边；同时令 `ml=1/ln(m)` 参与随机层级分布 | 图更大，构建与查询要检查更多邻接点       |
| `ef_construction` | 插入节点时 `search_level` 保留的候选数                                       | 构建距离计算、临时候选内存与构建时间增加 |

`m` 不是“每层严格等于 m 条边”。`prune` 对 level 0 使用 `2*m`，其余 level 使用 `m`，见 [`HNSWBuilder::prune`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L824-L839)；随机层级使用 `ml=1/ln(m)`，见 [`random_level`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L719-L728)。最终边数还会受到候选数量、重复节点和 neighbor selection heuristic 影响。

### 一个插入示例

假设 `max_level=3, m=2, ef_construction=4`，新节点 `N7` 只存在于 level 0 和 level 1：

```text
level 2: entry N0 --greedy--> N4
level 1: start N4 --beam(ef=4)--> candidates [N6, N4, N2, N1]
         connect N7 to selected neighbors, keep at most m=2
level 0: start from best level-1 candidate
         search again, connect N7, keep at most 2*m=4
backlink: add N7 to chosen neighbors, then prune those neighbor lists
```

这只是控制流示例，不代表固定的 neighbor selection 结果。真正被保留的边由向量距离、候选图结构和 heuristic 共同决定。

## 4\. 查询：高层 Greedy，底层 `ef` Beam

查询同样从 partition metadata 的 entry point 开始。高层每一步只接受更近的节点；到达底层后，不再只保留单一路径，而是同时维护 candidate heap 与 result heap。`ef` 硬限制 result heap 最多保留多少个当前结果；candidate heap 保存尚未展开的有希望节点，并没有相同的长度裁剪。

```rust
let mut ep = distance_from_query(entry_point);

for level in (0..self.max_level()).rev() {
    ep = greedy_search_borrowed(level_view(level), ep, &dist_calc, prefetch);
}

beam_search_borrowed(
    &bottom_level,
    &ep,
    params, // params.ef controls the beam
    &dist_calc,
    bitset,
    prefetch,
    &mut visited,
)
.into_iter()
.take(k)
.collect()
```

完整查询路径见 [`search_inner` 与 `run_search`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L263-L366)。底层搜索在当前最优 candidate 已经比 result heap 的最远结果更差、并且 heap 已满时停止；见 [`beam_search`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/graph.rs#L364-L467)。

### 一个 Query Trace

仍在某一个已选 IVF partition 内，假设 `k=2, ef=4`：

```text
level 2: N0(d=8.0) -> N4(d=3.0)
level 1: N4(d=3.0) -> N7(d=1.4)

level 0 beam:
  expand N7 -> push N8(d=0.5), N6(d=2.0), N4(d=3.0)
  expand N8 -> push N9(d=0.2), N3(d=1.1)
  expand N9 -> no better unvisited neighbor
  result heap keeps up to ef=4 candidates

partition result: N9(d=0.2), N8(d=0.5)
```

距离仅用于说明 heap 如何扩展，不是 benchmark。关键区别是：高层路径宽度接近 1，底层最多保留 `ef` 个有希望的结果；因此 `ef` 越大，通常会访问更多节点，也给遗漏旁路近邻留下更小的机会，但源码没有承诺固定召回率或固定延迟倍率。

### `ef` 与 `refine_factor` 会相互影响

Lance 先计算有效候选数：

```rust
let k_eff = query.k * query.refine_factor.unwrap_or(1) as usize;

HnswQueryParams {
    ef: query.ef.unwrap_or(k_eff + k_eff / 2),
    // ...
}
```

所以未显式设置 `ef` 时，默认是 `floor(1.5 * k_eff)`，而不是永远围绕最终 `k`。HNSW 搜索还会拒绝 `ef < k_eff`，因为 beam 连需要返回的候选数都容纳不下。相关实现见 [`HnswQueryParams::from`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L1098-L1117) 与 [`ef validation`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/builder.rs#L1268-L1282)。

例如最终 `k=10, refine_factor=2` 时，HNSW 每个 partition 的目标候选数是 20，默认 `ef=30`。显式写 `ef=16` 会失败，而不是悄悄缩小 beam。精排仍只能重排 HNSW 已返回的候选，不能恢复没有被图遍历访问到的节点。

## 5\. 为什么 Lance 是 Partition-local Graph

查询计划先用 IVF centroids 对 partitions 排序，再对选中的 partitions 执行 sub-index 搜索。每个 partition 的 HNSW 图和对应 quantization storage 一起装载：

```rust
let metadata = self.get_partition_metadata(partition_id)?;
let storage = self.partition_storage.load_partition(partition_id).await?;
let batch = reader.read_range(offset..offset + length, reader.schema()).await?;
let hnsw = HNSW::load(batch.with_metadata(metadata))?;
```

完整装载逻辑见 [`HNSWIndex::load_partition`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/hnsw/index.rs#L242-L270)，IVF 顶层如何将 query 预处理后交给 partition sub-index 见 [`IVFIndex::search_in_partition`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/vector/ivf/v2.rs#L1432-L1484)。

从这份结构可以推出几个工程结果；这是对 v10 实现的架构推论，不是源码对所有 workload 的性能承诺：

1. 查询只需装载 `nprobes` 选中的 graph/storage slices，不必为每次查询遍历一张覆盖全部向量的图；
2. FLAT/HNSW 与 FLAT/PQ/SQ 两个轴可以独立组合，graph 只依赖统一的 `VectorStore` 距离接口；
3. partition 是图构建、metadata 和装载的自然边界，代价是召回率出现两道门。

两道门可以写成：

```text
true neighbor
  ├─ IVF selected its partition?       controlled by nprobes
  └─ HNSW reached it in that graph?     controlled by graph quality + ef
```

增大 `ef` 不能搜索一个没有被 `nprobes` 选中的 partition；增大 `nprobes` 也不能保证一个低质量或过窄 beam 的图一定访问到正确节点。调参时应把 `nprobes`、`ef`、quantization 与 `refine_factor` 作为四个不同阶段观察。

## 6\. v10.0.0 实际支持哪些组合

公开 Python/Rust 构建分派在 10.0.0 中支持三种 HNSW 组合：

```rust
IVF_HNSW_FLAT => stages [IVF, HNSW]
IVF_HNSW_PQ   => stages [IVF, HNSW, PQ]
IVF_HNSW_SQ   => stages [IVF, HNSW, SQ]
```

对应实现见 [`VectorIndexParams constructors and index_type`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/vector.rs#L422-L493)，Python 的实际 allowlist 见 [`valid_index_types`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L3732-L3748)。10.0.0 没有公开 `IVF_HNSW_RQ` 构建分支；不能因为 `HNSWIndex<Q>` 在 Rust 内部对 quantizer 使用泛型，就推断所有 `Q` 都已经成为受支持的产品组合。

| 组合            | 图比较时读取的向量表示 | 近似来自哪里                         | 主要成本边界                         |
| --------------- | ---------------------- | ------------------------------------ | ------------------------------------ |
| `IVF_HNSW_FLAT` | 原始精度向量           | IVF partition 选择 + HNSW 图遍历     | auxiliary storage 较大，距离读取更多 |
| `IVF_HNSW_PQ`   | PQ code                | IVF + 图遍历 + PQ distance estimate  | code 更短，但需 codebook 与量化训练  |
| `IVF_HNSW_SQ`   | SQ8 code               | IVF + 图遍历 + SQ8 distance estimate | 每维 1 byte，共享 bounds             |

这张表刻意不做“谁最快”排序。FLAT 避免量化误差，但 HNSW 路径仍是近似的；PQ/SQ 减少 storage 和距离数据量，却会把量化误差带进建图与查询距离。最终结果取决于数据分布、维度、partition 大小、cache、metric 和参数组合。

## 7\. 一组可解释的配置与验证方式

下面的配置显式写出构建参数，并在查询时让 `ef` 大于 `k * refine_factor`：

```python
import lance

dataset = lance.dataset("/data/embeddings.lance")
dataset.create_index(
    "vector",
    "IVF_HNSW_SQ",
    metric="cosine",
    num_partitions=256,
    max_level=7,
    m=20,
    ef_construction=150,
)

result = dataset.scanner(
    nearest={
        "column": "vector",
        "q": query_vector,
        "k": 10,
        "nprobes": 8,
        "ef": 64,
        "refine_factor": 2,
    }
).to_table()
```

10.0.0 的 `LanceDataset.scanner` 通过 `nearest` dictionary 接收这组查询参数，并返回 `LanceScanner`；对应签名见 [`LanceDataset.scanner`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L1130-L1162)。

这些值只是展示参数属于哪个阶段，不是针对任意 Dataset 的推荐最优值。实际调优应至少同时记录：

- Recall@K：与同一批 query 的 exact scan ground truth 比较；
- P50/P95 latency：分别观察 partition ranking、sub-index search 和 refine；
- 构建时间与峰值内存：HNSW builder 为 partition 构图时会维护 nodes、邻接表和候选集；
- `index.idx` 与 `auxiliary.idx` 体积：区分 graph 边与 vector codes 的成本；
- 查询触达的 partitions、nodes 与原始向量随机读取量。

如果 Recall@K 不够，先判断 miss 发生在哪一层：增加 `nprobes` 后明显改善，问题更可能在 IVF gate；固定 `nprobes`、增加 `ef` 后改善，问题更可能在 graph traversal；两者已稳定而 refine 才改善，主要误差来自量化排序。这样的分层实验比一次同时放大所有参数更容易找到真正的成本来源。

至此，前四篇已经把向量数据、IVF、量化与 HNSW 四个边界拆开。后续讨论查询执行或调优时，可以沿着同一条路径定位：先选 partition，再走 sub-index，再由 quantizer 估算距离，最后按需读取原始向量精排。
