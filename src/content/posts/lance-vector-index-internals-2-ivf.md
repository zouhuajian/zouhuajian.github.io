---
author: "Jay H. Zou"
pubDatetime: 2026-08-10T10:10:00+08:00
title: "Lance 向量索引原理与源码分析（二）"
lang: zh-CN
tags:
  - Lance
  - Vector Index
  - IVF
  - 源码分析
description: "Vector Index V3 与 IVF 分区"
---

> 本文源码基于 Lance `v10.0.0` 的提交 [`95f2f36b2`](https://github.com/lance-format/lance/tree/95f2f36b22043c3face00afe088c34e0742d01df)。文中的代码片段保留关键控制流，并在注释中标出阅读重点。

[上一篇](/posts/lance-vector-index-internals-1-data-flat/) 建立了 Flat KNN 基线：扫描所有有效向量，计算真实距离，再按 `_distance, _rowid` 取 Top-K。它结果精确，但无法避免全量向量 IO 和距离计算。

IVF 的切入点很直接：**先用少量 centroids 把向量空间划成互不重叠的 partitions；查询时只进入最接近 query 的若干 partitions。**

这一步减少了搜索范围，也第一次引入 recall 风险。理解风险来自哪里，需要把三个问题连起来看：

1. Vector Index V3 如何组合 clustering、sub-index 与 quantization？
2. K-Means 如何训练 centroids，并把每一行分配到唯一 partition？
3. `minimum_nprobes` 与 `maximum_nprobes` 如何控制实际搜索范围？

## 1\. V3 将向量索引拆成三个正交阶段

Lance 10.0 不把 `IVF_PQ`、`IVF_HNSW_SQ` 视为几套完全独立的格式，而是用三个阶段描述一个索引：

```text
clustering × sub-index × quantization
```

| 阶段         | Lance 10.0 选择         | 负责的问题                           |
| ------------ | ----------------------- | ------------------------------------ |
| Clustering   | IVF                     | query 先进入哪些互斥 partitions      |
| Sub-index    | FLAT / HNSW             | 一个 partition 内如何产生候选        |
| Quantization | FLAT / PQ / SQ / RabitQ | partition 内的向量怎样存储和估算距离 |

常见名称只是这三个维度的简写：

```text
IVF_FLAT
  clustering = IVF
  sub-index   = FLAT
  quantizer   = FLAT

IVF_PQ
  clustering = IVF
  sub-index   = FLAT      # 名称中省略
  quantizer   = PQ

IVF_HNSW_SQ
  clustering = IVF
  sub-index   = HNSW
  quantizer   = SQ
```

源码中的 `StageParams` 保留了这个组合关系：

```rust
pub enum StageParams {
    Ivf(IvfBuildParams),
    Hnsw(HnswBuildParams),
    PQ(PQBuildParams),
    SQ(SQBuildParams),
    RQ(RQBuildParams),
}

match (stages.len(), stages.get(1), stages.last()) {
    (1, _, Some(StageParams::Ivf(_))) => IndexType::IvfFlat,
    (2, _, Some(StageParams::PQ(_))) => IndexType::IvfPq,
    (3, Some(StageParams::Hnsw(_)), Some(StageParams::SQ(_))) => {
        IndexType::IvfHnswSq
    }
    // ...
}
```

完整定义见 [`StageParams`、`VectorIndexParams` 与 `index_type`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/vector.rs#L228-L495)。

这也解释了一个容易误读的名称：**IVF_FLAT 只保证被选中 partition 内使用原始向量做精确距离计算；只要没有搜索全部 partitions，它就不是全局精确 KNN。** 近似首先来自 IVF routing，而不一定来自量化。

## 2\. `index.idx` 与 `auxiliary.idx` 分别保存什么

Vector Index V3 的一个物理 segment 位于 `_indices/{uuid}/`，核心是两个普通 Lance 文件：

```text
_indices/{uuid}/
├── index.idx
└── auxiliary.idx
```

它们不是“主文件 + 临时文件”，而是两个职责不同的持久化部分。

### `index.idx`：搜索结构与全局 IVF 模型

`index.idx` 保存 sub-index 结构：

- FLAT sub-index 当前只需一个 marker，不复制全部向量；
- HNSW sub-index 保存 vector id、neighbors 和 neighbor distances；
- Arrow schema metadata 记录 index type、distance type 和各 partition 的 sub-index metadata；
- global buffer 中的 IVF protobuf 保存 centroid tensor、partition offsets、lengths 和可选 K-Means loss。

### `auxiliary.idx`：按 partition 排列的向量存储

`auxiliary.idx` 保存 `_rowid` 与实际用于搜索的 vector storage：

- FLAT 保存未量化向量；
- PQ 保存每个 sub-vector 的 code；
- SQ 保存逐维 quantized code；
- RabitQ 保存 binary code 与距离修正因子；
- 它自己的 IVF metadata 只追踪各 partition 在本文件中的 offset 与 length，不再保存 centroids；
- PQ codebook 或 RabitQ rotation matrix 放在该文件的 global buffer。

完整格式见 [Vector Index V3 的 storage layout](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/docs/src/format/index/vector/index.md#L56-L286)。

![IVF 的构建、V3 双文件布局与查询路由](/images/lance-vector-index-internals/vector-ivf-v3-routing.svg)

可编辑图源：[vector-ivf-v3-routing.excalidraw](/images/lance-vector-index-internals/vector-ivf-v3-routing.excalidraw)

两个文件中的 partitions 都必须按 partition id 顺序写入。这样 `offsets[i] + lengths[i]` 就能定位第 `i` 个 partition 的连续范围，不需要为每个 partition 创建独立对象。对对象存储而言，这避免了 partition 数量直接变成文件数量；代价是构建阶段必须先算出 partition id，再按 partition 重排数据。

## 3\. `target_partition_size` 决定 partition 数量

Lance 10.0 仍接受 `num_partitions`，但 Python API 已将它标为 deprecated，推荐给出目标 partition 大小：

```python
dataset.create_index(
    "vector",
    "IVF_FLAT",
    metric="L2",
    target_partition_size=8192,
)
```

两者的区别不是语法，而是配置意图：

- `num_partitions=128` 把某次数据规模下的结果写死；
- `target_partition_size=8192` 表达“希望每个 partition 大约容纳多少 vectors”，构建时再根据行数推导数量。

Rust 的推导函数非常直接：

```rust
pub fn recommended_num_partitions(
    num_rows: usize,
    target_partition_size: usize,
) -> usize {
    const MAX_PARTITIONS: usize = 4096;
    (num_rows / target_partition_size).clamp(1, MAX_PARTITIONS)
}
```

构建入口的优先级是：

```rust
let num_partitions = match (params.num_partitions, params.centroids.as_ref()) {
    (Some(n), _) => n,                           // [1] 兼容显式数量
    (None, Some(centroids)) => centroids.len(), // [2] 预训练模型决定数量
    (None, None) => recommended_num_partitions(
        num_rows,
        params.target_partition_size
            .unwrap_or(index_type.target_partition_size()), // [3]
    ),
};
```

完整实现见 [`IvfBuildParams` 与 `recommended_num_partitions`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/ivf/builder.rs#L17-L150) 和 [`prepare_vector_segment_build`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/vector.rs#L507-L599)。

例如 `1_000_000` 行、`target_partition_size=8192` 会得到：

```text
num_partitions = floor(1_000_000 / 8192) = 122
```

这里的 8192 不是 partition size 上限。它只用于决定 K-Means 的 `k`；真实 partition size 由数据分布和训练结果决定，可能明显不均衡。

如果不指定 target，Lance 会按 index type 选择默认值。10.0 中 IVF_FLAT 与 IVF_RQ 默认 4096，IVF_PQ / IVF_SQ 默认 8192，IVF_HNSW_* 默认 `1 << 20`。这些是通用起点，不是对任意 workload 的最优保证，定义见 [`IndexType::target_partition_size`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index-core/src/lib.rs#L245-L263)。

## 4\. IVF 训练：采样、K-Means 与 centroids

确定 `num_partitions = k` 后，Lance 默认准备：

```text
sample_size_hint = k × sample_rate
sample_rate default = 256
```

如果 Dataset 小于这个数量，就扫描全部有效 vectors；否则从 Dataset 采样。NULL 在采样阶段被排除，NaN 和 Infinity 在训练前通过 finite mask 移除。

```rust
let sample_size_hint = num_partitions * params.sample_rate;
let training_data = maybe_sample_training_data(
    dataset, column, sample_size_hint, fragment_ids,
).await?;

let (training_data, metric) = if metric_type == MetricType::Cosine {
    (normalize_fsl_owned(training_data)?, MetricType::L2) // [1]
} else {
    (training_data, metric_type)
};

let training_data = filter_finite_training_data(training_data)?; // [2]
let ivf = train_ivf_model(centroids, &training_data, metric, params, progress).await?;
```

- `[1]` 表示 Cosine 的 IVF 训练先归一化向量，再使用 L2 做 clustering。
- `[2]` 保证坏值不会污染 centroid。

完整入口见 [`build_ivf_model`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/vector/ivf.rs#L1594-L1715)。

K-Means 的核心循环仍然是 assignment 与 centroid update，但 Lance 10.0 还加入 balance loss，不应把它描述成“严格等大的聚类”：

```rust
for _ in 1..=max_iters {
    let (membership, radius, losses) = compute_membership_and_loss(
        centroids,
        vectors,
        balance_factor,
        cluster_sizes,
    );                                  // [1] 分配到 centroid

    adjusted_balance_factor = compute_cluster_sizes(
        &membership, &radius, &losses, &mut cluster_sizes,
    );
    let loss = losses.iter().sum::<f64>() + compute_balance_loss(...); // [2]

    kmeans = to_kmeans(vectors, &membership, cluster_sizes); // [3] 更新 centroid
    if converged(loss, last_loss) {
        break;
    }
}
```

完整训练循环见 [`KMeans::train_kmeans`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/kmeans.rs#L767-L884)。`IvfBuildParams` 默认 `max_iters=50`，[`KMeansParams`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/kmeans.rs#L52-L118) 默认 `redos=1`；实际可提前收敛。

`sample_rate` 决定训练最多观察多少 vectors，不决定每个 partition 最终写入多少 rows。训练样本缺少某类数据时，即使 partition 数量很多，也不会自动产生代表那类数据的好 centroid。

## 5\. 构建分配：每个向量只属于一个 partition

训练完成后，构建流程重新处理待索引数据，为每行选择最近 centroid，并增加 `__ivf_part_id`：

```rust
let (part_ids, dists) = match &self.index {
    Some(index) => vectors
        .iter()
        .map(|vector| index.search(vector)) // [1] 最近 centroid
        .unzip(),
    None => compute_partitions_arrow_array(
        &self.centroids,
        vectors,
        self.distance_type,
    )?,
};

batch.try_with_column(PART_ID_FIELD.clone(), part_ids) // [2]
```

完整实现见 [`PartitionTransformer`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/ivf/transform.rs#L27-L128)。之后 shuffler 按 partition 重排，V3 writer 再按 partition 顺序将 vector storage 与 sub-index 写入对应文件。

关键不变量是：

```text
一个有效 vector -> 一个 partition id
```

查询无需合并构建时的重叠 partition；代价是靠近 Voronoi 边界的两个近邻可能被分到不同 partitions。IVF recall 风险正是从这里产生。

## 6\. 查询路由：先对 centroids 做一次小型 Flat KNN

查询阶段不先读取所有 vectors，而是先计算 query 到每个 centroid 的距离，并按距离排序：

```rust
let distances = distance_to_all_centroids(query, centroids);
let partition_ids = sort_to_indices(
    &distances,
    None,
    Some(nprobes),
)?;
let centroid_distances = take(&distances, &partition_ids)?;
```

完整实现见 [`kmeans_find_partitions`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/kmeans.rs#L1303-L1362)。返回结果必须按“离 query 最近的 centroid”排序，执行器再依次搜索对应 partitions。

Cosine index 在 ranking 前会把 query 归一化，然后用 L2 距离与“从归一化训练数据学到的 centroids”比较。K-Means 更新 centroid 时取成员向量的算术均值，不会再把 centroid 单独单位归一化。训练、更新与查询入口分别见 [`build_ivf_model`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/vector/ivf.rs#L1696-L1710)、[`KMeans::to_kmeans`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/kmeans.rs#L420-L440) 和 [`normalize_query_for_index`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/io/exec/knn.rs#L128-L139)。

### 贯穿示例：最近向量不一定在最近 centroid 中

假设训练后有三个二维 centroids：

```text
c0 = [0, 0]   c1 = [5, 0]   c2 = [0, 5]
```

边界两侧有两行：

```text
r1 = [2.4, 0] -> P0
r2 = [2.6, 0] -> P1
q  = [2.49, 0]
```

query 到 centroids 的平方 L2 是：

```text
dist(q, c0) = 6.2001
dist(q, c1) = 6.3001
dist(q, c2) = 31.2001
```

`nprobes=1` 只搜索 P0。它能找到 r1：

```text
dist(q, r1) = 0.0081
```

但真正的第二近邻 r2 位于 P1：

```text
dist(q, r2) = 0.0121
```

如果 P0 中下一行距离大于 0.0121，`k=2` 的结果就会漏掉 r2。将 probes 增加到 2 才会在这个例子中恢复 Flat Top-2。

这个例子说明：centroid ranking 选择的是**最可能包含近邻的 partitions**，不是对 partition 内所有向量距离的下界证明。

## 7\. `minimum_nprobes` 与 `maximum_nprobes` 不是同一个参数

Lance 10.0 的 `Query` 将 probes 拆成上下界：

```rust
pub struct Query {
    pub minimum_nprobes: usize,        // 至少搜索多少 partitions，默认 1
    pub maximum_nprobes: Option<usize>,// 最多搜索多少；None 表示需要时可到全部
    pub k: usize,
    // ...
}
```

执行过程分两段：

```text
1. initial search
   搜索距离最近的 minimum_nprobes 个 partitions

2. late search
   如果还没有得到 k 个有效候选，继续按 centroid 距离搜索
   直到得到 k 个候选，或达到 maximum_nprobes
```

对应控制流见 [`Query` 的 probes 定义](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector.rs#L104-L168)、[`initial_search`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/io/exec/knn.rs#L1778-L1848) 和 [`late_search`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/io/exec/knn.rs#L1627-L1775)。

Python 查询可以显式表达这个范围：

```python
result = dataset.to_table(
    nearest={
        "column": "vector",
        "q": [2.49, 0.0],
        "k": 10,
        "minimum_nprobes": 2,
        "maximum_nprobes": 8,
    }
)
```

旧的单值 `nprobes=8` 仍可使用，但它会同时设置 minimum 与 maximum，相当于固定搜索 8 个 partitions。Python binding 还会拒绝 `< 1` 或 `minimum > maximum` 的组合，见 [`vector_query_params_from_dict`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/src/dataset.rs#L5415-L5480)。

自适应继续搜索只解决“初始 partitions 连 k 个有效候选都没有”的问题。**已经找到 k 个候选，不代表未搜索 partition 中没有更近的向量。** 所以 `maximum_nprobes=None` 也不等于每次都搜索全部 partitions，更不等于自动获得 Flat recall。

## 8\. 分区不均衡怎样影响 recall 与 latency

`target_partition_size` 是期望，K-Means balance loss 也是优化项，两者都不提供等大保证。真实长度会写入 IVF metadata 的 `lengths`。

不均衡主要产生三类影响：

1. **大 partition 延迟更高**：IVF_FLAT 要在其中计算更多真实距离；HNSW 或量化也要处理更大的局部结构。
2. **小 partition 更容易触发 late search**：过滤或删除后，前几个 partitions 可能凑不够 `k`，执行器继续 probe。
3. **边界与稀疏区域 recall 更脆弱**：数据形状不能被有限 centroids 良好描述时，近邻更容易跨 partition。

因此三个参数不能互相替代：

| 参数                    | 主要控制                       | 不能保证                        |
| ----------------------- | ------------------------------ | ------------------------------- |
| `target_partition_size` | 构建时推导 partition 数量      | 每个 partition 恰好同样大       |
| `sample_rate`           | K-Means 最多使用的训练样本规模 | 训练样本一定代表线上 query 分布 |
| `nprobes` 范围          | 每次查询搜索多少 partitions    | 较少 probes 仍保持固定 recall   |

较小的 target 通常产生更多、更小的 partitions：单个 probe 扫描更少 vectors，但 centroid ranking、metadata 和构建 shuffle 的开销会上升。较大的 target 则相反。这个取舍没有脱离数据分布的固定答案。

一个可信的调参过程应至少记录：

```text
partition lengths: min / p50 / p95 / max
query probes actually searched
Recall@K against article 1's Flat ground truth
end-to-end p50 / p95 latency
index size and build time
```

先固定 index type 和训练模型，扫描 probes；再调整 target 并重建索引。否则同时修改 partitions 与 query probes，很难判断 recall 变化来自 routing 还是 partition 内搜索。

## 9\. 总结

Vector Index V3 与 IVF 的核心边界是：

1. V3 将 clustering、sub-index 与 quantization 分开组合；`IVF_FLAT` 的 FLAT 只描述 partition 内搜索。
2. `index.idx` 保存 sub-index 结构与带 centroids 的 IVF 模型，`auxiliary.idx` 保存按 partition 排列的 row ids 与 vector storage。
3. `target_partition_size` 用数据行数推导 partition 数量；deprecated `num_partitions` 仍然优先，但更容易把数据规模写死。
4. K-Means 从最多 `num_partitions × sample_rate` 个有效 vectors 训练 centroids；Cosine 先归一化再按 L2 聚类。
5. 每个有效 vector 构建时只属于一个 partition；query 再按 centroid 距离选择 partitions。
6. `minimum_nprobes` 是初始搜索下界，`maximum_nprobes` 是继续搜索的上界；拿到 k 个候选不是 Flat recall 的证明。
7. partition size、训练代表性和 probes 共同决定 recall、延迟与构建成本，必须用 Flat ground truth 做端到端评估。

下一篇将进入 PQ、SQ 与 RabitQ：`auxiliary.idx` 为什么不一定保存原始向量，以及量化误差如何通过候选扩张与 refine 控制。
