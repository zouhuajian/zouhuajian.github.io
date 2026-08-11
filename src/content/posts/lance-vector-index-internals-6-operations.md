---
author: "Jay H. Zou"
pubDatetime: 2026-08-10T10:50:00+08:00
title: "Lance 向量索引原理与源码分析（六）"
lang: zh-CN
tags:
  - Lance
  - Vector Index
  - 索引维护
  - 性能调优
  - 源码分析
description: "索引维护、调参与工程选型"
---

<style>
  @media (max-width: 640px) {
    article table {
      display: block;
      max-width: 100%;
      overflow-x: auto;
    }
  }
</style>

> 本文源码基于 Lance `v10.0.0` 的提交 [`95f2f36b`](https://github.com/lance-format/lance/tree/95f2f36b22043c3face00afe088c34e0742d01df)。文中的代码片段保留关键控制流，省略日志、错误处理与本文无关的分支。

[上一篇](/posts/lance-vector-index-internals-5-query/)已经说明：正常向量查询会把索引支路与未索引 fragment 的 Flat KNN 支路合并，所以 append 后通常不会立即返回错误结果。但“仍然正确”不等于“索引不需要维护”。

随着 append、update、delete 和 compaction 持续发生，同名向量索引会形成多个不可变 segments；查询要打开更多索引文件，或扫描更多未覆盖数据。工程上真正要管理的是三件互不等价的事：

| 维度       | 问题                                   | 验证方法                                        |
| ---------- | -------------------------------------- | ----------------------------------------------- |
| 正确性     | 当前有效行、过滤和删除语义是否完整     | 与当前 Dataset 的 Flat 结果、行数和过滤条件核对 |
| ANN 召回率 | 已索引行中的真实 Top-K 是否进入候选    | 与 Flat Top-K 的 row ids 计算 Recall@K          |
| 速度       | 查询、构建和维护是否满足资源与延迟目标 | p50/p95、构建时间、索引字节、内存与缓存命中     |

本文不提供脱离数据集的“最佳参数”，而是从 v10.0.0 的生命周期实现出发，建立一套可复现实验和选型方法。

## 1\. 向量索引 Segment 为什么保持不可变

Lance 不会在每次写入时就原地修改 IVF partitions、量化编码和 HNSW 图。索引 metadata 记录每个 segment 的 UUID、创建时对应的 Dataset version 和 fragment bitmap；新数据可以先保持未索引，也可以写成新的 delta segment，最终再通过一次 Dataset transaction 发布新的 segment 集合。

这种设计把长时间运行的索引构建与 manifest 提交分离：读者要么看到旧集合，要么看到提交后的新集合，不会看到一个只写完部分 partitions 的原地索引。但查询成本会随状态变化：

```text
base segment(F0..F99)
  + append F100..F109       -> Flat fallback for uncovered fragments
  + optimize append         -> base segment + delta segment
  + more delta segments     -> query fan-out grows
  + merge / retrain         -> publish replacement segment(s)
```

`optimize_indices` 的实现先按 logical index name 收集 segments，为每组构建新 segment，然后在一个 `CreateIndex` operation 中同时提交 `new_indices` 与 `removed_indices`。完整控制流见 [`DatasetIndexExt::optimize_indices`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index.rs#L1869-L1943)。

```rust
for segments in group_by_logical_name(indices) {
    let Some(merged) = merge_indices(dataset, segments, options).await? else {
        continue;
    };
    new_indices.push(merged.new_metadata());
    removed_indices.extend(merged.removed_indices());
}

commit(CreateIndex {
    new_indices,
    removed_indices,
}).await?;
```

这是维护时的首要不变量：**先生成完整的新索引文件，再原子替换 manifest 中的引用**。

## 2\. 四种数据操作分别怎样改变索引状态

### Append：新增未覆盖 fragments

append 产生新 fragments，但旧 segment 的 fragment bitmap 不会变化。普通查询使用 Flat fallback 扫描这部分数据，因而覆盖仍完整；未索引行持续增长时，查询会越来越像“ANN + 一段全量扫描”。`fast_search=True` 则主动跳过这部分数据，换取 indexed subset 上的低延迟。

### Update：删除旧行，再把新值写到新 fragments

v10.0.0 的 update 是 rewrite rows。执行层扫描目标行并写出新 fragments，然后对原地址应用 deletion；提交注释明确写着 updated rows are moved，即 deleted and appended。源码见 [`UpdateJob::execute_impl` 与 `commit_impl`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/write/update.rs#L281-L439)。

因此，更新向量后会同时出现两个状态：

1. 旧 segment 中仍有旧 row address，但 deletion mask 会阻止它被返回；
2. 新向量位于新 fragment，在索引追上前由 Flat fallback 搜索。

这保证的是当前版本语义，不代表 update 会增量修改原 HNSW 图或原 PQ codes。

### Delete：逻辑删除先于索引物理清理

delete 同样不会逐条改写不可变索引。查询创建 `DatasetPreFilter`，为索引覆盖的 fragments 加载 deletion files 或 stable row-id allow list，并把删除行排除在 ANN 结果之外。源码见 [`DatasetPreFilter`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/prefilter.rs#L40-L93) 与 [`create_deletion_mask_impl`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/prefilter.rs#L251-L360)。

删除比例升高时，即使结果正确，索引仍可能读取和遍历大量最终会被 mask 掉的条目。compaction 可以物理移除删除行，索引维护再处理新的覆盖状态。

### Compaction：重写文件，因此必须处理行身份

`compact_files` 会移除 deleted rows、移除已 drop 的列并合并小 fragments，见 [`compact_files`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/optimize.rs#L839-L892)。它不改变向量本身，却可能改变 fragment id 与物理 row address。

v10.0.0 有三条边界清晰的路径：

- address-style row ids 且没有 defer 时，提交 compaction 前立即生成地址映射，并只 remap 覆盖受影响 fragments 的 indices；
- stable row ids 不依赖物理地址重写索引条目，但提交仍要更新 fragment 覆盖信息；
- `defer_index_remap=True` 时，先写 Fragment Reuse Index 记录旧地址到新地址的关系，后续再让 index remap 追上。

相关实现见 [`commit_compaction`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/optimize.rs#L2021-L2251) 与 [`DatasetIndexRemapper::remap_indices`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/index.rs#L32-L100)。remap 修正的是 row identity 和 fragment coverage，不会重新训练 IVF centroids、PQ codebook 或 HNSW 图结构。

![Lance 向量索引维护状态图](/images/lance-vector-index-internals/vector-index-operations.svg)

可编辑图源：[vector-index-operations.excalidraw](/images/lance-vector-index-internals/vector-index-operations.excalidraw)

## 3\. 先观察覆盖与 Segment，再决定维护动作

v10.0.0 的首选元数据入口是 `describe_indices()`。它返回 logical index 的字段、已索引行数、segments 和总字节数；每个 segment 还包含 UUID、fragment ids、index version 与 `size_bytes`。结构定义见 [`IndexDescription` 与 `IndexSegmentDescription`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/src/indices.rs#L603-L718)。

```python
desc = next(d for d in dataset.describe_indices() if d.name == "vector_idx")

print(desc.index_type, desc.num_rows_indexed, desc.total_size_bytes)
for segment in desc.segments:
    print(segment.uuid, len(segment.fragment_ids), segment.size_bytes)

stats = dataset.stats.index_stats("vector_idx")
print(stats["num_indexed_rows"], stats["num_unindexed_rows"])
print(stats["num_indices"], stats["num_indexed_rows_per_delta"])
```

`dataset.stats.index_stats()` 提供维护视角的 JSON 统计；其 Rust 结果包含 `num_indices`、每个 delta 的行数、indexed rows 与 unindexed rows，见 [`index_statistics`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index.rs#L2097-L2118)。旧的 `dataset.index_statistics()` 在 v10.0.0 仍存在，但 Python 已标记 deprecated，应改用前述两个入口，见 [`LanceDataset.index_statistics`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L1099-L1119)。

不要只用“每天跑一次”作为维护策略。至少同时记录：

- `num_unindexed_rows / current_rows`：正常查询 Flat fallback 的规模；
- segment 数与每个 segment 的行数：ANN fan-out 是否持续增加；
- `total_size_bytes` 与 segment size：索引存储和缓存工作集；
- append 后的 p95 与 Recall@K：新数据分布是否仍适合旧模型；
- compaction 的 remap backlog：是否长期依赖 Fragment Reuse Index。

触发阈值必须来自业务 SLO 与实测曲线，不应从另一份数据集复制一个固定百分比。

## 4\. `optimize_indices` 的三个动作不要混用

v10.0.0 的 `OptimizeOptions` 把维护分为 append、merge 与 retrain，源码见 [`OptimizeOptions`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/optimize.rs#L9-L93)。

### 创建 Delta Segment

```python
dataset.optimize.optimize_indices(
    index_names=["vector_idx"],
    num_indices_to_merge=0,
)
```

`num_indices_to_merge=0` 只把当前未索引 fragments 写成新 segment，不合并旧 segments；若没有未索引数据则 no-op。向量路径复用 logical index 最后一个 segment 的模型，使连续 delta 可保持可合并性。实现见 [`merge_indices_with_unindexed_frags`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/append.rs#L826-L895)。

适合：先快速消除不断变大的 Flat fallback。代价是 segment fan-out 增加。

### 合并 Delta Segments

```python
dataset.optimize.optimize_indices(
    index_names=["vector_idx"],
    num_indices_to_merge=3,
)
```

显式 `N` 会把未索引更新与最新 N 个 segments 合并成一个新 segment。适合：delta 已经积累，查询打开的 segments 过多。合并行为由 [`merge_indices`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/append.rs#L627-L685) 驱动。

### 重新训练

v10.0.0 的 Python docstring 虽然仍列出 deprecated 的 `retrain=True`，但 native binding 实际只读取 `num_indices_to_merge` 与 `index_names`；传入 `retrain` 会被忽略，不能作为可用的 Python 重训入口。这个不一致可直接对照 [`DatasetOptimizer.optimize_indices`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L7156-L7187) 与 [native binding](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/src/dataset.rs#L2407-L2431)。

因此需要重新训练时，应通过 `create_index(..., replace=True)` 完整重建同名索引，并在切换前后用固定查询集对照。只有当 Recall@K 已随新分布下降，而放大 nprobes / ef / refine 不能以可接受成本恢复时，才应承担完整重建成本。

这三类操作的顺序通常是：先 append delta 控制 fallback，再按 fan-out 合并；只有数据分布漂移有证据时才重新训练。不要把“覆盖已经追平”和“模型仍适合新分布”视为同一结论。

## 5\. v10.0.0 的分布式构建边界

公开流程必须使用 `create_index_uncommitted(...)`：它只为指定 fragments 创建 segment，并返回提交阶段需要的 `Index` metadata。普通的 `create_index(fragment_ids=...)` 包装层会丢弃这个返回值，因此不能串起分布式提交。最小流程如下：

```python
segments = [
    dataset.create_index_uncommitted(
        "vector",
        "IVF_PQ",
        name="vector_idx",
        metric="cosine",
        fragment_ids=worker_fragment_ids,
        # Workers that must share model semantics also receive the same
        # ivf_centroids and pq_codebook here.
    )
    for worker_fragment_ids in fragment_assignments
]

dataset.commit_existing_index_segments(
    "vector_idx",
    "vector",
    segments,
)
```

各 worker 的 metadata 最后通过 `commit_existing_index_segments(...)` 一次发布。Python 文档明确支持两种 model scope，见 [`create_index_uncommitted`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L4295-L4349) 与 [`commit_existing_index_segments`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L4544-L4551)：

1. workers 共享相同 IVF centroids；IVF_PQ 还共享相同 PQ codebook。这样 segments 的模型语义一致，才适合做物理 merge；
2. workers 各自训练模型。它们可以作为多个 UUID 一起提交，查询会分别解释各自的 partition ids，但不应直接物理合并。

分布式构建还必须固定同一个 Dataset snapshot，并让 fragment assignments 不重叠。全局训练辅助流程可以先准备 IVF/PQ artifacts，再让 workers 加载同一份模型；实现入口见 [`prepare_global_ivf_pq`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/indices/builder.py#L224-L279)。

这里没有“worker 越多一定越快”的保证。对象存储吞吐、shuffle、训练样本、commit 冲突和最终 segment fan-out 都必须单独测量。

## 6\. 一套可重复的评估方法

### 固定实验输入

每轮比较都应固定：

1. 同一个 Dataset version 与相同的非空向量集合；
2. 一批从真实 workload 抽样并冻结的 query vectors；
3. 相同 metric、K、过滤条件分布和返回列；
4. 相同机器、并发度、对象存储位置与 cache 配置；
5. warm-cache 与 cold-cache 两套独立结果。

Flat 基线必须显式关闭索引。测试 ANN 时，只改变索引类型及其参数，不改变 query set：

```python
def search(dataset, vector, *, use_index, **ann):
    return dataset.scanner(
        columns=[],
        nearest={
            "column": "vector",
            "q": vector,
            "k": K,
            "metric": "cosine",
            "use_index": use_index,
            **ann,
        },
        with_row_id=True,
        prefilter=True,
        filter="tenant_id = 42",
    ).to_table()

truth = [search(dataset, q, use_index=False) for q in queries]
actual = [search(dataset, q, use_index=True, nprobes=16) for q in queries]

def recall(truth_table, actual_table):
    truth_ids = set(truth_table["_rowid"].to_pylist())
    if not truth_ids:
        return None  # Report empty-truth queries separately.
    actual_ids = set(actual_table["_rowid"].to_pylist())
    return len(truth_ids & actual_ids) / len(truth_ids)


per_query_recall = [
    value
    for t, a in zip(truth, actual)
    if (value := recall(t, a)) is not None
]
if not per_query_recall:
    raise ValueError("All filtered Flat truth sets are empty")
recall_at_k = sum(per_query_recall) / len(per_query_recall)
```

这里的 Recall@K 衡量 ANN 与同一语义下精确 Flat Top-K 的 row-id overlap。过滤后真值可能不足 K 条，所以分母使用实际 `len(truth_ids)`；空真值 query 要单独计数，不能混入均值。若过滤、metric、Dataset version 或 K 不一致，这个数字没有可比性。

### 同时记录五组结果

| 指标                | 记录方式                                                               |
| ------------------- | ---------------------------------------------------------------------- |
| Recall@K            | overlap / 实际非空 Flat truth 条数；空真值单独统计，再报告均值和低分位 |
| 查询延迟            | 足够多次重复后的 p50、p95；cold/warm 分开                              |
| 构建与维护时间      | 初次 create、append delta、merge 分开计时                              |
| 索引字节            | `describe_indices().total_size_bytes`，并保留每个 segment size         |
| 内存 / cache 工作集 | 进程 peak RSS、index cache 配置、对象存储读取量或 cache hit 指标       |

此外要在 append、update、delete、compaction 之后重复 correctness suite。稳定态上的高 Recall 不能证明生命周期中的覆盖与 deletion mask 正确。

## 7\. 只比较 v10.0.0 实际支持的索引类型

Python 在 v10.0.0 接受的向量索引类型只有七种，验证列表见 [`valid_index_types`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L3734-L3747)。Flat 是精确扫描基线，不是一个持久化 vector index type。

| 候选          | 主要近似来源                     | 适合首先验证的场景                | 主要代价 / 风险                     |
| ------------- | -------------------------------- | --------------------------------- | ----------------------------------- |
| Flat          | 无                               | 小数据、正确性基线、低频查询      | 扫描成本随行数增长                  |
| IVF_FLAT      | IVF 未探测 partitions            | 原始向量可接受、希望隔离量化误差  | 索引字节与向量读取较大              |
| IVF_PQ        | IVF + PQ 编码距离                | 存储、内存或 I/O 受限的大规模向量 | 需要训练 codebook，通常要测 refine  |
| IVF_SQ        | IVF + scalar quantization        | 希望简单压缩且保留逐维结构        | 仍需实测量化召回与原始向量精排成本  |
| IVF_RQ        | IVF + RabitQ 近似                | 需要紧凑编码并愿意以实验验证召回  | 不能套用 PQ 参数经验                |
| IVF_HNSW_FLAT | IVF + partition 内 HNSW          | partition 内线性扫描成为瓶颈      | 图的字节、cache 与 ef 调参成本      |
| IVF_HNSW_PQ   | IVF + HNSW + PQ                  | 同时需要图搜索与压缩              | 参数耦合最多，构建和诊断复杂        |
| IVF_HNSW_SQ   | IVF + HNSW + scalar quantization | 想要 HNSW 与较简单的定长量化组合  | 仍要分别评估 nprobes、ef 与量化误差 |

“近似层更多”不等于“延迟一定更低”。当候选数很少、cache miss 严重或过滤已经把集合缩小后，复杂索引可能没有优势。必须让表中每一行跑同一套 workload，再画出 Recall@K 与 p95、index bytes 的 Pareto frontier。

## 8\. 从场景而不是索引名称做决策

| 场景                            | 首批候选                          | 必须守住的边界                                            |
| ------------------------------- | --------------------------------- | --------------------------------------------------------- |
| 数据量小或查询低频              | Flat、IVF_FLAT                    | 先证明索引收益高于维护成本；IVF_FLAT 仍受 probes 近似影响 |
| 要求全局精确                    | Flat                              | 不跳过 partitions；把 Flat 结果作为 correctness contract  |
| 存储 / 内存 / I/O 是主要瓶颈    | IVF_PQ、IVF_SQ、IVF_RQ            | Recall@K、refine 原始向量 I/O                             |
| partition 内搜索成为主要 p95    | 三种 IVF_HNSW 变体                | HNSW cache、ef 与构建成本                                 |
| 高频 append，但新数据分布稳定   | 现有类型 + delta append / merge   | unindexed ratio 与 segment fan-out                        |
| 新数据出现明显概念漂移          | 当前索引和 retrained rebuild 对照 | 先用固定 query set 证明召回下降，不凭数据量猜测           |
| 高选择性 tenant / category 过滤 | Scalar prefilter + 各向量候选     | Scalar Index 覆盖、返回行数、过滤后的 Recall@K            |
| 强 read-after-append            | 正常 search + 及时 optimize       | 不用 `fast_search` 掩盖未索引覆盖                         |

最终选型不是“选择一个索引类型”，而是确定一份可操作契约：索引类型与构建参数、查询 nprobes / ef / refine、prefilter 策略、允许的 unindexed ratio、delta 合并策略、重训证据和回滚基线。

## 9\. 维护闭环

一套最小但完整的生产闭环可以写成：

```text
Observe
  -> correctness suite still passes?
  -> Recall@K still meets target?
  -> p95 / unindexed rows / segments / bytes within budget?

Act
  -> append delta to remove Flat fallback
  -> merge segments to reduce fan-out
  -> retrain or rebuild only after distribution-drift evidence

Verify
  -> rerun Flat comparison and lifecycle cases
  -> publish new index metadata atomically
```

正确性是不能交换的底线；Recall 是近似搜索的质量目标；速度是同一正确性和召回约束下的优化结果。把这三者分开，再把 append、update、delete 与 compaction 放进同一套持续实验，Lance 向量索引才从一次性构建参数变成可维护的工程系统。
