---
author: "Jay H. Zou"
pubDatetime: 2026-08-10T10:00:00+08:00
title: "Lance 向量索引原理与源码分析（一）"
lang: zh-CN
tags:
  - Lance
  - Vector Index
  - Flat KNN
  - 源码分析
description: "向量数据模型与精确检索"
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

> 本文源码基于 Lance `v10.0.0` 的提交 [`95f2f36b2`](https://github.com/lance-format/lance/tree/95f2f36b22043c3face00afe088c34e0742d01df)。文中的代码片段只保留理解数据边界和查询控制流所需的部分。

向量索引首先要回答的不是“IVF 该分多少区”，而是一个更基础的问题：**一行向量在 Lance 中究竟是什么，距离又怎样变成可排序的查询结果？**

如果这两个边界不清楚，后续 ANN 的 recall 很难解释。返回结果不同，可能来自索引近似，也可能只是维度、数据类型、NULL、NaN 或距离定义不同。因此本篇暂时不创建 ANN 索引，而是先建立后面所有实验共用的正确性基线：Flat KNN。

## 1\. 一行向量是一个定长 Arrow List

最常见的 Lance 向量列是 Arrow `FixedSizeList`：一行对应一个 list，list 长度就是向量维度。

```python
import pyarrow as pa

values = pa.array(
    [
        1.0, 0.0,  # r0
        0.8, 0.6,  # r1
        0.0, 1.0,  # r2
    ],
    type=pa.float32(),
)
vectors = pa.FixedSizeListArray.from_arrays(values, 2)
```

它的逻辑布局可以写成：

```text
FixedSizeList<Float32, 2>
├── r0 -> [1.0, 0.0]
├── r1 -> [0.8, 0.6]
└── r2 -> [0.0, 1.0]

values buffer -> [1.0, 0.0, 0.8, 0.6, 0.0, 1.0]
```

固定维度不是 API 约定，而是物理类型的一部分。reader 可以用 `row * dimension` 定位一行，不需要为每行保存起止 offset；距离 kernel 也可以把连续 values buffer 按 dimension 切片。

Lance 10.0 的 Python 创建索引入口还接受一维 `FixedShapeTensor`。它从 `shape[0]` 取得维度；多维 tensor 不在这个向量索引入口的范围内。

```python
if pa.types.is_fixed_size_list(field.type):
    dimension = field.type.list_size
elif isinstance(field.type, pa.FixedShapeTensorType) \
        and len(field.type.shape) == 1:
    dimension = field.type.shape[0]
else:
    raise TypeError("Vector column must be fixed-size and one-dimensional")
```

完整校验见 [`LanceDataset._create_index_impl`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L3681-L3721)。因此 `FixedShapeTensor` 更适合表达上游的 tensor 语义，但进入向量检索后，本篇仍将它理解为“每行恰好有 `dimension` 个元素”的定长向量。

### `List<FixedSizeList>` 是另一种查询语义

Lance 10.0 也能识别：

```text
List<FixedSizeList<Float32, dimension>>
```

它表示一行包含数量可变的多个向量，例如一篇文档的多个 token vectors。源码会从内层 `FixedSizeList` 推断维度，训练采样时再把多向量列展平。它不是“维度可变的单向量”。

10.0 的索引构建明确限制 multivector 只使用 Cosine。对每个 query vector，Flat 路径会在该行的 stored vectors 中取最大 similarity，再把这些最大值求和；最终写出的距离是：

```text
_distance = 1 - sum(max(1 - cosine_distance(q_i, v_j)))
```

它与“各 query vector 的最小 Cosine distance 之和”只相差常数 `1 - query_vector_count`，所以排序相同，但 `_distance` 数值并不相同。相关边界见 [`infer_vector_dim`、`infer_vector_element_type`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/vector/utils.rs#L146-L263)、[`multivec_distance`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-linalg/src/distance.rs#L206-L311) 和 [索引构建校验](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/vector.rs#L542-L549)。

这是独立的检索模型，后续可以单独分析；本系列主线先使用一行一个 `FixedSizeList`。

## 2\. 元素类型决定可用距离

这里要区分三层边界：Python 构建入口、Rust 核心校验，以及查询默认值。Python `create_index()` 接受浮点元素或 `UInt8`；Rust 核心还接受 `Int8` 与 L2 / Cosine / Dot 的组合：

```rust
let supported = match element_type {
    DataType::UInt8 => matches!(distance_type, DistanceType::Hamming),
    DataType::Int8
    | DataType::Float16
    | DataType::Float32
    | DataType::Float64 => {
        matches!(
            distance_type,
            DistanceType::L2 | DistanceType::Cosine | DistanceType::Dot
        )
    }
    _ => false,
};
```

完整实现见 [`validate_distance_type_for`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/vector/utils.rs#L190-L225)。Python 公共构建入口的范围可整理为：

| Vector element | 距离              | Flat 查询默认值 | Python 构建参数               |
| -------------- | ----------------- | --------------- | ----------------------------- |
| `Float16`      | L2 / Cosine / Dot | L2              | 默认 `L2`                     |
| `Float32`      | L2 / Cosine / Dot | L2              | 默认 `L2`                     |
| `Float64`      | L2 / Cosine / Dot | L2              | 默认 `L2`                     |
| `UInt8`        | Hamming           | Hamming         | 必须显式传 `metric="hamming"` |

`create_index()` 的 Python 默认参数始终是 `metric="L2"`，并不会因为列是 `UInt8` 自动改成 Hamming；只有未显式指定 metric 的 Flat 查询会按元素类型选择默认值。Python 入口的类型检查与构建默认值见 [`_create_index_impl`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L3681-L3721) 和 [`create_index`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L4021-L4027)；Flat 查询默认值见 [`default_distance_type_for`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/vector/utils.rs#L190-L197)。`Int8` 是 Rust 核心能力，不应据此推断 v10 Python 公共构建入口也接受它。

这里的 `UInt8` 不表示“用 0 到 255 保存普通浮点向量”。Hamming 对每个 byte 做 XOR，再累计不同 bit 的数量。例如：

```text
query  = 1011_0000
row    = 1001_0100
XOR    = 0010_0100
distance = popcount(XOR) = 2
```

实现见 [`hamming`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-linalg/src/distance/hamming.rs#L24-L63)。所以 `FixedSizeList<UInt8, 32>` 是 32 bytes、256 bits 的 hash，Hamming dimension 在 schema 中仍然是 32。

## 3\. Lance 排序的是 distance，不是 similarity

四种距离最终都写入 `_distance: Float32`，并按升序选 Top-K。这个约定会影响对 Dot 的理解。

| 名称    | Lance 10.0 实际排序值                | 越近意味着       |
| ------- | ------------------------------------ | ---------------- |
| L2      | `sum((x[i] - y[i])²)`                | 平方欧氏距离越小 |
| Cosine  | `1 - dot(x,y) / (norm(x) * norm(y))` | 方向越接近       |
| Dot     | `1 - dot(x,y)`                       | 点积越大         |
| Hamming | `sum(popcount(x[i] XOR y[i]))`       | 不同 bit 越少    |

两个细节需要特别注意：

1. Lance 的 L2 kernel 返回**平方欧氏距离**，没有再开平方。开平方不会改变 Top-K 顺序，但 `_distance` 的数值不能直接当成欧氏长度。
2. Dot 使用 `1 - dot` 转换成升序距离；它可以小于 0。若希望 Dot 与 Cosine 给出相同排序，数据和 query 必须由调用方保证已经归一化。

对应源码见 [`l2_scalar`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-linalg/src/distance/l2.rs#L119-L159)、[`cosine_fast`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-linalg/src/distance/cosine.rs#L943-L966) 和 [`dot_distance`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-linalg/src/distance/dot.rs#L108-L112)。

### 贯穿示例

用 query `q = [1.0, 0.0]` 查询前三行：

| Row | Vector       | L2                     | Cosine | Dot distance |
| --- | ------------ | ---------------------- | ------ | ------------ |
| r0  | `[1.0, 0.0]` | `0² + 0² = 0`          | `0`    | `0`          |
| r1  | `[0.8, 0.6]` | `0.2² + (-0.6)² = 0.4` | `0.2`  | `0.2`        |
| r2  | `[0.0, 1.0]` | `1² + (-1)² = 2`       | `1`    | `1`          |

这三个向量恰好都已归一化，所以 Cosine 和 Dot distance 相同。再加入 `r3 = [2.0, 0.0]` 后：

```text
Cosine(q, r3) = 0
Dot(q, r3)    = 1 - 2 = -1
```

Dot 会把 r3 排在最前，Cosine 则认为 r0 与 r3 方向完全相同。两者没有谁“算错”，只是目标函数不同。

## 4\. 查询入口先拒绝不完整的向量定义

Rust scanner 在生成执行计划前，先验证 `k`、query 长度和列维度：

```rust
if k == 0 {
    return Err(Error::invalid_input("k must be positive"));
}
if q.is_empty() {
    return Err(Error::invalid_input("Query vector must have non-zero length"));
}

let dim = get_vector_dim(self.dataset.schema(), column)?;
if q.len() != dim {
    return Err(Error::invalid_input(format!(
        "query dim({}) doesn't match vector dim({})", q.len(), dim
    )));
}
```

完整入口见 [`Scanner::nearest`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/scanner.rs#L1487-L1608)。

Python 的 list / tuple / NumPy query 通常先转换成 `Float32Array`；如果列是其他浮点类型，scanner 再把 query 转成列的元素类型。不同维度不会被截断、补零或广播。

这条边界很重要：**dimension 属于 schema 和索引模型，不是每次查询可以协商的参数。** 模型升级导致 embedding 从 768 维变成 1024 维时，应写入新列或新 Dataset，而不是让 reader 猜测如何适配。

## 5\. NULL、NaN 与零向量如何处理

向量无效有三个层次，不能混在一起：

### 整行向量为 NULL

Flat 路径将 vector row 的 validity 与 `_rowid` validity 合并，距离 kernel 继续传播这个 null bitmap：

```rust
let validity = if let Some(rowids) = batch.column_by_name(ROW_ID) {
    NullBuffer::union(rowids.nulls(), vectors.nulls())
} else {
    vectors.nulls().cloned()
};

let distances = distance_type.arrow_batch_func()(query, vectors)?;
```

随后执行器排除 NULL distance。完整过程见 [`compute_distance`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/flat.rs#L88-L137) 与 [`KNNVectorDistanceExec::execute`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/io/exec/knn.rs#L898-L945)。

### 某个分量为 NULL、NaN 或 Infinity

索引构建的 `KeepFiniteVectors` 会丢弃以下向量：

- 整行是 NULL；
- 任意分量是 NULL；
- 浮点向量任意分量不是 finite，即 NaN、`+Inf` 或 `-Inf`。

训练样本还会单独经过 `filter_finite_training_data`，避免坏值进入 centroid。实现见 [`KeepFiniteVectors`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/transform.rs#L82-L152) 和 [`filter_finite_training_data`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/index/vector/utils.rs#L345-L357)。

无索引 Flat 查询则在计算后明确排除 NaN distance。它的 row validity 是 `FixedSizeList` 这一层；不要依赖 component NULL 的底层 buffer 值来产生有意义的距离。工程上应在写入边界保证：

```text
每行长度固定
AND 每个分量 non-null
AND 每个浮点分量 finite
```

### Cosine 的零向量

零向量虽然每个分量都是 finite，但 norm 为 0，Cosine 公式会产生 NaN。无索引 Flat 路径最终将 NaN distance 排除；索引构建先归一化，再由 finite 过滤移除无效结果。

因此，“列 non-null”仍不足以保证 Cosine 数据有效。写入前还应检查 `norm(vector) > 0`。这比等查询时发现结果少了一行更容易排障。

## 6\. Flat KNN 是怎样得到精确 Top-K 的

不使用索引时，可以显式设置：

```python
result = (
    dataset.scanner(
        nearest={
            "column": "vector",
            "q": [1.0, 0.0],
            "k": 3,
            "metric": "l2",
            "use_index": False,
        }
    )
    .to_table()
)
```

Flat KNN 的“精确”不是使用了另一种索引，而是对当前扫描输入中的每个有效向量都计算真实距离：

![Flat KNN 从向量列到精确 Top-K 的执行边界](/images/lance-vector-index-internals/vector-data-flat-knn.svg)

可编辑图源：[vector-data-flat-knn.excalidraw](/images/lance-vector-index-internals/vector-data-flat-knn.excalidraw)

核心计划可以裁剪成：

```rust
let distances = KNNVectorDistanceExec::try_new(
    scan, column, query, distance_type,
)?;                                      // [1] 每行计算 _distance

let sort = SortExec::new(
    [
        sort_asc(DIST_COL),               // [2] 距离升序
        sort_asc(ROW_ID),                 // [3] 同距离时稳定地按 row id 排序
    ],
    distances,
)
.with_fetch(Some(k));                     // [4] 只保留 Top-K
```

对应实现见 [`flat_knn`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/scanner.rs#L4958-L5075)。它建立了后续 ANN 实验的参照：

```text
Flat ground truth
  = 在相同 Dataset version、filter、distance 和 k 下
    对所有有效候选计算真实距离后的 Top-K
```

这里的“所有”仍受 query filter、删除向量和所选 Dataset version 限制。比较 ANN recall 时，Flat 与 ANN 必须使用完全相同的数据可见性和距离定义。

## 7\. 为什么 Flat 不能直接替代 ANN

Flat 的代价随候选向量数线性增长。假设有 `N` 行、每行 `D` 个元素，它至少需要读取向量列，并完成数量级为 `N × D` 的距离计算；Top-K sort 可以只维护有限候选，却无法跳过前面的全量距离。

它仍然有四个不可替代的用途：

1. 小数据集直接查询，省去训练与维护索引。
2. 生成 Recall@K 的 ground truth。
3. 验证 metric、归一化和数据清洗是否符合预期。
4. 在索引尚未覆盖新 fragment 时，为查询提供精确补充路径。

第 4 点将在查询执行篇详细分析。现阶段只需要记住：ANN 的目标不是重新定义“最近”，而是少读取、少计算一些不可能进入 Top-K 的向量；最终应以 Flat 的结果作为正确性参照。

## 8\. 总结

Lance 10.0 向量查询的基础边界可以归纳为：

1. 单向量列使用 `FixedSizeList<element, dimension>`；一维 `FixedShapeTensor` 是 Python API 接受的等价定长表达。
2. `List<FixedSizeList>` 表达一行多个向量，不是可变维度；它有独立的匹配聚合语义。
3. Float16/32/64 使用 L2、Cosine 或 Dot，UInt8 binary vector 使用 Hamming。
4. L2 返回平方距离，Cosine 与 Dot 都被转换为越小越近的 distance。
5. query 必须非空、`k > 0` 且维度精确匹配；无效向量不应靠查询路径猜测修复。
6. Flat KNN 扫描全部有效候选、计算真实距离并按 `_distance, _rowid` 取 Top-K，是后续评估 IVF、量化和 HNSW 的 ground truth。

下一篇将进入 Vector Index V3 和 IVF：同一批向量如何训练 centroids、分配到 partitions，以及 query 为什么只搜索距离最近的若干分区。
