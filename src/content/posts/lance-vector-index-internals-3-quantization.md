---
author: "Jay H. Zou"
pubDatetime: 2026-08-10T10:20:00+08:00
title: "Lance 向量索引原理与源码分析（三）"
lang: zh-CN
tags:
  - Lance
  - Vector Index
  - PQ
  - SQ
  - RabitQ
  - 源码分析
description: "PQ、SQ 与 RabitQ 向量量化"
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

> 本文源码基于 Lance `v10.0.0` 的提交 [`95f2f36b2`](https://github.com/lance-format/lance/tree/95f2f36b22043c3face00afe088c34e0742d01df)。文中的存储公式只计算向量 code 本身；row id、校正因子、Lance page 和 metadata 等开销会另外说明。

[上一篇](/posts/lance-vector-index-internals-2-ivf/) 介绍了 IVF 如何用 K-Means 将全局搜索缩小到少量 partitions。但进入 partition 之后仍有一个问题：如果每条 1536 维 `Float32` 向量都保留原值，一百万条向量仅原始数值就约占 5.7 GiB，距离计算还要反复读取这些数据。

量化解决的不是“如何找到 partition”，而是另一个独立问题：

> 用更短的 code 代替原始浮点向量，并直接在 code 上估算距离；需要更高精度时，再回表读取少量原始向量精排。

Lance 10.0.0 提供 PQ、SQ 与 RabitQ 三条不同的压缩路径。它们共享同一套 quantizer 边界，却丢失不同的信息，也有不同的训练、存储和距离估算方式。

## 1\. Quantizer 是向量存储边界

Vector Index V3 将索引拆成三个正交部分：IVF clustering、partition 内的 FLAT/HNSW sub-index，以及 vector quantization。量化后的向量位于 `auxiliary.idx`；sub-index 只用本地 vector id 访问这份 storage。

这个边界在源码中由 `Quantization` 定义：

```rust
pub trait Quantization {
    type BuildParams: QuantizerBuildParams;
    type Metadata: QuantizerMetadata;
    type Storage: QuantizerStorage<Metadata = Self::Metadata>;

    fn build(data: &dyn Array, metric: DistanceType, params: &Self::BuildParams)
        -> Result<Self>;
    fn code_dim(&self) -> usize;
    fn use_residual(_: DistanceType) -> bool { false }
    fn quantize(&self, vectors: &dyn Array) -> Result<ArrayRef>;
    fn field(&self) -> Field;
}

pub enum Quantizer {
    Flat(FlatQuantizer),
    FlatBin(FlatBinQuantizer),
    Product(ProductQuantizer),
    Scalar(ScalarQuantizer),
    Rabit(RabitQuantizer),
}
```

完整定义见 [`Quantization`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/quantizer.rs#L31-L72) 与 [`Quantizer`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/quantizer.rs#L121-L127)。这个边界统一了四件事：

1. 从训练数据构造量化模型；
2. 将原始向量变成固定 schema 的 code；
3. 保存并恢复 codebook、bounds 或 rotation metadata；
4. 为 FLAT/HNSW 提供同一种 `VectorStore` 距离接口。

因此，`IVF_HNSW_PQ` 不是另一套 HNSW 算法。它仍然是 HNSW sub-index，只是图在比较节点时通过 `ProductQuantizationStorage` 估算距离；换成 SQ 后，图的遍历逻辑不变，距离计算器和 `auxiliary.idx` schema 改变。

![PQ、SQ 与 RabitQ 在 Lance 查询路径中的位置](/images/lance-vector-index-internals/quantization-paths.svg)

可编辑图源：[quantization-paths.excalidraw](/images/lance-vector-index-internals/quantization-paths.excalidraw)

## 2\. PQ：每个 Subvector 只保存一个 Codeword 编号

Product Quantization 将一个 `d` 维向量切成 `m=num_sub_vectors` 段，每段维度为 `d/m`。每个位置分别训练一份包含 `2^b` 个 centroids 的 codebook，其中 `b=num_bits`。

### 一个 8 维例子

假设：

```text
d = 8
m = 2
b = 4                 每段有 16 个 codewords

x = [8.2, 7.9, 8.4, 8.0 | 1.1, 0.8, 1.2, 0.9]
c = [8.0, 8.0, 8.0, 8.0 | 1.0, 1.0, 1.0, 1.0]  IVF centroid
r = x - c
  = [0.2,-0.1, 0.4, 0.0 | 0.1,-0.2, 0.2,-0.1]
```

对于 L2 和归一化后的 Cosine，Lance 量化的是 residual `r=x-c`，而不是直接量化 `x`。两段 residual 分别在各自 codebook 中寻找最近 codeword。假设命中编号 3 和 12：

```text
subvector 0 -> codeword  3 -> 0011
subvector 1 -> codeword 12 -> 1100

PQ code -> 1100_0011 -> 1 byte
```

查询时不需要把每条 code 都还原成浮点向量。Lance 先为查询的每个 subvector 计算“到该段所有 codewords 的距离表”，再按 code 取值并求和：

```text
approx_distance(q, code)
  = table[0][3] + table[1][12]
```

同一 partition 中的所有向量共享这张 query distance table，逐条比较主要变成 byte lookup 和加法。源码中的编码路径正是逐段选择最近 centroid；4-bit 模式再把两个编号塞进一个 byte：

```rust
let sub_dim = dim / num_sub_vectors;

let sub_vec_code: Vec<u8> = vector
    .chunks_exact(sub_dim)
    .enumerate()
    .map(|(sub_idx, sub_vector)| {
        let centroids = get_sub_vector_centroids(
            codebook, dim, num_sub_vectors, sub_idx,
        );
        compute_partition(centroids, sub_vector, distance_type) as u8
    })
    .collect();

if NUM_BITS == 4 {
    // Two 4-bit codeword ids share one byte.
    pack_pairs(sub_vec_code)
} else {
    sub_vec_code
}
```

代码经过裁剪，完整实现见 [`ProductQuantizer::transform_impl`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/pq.rs#L159-L265)，distance table 路径见 [`compute_distances`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/pq.rs#L268-L303)。

### Residual 在哪里发生

PQ 自己只知道如何训练 codebook 和编码。IVF transformer 决定是否先计算 residual：

```rust
if ProductQuantizer::use_residual(distance_type) {
    transforms.push(ResidualTransform::new(
        centroids,
        PART_ID_COLUMN,
        vector_column,
    ));
}
transforms.push(PQTransformer::new(pq, vector_column, PQ_CODE_COLUMN));
```

`ResidualTransform` 根据每行的 `partition_id` 执行 `vector - centroid`。PQ 对 L2 和 Cosine 返回 `use_residual=true`；Cosine 向量会先归一化，再转成 L2 路径。相关控制流见 [`IvfTransformer::with_pq`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/ivf.rs#L184-L231) 和 [`compute_residual`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/residual.rs#L56-L96)。

### 参数约束与空间

PQ 的 code 成本可以直接从实现推出：

```text
code bytes/vector = m * b / 8
codebook values    = 2^b * d
```

例如 `d=768, m=48, b=8` 时，每条 PQ code 是 48 bytes；codebook 一共有 `256 * 768` 个浮点值。这里尚未计算每行 8-byte `_rowid`、Lance 编码与 page metadata。

参数有两个硬边界：

- `dimension % num_sub_vectors == 0`，否则无法等长切分；Python 入口会直接拒绝，见 [`create_index` validation](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/python/python/lance/dataset.py#L3688-L3710)。
- 10.0.0 的 PQ 编码实现支持 4 或 8 bits；4-bit 时 `num_sub_vectors` 还必须是偶数，才能完整地两两打包，见 [`num_bits dispatch`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/pq.rs#L144-L179)。

增大 `m` 会缩短每个 subvector，让 codebook 更容易拟合局部形状，但每条 code 也线性变长；增大 `b` 会增加每段可选的 codewords，同时让 codebook 按 `2^b` 增长。这是可由结构推出的成本关系，不等于固定的召回率承诺。

## 3\. SQ：逐坐标编码，但共享一组 Bounds

Scalar Quantization 不训练多个 codebooks。Lance 10.0.0 的 SQ8 先扫描训练值，得到一组全局 `min..max`，再把每个坐标线性映射到 `0..255`：

```rust
let range = bounds.end - bounds.start;
values.iter().map(|v| {
    let scaled = (v - bounds.start) * 255.0 / range;
    scaled as u8 // float -> u8 is saturating; fractional part is truncated
})
```

完整实现见 [`update_bounds` 与 `scale_to_u8`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/sq.rs#L66-L111) 和 [`scale_to_u8`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/sq.rs#L244-L258)。

一个容易误解的细节是：**SQ 是逐坐标编码，但 10.0.0 只保存一组全向量共享的 bounds，不是每个维度各有一组 min/max。** Metadata 只有 `dim`、`num_bits` 和单个 `Range<f64>`，见 [`ScalarQuantizationMetadata`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/sq/storage.rs#L39-L46)。

假设训练 bounds 是 `[-2, 6]`：

```text
x       = [-2, 0, 2, 6]
SQ code = [ 0,63,127,255]

step = (6 - (-2)) / 255 ≈ 0.03137
```

每个坐标都会产生量化误差。L2 / Cosine 查询会用相同 bounds 转换为 `u8`，在整数 code 上计算距离，再乘 `step²`；Dot 查询则保留浮点 query，通过 `lower_bound * sum(query) + value_scale * dot(code, query)` 补回量化偏移。两条路径见 [`SQDistCalculator::new`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/sq/storage.rs#L502-L557) 和 [`dot_distance`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/sq/storage.rs#L599-L613)。

SQ8 的 code 固定为：

```text
code bytes/vector = d
```

它不需要 PQ 的 `2^b * d` codebook，训练也只是统计 bounds；但如果少数异常值把全局范围拉得很宽，主要数据会挤在更少的整数刻度里。Lance 10.0.0 源码中还有 `TODO: support SQ4`，实际 `field()` 仍为每维一个 `UInt8`，因此不要仅凭 `num_bits` 字段推导已有紧凑 SQ4 存储。

## 4\. RabitQ：旋转 Residual，再保存符号与校正信息

RabitQ 也不是把原向量直接变成普通 binary vector。IVF_RQ 的构建顺序是：

```text
original vector
  -> assign IVF partition
  -> residual = vector - centroid
  -> random orthogonal / fast rotation
  -> 1-bit sign code
  -> optional extra bits
  -> distance correction factors
```

Lance 支持 dense matrix rotation 和默认的 fast rotation。以旋转后的 8 维 residual 为例：

```text
rotated residual = [ 0.7, -0.2,  0.1, -0.6,  0.3,  0.8, -0.4, -0.1]
sign bits        = [   1,    0,    1,    0,    1,    1,    0,    0]
packed byte      = 0b00110101
```

源码将正负号按位写入 `_rabit_codes`：

```rust
for (bit_idx, value) in rotated.iter().enumerate() {
    if value.is_sign_positive() {
        codes[bit_idx / 8] |= 1 << (bit_idx % 8);
    }
}
```

完整的 fast/dense rotation 与编码路径见 [`RabitQuantizer::new_with_rotation`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/bq/builder.rs#L192-L239) 和 [`transform_split`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/bq/builder.rs#L568-L669)。

只保留符号会丢掉模长和幅度分布，所以 `auxiliary.idx` 还为每行保存校正列：

- `__add_factors` 与 `__scale_factors`：把 binary inner product 映射回距离估计；
- `__error_factors`：raw-query estimator 用于构造保守的距离 lower bound；
- `num_bits>1` 时增加 `__ex_codes`、`__add_factors_ex` 与 `__scale_factors_ex`，补充幅度信息。

这些因子不是装饰性 metadata，而是距离公式的一部分。L2 与 Dot 的 factor 计算见 [`compute_raw_query_factors`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/bq/transform.rs#L138-L273)，V3 的实际列布局见 [`Vector Index format`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/docs/src/format/index/vector/index.md#L191-L204)。

RabitQ 的基础 binary code 约为 `ceil(d/8)` bytes/vector。多 bit 模式另外保存约 `d*(num_bits-1)/8` bytes 的 extra code，实际还会受 blocked packing 与向上取整影响，并且每行另有多个 `Float32` factors。10.0.0 允许 `num_bits=1..=9`，默认是 1；当前 IVF_RQ 构建还要求维度能被 8 整除，见 [`RQ validation`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/bq.rs#L123-L153) 和 [`RabitQuantizer::build`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance-index/src/vector/bq/builder.rs#L673-L719)。

### RabitQ Code 不等于输入 Binary Vector

二者都用 bit packing，但语义完全不同：

| 维度     | 输入 binary vector     | RabitQ internal code                        |
| -------- | ---------------------- | ------------------------------------------- |
| 原始列   | `FixedSizeList<UInt8>` | 浮点 vector                                 |
| 距离     | Hamming                | 近似 L2/Dot；Cosine 会归一化到 L2 路径      |
| 编码来源 | 用户已经提供的 bits    | IVF residual 经过 rotation 后生成           |
| 辅助信息 | 不需要 RabitQ factors  | rotation metadata 与每行 correction factors |
| 存储实现 | `FlatBinQuantizer`     | `RabitQuantizer`                            |

因此，不能把已有的图像哈希、指纹 bitset 当作 IVF_RQ 的输入语义，也不能拿 `_rabit_codes` 单独做 Hamming 排序来替代 RabitQ 的距离估计。

## 5\. 量化误差如何由 `refine_factor` 收口

三种量化都可能改变候选顺序。PQ 以 codeword 代替 subvector，SQ 将连续值落到 256 个刻度，RabitQ 的 1-bit 路径主要保留旋转后的符号。ANN 阶段得到的是“按近似距离看起来最近”的候选，不保证其顺序等于原始浮点距离。

设置 `refine_factor=R` 后，Lance 的执行计划会：

```text
1. ANN 按近似距离取 k * R 个候选
2. 根据 _rowid 从 Dataset 读取候选的原始 vector
3. 用原始 vector 重新计算精确距离
4. 重排并保留 top k
```

对应控制流很直接：

```rust
let ann_node = self.ann(&q, index_segments, filter_plan, overlay_block).await?;

let knn_node = if q.refine_factor.is_some() {
    let with_vector = self.take(ann_node, vector_projection)?; // random reads
    self.flat_knn(with_vector, &q)?                            // exact distance
} else {
    ann_node
};
```

完整实现见 [`Scanner::nearest` plan](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/scanner.rs#L3951-L3997)；ANN 节点的 fetch 上限是 `k * refine_factor`，见 [`ann`](https://github.com/lance-format/lance/blob/95f2f36b22043c3face00afe088c34e0742d01df/rust/lance/src/dataset/scanner.rs#L5131-L5166)。

`refine_factor` 改善的是**已进入候选集的排序精度**。如果正确邻居因为 IVF 的 `nprobes` 太小，或 HNSW 的 `ef` 太小而根本没有进入候选集，精排无法把它找回来。另一方面，`R` 越大，需要随机读取和精确计算的原始向量越多；它不是免费的“召回率开关”。

## 6\. 三条路径如何选择

先从数据表示和可接受误差出发，而不是把名称当成固定性能等级：

| 方案   | 每条主要 code                         | 需要训练什么               | 主要丢失的信息                  | 工程关注点                               |
| ------ | ------------------------------------- | -------------------------- | ------------------------------- | ---------------------------------------- |
| PQ     | `m*b/8` bytes                         | 每个 subvector 的 codebook | subvector 内只保留最近 codeword | `d % m`、codebook 规模、4/8 bits、refine |
| SQ8    | `d` bytes                             | 一组全局 bounds            | 每个坐标落入离散刻度            | 异常值拉宽 bounds、目前无紧凑 SQ4        |
| RabitQ | `ceil(d/8)` 起，加 factors/extra code | rotation，不需要数据采样   | 1-bit 时主要保留旋转后符号      | 维度被 8 整除、factor 开销、1..=9 bits   |

这张表没有给出“谁一定更快、谁召回一定更高”，因为结果还取决于维度、数据分布、metric、IVF partition 质量、sub-index 与查询参数。可靠的做法是固定一组真实 query 和 ground truth，同时测 Recall@K、P95 latency、索引体积、构建时间与 refine 产生的原始向量读取量。

下一篇将保持 quantizer 不变，进入另一个正交维度：HNSW 如何在每个 IVF partition 内组织图，`m`、`ef_construction` 和查询 `ef` 分别控制什么。
