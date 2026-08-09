---
author: "Jay H. Zou"
pubDatetime: 2026-08-09T13:30:00+08:00
title: "HDFS Block Reconstruction 与 RedundancyMonitor 源码分析"
lang: zh-CN
tags:
  - Hadoop
  - HDFS
  - NameNode
  - 源码分析
description: "从冗余状态判定、优先级调度到 DataNode 副本复制与 EC 重构"
---

> 本文源码基于 Apache Hadoop 3.4.1 的 [`rel/release-3.4.1`](https://github.com/apache/hadoop/tree/rel/release-3.4.1)。文中的代码片段均为从原始实现中提炼出的等价伪代码，用于突出控制流和状态不变量。
>
> 本文讨论已经完成写入的 block 如何恢复冗余。写入期间的 pipeline recovery、lease recovery 和 `BlockUnderConstruction` recovery 不在本文范围内。

HDFS 的 block reconstruction 不是一个“发现副本少了就立即复制”的单步动作。NameNode 必须先回答四个问题：

1. 当前有多少份真正可用的 replica？
2. block 是否真的需要 reconstruction，还是只需要删除 excess replica？
3. 哪个 DataNode 可以作为 source，哪些 DataNode 可以作为 target？
4. 任务下发后，NameNode 如何确认它已经完成，并在失败时重新调度？

这四个问题分别对应副本状态判定、重构队列、`RedundancyMonitor`、DataNode 执行和 block report 反馈。下面先看完整架构，再进入每个环节。

## 1\. 整体架构

![HDFS Block Reconstruction 整体架构](/images/hdfs-block-reconstruction/reconstruction-architecture.svg)

这张图可以分成四层。

**第一层是触发条件。** DataNode 宕机、decommission、maintenance、corrupt replica、修改 replication factor、block report 变化和 NameNode failover，都可能改变一个 block 的有效冗余状态。副本总数不变也可能需要 reconstruction：例如三份副本都落在同一个 rack，数量满足要求，但 placement policy 不满足。

**第二层是 NameNode 控制面。** `BlockManager` 通过 `countNodes()` 将每个已存储副本分类到 `NumberReplicas`，再决定 block 应进入低冗余队列、超额副本删除流程，还是暂缓处理。`RedundancyMonitor` 从低冗余队列选择工作，构造 `ReplicationWork` 或 `ErasureCodingWork`，最后把命令放进相应 DataNode 的待执行队列。

**第三层是 DataNode 数据面。** 连续块由一个已有副本的 DataNode 向 target 发送完整 block；EC block group 则通常由一个 target DataNode 协调，从多个 source 读取 internal blocks，解码缺失数据并写向一个或多个 target。

**第四层是反馈闭环。** NameNode 将任务加入 `pendingReconstruction` 只表示“已安排尝试”，不表示新副本已经存在。target 的 IBR（Incremental Block Report）会直接匹配并移除 pending target；FBR（Full Block Report）则刷新 NameNode 看到的实际 replica 事实，在 timeout 后的重新评估中阻止不必要的重试。如果迟到的任务最终成功，多出来的 replica 会通过 excess/invalidation 流程收敛。

因此，整套机制可以概括为：

```text
事实变化
  → 重新计算冗余状态
  → 进入风险优先级队列
  → RedundancyMonitor 生成工作
  → heartbeat 下发命令
  → DataNode 执行复制或 EC 解码
  → block report 确认结果
  → 完成、重试或删除 excess replica
```

其中有四条贯穿全文的不变量：

- 只有已经完成写入的 block 才进入常规 reconstruction 流程；
- block 的实际存储状态和 block report 是事实来源，内存队列不是持久化任务日志；
- 慢速 target placement 在 FSNamesystem 全局锁外执行，但结果必须重新加锁验证；
- timeout 触发的是重新评估，不是对 DataNode 任务的分布式取消。

## 2\. Reconstruction 与 Recovery 的边界

Hadoop 源码同时使用 recovery、replication 和 reconstruction，这三个词指向不同的问题。

- **Pipeline recovery**：处理正在写入的 pipeline。典型场景是 DataNode 在写入过程中退出，主要由 DFSClient 的 `DataStreamer` 发起恢复。
- **Block recovery**：处理 `UNDER_CONSTRUCTION` block。它服务于 lease recovery，需要恢复 generation stamp 和最终长度，DataNode 侧入口是 `BlockRecoveryWorker`。
- **Replication**：连续布局 block 的冗余恢复方式。例如 replication factor 为 3、当前只剩两份有效副本，NameNode 会创建 `ReplicationWork`。
- **Reconstruction**：已完成 block 的统一冗余恢复概念，既包括普通副本复制，也包括 EC internal block 解码，对应抽象基类 `BlockReconstructionWork`。

`processMisReplicatedBlock()` 对未完成 block 直接返回 `UNDER_CONSTRUCTION`，不会把它加入 `neededReconstruction`：

```java
MisReplicationResult processMisReplicatedBlock(BlockInfo block) {
  if (block.isDeleted()) {
    addToInvalidates(block);
    return INVALID;
  }
  if (!block.isComplete()) {
    return UNDER_CONSTRUCTION;
  }

  // only completed blocks continue into redundancy evaluation
  ...
}
```

源码见 [`BlockManager.processMisReplicatedBlock()`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java#L4120-L4154)。

这个边界很重要。写入期间的 block recovery 要协商长度、generation stamp 和 primary DataNode；reconstruction 面对的是已经有稳定 block identity 和最终长度的数据，目标是恢复冗余强度或 placement 分布。两者都可能“产生一个可用 block”，但一致性协议完全不同。

## 3\. 副本状态模型与冗余判定

### 3.1 `NumberReplicas` 不是简单计数器

`BlockManager.countNodes()` 遍历一个 block 当前关联的所有 storage，通过 `checkReplicaOnStorage()` 将副本分类到 [`StoredReplicaState`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/NumberReplicas.java#L39-L69)：

- `LIVE`：正常有效副本，可以计入当前冗余。
- `READONLY`：可读但不作为普通可写副本使用，例如 PROVIDED storage。
- `DECOMMISSIONING`：DataNode 正在退出服务，通常需要把唯一数据迁出。
- `DECOMMISSIONED`：DataNode 已退出服务，只在极端情况下作为连续块 source 兜底。
- `MAINTENANCE_FOR_READ`：DataNode 正在进入 maintenance 且仍然存活，可以继续读取。
- `MAINTENANCE_NOT_FOR_READ`：副本已经不可读，不能作为 source。
- `CORRUPT`：副本损坏，不能作为 reconstruction 输入。
- `EXCESS`：副本已被选为多余副本，等待删除。
- `STALESTORAGE`：block report 可能过期，删除决策必须保持保守。
- `REDUNDANT`：EC 中同一 internal block 的重复副本。

这些状态不是简单互斥标签。例如一个正常 storage 上的 replica 可以同时使 `LIVE` 和 `STALESTORAGE` 计数增加；`STALESTORAGE` 表达的是报告新鲜度，而不是数据可读性的替代状态。

对于连续块，每个 storage 上保存的是完整 block。对于 EC block group，NameNode 还必须按 internal block index 去重：同一个 internal block 出现两份，不能把它们当成两个独立的编码输入。

### 3.2 “需要 reconstruction”包含数量和拓扑两部分

最终判断并不是：

```text
liveReplicas < replicationFactor
```

Hadoop 3.4.1 的核心条件可以简化为：

```java
boolean isNeededReconstruction(
    BlockInfo block, NumberReplicas replicas, int pending) {
  return block.isComplete()
      && !hasEnoughEffectiveReplicas(block, replicas, pending);
}

boolean hasEnoughEffectiveReplicas(
    BlockInfo block, NumberReplicas replicas, int pending) {
  int required = getExpectedLiveRedundancyNum(block, replicas);
  int effective = replicas.liveReplicas() + pending;

  return effective >= required
      && (pending > 0 || isPlacementPolicySatisfied(block));
}
```

源码见 [`isNeededReconstruction()`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java#L5134-L5151) 和 [`hasEnoughEffectiveReplicas()`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java#L2220-L2228)。

这里有三个关键点。

第一，正在执行的 pending targets 会暂时计入 effective replicas，避免同一个 block 在每轮 `RedundancyMonitor` 中被无限重复调度。

第二，maintenance replica 会影响 expected live redundancy。系统允许 maintenance 期间减少临时 live copies，但仍要满足 `dfs.namenode.maintenance.replication.min`。

第三，即使 replica 数量已经达到要求，placement policy 不满足时仍需要 reconstruction。此时目的不是补数量，而是把副本复制到新的 rack 或 upgrade domain，降低相关故障造成的同时丢失风险。

## 4\. Reconstruction 需求如何进入队列

### 4.1 全量扫描与增量更新

NameNode 成为 Active 并完成首次 SafeMode 阶段后，会初始化 reconstruction queues。`BlockManager.processMisReplicatedBlocks()` 清空 `neededReconstruction`，启动 `Reconstruction Queue Initializer`，分批扫描整个 `BlocksMap`：

```java
while (namesystem.isRunning() && blocks.hasNext()) {
  writeLock();
  try {
    for (int i = 0; i < numBlocksPerIteration && blocks.hasNext(); i++) {
      processMisReplicatedBlock(blocks.next());
    }
  } finally {
    writeUnlock();
    sleepOutsideWriteLock();
  }
}
```

分批持有 write lock 的目的，是避免大 namespace 在 queue 初始化期间长期阻塞其他 NameNode 操作。扫描同时记录 `ReconstructionQueuesInitProgress`。源码见 [`processMisReplicatesAsync()`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java#L3950-L4060)。

全量扫描并不是唯一入口。运行期间，以下事件都会增量调用冗余更新逻辑：

- IBR/FBR 增加或删除 replica；
- corrupt replica 被发现或清理；
- 文件关闭后 block 变为 complete；
- `setReplication()` 修改目标副本数；
- DataNode dead、decommission 或 maintenance 改变有效副本；
- placement policy 的有效结果发生变化。

全量扫描负责建立初始状态，增量路径负责维持状态。初始化过程中即使 `BlocksMap` 出现新增 block，也不会因为迭代器错过而永久漏检，因为新增 replica 的正常处理路径还会进行增量判断。

### 4.2 `processMisReplicatedBlock()` 的分类决策

核心控制流如下：

```java
if (block.isDeleted()) {
  addToInvalidates(block);
  return INVALID;
}
if (!block.isComplete()) {
  return UNDER_CONSTRUCTION;
}

NumberReplicas replicas = countNodes(block);

if (isNeededReconstruction(block, replicas)) {
  neededReconstruction.add(block, ...);
  return UNDER_REPLICATED;
}

if (shouldProcessExtraRedundancy(replicas, expected)) {
  if (!canSafelyChooseExcessReplica(block)) {
    return POSTPONE;
  }
  return OVER_REPLICATED;
}

return OK;
```

注意：`postponedMisreplicatedBlocks` 不是“所有 over-replicated blocks 的集合”。它保存的是当前无法安全完成删除判断的 mis-replicated blocks。典型场景是 failover 后 storage 的 block report 仍然 stale：新 Active NameNode 不知道旧 Active 是否已经下发过删除，如果此时继续删除，可能把实际冗余降到安全线以下。

### 4.3 三个集合组成一个状态机

![HDFS reconstruction 队列状态机](/images/hdfs-block-reconstruction/reconstruction-state-machine.svg)

三个集合的语义可以精确表述为：

- `neededReconstruction` 保存按风险分级的 `BlockInfo`。当前冗余不足或 placement 不满足时进入；安排到足够 targets，或重新计算后确认不再需要重构时离开。
- `pendingReconstruction` 保存 block、target storages 和最近调度时间。reconstruction work 写入 DataNode 任务队列后进入；target replica 被汇报，或等待超过 pending timeout 时离开。
- `postponedMisreplicatedBlocks` 保存等待重扫的 block。stale 信息使删除决定不安全时进入；storage reports 变新后，由 rescan 重新分类并移出。

`corruptReplicas`、`excessRedundancyMap` 和 `InvalidateBlocks` 与它们相关，但职责不同：前者记录坏副本事实，中间记录已选择的 excess replica，后者保存准备通过 heartbeat 下发的删除命令。删除 excess replica 与生成新 replica 是两条不同的收敛分支。

## 5\. `LowRedundancyBlocks` 的风险优先级

`neededReconstruction` 的实际类型是 [`LowRedundancyBlocks`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/LowRedundancyBlocks.java)。它内部不是一个 FIFO，而是五个 `LightWeightLinkedSet<BlockInfo>`：

- `QUEUE_HIGHEST_PRIORITY`：再丢一份就可能无法恢复，例如只剩 1 个 live replica。
- `QUEUE_VERY_LOW_REDUNDANCY`：当前冗余低于期望值的三分之一，例如 replication factor 为 10、只剩 2 份。
- `QUEUE_LOW_REDUNDANCY`：普通低冗余，例如 replication factor 为 3、只剩 2 份。
- `QUEUE_REPLICAS_BADLY_DISTRIBUTED`：副本数量足够但 placement 不满足，例如三份副本都位于同一 rack。
- `QUEUE_WITH_CORRUPT_BLOCKS`：没有可用输入，当前无法重构，例如所有副本均已 corrupt。

最后一类放在队尾看似反直觉，但它没有可用 source，优先选择也无法产生进展。将调度能力优先给仍可恢复的数据，反而能减少新的不可恢复 block。

连续块与 EC 的 priority 计算不同。连续块主要看 live/expected 比例；EC 必须先保证至少有 `dataBlockNum` 个不同 internal blocks：

```java
// contiguous block
if (live == 0 && hasOutOfServiceCopy) HIGHEST;
else if (live == 0)                    CORRUPT;
else if (live == 1)                    HIGHEST;
else if (live * 3 < expected)          VERY_LOW;
else                                   LOW;

// striped block group
if (live < dataBlocks && live + outOfService >= dataBlocks) HIGHEST;
else if (live < dataBlocks)                                  CORRUPT;
else if (live == dataBlocks)                                 HIGHEST;
else if ((live - dataBlocks) * 3 < parityBlocks + 1)          VERY_LOW;
else                                                         LOW;
```

EC 中 `live == dataBlocks` 意味着仍然可解码，但已经没有任何额外容错，因此属于最高风险。

每个优先级集合使用 bookmark 记录上次扫描位置，下一轮从 bookmark 继续，避免队列头部无法调度的 block 永久阻挡后续 block。`dfs.namenode.redundancy.queue.restart.iterations` 又会定期把扫描位置重置到队头，确保新进入的高风险 block 不会长时间等待。

## 6\. `RedundancyMonitor` 的调度周期

`BlockManager.activate()` 启动两个相关线程：

- `RedundancyMonitor`：周期性生成 reconstruction 和 invalidation 工作，并处理 timeout/rescan；
- `PendingReconstructionMonitor`：扫描 `pendingReconstruction` 中超过 timeout 的项目。

`RedundancyMonitor.run()` 的主体非常直接：

```java
while (namesystem.isRunning()) {
  if (isPopulatingReplQueues()) {
    computeDatanodeWork();
    processPendingReconstructions();
    rescanPostponedMisreplicatedBlocks();
    processTimedOutExcessBlocks();
  }
  sleep(redundancyRecheckIntervalMs);
}
```

源码见 [`RedundancyMonitor`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java#L5338-L5365)。`isPopulatingReplQueues()` 同时要求 HA state 允许填充队列，并且 reconstruction queue 已完成初始化。这确保 Standby NameNode 不会下发重构工作，Active 在首次 SafeMode 阶段也不会过早调度。

每轮候选预算由 live DataNode 数量决定：

```java
blocksToProcess = liveDatanodes
    * dfs.namenode.replication.work.multiplier.per.iteration;

nodesToInvalidate = ceil(liveDatanodes
    * dfs.namenode.invalidate.work.pct.per.iteration);
```

`work.multiplier` 控制的是每轮最多检查多少 low-redundancy blocks，不是保证产生多少任务。没有可用 source、找不到 target、source 达到 stream limit、block 已被其他事件修复，都会让实际 scheduled work 少于候选数。

## 7\. 三阶段调度与锁边界

`computeReconstructionWorkForBlocks()` 是整条 NameNode 调度链的核心。它有意拆成三个阶段：

```java
// Phase 1: under FSNamesystem write lock
for (BlockInfo block : selectedBlocks) {
  work.add(scheduleReconstruction(block, priority));
}

// Phase 2: without FSNamesystem global lock
for (BlockReconstructionWork item : work) {
  item.chooseTargets(placementPolicy, excludedNodes);
}

// Phase 3: reacquire write lock
for (BlockReconstructionWork item : work) {
  if (validateReconstructionWork(item)) {
    item.addTaskToDatanode();
    pendingReconstruction.increment(block, targets);
  }
}
```

源码见 [`computeReconstructionWorkForBlocks()`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java#L2126-L2216)。

### 7.1 Phase 1：基于一致的 namespace 状态构造 work

`scheduleReconstruction()` 在 write lock 下完成：

1. 排除 deleted、重新打开 append 等不再适用的 block；
2. 重新统计当前 replica；
3. 选择可用 source DataNodes；
4. 计算当前还缺多少 redundancy；
5. 连续块生成 `ReplicationWork`，条带块生成 `ErasureCodingWork`。

EC block group 还有一个硬条件：可用、不同 index 的 source 数量必须至少达到 real data block count。否则 decoder 没有足够输入，调度再积极也无法恢复数据。

### 7.2 Phase 2：锁外执行 target placement

target placement 需要查询网络拓扑、storage policy、可用空间和排除集合。它比内存状态判断更慢，因此源码明确标注：

```java
// choose replication targets: NOT HOLDING THE GLOBAL LOCK
```

排除集合至少包含：

- 已经保存该 block 的 DataNodes；
- corrupt 或 decommissioning 等 containing nodes；
- 同一 block 已经在 `pendingReconstruction` 中的 targets。

如果在整个 placement 过程中持续持有 FSNamesystem write lock，集群出现大量 low-redundancy blocks 时，重构调度会直接放大 NameNode RPC 延迟。

### 7.3 Phase 3：重新验证锁外结果

释放锁意味着 namespace 可能已经变化。因此 `validateReconstructionWork()` 必须重新验证：

- block 是否仍然存在并保持可重构状态；
- live + pending 是否已经达到 required redundancy；
- placement 是否已经被其他 replica 修复；
- 新 targets 是否至少改善原来的 placement violation。

验证通过后才会：

```text
addTaskToDatanode()
→ incrementBlocksScheduled(targets)
→ pendingReconstruction.increment(block, targets)
→ 必要时从 neededReconstruction 移除
```

这是一个典型的 optimistic pattern：锁内读取状态，锁外执行昂贵计算，再锁内检查前提是否仍成立。性能依赖锁外 placement，正确性依赖最后的 revalidation。

## 8\. Source 限流与 target 选择

source DataNode 同时承担客户端流量和后台复制流量。`chooseSourceDatanodes()` 会检查节点当前排队的普通复制任务与 EC 任务总数：

```java
queued = blocksToBeReplicated + blocksToBeErasureCoded;

if (priority != HIGHEST
    && !nodeIsLeavingService
    && queued >= maxReplicationStreams) {
  skipSource();
}

if (queued >= replicationStreamsHardLimit) {
  skipSource();
}
```

soft limit 可以被最高优先级任务，以及 decommission/entering-maintenance 的数据迁出需求突破；hard limit 对所有任务生效。这两个参数保护的是 source 侧并发，不是 target 数量，也不是整个集群的全局重构并发。

生产集群可能把 soft/hard limit 配置为 `256 / 512`，并把每轮 work multiplier 配置为 `20`；这些都不是 Hadoop 默认值。Hadoop 3.4.1 的默认 soft/hard limit 为 `2 / 4`，work multiplier 为 `2`。大幅提高这些值可能提升 backlog 消化速度，也可能同时增加 source 磁盘读、网络发送、DataNode xceiver 和下游 target 写压力，不能只根据 `neededReconstruction.size()` 调大。

target 选择则由 block type 对应的 `BlockPlacementPolicy` 完成。连续块通常使用 replication policy；EC 使用 striped placement policy。除了避开 containing/pending nodes，还要满足 storage type、rack、upgrade domain、剩余空间和写入负载等约束。

## 9\. 两条 DataNode 执行路径

### 9.1 连续块：`ReplicationWork`

`ReplicationWork.addTaskToDatanode()` 将任务加入第一个 source DataNode：

```java
srcNodes[0].addBlockToBeReplicated(block, targets);
```

source 的下一次 heartbeat 到达 Active NameNode 时，`DatanodeManager` 从该节点的待执行队列取出 `BlockTargetPair`，返回：

```text
BlockCommand(DNA_TRANSFER)
```

DataNode 的 `BPOfferService` 收到命令后调用：

```java
dn.transferBlocks(blockPoolId, blocks, targets, storageTypes, storageIds);
```

每个 block 最终进入 `DataTransfer` 线程，由 source 读取本地 replica 并发送给 targets。目标 DataNode 落盘成功后，再通过 IBR 或后续 FBR 告诉 NameNode。

相关源码：

- [`ReplicationWork`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/ReplicationWork.java)
- [`DatanodeManager.handleHeartbeat()`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/DatanodeManager.java#L1860-L1920)
- [`BPOfferService.processCommandFromActive()`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/BPOfferService.java#L716-L808)
- [`DataNode.transferBlocks()`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataNode.java#L2895-L2907)

### 9.2 条带块：不一定总是解码

`ErasureCodingWork` 有三种执行策略。

**Placement-only。** 所有 internal blocks 都存在，只是 rack 分布不满足。此时不需要 EC decode，只复制一个 internal block 到新 rack。

**节点退出服务。** internal blocks 完整，但某些唯一副本位于 decommissioning 或 entering-maintenance DataNode。此时可以直接复制这些 internal blocks。

**真正缺失 internal block。** target DataNode 收到 `BlockECReconstructionCommand`，作为 reconstruction coordinator 从 source DataNodes 读取输入并执行解码。

这意味着：

```text
EC block group 需要 reconstruction
    ≠ 每次都必须运行 decoder
```

如果只是 placement 或迁出节点问题，复制已有 internal block 比读取 k 个 source 再解码更便宜。

### 9.3 `StripedBlockReconstructor` 的数据流

真正的 EC reconstruction 由 `ErasureCodingWorker` 创建 `StripedBlockReconstructor` 并提交到 DataNode 的 striped reconstruction thread pool：

```java
StripedBlockReconstructor task =
    new StripedBlockReconstructor(worker, reconstructionInfo);

stripedReconstructionPool.submit(task);
incrementXmitsInProcess(weightedTaskCost);
```

每轮 buffer 的执行步骤是：

```java
while (position < maxTargetLength) {
  stripedReader.readMinimumSources(length); // [1] read k inputs
  decoder.decode(inputs, erasedIndices, outputs); // [2] reconstruct
  stripedWriter.transferData2Targets(); // [3] write targets
  updatePosition(length);
}
```

源码见 [`StripedBlockReconstructor.reconstruct()`](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/erasurecode/StripedBlockReconstructor.java#L85-L129)。

`StripedReader` 会从满足解码要求的最少 source 集合读取数据。某些 source 读取失败时，它可以切换到额外 source；decoder 根据 erased indices 恢复缺失 buffers；`StripedWriter` 再将输出发送到目标 storage。读、decode、写分别有独立 metrics，Hadoop 3.4.1 也支持 reconstruction read/write throttler。

EC coordinator 的 CPU、网络读和网络写可能集中在同一 target DataNode，因此 EC backlog 的瓶颈不一定在 NameNode 调度，也不一定能通过调大 NameNode work multiplier 解决。

## 10\. 完成确认、Timeout 与重复任务收敛

### 10.1 `pendingReconstruction` 记录的是尝试，不是完成

`PendingReconstructionBlocks` 保存：

```text
BlockInfo
  → last scheduled timestamp
  → target DatanodeStorageInfo list
```

当 `BlockManager.addBlock()` 接受 target storage 通过 IBR 汇报的 `RECEIVED_BLOCK`，并确认 generation stamp 与当前 `BlockInfo` 一致时，会执行：

```java
pendingReconstruction.decrement(storedBlock, storageInfo);
```

同一个 block 可能有多个 targets。只有所有已记录 target 都被移除后，这个 pending entry 才消失。随后 `updateNeededReconstructions()` 根据最新 live + pending 状态决定 block 是否还应存在于低冗余队列。FBR 能更新 `BlocksMap` 中的实际 replica，但源码中直接执行 `pendingReconstruction.decrement()` 的是 IBR `blockReceived` 路径；如果 IBR 丢失，pending entry 仍需依靠 timeout 被清理，再按 FBR 已更新的事实重新判断。

### 10.2 Timeout 不会取消 DataNode 上的任务

`PendingReconstructionMonitor` 定期扫描 map：

```java
if (now > pending.timestamp + timeout) {
  timedOutItems.add(block);
  pendingReconstructions.remove(block);
}
```

下一轮 `RedundancyMonitor.processPendingReconstructions()` 从 `BlocksMap` 取得最新 `BlockInfo`，重新统计副本；如果仍需要 reconstruction，就把 block 再次加入 `neededReconstruction`。

这里不存在一个跨 NameNode/DataNode 的 cancel RPC。原来的 DataNode 任务可能仍在排队、执行或即将汇报。因此 timeout 之后可能出现：

1. NameNode 对同一 block 生成新的 reconstruction work；
2. 原任务稍后成功；
3. block 临时出现 extra redundancy；
4. excess/invalidation 流程选择并安全删除多余 replica。

这是一种依赖幂等状态收敛的设计，而不是 exactly-once task execution。最终安全性来自重新计算 block 当前事实，不来自某个 task id 的唯一执行承诺。

## 11\. NameNode 重启、Failover 与 `postponedMisreplicatedBlocks`

`neededReconstruction` 和 `pendingReconstruction` 都是 NameNode 内存结构，不写入 edit log。NameNode 重启后不会恢复“上次调度到哪个 target”的任务日志，而是通过 namespace 中的 `BlockInfo`、DataNode block reports 和全量 mis-replication scan 重建当前状态。

这带来两个结果。

第一，pending work 可以丢失，但 block 不会永久漏修。新的 Active 重新统计后，仍然低冗余的 block 会重新进入 `neededReconstruction`。

第二，failover 后删除 replica 必须更加保守。假设旧 Active 已经向某个 DataNode 下发删除命令，但新 Active 尚未收到该节点的新 block report。新 Active 看到的副本集合可能包含实际上已被删除的 replica。如果它根据这个旧视图继续选择 excess replica，就可能删除过多数据。

因此 `BlockManager` 对 `postponedMisreplicatedBlocks` 的注释明确说明：failover 后 over-replicated blocks 可能要等相关 replicas 完成 block report，才进行处理。`rescanPostponedMisreplicatedBlocks()` 每轮只检查有限数量，仍然需要延迟的 block 会重新放回集合。

同一保护也用于 corrupt replica invalidation：当其他 replica 位于 stale storage，NameNode 默认会推迟删除 corrupt replica，避免根据不完整信息删除最后一份可能仍有价值的数据。

## 12\. 两个完整示例

### 12.1 replication=3，丢失一个 DataNode

假设 block `B` 原来位于 `DN1 / DN2 / DN3`，目标 replication 为 3，`DN3` 宕机。

```text
countNodes(B)
  live=2, expected=3
  → neededReconstruction: QUEUE_LOW_REDUNDANCY

RedundancyMonitor
  → source=DN1
  → target=DN4
  → ReplicationWork
  → pending(B)={DN4}

DN1 heartbeat
  ← DNA_TRANSFER(B, DN4)

DN4 receives B
  → IBR(B, RECEIVED)

NameNode
  → pending.decrement(B, DN4)
  → live=3
  → remove B from needed/pending
```

如果 DN4 长时间没有汇报，pending timeout 后 B 会重新进入候选队列。若原任务随后成功，而新任务又复制到 DN5，最终 live=4，系统再选择一份 excess replica 删除。

### 12.2 RS-6-3，缺失一个 internal block

RS-6-3 每个完整 stripe 需要 6 个 data blocks 和 3 个 parity blocks。假设 9 个 internal blocks 中丢失 index 2，仍有 8 个不同 index 的 live blocks。

```text
dataBlocks=6, parityBlocks=3
live unique internal blocks=8
  → 仍可解码
  → neededReconstruction

scheduleReconstruction()
  → 至少选择 6 个不同 index 的 source
  → 选择 missing index 2 的 target
  → ErasureCodingWork

target EC Worker
  → StripedReader 读取最少 6 个 source
  → decoder 恢复 index 2
  → StripedWriter 写入 target
  → target block report
```

如果只剩 5 个不同 internal blocks，且 out-of-service nodes 上也没有可用输入，那么 block group 进入 corrupt queue。此时增加调度线程、target 数量或 work multiplier 都无法恢复数据，因为解码需要的最小信息量已经不存在。

如果 9 个 internal blocks 全部存在，只是 rack 分布不合格，`ErasureCodingWork` 会选择一个 internal block 做普通复制，不运行 decoder。

## 13\. 配置、指标与排障边界

### 13.1 关键配置

- `dfs.namenode.redundancy.interval.seconds`：默认 `3s`，控制 `RedundancyMonitor` 的运行周期。
- `dfs.namenode.replication.work.multiplier.per.iteration`：默认 `2`，每轮最多检查的 block 数约为 live DataNode 数量的两倍。
- `dfs.namenode.replication.max-streams`：默认 `2`，是 source 侧普通优先级任务的 soft limit。
- `dfs.namenode.replication.max-streams-hard-limit`：默认 `4`，是 source 侧所有 reconstruction work 的 hard limit。
- `dfs.namenode.reconstruction.pending.timeout-sec`：默认 `300s`，控制 pending work 重新进入评估的等待时间。
- `dfs.namenode.blocks.per.postponedblocks.rescan`：默认 `10000`，控制每轮 postponed block rescan 的上限。
- `dfs.namenode.redundancy.queue.restart.iterations`：默认 `2400`，控制低冗余队列 bookmark 回到队头的周期。

这些参数控制不同阶段，不能互相替代：

- `work.multiplier` 高但 source stream limit 低，候选很多，实际任务仍然少；
- stream limit 高但 target placement 持续失败，增加 source 并发没有帮助；
- pending timeout 太短会增加重复工作，太长会延迟真实失败后的重试；
- postponed 持续增长通常指向 block report 新鲜度或 failover 收敛问题，不是 replication bandwidth 不足。

### 13.2 应联合观察的指标

NameNode 暴露了以下直接相关指标：

- `LowRedundancyBlocks`
- `PendingReconstructionBlocks`
- `ScheduledReplicationBlocks`
- `NumTimedOutPendingReconstructions`
- `PostponedMisreplicatedBlocks`
- `ReconstructionQueuesInitProgress`
- `CorruptReplicatedBlocks` / `CorruptECBlockGroups`
- `MissingReplicatedBlocks` / `MissingECBlockGroups`
- `PendingDeletionBlocks` / `ExcessBlocks`

单个队列值无法直接给出根因，可以用趋势组合缩小范围：

- `LowRedundancyBlocks` 上升，而 `ScheduledReplicationBlocks` 接近 0：优先检查 source/target 是否可选、stream limit、placement 约束，以及数据是否已经不可恢复。
- `PendingReconstructionBlocks` 维持高位，且 timeout 持续增加：优先检查 DataNode 执行速度、命令领取延迟、传输失败和 block report 反馈链路。
- `PostponedMisreplicatedBlocks` 长时间不下降：通常意味着 failover 后 block reports 尚未收敛，或 storage 持续处于 stale 状态。
- `LowRedundancyBlocks` 下降，但 `ExcessBlocks` 上升：timeout 或迟到任务可能产生了临时重复副本，系统正在通过删除流程收敛。
- EC backlog 高，而 NameNode scheduled work 正常：瓶颈更可能位于 DataNode 的 EC read、decode 或 write 阶段。

调优前需要先区分调度受限、数据面受限和反馈受限。把 `max-streams` 从默认值直接提高到数百，会同时放大磁盘读、网络、target 写入和 block report 压力；如果真正瓶颈是 target placement 或 EC decode，反而可能使集群恢复更慢。

## 14\. 总结

HDFS Block Reconstruction 的核心不是“复制一个 block”，而是围绕当前存储事实持续收敛：

1. `countNodes()` 将 replica 按可用性、节点管理状态和报告新鲜度分类；
2. `isNeededReconstruction()` 同时检查有效冗余数量和 placement policy；
3. `LowRedundancyBlocks` 按数据丢失风险决定调度顺序；
4. `RedundancyMonitor` 在锁内构造 work、锁外选择 target、锁内重新验证；
5. 连续块走 source-to-target replication，EC block group 根据场景选择直接复制或 decode reconstruction；
6. `pendingReconstruction` 只记录尝试，IBR 直接完成 pending target，FBR 为重新评估补充实际 replica 事实；
7. timeout、迟到任务和 failover 都通过重新计算事实以及 excess deletion 最终收敛。

理解这套状态机后，`neededReconstruction`、`pendingReconstruction` 和 `postponedMisreplicatedBlocks` 就不再是三个孤立计数器。它们分别表达等待决策、正在尝试和信息不足，而 `RedundancyMonitor` 负责在不长时间占用 NameNode 全局锁的前提下，把这些状态推进到可验证的存储事实。

## 参考资料

- [Apache Hadoop 3.4.1 source](https://github.com/apache/hadoop/tree/rel/release-3.4.1)
- [BlockManager.java](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/BlockManager.java)
- [LowRedundancyBlocks.java](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/LowRedundancyBlocks.java)
- [PendingReconstructionBlocks.java](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/blockmanagement/PendingReconstructionBlocks.java)
- [HDFS 块重构和 RedundancyMonitor 详解](https://blog.csdn.net/zhanyuanlin/article/details/140335982)
