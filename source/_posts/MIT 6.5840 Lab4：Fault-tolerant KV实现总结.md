---
title: MIT 6.5840 Lab4：Fault-tolerant KV实现总结
date: 2026-11-15 21:43:00
categories:
  - MIT 6.5840
tags:
  - KV Server
  - Raft
  - Go
---

- 在之前的`Lab2`中，已经实现基础的`KV Server`，但它依赖单个服务器保存数据，一旦服务器发生故障，服务就可能不可用
- `Lab3`实现了`Raft`共识算法，使用多个节点能够通过日志复制，在部分节点发生故障的情况下维护一致的操作顺序

`Lab4`将两者结合起来：
- 使用`Raft`再多个副本之间复制客户端操作
- 使用`RSM`保证所有副本按照相同顺序执行操作
- 使用`ClientId`和`RequestId`实现请求去重
- 使用`Snapshot`压缩`Raft`日志，降低持久化状态占用空间
- 在节点重启时恢复快照和状态机数据

---
# 一、核心架构

## 1. `Clerk`：客户端请求与重试

`Clerk`负责向服务器发送`Get`和`Put`请求。

为了实现请求去重，每个客户端需要维护：
- `ClientId`：客户端唯一表标识
- `RequestId`：客户端请求的递增序号
- `Leader`：上一次成功通信的服务器索引

> 同一个请求在重试过程中必须复用相同的`RequestId`，否则服务器可能把重试当成一个全新的操作，再次执行
## 2. `KV Server`：业务逻辑与状态机

KV Server负责实现实际的Get和Put业务逻辑。
但它不直接修改本地数据，而是将请求提交给RSM：
```text
Get / Put
    |
    v
rsm.Submit(args)
    |
    v
等待 Raft 提交并应用
    |
    v
返回执行结果
```

`KV Server`还维护两类状态：
- `data`：保存`Key/Value`数据
- `requests`：保存客户端最近一次请求及其执行结果，用于去重
## 3. `RSM`：连接`Raft`与状态机

它负责：
1. 将客户端操作封装成`Op`
2. 调用`Raft.Start()`提交日志
3. 维护等待中的请求
4. 从`applyCh`接受已经提交的日志
5. 调用`DoOp()`执行操作
6. 将执行结果返回给对应的`Submit()`
7. 再需要时生成和恢复`Snapshot`

`RSM`不关心具体的`KV`业务逻辑，只负责保证状态机操作按照`Raft`确定的顺序执行
## 4. `Raft`：提供一致的日志复制

`Raft`负责`Leader`选举、日志复制、提交日志以及持久化

---
# 二、`RSM`

## 1. `Submit()`：提交请求并等待结果

```text
Submit(Op)
    |
    v
生成请求 ID
    |
    v
调用 Raft.Start(Op)
    |
    v
检查是否为 Leader
    |
    +---- 不是 Leader ----> 返回 ErrWrongLeader
    |
    v
创建等待 channel
    |
    v
注册 index -> pendingRequest
    |
    v
等待 Reader 返回结果
    |
    v
返回执行结果
```

为什么等待`channel`的缓存区大小是1？
创建`channel`时使用：
```go
ch := make(chan result, 1)
```
为了避免`Reader`和`Submit`之间产生不必要的阻塞
使用缓冲区为1后，`Reader`可以先将结果放入`channel`，随后继续处理其他日志，`Submit`则可以在稍后读取结果
## 2. `reader`：按顺序执行已提交日志

对于普通日志：
```text
收到 ApplyMsg
    |
    v
判断 CommandValid
    |
    v
取出 Op
    |
    v
调用 sm.DoOp()
    |
    v
根据 CommandIndex 查找等待请求
    |
    v
比较 Op.Me 和 Op.Id
    |
    +---- 匹配 ----> 返回执行结果
    |
    +---- 不匹配 --> 返回 ErrWrongLeader
    |
    v
检查是否需要 Snapshot
```

为什么要比较`Op.Me`和`Op.Id`？
假设某个请求提交到`index = 10`
在它真正提交之前，`Leader`发生变化，新的日志可能覆盖原来`index = 10`的日志位置。
此时`Reader`收到`index = 10`的`ApplyMsg`，不能仅凭`index`就认为这是原请求的执行结果。

因此还需要比较：
```go
request.Op.Me == op.Me && request.Op.Id == op.Id
```
只有提交时记录的请求身份与实际应用的请求身份一致，才能将结果返回给原来的`Submit`
## 3. `RSM`中的异常处理
| 异常情况         | 需要解决的问题                              |
| ------------ | ------------------------------------ |
| 日志被覆盖        | 原`Submit`不能一直等待，需要返回`ErrWrongLeader` |
| `applyCh`关闭  | 通知所有等待中的`Submit`退出                   |
| `Leader`身份变化 | 及时判断原请求是否仍然有效                        |
| `Snapshot`恢复 | 清理快照覆盖范围内已经失效的等待请求                   |

**`applyCh`关闭时为什么要通知所有请求？**
如果Reader被关闭，`applyCh`关闭，对应的`Submit`让会阻塞在`res := <- ch`
这里在采用：
1. 复制：先复制等待请求的信息
2. 清空：清空`RSM`中的等待请求表
3. 解锁：释放互斥锁，避免在持锁状态下发送通知
4. 通知：逐个向等待`channel`发送`ErrWrongLeader`

**`Leader`身份变化如何处理？**
`RSM`使用`ticker`定期检查`Raft`状态

如果发现：
- 当前节点已经不是`Leader`
- 或者请求提交时的`Term`小于当前`Term`
那么对应的等待请求就应该失效，返回`ErrWrongLeader`

---
# 三、`KVServer`：业务逻辑与请求去重

## 1. `DoOp()`：真正执行Get和Put

```text
DoOp(request)
    |
    v
判断请求类型
    |
    +---- Get
    |       |
    |       v
    |   检查请求是否重复
    |       |
    |       v
    |   查询 data
    |       |
    |       v
    |   保存执行结果
    |
    +---- Put
            |
            v
        检查请求是否重复
            |
            v
        检查 Version
            |
            v
        更新 data
            |
            v
        保存执行结果
```
当收到新请求时：
- `RequestId`等于已记录的`ID`：说明是重复请求，直接返回之前的结果
- `RequestId`小于已记录的`ID`：说明是旧请求，需要按协议处理，不能再次执行
- `RequestId`大于已记录的`ID`：说明是新请求，可以执行并更新记录

---
# 四、`Clerk`：客户端重试机制

```text
创建请求
    |
    v
生成 RequestId
    |
    v
向当前 Leader 发送 RPC
    |
    v
检查 RPC 是否成功
    |
    +---- RPC 失败 ------> 切换服务器并重试
    |
    +---- ErrWrongLeader -> 切换服务器并重试
    |
    +---- 成功 ----------> 返回结果
```
**`Put`需要区分`ErrVersion`和`ErrMaybe`**
- `ErrVersion`：服务器明确返回版本不匹配
- `ErrMaybe`：`RPC`失败导致操作结果不确定，不能简单地认为`Put`一定失败

---
# 五、`Snapshot`：日志压缩与壮态恢复

## 1. `Snapshot()`

```text
Reader 应用日志
    |
    v
调用 RSM.Snapshot(index)
    |
    v
检查 maxraftstate
    |
    +---- 未超过阈值 ----> 返回
    |
    v
调用状态机 Snapshot()
    |
    v
生成状态机快照数据
    |
    v
调用 Raft.Snapshot(index, snapshot)
    |
    v
压缩已经应用的日志
    |
    v
持久化 Raft 状态和 Snapshot
```

## 2. 节点重启时如何恢复`Snapshot`？

节点重启时：
1. 通过`ReadRaftState()`恢复`Raft`日志和快照元数据
2. 通过`ReadSnapshot()`读取持久化快照字节
3. 将快照封装为`SnapshotValid`的`ApplyMsg`
4. 将该消息交给`applier`
5. `applier`将快照发送到`applyCh`
6. `RSM Reader`收到`SnapshotValid`
7. 调用状态机`Restore()`恢复`KV`数据和去重记录

---
# 六、测试中遇到的`Bug`

## 1. `4A`：旧`Leader`的`Submit`一直阻塞

测试大致经历以下步骤：
1. 原`Leader`被隔离到少数派
2. 测试向旧`Leader`连续提交100个`Dec`请求
3. 少数派无法提交这些日志，因此这些`Submit`应该继续等待
4. 多数派选出新的`Leader`，并继续提交`Inc`请求
5. 网络恢复，旧`Leader`重新加入集群
6. 新`Leader`的日志覆盖旧`Leader`尚未提交的日志
7. 旧`Leader`上等待的`Submit`应该被唤醒，并返回`ErrWrongLeader`

**问题表现**
测试中至少有一个`Submit`一直没有返回
这说明原来的等待请求没有被正确识别为失效请求

最终需要确保：
```go
request.Op.Me == op.Me &&
request.Op.Id == op.Id
```
只有请求身份匹配，才可以向原来的`Submit`返回成功结果
# 2. `4C`：重启后状态机没有恢复`Snapshot`

**问题表现**
在`Snapshot`相关测试中，出现历史记录不满足**线性一致性**的问题
其中一个关键问题是：`Make()`恢复了`Raft`的日志和快照元数据，却没有把持久化的`Snapshot`数据交给`RSM`

**修复思路**
在`Make()`中读取`Snapshot`：
```go
snapshot := persister.ReadSnapshot()
```
如果存在有效快照，就构造`SnapshotValid`的`ApplyMsg`，并暂存起来，等待`applier`发送给上层

这样`RSM Reader`才能调用：
```go
rsm.sm.Restore(msg.Snapshot)
```
恢复状态机
## 3. `4C`：`applier`快照恢复时发生索引越界

`Raft`中几个重要索引的含义：
- `snapshotIndex`：快照包含的最后一条日志索引
- `lastApplied`：状态机已经应用到的最后一条日志索引
- `commitIndex`：已经提交的最后一条日志索引

因此需要满足：
```text
snapshotIndex <= lastApplied <= commitIndex
```

**修复思路**
关键是调整执行顺序：
先保证`lastApplied`不小于`snapshotIndex`，再判断是否还有已提交的新日志需要应用

---
## 七、总结

## 1. `Raft`和状态机是两个不同的层次

`Raft`负责保证日志复制和提交顺序
`RSM`负责按照提交顺序执行状态机操作
`KVServer`则负责具体的业务逻辑
## 2. 分布式请求需要明确的身份标识

`Raft`日志`index`并不能唯一代表一个客户端请求
因此需要结合客户端身份、请求`ID`，以及日志应用时的请求身份检查，避免日志覆盖后错误地返回结果
## 3. 请求去重也是状态机的一部分

仅仅保存`KV`数据是不够的
请求执行记录同样影响状态机的正确性，因此也必须参与`Snapshot`和`Restore`
