---
title: MIT 6.5840 Lab5：ShardKV实现笔记
date: 2026-11-24 21:36:00
categories:
  - MIT 6.5840
tags:
  - Shard KV
  - Raft
  - Go
---

# 一、核心架构

| 角色                         | 文件                                        | 职责                                                        |
| -------------------------- | ----------------------------------------- | --------------------------------------------------------- |
| `shardkv Clrek`            | `shardkv1/client.go`                      | 把`key`路由到组；处理`ErrWrongGroup`，必要时把`ErrVersion`改报`ErrMaybe` |
| `shardgrp(Clerk + Serevr)` | `shardgrp/client.go`、`shardgrp/server.go` | 用`Raft`复制一个组的状态：数据、每个`shard`的状态、配置号                       |
| `ShardCtrler`              | `shardctrler/shardctrler.go`              | 把系统从`current`迁移到`next`：冻结->拷贝->删除->发布新配置                  |
**设计要点**
- 组之间从不直接通信。`shard`搬迁完全由`controller`驱动：从源组`Freeze`拿到数据->`Install`到目标组->`Delete`源组副本
- 任何时刻每个`shard`最多只有一个组在服务。源组先`Freeze`（不再接受读写），目标组`Install`之后才开始服务
- 所有改变组状态的操作都走`Raft`，保证组内3个副本一致
- 所有控制操作都用配置号`Num`做栅栏：旧`controller`、延迟到达的`RPC`都会被拒绝；相同`Num`的重复请求视为已成功（幂等）
- `controller`的进度持久化在`kvsrv（current/next）`，所以 `controller`崩溃后新`controller`能接着做完
## 1. 关键数据结构
### 1.1 `ShardConfig`
```go
type ShardConfig struct {
    Num    Tnum // 配置号，单调递增，NumFirst = 1
    Shards [NShards]tester.Tgid // shard i → 负责的组 gid（0 = 无人负责）
    Groups map[tester.Tgid][]string // gid → 该组的服务器名字
}
```
### 1.2 `kvsrv`里的两个`key`

| key         | 内容                 | 版本号的作用                                        |
| ----------- | ------------------ | --------------------------------------------- |
| `"current"` | 已经生效的配置            | 带 `cversion` Put：只有第一个完成者能推进                  |
| `"next"`    | 正在进行（或做到一半中断）的目标配置 | 带 `nversion` Put：同一 `Num` 只有一个 controller 能发布 |
**状态判断规则：**

| 条件                                     | 含义                 |
| -------------------------------------- | ------------------ |
| `next` 不存在，或 `next.Num <= current.Num` | 空闲，没有进行中的变更        |
| `next.Num == current.Num + 1`          | 有一次变更正在进行或被中断，需要做完 |
### 1.3 shardgrp服务端状态
```go
type ShardState int
const (
    ShardAbsent  ShardState = iota // 不归我 / 已删除（map的零值，未出现的shard就是Absent）
    ShardServing // 正在服务读写
    ShardFrozen // 已冻结：拒绝读写，等待被Delete
)

type KVServer struct {
    data       map[string]Value // 所有shard的kv混放在一个map 里
    shardState map[Tshid]ShardState
    configNum  Tnum // 本组已知的最新配置号
    frozenNum  map[Tshid]Tnum // 每个shard最近一次被Freeze的Num
    installNum map[Tshid]Tnum // 每个shard最近一次被Install的Num
    deleteNum  map[Tshid]Tnum // 每个shard最近一次被Delete的Num
}
```
- `frozenNum / installNum / deleteNum` 的用途只有一个：**识别重复请求**（同一 `Num` 再来一次 → 直接回`OK`）
- 这些字段全部进入 `Snapshot()` / `Restore()`，否则重启后会丢失幂等信息
- **`Gid1`启动时自己初始化**：`StartServerShardGrp` 里若 `gid == shardcfg.Gid1`，把所有 shard 设为 `ShardServing`，`configNum = NumFirst`。若有快照，`Restore` 会覆盖这些初值

---
## 2. 一次`Get`/`Put`的完整路径
```
测试代码  ck.Put(key, value, version)
   │
   ▼
shardkv Clerk.Put (shardkv1/client.go)
   │ shard = Key2Shard(key)
   │ config = cachedConfig() ← 缓存的配置，第一次才Query
   │ gid = config.Shards[shard]
   │ clerk = GetOrCreateClerk(gid, config.Groups[gid])
   ▼
shardgrp Clerk.Put (shardgrp/client.go)
   │ 轮询组内服务器找leader，最多maxRounds(=10)轮
   │ Call("KVServer.Put", ...)
   ▼
KVServer.Put (RPC handler) (shardgrp/server.go)
   │ err, result := kv.rsm.Submit(*args)
   ▼
rsm → Raft.Start → 复制到多数派 → commit
   ▼
每个副本的rsm reader → kv.DoOp(PutArgs)
   │ 检查shard是否Serving → 检查版本 → 写data
   ▼
leader上的Submit拿到DoOp返回值 → RPC reply
```
### 2.1 为什么`Get`也要走`Raft`？
如果`leader`直接读本地数据，一个已经被分区、自己还不知道的旧`leader`可能返回过期值。走`Raft`后，`Get`和`Put`一样在日志里有确定的位置，读一定能看到排在它前面的所有写，满足线性一致性。

---
## 3. `DoOp`：`shardgrp`的状态机
> `DoOp(req any) any`由`rsm`在每个副本上、按`Raft`日志顺序调用，持有`kv.mu`
### 3.1 `shard`状态转换图
![[Pasted image 20261124175447.png]]
### 3.2 `Get`/`Put`分支
```
case GetArgs / PutArgs:
    shard = Key2Shard(key)
    if shardState[shard] != Serving → ErrWrongGroup ← Absent 和Frozen都拒绝
    Get: key不存在 → ErrNoKey；否则返回 (Value, Version, OK)
    Put: key不存在且Version == 0 → 创建，Version=1，OK
         key存在且Version匹配 → 覆盖，Version+1，OK
         其他 → ErrVersion
```
**`Frozen`也必须拒绝`Get`**：`Freeze`时导出的数据已经交给`controller`；冻结后如果还允许写，新写入会丢失；允许读则可能和目标组的新写入不一致
### 3.3 Config分支
```
case ConfigArgs:
    if args.Config.Num <= configNum  → OK（旧的或重复的，忽略）
    configNum = args.Config.Num      → OK
```

- **接受任何更新的配置号，而不只是 `configNum+1`**：新加入的组`configNum`是 0，第一次收到的可能是 `Config(5)`；离开后又重新加入的组也落后好几个版本

> `Config`的真正作用是**推进 `configNum`**，因为`Freeze`和`Install`都要求 `Num == configNum + 1`
### 3.4 `FreezeShard`分支
```
case FreezeShardArgs (Shard s, Num N):
    switch shardState[s]:
    Serving:
    if N != configNum+1 → ErrWrongGroup ← 过期或超前的Freeze
        (继续往下执行冻结)
    Frozen:
        if N != frozenNum[s] → ErrWrongGroup
        (N 相同：重复 Freeze，继续往下，重新导出一次数据)
    Absent:
        if deleteNum[s] == N → OK, Moved=true ← 已经搬完并删除
        else → ErrWrongGroup

    shardState[s] = Frozen
    frozenNum[s]  = N
    data' = 复制出属于s的所有kv（新map，不能直接引用kv.data）
    State = labgob编码data
    → OK, State
```
要点：
1. **为什么要求 `N == configNum+1`**：`Freeze`属于从 `current` 到 `next` 的迁移，`N = next.Num`，而源组此时的 `configNum` 应该等于 `current.Num`。旧`controller`的`Freeze`（N 更小）或乱序到达的`RPC`会被拒绝
2. **重复 Freeze 返回 OK 并重新导出数据**：shard 冻结后不再有写入，数据不会变，所以重新导出的结果和第一次一样，可以安全地再 Install 一次
3. **为什么需要 `Moved`**：新 controller 接手时会从头重放所有 shard 的迁移。如果旧 controller 已经做完了 Delete，源组上该 shard 是 Absent，原本会返回 `ErrWrongGroup`，新 controller 就会中止，恢复永远做不完。`Moved=true` 告诉 controller：跳过 Install，直接做 Delete（Delete 也是幂等的）
4. **不要用返回空 map表示已搬走**：如果空 map 被 Install 到目标组，会覆盖目标组上的真实数据
5. **返回数据的副本**：RPC 框架会在不持锁的情况下读 reply 去序列化，必须给它一份拷贝（这里编码成 `[]byte`，自然就是拷贝）。
### 3.5 `InstallShard`分支
```
case InstallShardArgs (Shard s, State, Num N):
    if shardState[s] == Serving && installNum[s] == N → OK ← 重复Install
    if shardState[s] != Absent → ErrWrongGroup
    if N != configNum+1 → ErrWrongGroup
    decode State → value (失败 → ErrMaybe)
    检查value里所有key都属于s
    删除data里属于s的旧key
    把value写入data
    shardState[s] = Serving
    installNum[s] = N
    → OK
```
- **重复 Install 直接回 OK，不再覆盖数据**：Install 成功后目标组可能已经接受了新的 Put；如果重复的 Install 把旧数据再写一遍，这些 Put 会丢失。
- **为什么目标组的 `configNum` 等于 `current.Num`**：`finish` 第一阶段会先对 `next` 里的所有组发 `Config(current)`，让新组和落后的组追上
### 3.6 DeleteShard分支
```
case DeleteShardArgs (Shard s, Num N):
    if shardState[s]==Absent && deleteNum[s]==N  → OK         ← 重复Delete
    if shardState[s] != Frozen → ErrWrongGroup
    if frozenNum[s]  != N → ErrWrongGroup
    删除data里属于s的所有key
    shardState[s] = Absent
    deleteNum[s]  = N
    → OK
```
只能删除**以相同 Num 冻结过**的 shard，防止旧 controller 的 Delete 误删一个后来又搬回来的 shard
### 3.7 幂等性总表（5B 的核心）
新`controller`重放迁移时，旧`controller`可能做到了任意一步：

| 旧 `controller `做到 | 重放 `Freeze(N)`                       | 重放`Install(N)`                 | 重放`Delete(N)`                  |
| ----------------- | ------------------------------------ | ------------------------------ | ------------------------------ |
| 还没开始              | 正常冻结                                 | 正常安装                           | 正常删除                           |
| Freeze 完成         | `Frozen`且 `frozenNum==N → OK`        | 正常安装                           | 正常删除                           |
| Install 完成        | 同上 → OK                              | Serving 且 `installNum==N → OK` | 正常删除                           |
| Delete 完成         | `Absent`且 `deleteNum==N → OK, Moved` | 跳过（因为`Moved`）                  | `Absent `且 `deleteNum==N → OK` |

**原则：相同 Num 的重复请求 → 视为成功；更旧的 Num → 拒绝

---
## 4. `Controller`：`InitConfig / Query / InitController`
### 4.1 `InitConfig`

```go
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
    sck.IKVClerk.Put("current", cfg.String(), 0)
}
```
只写 `kvsrv`，不联系任何组
### 4.2 `Query`
读 `"current"` 并 `FromString`，失败返回 `nil`
### 4.3 `InitController`（5B 恢复入口）

```
cur, cver = Get("current")
next = Get("next")
if next 存在 && next.Num == cur.Num + 1:
    finish(cur, cver, next) ← 把上一个controller没做完的变更做完
```

---
## 5. `ChangeConfigTo`
### 5.1 `new` 和 `next` 的区别

- **`new`**：调用方希望切换到的配置，是**函数参数**
- **`next`**：已经**写进 kvsrv** 的、正在执行的目标配置，是**持久化状态**，所有 controller 都能看到

> `new` 只有在成功写成 `next` 之后才会被执行。多个 controller 可能各拿一个 Num 相同、内容不同的 `new`，但只有一个能成为 `next`。
### 5.2执行流程
```
ChangeConfigTo(new)
│
├─① cur, cver = Get("current") 失败 → return
├─② next, nver, nerr = Get("next")
│     nerr == ErrNoKey → next = nil, nver = 0
│     其他错误  → return
│
├─③ if next != nil && next.Num > cur.Num: ← 有一个未完成的变更
│       if next.Num != cur.Num+1 → return 
│       finish(cur, cver, next) ← 帮它做完
│       return    ← 自己的 new 基于旧 current 计算，已经过时，丢弃
│
├─④ if new.Num != cur.Num+1 → return ← new过时（别人已经推进了current）
│
├─⑤ res = Put("next", new.String(), nver) ← 带版本号发布：同一 Num只有一个能成功
│     res == ErrMaybe:
│         读回 "next"，内容 != new.String() → return ← 别人写成功了，不是我
│     res != OK (ErrVersion) → return ← 别人抢先了
│
└─⑥ finish(cur, cver, new)
```

### 5.3 每个分支的设计理由

| 步骤                          | 为什么这样做                                                                                        |
| --------------------------- | --------------------------------------------------------------------------------------------- |
| ③ 先检查未完成的 next              | 如果不先做完它就发布新 next，会覆盖一个已经冻结了部分 shard 的变更，这些 shard 永远没人服务                                       |
| ③ 做完后 return                | 调用方的 `new` 是基于旧 `current` 算出来的，Num 已经被用掉了，不能再用                                                |
| ④ 检查 `new.Num == cur.Num+1` | 并发情况下 current 可能已被其他 controller 推进，`new` 过时                                                   |
| ⑤ 带 `nver` 的 Put            | 多个 controller 读到相同的 `nver`，只有第一个 Put 成功，其他收到 `ErrVersion` 后直接返回                               |
| ⑤ `ErrMaybe` 读回比较           | 不能把 `ErrMaybe` 当成功：可能是我的请求丢了、别人的写成功了。内容相同才说明是自己写的                                             |
| ⑤ 在 ⑥ 之前                    | **先持久化意图，再shardgrp。** 如果先 Freeze 再崩溃而 `next` 没写进去，新 controller 不知道有一次迁移做了一半，被冻结的 shard 永远无人服务 |
| 不清空 next                    | 只靠 `next.Num` 与 `cur.Num` 比较就能判断状态；去掉 `Put("next","")` 少一次写，也少一类版本竞争                          |

---
## 6. `finish`和`retry`
### 6.1 四个阶段

```
finish(cur, cver, next)
│
├─ 前置检查：next.Num == cur.Num + 1，否则 return
│
├─ 阶段 1：对 next.Groups 中的每个组发 Config(cur)
│          让新组 / 重新加入的组 / 落后的组把configNum推进到cur.Num
│
├─ 阶段 2：对每个 shard（0..NShards-1）：
│   old = cur.Shards[s], new = next.Shards[s]
│   old == new → 跳过
│   old == 0 → Install(new组 空数据, next.Num)
│   new == 0 → Freeze(old 组) → Delete(old组)
│   其他（搬迁）→ Freeze(old 组) 拿到 state, moved
│                if !moved: Install(new 组, state, next.Num)
│                Delete(old 组)
│
├─ 阶段 3：对 next.Groups 中的每个组发 Config(next)
│          把参与的组推进到 next.Num，为下一次变更做准备
│
└─ 阶段 4：Put("current", next.String(), cver)
           失败（ErrVersion / ErrMaybe）→ 读回 current，内容不是 next → 被取代，return
```

每一个 RPC 都通过 `sck.retry(next, op)` 调用；`retry` 返回 false 时，`finish` 立刻 return。
### 6.2 阶段 1 为什么必须先发 Config(current)？

Freeze 和 Install 都要求 `Num == configNum + 1`，即组的 `configNum` 必须等于 `current.Num`：
- **目标组**：新加入的组 configNum 是 0，必须先追上。
- **源组**：一定在 `current.Groups` 里，而上一次变更的阶段 3 已经对 `current.Groups`（就是上一个 next 的 Groups）发过 `Config(current)`，所以已经是 `current.Num`。这也是阶段 1 只发给 `next.Groups` 就足够的原因：**正在离开的组**不在 `next.Groups` 里，但它在上一轮已经被推进过。
### 6.3 为什么 Freeze/Install/Delete 都传 `next.Num`？
迁移操作属于「通往 next 的这一步」。服务端用 `Num == configNum+1` 检查，`configNum` 此时是 `current.Num`，所以只有 `next.Num = current.Num + 1` 能通过。三个操作用同一个 Num，服务端才能用 `frozenNum == N`、`installNum == N`、`deleteNum == N` 识别重复
### 6.4 shard 处理顺序和「不受影响的 shard 继续服务」
- 只对 `old != new` 的 shard 发 RPC，其他 shard 在整个过程中保持 Serving，客户端不受影响（`TestProgressJoin5A` 测的就是这个）。
- 一次只搬一个 shard，从 Freeze 到 Install 的窗口内该 shard 不可用，客户端收到 `ErrWrongGroup` 后会重新 Query 并等待。
- 阶段 3、4 之前，客户端的配置里新 shard 仍然指向旧组；旧组回 `ErrWrongGroup`，客户端反复 refresh，直到 `current` 被更新。
### 6.5 retry：等下去还是退出？
shardgrp Clerk 的 Freeze/Install/Delete/Config 都有重试上限，连不上时返回 `ErrWrongLeader`，由 `retry` 判断：

| 情况                   | 判断依据                            | 动作  |
| -------------------- | ------------------------------- | --- |
| 组暂时挂了，之后会重启          | `current.Num < next.Num`（还没人做完） | 继续等 |
| 自己已被取代，组可能已离开并被永久关掉  | `current.Num >= next.Num`       | 退出  |
| 组拒绝（`ErrWrongGroup`） | 组的 configNum 已超过                | 退出  |

### 6.6 阶段 4 的并发安全
多个 controller 同时 finish 同一个 next：它们读到相同的 `cver`，只有一个 `Put("current", next, cver)` 成功；其他收到 `ErrVersion`，读回发现内容就是 next，也正常返回。被取代的旧 controller（cver 更旧）读回的内容不是自己的 next，直接返回，不会把 current 改回旧值

---

## 7. 故障与并发场景

### 7.1 controller 在各个时间点崩溃

| 崩溃时刻                   | kvsrv 状态                | 新 controller 的 InitController      |
| ---------------------- | ----------------------- | ---------------------------------- |
| Put next 之前            | next 是旧的（Num ≤ current） | 什么都不做；这次变更从未发生                     |
| Put next 之后，阶段 2 进行中   | next = current+1        | 重放 finish：已完成的 shard 被幂等处理         |
| 阶段 3 之后，Put current 之前 | next = current+1        | 重放 finish，所有 RPC 都回 OK，最后写 current |
| Put current 之后         | current = next          | next.Num == current.Num，空闲         |


---
## 8. 重试策略

| 位置                 | 操作                           | 策略                                                |
| ------------------ | ---------------------------- | ------------------------------------------------- |
| kvsrv Clerk        | Get/Put                      | 无限重试，失败后 sleep 10ms                               |
| shardgrp Clerk     | Get/Put                      | 最多 10 轮，否则 `ErrWrongLeader`                       |
| shardgrp Clerk     | Freeze/Install/Delete/Config | 最多 `10*len(servers)` 次，否则 `ErrWrongLeader`        |
| shardkv Clerk      | Get/Put                      | 遇到 `ErrWrongGroup/ErrWrongLeader` 无限 refresh + 重试 |
| controller `retry` | 所有 shardgrp RPC              | 组不可达时：无人完成就等，已有人完成就退出                             |

---
## 9. Bug 复盘

### 9.1 早期阶段（TestStaticOneShardGroup5A）

| 问题                                              | 原因                                                            | 当时的修法                     | 后来的变化                                                   |
| ----------------------------------------------- | ------------------------------------------------------------- | ------------------------- | ------------------------------------------------------- |
| Put 一直 `ErrWrongGroup`                          | `InitConfig` 发 `Config` 时组还没选出 leader，只试一次，shard 一直不是 Serving | `InitConfig` 里循环重试 Config | **被推翻**：`TestInitQuery5A` 的地址是假的，会永远卡住。改为 Gid1 启动时自己初始化 |
| Join 时 `Config(current)` 返回 `ErrMaybe` 就 return | 新组还没选出 leader，只试一次                                            | 重试                        | 最终由 `retry` 统一处理                                        |

教训：`ok=false`（RPC 没到达）和 `ErrWrongLeader`（到达了但对方不是 leader）都不代表请求失败，leader 选举需要时间，客户端必须重试；但「无限重试」也不对
### 9.2 5A
**Bug 1：InitConfig 联系 shardgrp（TestInitQuery5A 卡死）**
- 原因：测试里 Gid1 的地址是 `"xxx"`，循环重试 Config 永远不会成功。
- 解决：InitConfig 只写 kvsrv；Gid1 在 `StartServerShardGrp` 里自己初始化为拥有所有 shard、`configNum = 1`。

**Bug 2：shardgrp Put 只试一轮（Put 之后 Get 返回 ErrNoKey）**
- 原因：组刚启动还没 leader，Put 每台都试一遍就返回 `ErrMaybe`，实际没写入。之前 InitConfig 的循环恰好等到了 leader，掩盖了这个问题。
- 解决：循环重试；重发过之后收到的 `ErrVersion` 报 `ErrMaybe`

**Bug 3：服务端 Config 只接受 configNum+1（ManyJoinLeave 卡死）**
- 原因：新组 configNum=0，收到 `Config(2)` 被拒；重新加入的组也落后。
- 解决：接受任何更新的配置号

**Bug 4：shardgrp Clerk.Config 只试一轮（不可靠网络下 leave 没完成）**
- 原因：丢包时返回 `ErrMaybe`，ChangeConfigTo 中途 return，current 没更新；之后组被关掉，shard 仍指向它，Get 永远卡住。
- 解决：Config 是幂等的，重试到拿到明确结果。
**Bug 5：内层无限重试导致客户端卡在已离开的组（ConcurrentClerk 卡死）**
- 原因：Bug 4 改成无限重试后，客户端拿着旧配置访问已经 leave 并关闭的组，永远回不到外层重新 Query
- 解决：内层 Get/Put 最多 10 轮，返回 `ErrWrongLeader`；外层收到后重新 Query，并置 `maybe=true`，之后的 `ErrVersion` 改报 `ErrMaybe`

**Bug 6：kvsrv Clerk 失败后无间隔重试（ProgressJoin 只有 66 次 Get）**
- 现象：单独跑通过（220 次），全量跑失败（66 次，需要 > 100）。
- 原因：`checkShutdownSharding` 等测试会留下还在运行的 Get goroutine；它们在 Bug 5 的修改后回到 `Query()`，而那个测试的 kvsrv 已经关闭，`Call` 立刻失败，`for { continue }` 空转，占了 3～4 个 CPU 核，拖慢之后所有测试。
- 解决：`kvsrv1/client.go` 的 Get/Put 失败后 `time.Sleep(10ms)`。全量耗时从约 460s 降到约 250s。
### 9.3 5B

**Bug：InitController 为空，新 controller 不接手**
- 现象：`TestRecoverCtrler5B` 报 `didn't recover 2 correctly after leave`。
- 解决：重构为 `ChangeConfigTo` + `finish`；`InitController` 检查 `next.Num == current.Num+1` 就调用 finish

**Bug：重放 Freeze 时源组已删除，返回 ErrWrongGroup**
- 原因：旧 controller 已经做完 Delete，源组上的 shard 是 Absent。
- 解决：Absent 且 `deleteNum == N` 时返回 OK + `Moved=true`，controller 跳过 Install 直接 Delete
### 9.4 5C

**Bug：被取代的 controller 一直等一个已经关掉的组（TestConcurrentReliable5C 超时 15 分钟）**
- 过程：
  1. controller X 发布 next（加入新组 g8），开始搬 shard；
  2. controller Y 在 ChangeConfigTo ③ 里帮它做完，测试接着 `leaveGroups` 把 g8 移出，并 `ExitGroup` 把 g8 进程永久关掉；
  3. X 还在自己那份旧的 finish 里对 g8 发 RPC，而 shardgrp Clerk 的控制 RPC 都是 `for {}` 无限重试，X 永远回不来。
- 为什么 5B 能过：5B 里关掉的组总会重启；5C 里离开的组会被永久关掉。
- 解决：shardgrp Clerk 的控制 RPC 加重试上限；controller 用 `retry` 判断