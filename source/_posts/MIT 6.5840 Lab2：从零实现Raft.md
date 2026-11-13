# 一、整体架构：`Raft`节点需要维护什么？

## 1. `Persistent State`

- `currentTerm`：当前任期
- `votedFor`：当前任期内投票给了哪个节点
- `log`：日志条目
## 2. `Volatile State`

- `commitIndex`：已经提交的最高日志索引
- `lastApplied`：已经应用到状态机的最高日志索引
> `lastApplied <= commitIndex`
## 3. `Leader Volatile State`

- `nextIndex[i]`：下一次向该`Follower`发送日志时，应该从哪个索引开始
- `matchIndex[i]`：`Leader`已经确认该`Follower`拥有最高日志索引

---
# 二、`3A`：`Leader Election`

## 1. `Make()`

`Make()`是`Raft`节点的入口，主要负责：
- 初始化节点状态
- 初始化日志和任期
- 从`Persister`恢复持久化状态
- 启动后台`goroutine`
## 2. `ticker()`

`ticker()`周期性检查当前节点状态和选举超时

超时后还需要检查自己的角色，因为不同角色的处理方式不同：
- `Follower`：选举超时，说明可能长时间没有收到`Leader`的有效心跳，应该发起选举
- `Candidate`：选举超时，说明上一轮选举没有成功，应该开启新一轮选举
- `Leader`：应该主动发送心跳，维持自己的领导地位
## 3. `startElection()`

节点决定发起选举后，需要：
1. 将角色切换为`Candidate`
2. 增加`currentTerm`
3. 给自己投票
4. 重置选举计时器
5. 向其他节点发送`RequestVote RPC`
## 4. 固定本轮选举的`Term`

同一轮选举发送的所有`RPC`必须使用相同的任期
`RPC`返回后，还需要重新检查当前节点是否仍然是`Candidate`，以及当前任期是否仍然等于发起请求时的`Term`

---
# 三、`3B`：`Log Replication`

## 1. `AppendEntries RPC`

> `Leader`通过`AppendEntries RPC`向`Follower`发送日志，也通过不携带日志条目的`AppendEntries`发送心跳

- `PrevLogIndex`：待追加日志之前的那条日志索引
- `PrevLogTerm`：该位置对应的任期
- `Entries`：本次需要复制的日志
- `LeaderCommit`：`Leader`当前的提交索引
## 2. 为什么需要`PrevLogIndex`和`PrevLogTerm`

`Raft`需要保证`Leader`和`Follower`的日志在相同索引之前具有相同的前缀。

因此，`Follower`不能直接接受`Leader`发来的日志，而是需要先检查：
`Follower.log[PrevLogIndex].Term == PrevLogTerm`
如果前缀不匹配，就拒绝此次请求；如果匹配，才进入后面的日志追加阶段。

这里需要特别区分两个阶段：
==🟣前缀检查阶段==：
如果`PrevLogIndex`不存在，或者对应`Term`不匹配，就返回`reply.Success = false`
==🟣日志追加阶段==：
如果前缀检查已经成功：
- 目标`index`不存在：追加剩余的`Entries`，然后结束循环
- 目标`index`存在，且`Term`相同：保留当前日志，继续检查下一条
- 目标`index`存在，且`Term`不同：删除该位置及以后的日志，追加`Leader`剩余的日志，然后结束循环
## 3. `Leader`如何维护`nextIndex`？

刚成为`Leader`时，需要为每个`Follower`初始化：
```go
rf.nextIndex[i] = rf.lastLogIndex() + 1
rf.matchIndex[i] = 0
```
当`Follower`返回`Success = true`，`Leader`更新：
```go
rf.matchedIndex[server]
rf.nextIndex[server]
```
如果返回`Success = false`，说明`Follwer`的日志与`Leader`不匹配。
需要：
- 将该`Follower`对应的`nextIndex[server]`减小
- 根据新的`nextIndex`重新构造`AppendEntriesArgs`
- 再次发送`AppendEntriesArgs`
- 直到`Follower`接受日志，返回`Success = true`
## 4. `Conflict Optimization`

在基本实现中，`Leader`每次只将`nextIndex`减小 1

但是，如果`Follower`的日志与`Leader`相差很多，就需要发送大量`RPC`才能找到匹配位置

`Raft`论文提供了优化方法：

`Follower`在拒绝`AppendEntries`时，告诉`Leader`自己发生冲突的位置和任期
`Leader`根据`ConflictTerm`和`ConflictIndex`快速跳过一整段冲突日志，而不是每次只退一条
## 5. 旧`RPC`回复不能覆盖新进度

处理`RPC`回复时，不能只看`Success`，还需要判断这条回复是否仍然对应当前有效的复制进度，通过发送请求时记录的`Term`、请求对应的索引，以及当前的`matchIndex`和`nextIndex`来判断是否过期
## 6. 不能直接把日志切片交给`RPC goroutine`

`Leader`构造`AppendEntries`请求时，不能直接把`rf.log`的底层切片交给`RPC goroutine`，因为接下来可能释放锁，而其他`goroutine`可能修改日志

因此需要复制：
```go
Entries: append([]LogEntry(nil), rf.log[startIndex:]...),
```

---
# 四、`3C`：`Persistence`

## 1. `persist()`

恢复时必须按照完全相同的顺序解码，否则会导致状态恢复错误
## 2. `readPersist()`

节点重新启动时，通过`Persister`读取之前保存的数据

---
# 五、`3D`：`Log Compaction`——`Snapshot`

当状态机已经应用了某个索引之前的日志，就可以将这些日志对应的状态保存成`Snapshot`，并删除不再需要的旧日志
# 1. 引入`snapshotIndex`和`snapshotTerm`

- `snapshotIndex`：`Snapshot`覆盖到的最后一个日志索引
- `snapshotIndex`：`Snapshot`边界对应日志的任期
```go
// 逻辑索引 -> slice 下标
sliceIndex := index - rf.snapshotIndex

// 最后一条日志的逻辑索引
lastLogIndex := rf.snapshotIndex + len(rf.log) - 1
```
## 2. `Snapshot()`

主要流程：
1. 检查`Snapshot`索引是否合法
2. 找到`Snapshot`边界对应的`Term`
3. 保留边界位置之后的日志
4. 将边界位置设置成`dummy entry`
5. 更新`snapshotIndex`和`snapshotIndex`
6. 持久化`Raft`状态和`Snapshot`数据

> 需要保留一个`dummy entry`，因为后续`AppendEntries`仍然需要通过`PrevLogIndex`和`PrevLogTerm`检查日志前缀
## 3. `InstallSnapshot RPC`

`Leader`已经压缩了日志，但是某个`Follower`长时间断连
等这个`Follower`重新连接时，它需要的日志可能已经被`Leader`删除了
这时`Leader`无法再通过普通`AppendEntries`将缺失日志发送给`Follower`
因此需要增加`InstallSnapshot RPC`

`Follower`收到`Snapshot`后，需要判断：
1. 请求任期是否过期
2. 是否需要更新当前任期
3. `Snapshot`是否比本地已有状态更新
4. 本地是否还保留着`Snapshot`边界对应的日志
5. 应该保留边界之后的日志，还是丢弃全部旧日志

## 4. `Snapshot`和`ApplyMsg`的顺序

收到`Snapshot`后，不能直接在持有`rf.mu`的情况下向`applyCh`发送消息，因为`applyCh`可能是无缓冲`channel`，如果接收方暂时没有准备好，发送操作就会阻塞，如果此时仍然持有`rf.mu`，就可能造成死锁。

但直接启动一个`goroutine`发送`Snapshot`也不够稳妥，因为它可能与普通日志`ApplyMsg`发生顺序竞争。

```go
pendingSnapshot *raftapi.ApplyMsg
```

安装`Snapshot`时，不直接发送，而是先保存待发送消息：
```go
msg := raftapi.ApplyMsg{
    SnapshotValid: true,
    Snapshot:      args.Data,
    SnapshotTerm:  args.LastIncludedTerm,
    SnapshotIndex: args.LastIncludedIndex,
}

rf.pendingSnapshot = &msg
```

然后由`applier()`统一处理。

每次循环先检查：
```go
if rf.pendingSnapshot != nil {
    msg := *rf.pendingSnapshot
    rf.pendingSnapshot = nil

    rf.mu.Unlock()
    rf.applyCh <- msg
    continue
}
```
只有没有待发送`Snapshot`时，才继续发送普通日志。

这样可以保证：已经取出并正在发送的旧日志先完成发送，待发送`Snapshot`优先于之后的普通日志。

---
## 六、测试过程中遇到的`Bug`

## 1. `3B`：日志不一致时，`Leader`回退太慢

**现象：**
`Leader`遇到日志与自己不一致的`Follower`时，无法在测试规定的时间内让它重新达成一致。

**原因：**
最初的实现中，`Follower`返回`Success = false`后，`Leader`只将`nextIndex`减小1。
如果双方日志差异很大，就需要发送大量`RPC`才能找到匹配位置。

**解决思路：**
实现`Conflict Optimization`

`Follower`在拒绝`AppendEntries`时返回冲突任期和冲突索引，`Leader`根据这些信息快速回退，而不是每次只减小一个索引
## 2. `3C`：`Figure 8 Unreliable`间歇性失败

**现象：**
测试有时通过，有时失败。

**排查过程：**
观察`ticker`的休眠逻辑：
```go
ms := 50 + (rand.Int63() % 300)
time.Sleep(time.Duration(ms) * time.Millisecond)
```

尝试将其改为固定 10ms：
```go
time.Sleep(10 * time.Millisecond)
```

随后`Figure 8 Unreliable`连续5次通过，完整`3C`测试也全部通过
## 3. `3D`：`Snapshot`后逻辑索引和`slice`下标混淆

日志压缩后，原来直接使用`rf.log[index]`的代码不再成立。

统一通过：
```go
sliceIndex := index - rf.snapshotIndex
```
进行转换

同时使用`lastLogIndex()`和`termAt()`，避免不同函数各自实现索引转换。

---
# 七、总结

==🟣1. 并发程序需要特别关注锁和消息顺序==
`Raft`中有大量`RPC`和`goroutine`，释放锁之后，其他`goroutine`可能修改共享状态；`RPC`返回时，原来的请求也可能已经过期。

因此，必须明确哪些操作在锁内执行，哪些操作可以在锁外执行，以及`RPC`返回后需要重新检查哪些条件。

==🟣2. 逻辑索引和物理存储位置不能混为一谈==

`3D`的`Snapshot`让我真正意识到，逻辑上的日志索引与底层`slice`下标并不是一回事。

建立统一的索引转换模型，比在每个函数中单独修补索引错误更加可靠。
