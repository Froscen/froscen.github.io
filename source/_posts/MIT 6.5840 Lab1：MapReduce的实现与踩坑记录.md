---
title: MIT 6.5840 Lab1：MapReduce的实现与踩坑记录
date: 2026-11-06 15:00:00
categories:
  - MIT 6.5840
tags:
  - MapReduce
  - Go
---

# 一、整体设计

**`Lab1`要实现一个简单的`MapReduce`框架， 主要分为`Coordinator`和`Worker`两个部分。**

- `Coordinator`：负责维护任务状态、分配任务、处理任务完成汇报，以及检测超时任务。
- `Worker`：向`Coordinator`请求任务，执行`Map`或`Reduce`操作，并将执行结果汇报给`Coordinator`。

两者通过`RPC`通信。

---
## 1. `Task`的设计

首先需要设计一个`Task`结构体，用来记录任务的元数据。

我最初考虑过是否需要在`Task`中保存文件内容或者`KeyValue`切片，后来发现并不需要：`Coordinator`只负责调度，不负责实际计算。

因此，`Task`主要保存：

- `TaskID`：任务编号
- `TaskType`：任务类型（`Map`/`Reduce`）
- `State`：任务状态（`Idle`/`InProgress`/`Completed`）
- `StartTime`：任务开始执行时间，用于判断是否超时
- `Filename`：`Map`任务对应的输入文件
- `Generation`：任务执行版本号，用于区分超时重试前后的不同执行实例
---
## 2. `Coordinator`的任务调度

`Coordinator`的任务调度分为两个阶段：

1. `Map`阶段：所有`Map Task`完成之前，只分配`Map Task`
2. `Reduce`阶段：所有`Map Task`完成之后，才开始分配`Reduce Task`

多个`Worker`可能同时请求任务，因此`Coordinator`需要使用互斥锁保护任务状态。

此外，`Done()`和任务完成汇报`RPC`也需要使用同一把锁，避免并发读写状态时发生数据竞争。

---
# 二、核心函数的运行流程

## 1. `MakeCoordinator()`

创建`Coordinator`，并初始化任务列表。

主要工作：

- 保存`nReduce`
- 根据输入文件列表创建`Map Task`
- 根据`nReduce`创建`Reduce Task`
- 启动`RPC`服务，等待`Worker`请求任务

这里有一个需要注意的地方：`Coordinator`必须保存`nReduce`，因为`Map Worker`需要知道应该将中间结果划分为多少个分区。

---
## 2. `GetTask()`

这是`Coordinator`最核心的任务分配函数。

执行流程：

- 获取互斥锁
- 判断`Map Task`是否全部完成
- 如果没有完成，优先分配`Map Task`
- 如果已经完成，则开始分配`Reduce Task`
- 检查正在执行的任务是否超时
- 如果任务超时，将其重新设置为`Idle`，再重新分配
- 分配任务时更新任务状态、开始时间和`Generation`
- 如果暂时没有可分配的任务，返回`WaitTask`；如果所有`Reduce Task`都完成，则返回`ExitTask`

其中，`Generation`用来解决一个问题：如果`Worker A`执行超时，`Coordinator`将任务重新分配给`Worker B`，那么`A`可能在之后才完成。

通过`Generation`，`Generation`可以拒绝旧执行实例的完成汇报。

---
## 3. `Worker()`

`Worker`不断向`Coordinator`请求任务，根据返回的任务执行不同的操作。

- `MapTask`：读取输入文件，执行`Map`函数，生成中间文件
- `ReduceTask`：读取对应的中间文件，执行`Reduce`函数，生成最终输出文件
- `WaitTask`：等待一段时间后重新请求
- `ExitTask`：退出`Worker`

**`Map`阶段**

`Map Worker`首先读取输入文件，然后调用`mapf`得到`KeyValue`切片。
接下来，根据：
`ihash(kv,Key) % NReduce`
将`KeyValue`分配到不同的`Reduce`分区

> 这里需要注意：`Hash`分区保证相同`Key`会进入同一个分区，但不保证相同`Key`在分区内连续。因此，`Reduce`阶段仍然需要排序

每个分区的数据分别写入中间文件：
`mr-X-Y`
其中`X`表示`Map Task ID`，`Y`表示`Reduce`分区`ID`

所有分区都成功写入后，`Worker`才向`Coordinator`汇报`Map Task`完成。

**`Reduce`阶段**

`Reduce Worker`根据自己的`Reduce Task ID`，读取所有`Map Worker`为该分区生成的中间文件。

执行流程：

1. 遍历所有`Map Task`对应的中间文件
2. 使用`Gob Decoder`读取`KeyValue`
3. 将所有数据合并到一个切片
4. 根据相同`Key`排序
5. 将相同`Key`的`Value`收集到一起
6. 调用`reducef`，生成最终结果
7. 将结果写入`mr-out-X`
8. 成功后向`Coordinator`汇报任务完成

---
## 4. `ReportTask()`

`Worker`执行完任务后，通过`RPC`通知`Coordinator`。

`Coordinator`根据`TaskID`、`TaskType`和`Generation`检查汇报是否合法，只有任务仍处于`InProgress`状态，并且`Generation`匹配时，才将任务标记为`Completed`。

这样可以避免旧`Worker`的迟到汇报影响当前任务状态。

---
## 5. `Done()`

`Done()`用于让`Coordinator`主程序判断整个`MapReduce`作业是否完成。

由于`Coordinator`只有在所有`Map Task`完成之后才会分配`Reduce Task`，因此只需要检查所有`Reduce Task`是否完成。

> 访问任务状态时同样需要加锁

---
# 三、测试过程中遇到的`Bug`
## 1. `Mutex`死锁

最开始，我在`GetTask()`中获取了`c.mu`，然后调用`allMapTaskCompleted()`。
但是我又在`allMapTaskCompleted()`内部尝试获取同一把锁。

这就导致了死锁：

1. `GetTask`获取`c.mu`
2. 调用`allMapTaskCompleted()`
3. `allMapTaskCompleted()`再次尝试获取`c.mu`
4. 由于`Go`的`sync.Mutex`不支持同一个`Goroutine`重复加锁，因此一直等待

> 解决方法：
> 让`GetTask()`、`Done()`等外层函数负责加锁，辅助函数只负责检查任务状态，不再重复加锁。

---
## 2. `Coordinator`忘记保存`nReduce`

在`MakeCoordinator()`中，我创建了`Reduce Task`，但忘记执行：

`c.nReduce = nReduce`

导致`Coordinator`返回给`Map Worker`的`NReduce`为`0`。

`Worker`执行：

`ihash(kv.Key) % reply.NReduce`

时发生除零错误，`Map Worker`退出，`Coordinator`一直等待任务完成，最终测试超时。

---
# 四、总结

完成`Lab1`后，我对`MapReduce`的执行流程有了更具体的认识。

这次实验让我重点理解了几个问题：

1. 如何通过`Coordinator`和`Worker`的分工实现任务调度与分布式计算
2. 如何通过互斥锁保护共享任务状态
3. 如何使用超时检测和重新分配处理`Worker`故障
4. 如何通过中间文件连接`Map`和`Reduce`两个阶段
5. 如何通过`RPC`完成`Coordinator`与`Worker`之间的通信