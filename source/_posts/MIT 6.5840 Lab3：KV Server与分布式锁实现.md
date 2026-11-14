---
title: MIT 6.5840 Lab3：KV Server与分布式锁实现
date: 2026-11-14 15:00:00
categories:
  - MIT 6.5840
tags:
  - KV Server
  - Go
---

# 一、核心架构

整个`Lab`可以分成三个部分：
1. `KV Server`：负责保存`Key/Value`数据，并通过版本号控制并发写入
2. `KV Clerk`：负责向`Server`发送`RPC`请求，并处理网络故障和请求重试
3. `Distributed Lock`：利用`KV Server`已有的`Get`和`Put`操作，实现分布式锁的获取与释放

> 分布式锁不需要`KV Server`提供专门的`Lock RPC`，而是利用普通的`Get`和`Put`操作实现

---
# 二、`KV Server`：基于版本号的并发控制

## 1. `Get()`

`Get`根据`Key`查询数据：
- `Key`不存在：返回`ErrNoKey`
- `Key`存在：返回`Value`、`Version`和`OK`
## 2. `Put()`

情况一：`Key`不存在，客户端传入的版本号
- `Version == 0`：创建`Key`
- `Version != 0`：返回`ErrNoKey`

情况二：`Key`存在
只有客户端传入的`Version`与服务器当前`Version`相同，才允许更新
如果版本号不匹配，返回`ErrVersion`

---
# 三、`KV Clerk`：`RPC`重试与`ErrMaybe`

## 1. `ErrVersion`与`ErrMaybe`的区别

- `ErrVersion`：当前请求使用的版本号与服务器实际版本号不匹配
- `ErrMaybe`：之前某次请求肯哪个已经成功执行，但响应丢失；后续冲时遇到版本号冲突，因此客户端无法确认之前的操作结果

因此，客户端必须区分**第一次请求的结果**和**重试请求的结果**
## 2. `Clerk.Put()`

1. 初始化请求参数，然后进入重试循环，向`KV Server`发送`Put RPC`
2. 如果`RPC`调用失败，说明客户端没有收到服务器的有效响应。此时`Clerk`会记录发生过通信失败，并继续重试
3. 如果`RPC`调用成功，则根据服务器返回的错误类型进行判断：
- 如果返回`ErrVersion`，说明当前请求使用的版本号与服务器中的实际版本不一致。此时还需要检查之前是否发生过`RPC`通信失败。
	- 如果之前发生过`RPC`失败，那么之前的请求可能已经在服务器上执行成功，只是响应丢失了。因此，当前的`ErrVersion`可能是由之前请求成功导致的，`Clerk`返回`ErrMaybe`
	- 如果之前没有发生过`RPC`失败，那么这就是一次正常的版本冲突，`Clerk`返回`ErrVersion`
- 如果返回的是其他结果，则直接将服务器返回的结果交给调用者

另外，每次`RPC`尝试都应该使用新的`Reply`对象，避免上一次请求的响应数据影响本次结果

---
# 四、`Distributed Lock`：基于`KV Server`实现分布式锁

```go
type Lock struct {
    ck       kvtest.IKVClerk
    lockname string
    identity string
}
```
- `lockname`：对应`KV Server`中的`Key`
- `identity`：用于判断锁的所有权

创建`Lock`时，为每个实例生成随机`identity`：
```go
identity: kvtest.RandValue(12)
```

## 1. `Acquire`：获取锁

情况一：`Key`不存在
调用`Get`返回`ErrNoKey`

使用`version = 0`创建锁，可能出现三种结果：
- `OK`：创建成功，直接返回
- `ErrMaybe`：需要重新`Get`确认
- 其他错误：重试

情况二：锁已被其他客户端持有
`Get`成功，但`value != ""`，需要等待一段时间后重试

情况三：锁空闲
此时使用刚读到的`version`尝试获取锁：
- 如果返回`OK`则成功获得锁
- 如果返回`ErrVersion`，需要重试

情况四：`Put`返回`ErrMaybe`
需要重新`Get`，判断所有权是否属于自己，如果不是自己的`identity`继续循环
## 2. `Release`：释放锁

1. 读取锁状态，检查所有权，只有确认自己持有锁才能继续
2. 尝试释放，用该刚才读到的`version`
- `ErrVersion`：重新`Get`检查当前锁的持有者
	- 仍然是自己：重新尝试释放
	- 不是自己：不能释放
- `ErrMaybe`：重新`Get`确认
	- `value == ""`：释放成功
	- `value == identity`：尝试释放

---
# 五、总结

## 1. 乐观并发控制

客户端通过`Version`判断数据是否已经被其他客户端修改
`Get`负责读取当前状态，`Put`负责通过版本检查提交更新
## 2. `RPC`的不确定性

`RPC`失败不代表服务器一定没有执行请求，服务器可能已经执行成功，只是响应丢失。

因此需要通过`ErrMaybe`表示这种不确定性，并让上层逻辑重新读取状态进行确认。


