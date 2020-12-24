## Snail MQ 

Snail MQ 是一个简单易懂的消息队列实现。目的在于学习和巩固知识。

Snail在消息存储上借鉴了RocketMQ，具有CommitLog和CommitQueue。Log和Queue使用MappedFile进行尾追加写。

## 功能
- [x] 消息读写
- [x] ack
- [x] 多种ack模式
- [x] 异步同步发送(netty)
- [ ] 多种刷盘模式
- [x] key相同，顺序消息
- [x] 拉取消息push模式
- [x] boot-starter
- [x] 重平衡
- [ ] 多种重平衡策略