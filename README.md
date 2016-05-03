#### Golang 实现的Redis客户端

-----
##### 参照Jedis的思路实现的Redis客户端

> * GedisPool 普通客户端连接池

> * ShardedPool 分片客户端连接池，主从切换无感知

> * SentinelPool 哨兵模式的客户端连接池，可感知主从切换

> * ShardedJedisSentinelPool 参照[我另外一个Java实现版](https://github.com/jianfeng-parker/moat/blob/master/src/main/java/cn/ubuilding/moat/redis/pool/ShardedJedisSentinelPool.java)，支持以分片的方式使用主从；
    待实现...
    
> * Pub/Sub

> * 测试代码待实现...
    
##### 模型结构

```java
                                         +-----------+
                               +-------> | GedisPool |-----------+                   +-------+       +------------+
                               |         +-----------+           |------------------>| Gedis |------>| Connection |
                               |         +--------------+        |                   +-------+       +------------+
                               +-------> | SentinelPool |--------+                      /\ 
  +----------------------+     |         +--------------+                               |
  |   User Application   |-----+                                                        |
  +----------------------+     |         +--------------+                               |
                               +-------> | ShardedPool  |------------------+         +--------------+                   
                               |         +--------------+                  |-------->| ShardedGedis |
                               |         +--------------------------+      |         +--------------+
                               +-------> | ShardedJedisSentinelPool |------+        
                                         +--------------------------+ 
                               
```                               
                            
                            
                            
 








