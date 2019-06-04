## 离线电商数据分析项目


数据库连接池：C3P0、DBCP等

企业级编程
- 不能使用硬编码，使用常量来封装
- 代码应该做成可配置化
- 获取配置文件使用静态代码块

JavaBean的特征：
- 有成员变量field，但是必须是私有的
- 需要有对应的getter、setter方法
- 一般与数据库中的一个表对应起来使用


DAO模式
- data access object 数据访问对象
- 把数据库访问的代码封装到DAO中，后面对系统维护的时候不需要到业务逻辑代码修改，降低了业务逻辑层和数据访问的耦合
- 一般分为两部分：DAO接口、DAO实现类。业务代码通常面向接口编程


开发spark大型复杂项目的一些经验
- 尽量少生成RDD
- 尽量少对RDD进行算子操作，尽可能在一个算子里面实现多个需求
- 尽量少对RDD进行shuffle操作，比如groupByKey,reduceByKey,sortByKey，尽可能多用map，mapToPair等不需要shuffle的操作
    - 有shuffle的操作会导致大量的磁盘读写，严重降低性能
    - 有shuffle的操作也容易导致数据倾斜，数据倾斜是性能杀手
- 无论什么功能，性能第一
    - 在传统J2EE等系统、网站中，架构和可维护性、可拓展性比性能要重要
    - 在大数据项目中，性能则远重要于代码的规范、设计模式等
    
    
开发spark程序的注意点
- 在算子中使用的时候，需要对变量加上final修饰

性能调优
```
spark-submit \
--class cn.fancychuan.offline-shop.core.WordCount \
--num-executor 3 \
--driver-memory 100m \
--executor-memory 100m \
--executor-cores 3 \
/path/to/xxxxx.jar
```
- 首要是增加和分配更多的资源
    - 需要哪些资源？ executor、cpu per executor、 memory per executor、driver memory
        - 比如有3个executor，每个executor都有2个cpu核心，那么一次能够同时执行6个task
        - 增加内存带来性能的提升有两点：
            - 可以缓存更多的对象，减少数据写入磁盘甚至不写入磁盘
            - 可以创建更多的对象，避免JVM频繁的GC，垃圾回收
            - （对于shuffle操作，需要内存进行聚合，内存不够也会写入磁盘）
    - 调节并行度
        - 并行度：spark作业中，各个stage的task数量，也就代表了作业在各个阶段的并行度
        - 比如：50个executor各有3个cpu核心，那么一次性可以并行150个task。而如果只有100个task，那么每个executor只会运行2个cpu核心，浪费了一个
        - 提高了并行度以后，每个task需要处理的数据就变少了，就可以提升整个作业的性能
        - 经验：
            - task数量至少设置成与总cpu核心数量相同（理想情况下，每个task差不多在同一时间完成）
            - 官方推荐：task数量设置成application总cpu核心数量的2-3倍
        - 设置方法: conf.set("spark.default.parallelism", 500) 
- 对RDD的优化
    - 重构RDD，尽可能重用RDD：把差不多的RDD抽象到一起
    - 公共的RDD持久化
        - 内存+磁盘+序列化
        - 为了数据的高可靠行，可以考虑RDD的双副本机制持久化，一个副本丢了，可以从了一个副部获取，避免重复计算（内存充足时可以考虑使用）