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
- 广播大变量
    - task执行的算子中，如果使用了外部变量，那么每个task都会获取一份这个变量的副本，而获取是通过网络传输的（100M的变量，1000个task，瞬间就需要占用10G的内存）
    - 大变量可能带来的影响
        - 占用网络带宽
        - 占用内存，可能导致需要持久化到内存的RDD在持久化时内存不足，只能写入磁盘，从而导致磁盘IO上性能的消耗
    - 广播变量相关细节
        - 广播变量在driver中会有一份初始副本
        - BlockManager负责管理Executor对应的内存和磁盘上的数据
        - task首先会在BlockManager找变量，找不到就会去driver中拉取变量，然后交由BlockManager管理，那么后面另一个task来获取的时候就直接从本地BlockManager取
        - 另外，BlockManager可能会从远程的Driver获取变量，也可能从距离比较近的其他节点的Executor的BlockManager上获取
    - 也就是说，广播变量只会在每个Executor保存一个副本，那么这个Executor上的多个task都不需要再去获取这个变量，直接从Executor的BlockManager获取
- 使用Kryo序列化
    - 默认的序列化机制效率不高：速度慢，序列化以后的数据占用内存较多，而Kryo序列化速度快，而且序列化后的数据占用内存大概是java序列化机制的1/10
    - 可以使用到Kryo序列化的地方：
        - 算子函数用到的外部变量：可以优化网络传输性能、优化集群内存空间占用和消耗
        - 持久化RDD时进行序列化，StorageLevel.MEMORY_ONLY_SER：优化内存占用和消耗，避免频繁GC
        - shuffle：在进行stage之间task的shuffle操作时，节点间的task会通过网络进行大量的网络传输：优化网络传输性能
    - 使用步骤
        - set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        - 注册需要序列化的的自定义类
        - （Kryo之所以没有作为默认的序列化类库，是因为Kryo要求要达到最优性能，必须要注册需要序列化的自定义类）