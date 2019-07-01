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

### 性能调优
#### 1. 常规性调优
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
    - 持久化的策略
        - StorageLevel.MEMORY_ONLY() 纯内存，无序列化，可以用cache()代替
        - StorageLevel.MEMORY_ONLY_SER() 第二选择，内存+序列化
        - StorageLevel.MEMORY_AND_DISK()
        - StorageLevel.MEMORY_AND_DISK_SER()
        - StorageLevel.DISK_ONLY()
        - StorageLevel.MEMORY_ONLY_2() 内存充足，使用双副本高可靠机制
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
- 调节本地化等待时长
    - Driver在分配之前，会先计算出每个task需要计算的是哪个分片的数据，会优先把task分配到数据所在的节点上
    - 在无法满足时，可以设置等待一定的时长（比如3秒），如果还是无法有足够的资源分配执行task，那么这个时候就回到一个相对差的节点上执行task任务，这个时候需要网络传输
    - task获取数据一共有5种情况：
        - PROCESS_LOCAL 进程本地化：代码和数据在同一进程（Executor）中，task在Executor中执行，数据从同个Executor的BlockManager中获取，性能最优
        - NODE_LOCAL 节点本地化：代码和数据在同一节点的不同进程中，数据需要在进程间传输，性能次之，有两种情况
            - 数据作为一个HDFS block块，在节点上，task在某个Executor中执行
            - 数据在一个Executor的BlockManager中，task在同一节点的另一个Executor中执行
        - NO_PREF：对于task来说，数据从哪里获取都一样，没有好坏之分？？
        - RACK_LOCAL 机架本地化：数据和task在同一机架的不同节点，需要通过网络在节点间传输数据
        - ANY：数据和task可能在集群的任何地方，并且不在同一机架，性能最差
    - 设置方法：conf.set("spark.locality.wait",6) 默认是3秒
        - spark.locality.wait.process
        - spark.locality.wait.node
        - spark.locality.wait.rack
    - 一般先看执行日志，如果发现日志上面的本地化级别都是PROCESS_LOCAL，那就不需要调节了，如果大部分是其他级别，可以调整一下，但是要注意等待时间带来整体运行时长的增加

#### 2. JVM调优
通常不会有严重的性能问题，但在troubleshooting中，JVM占了重要的地位，有可能造成线上作业报错，甚至失败

JVM调优原理依据：
> JVM内存不够大时，年轻代可能因为内存溢满频繁的进行minor gc，有一些存活下来的对象可能因为多次gc导致年龄过大而跑到老年代中，进而导致老年代中存在一大堆短生命周期的对象。
> 当老年代空间溢满时，就会进行速度很慢的full gc（以为是针对老年代的gc，一般不会频繁，所以采取了不太复杂但消耗性能和时间的回收算法）

- 降低cache操作的内存占比
    - 内存不足时：
        - 批发minor gc，导致spark频繁停止工作
        - 老年代囤积大量活跃对象，导致频繁full gc，可能导致spark长时间不工作
    - spark中，堆内存划分为两块：专门给RDD的缓存持久化用的，另一块是给算子函数运行时使用的，存放函数创建的对象
    - 默认情况下，留给cache操作的内存占堆内存的60%
    - 通过日志查看每个stage的运行情况，如果gc太频繁，时间太长，就需要适当的考虑调整改参数
    - spark.storage.memoryFraction
- 调节executor堆外内存
    - 现象：数据量过大，作业运行时时不时报错：shuffle file cannot be found, task lost, OOM， 这个时候可能就是堆外内存不够用了
    - 主要体现在executor的对外内存不够时，可能会内存溢出，导致后续的task需要从executor去拉取shuffle map output文件的时候发现executor已经挂掉了，关联的BlockManager也没有，那么就会报错
    - 设置方式 ```--conf spark.yarn.executor.memoryOverhead=2048```
    - 默认情况下是300M，但是在处理大数据量时可能会导致作业反复崩溃
- 调节executor连接等待时长
    - executor会优先从自己本地关联的BlockManager中获取数据，没有的话会通过TransferSerice去远程节点的executor的BlockManager拉取
    - 在拉取的过程中可能遇到对面的executor正在进行垃圾回收，这个时候BlockManager等线程都会停止工作，导致无法建立连接，默认60s后就会报失败
    - 设置方法： ```--conf spark.core.connection.ack.wait.timeout=300```
> 在处理大数据量（不是千万级别，一般是亿级别以上）的时候，上述两个参数可以发挥作用
```
spark-submit \
--class xxxx.WordCount \
--num-executor 80 \
--driver-memory 6g \
--executor-memory 6g \
--executor-cores 3
--master yarn-cluster \
--queue root.default \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf spark.core.connection.ack.wait.timeout=300 \
/usr/local/spark/spark.jar \
${1}
```
    
#### 3. shuffle调优
一般shuffle操作的性能操作会站到整个作业的50%-90%，10用来运行map操作，90%在shuffle操作上。
##### spark shuffle相关
- shuffle原理：
    - 每一个shuffle前半部分stage0的每个task会创建跟后半部分stage1的task数量相同的文件，加入stage1有100个task，那么stage0的每个task都会创建100个文件（task写入数据到磁盘文件前会先写入一个内存缓冲，满了再溢写到文件）
    - stage0的每个task都会把相同key的数据会存到同一个他们即将被stage1的同个task使用的同一个文件中，如下图
    - stage1的task读取他们所要消费的那个文件（比如两个节点上的文件x都会同一个task读取），然后通过HashMap在内存中缓存，并对key所对应的values执行逻辑操作
- spark的stage划分依据是是否需要进行shuffle，每一个shuffle都会产生2个stage（shuffle觉得stage）
> 某个action触发job时，DAGScheduler会划分job为多个stage，依据是是否有shuffle算子，比如reduceByKey，然后将这个操作前半部分的、之前所有RDD和转化操作划分为一个stage，shuffle后半部分以及直到action为止的所有RDD和转化操作划分为另一个stage

![image](https://github.com/fancyChuan/bigdata-project/blob/master/offline-shop/img/spark中的shuffle过程.png?raw=true)

##### 调优
- 合并map端输出文件
    - map端输出文件过多时，磁盘IO对性能影响很大，需要优化
    - 设置方法 conf.set("spark.shuffle.consolidateFiles", "true") 默认是不开启的
    - 这个时候只有第一次并行执行的task会去创建文件，后面的task就会复用前面创建出来的文件。比如一个executor需要跑4个task，有2个cpu core，那么只有前两个运行的task才会创建文件
    - 在数据量比较大的时候，这个调优方法效果还是很可观的
- 调节map端内存缓冲与reduce端内存占比
    - 每个map的内存缓冲区默认大小是32Kb，reduce端内存缓存占比是0.2
    - 当map端处理的数据较大的时，需要溢写到磁盘文件的次数就越多，磁盘IO就更频繁；同理，reduce端的缓冲空间较小时，也会溢写到磁盘，影响reduce端性能
    - conf.set("spark.shuffle.file.buffer", "64")
    - conf.set("spark.shuffle.memoryFraction","0.3")
- 使用SortShuffleManager
    - 1.2版本以后默认的shuffle manager是 SortShuffleManager 而不是 HashShuffleManager
    - 每个map任务会生成两个文件，一个是数据文件、一个是索引文件。（一共生成的文件数=MaxTaskNum*2）
        - 数据文件：按照key分区，不同分区之间排序，同一分区中的数据不排序
        - 索引文件：记录文件中每个分区的偏移量offset和范围
    - reduce任务先读取索引文件找到offset和范围，再从数据文件读取
    - 设置方法：conf.set("spark.shuffle.manager", "sort")
    - conf.set("spark.shuffle.sort.bypassMergeThreshold","200") 高级参数，默认reduce任务数小于等于200的时候，不会sort并且会把文件合并成一份，节省reduce拉取数据是磁盘IO的开销
> 1.5版本以后，出现了tungsten-sort，官网说效果跟sort差不多，区别在于tungsten-sort使用了自己实现的内存管理机制，性能有提升，也能避免shuffle过程中产生OOM，GC等异常
    
![image](https://github.com/fancyChuan/bigdata-project/blob/master/offline-shop/img/spark中的SortShuffle过程.png?raw=true)

#### 4. spark操作调优（算子调优）
- mapPartitions提升Map类操作性能
    - spark最基本的原则：每个task仅处理一个partition的数据
    - mapPartitions跟普通的map操作比如mapToPair的区别在于：mapPartitions(func)一次性把整个分区的数据（假设有一万条）传到函数func中处理，func只需要处理一次。而普通的map操作，func需要操作一万次
    - mapPartitions() 操作的优缺点：
        - 优点：一次性处理整个分区的数据，func只需要执行一次，性能较高
        - 缺点：数据量过大时，可能导致内存不够，造成OOM
- filter过后使用coalesce减少分区数量
    - 经过filter操作之后每个分区的数据量会出现不同程度的减少，可能到出现某个分区的数据过大，有些分区数据又太少。会出现：
        - filter之后的操作，也需要跟partition数据量一样的task，有点浪费task资源
        - 可能出现数据倾斜
    - 解决思路：减少分区的数量
    - coalesce算子，能够根据每个分区不同的数据量合理的减少分区的数量，让数据尽可能**紧凑均匀**分布在不同的分区
> local模式下，不需要去设置分区和并行度的数量，因为local模式是在进程内模拟出来的
- 使用foreackPartition优化写数据库性能
    - foreach的性能缺陷
        - 对每条数据都需要单独去调用一次function
        - 读写数据库时，每条数据也需要去创建数据库链接，都需要去发送sql语句
    - foreachPartition把一整个分区的数据传入task，调用一次function，创建一次数据库连接，一次发送sql语句。要注意分区数据量过大的时候可能会出现OOM
    