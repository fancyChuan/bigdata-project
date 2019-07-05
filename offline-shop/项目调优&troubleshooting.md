### 一、性能调优
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
- 使用repartition解决sparkSQL并行度低的性能问题
    - 设置并行度的方法：
        - conf.set("spark.default.parallelism",200) 官方推荐：application总cpu核数的2-3倍
        - textFile(xxxx, 200) 在第2个参数也可以手动设置分区数 （比较少用）
    - 在spark SQL中无法设置，加入从hive读取数据，那么第1个stage是没有使用设置的并行度的，需要在第2个stage开始才会使用到通过spark.default.parallelism设置的并行度
        - 方法：使用repartition算子
- reduceByKey本地聚合
    - reduceByKey相对于普通的shuffle操作（比如groupByKey）的一个特点是：会进行map端的本地聚合（combiner操作）
    - 好处：下一个stage中，需要拉取的数据变少，需要shuffle的数据量也减少，下一阶段的计算量也减少

### 二、troubleshooting（解决线上故障）
- 控制shuffle reduce端缓冲大小以避免OOM
    - 默认缓冲大小是48M
    - reduce端不会等到map端全部处理完数据才开始去拉数据，而是map端写一点，reduce端拉取一点，能够拉取的大小由Buffer决定。但是数据量特别大的时候，每个reduce端每次拉取数据都拉满，加上代码本身创建的对象，就可能出现OOM
    - reduce端缓冲大小是把双刃剑：资源足够时，调大缓冲大小可以减少reduce端拉取数据的次数，加快任务速度。资源不够时，就需要减少缓冲大小，多拉取几次数据，避免OOM
- 解决JVM GC导致的shuffle文件拉取失败
    - GC发生时，reduce端去拉去map端的输出文件的时候就可能拉去不到，从而报shuffle file not found异常
    - 方法：
        - conf.set("spark.shuffle.io.maxRetries", 3) shuffle文件拉取失败时最多重试次数，默认3次
        - conf.set("spark.shuffle.io.retryWait", 5) shuffle文件拉取失败时重试拉取文件的时间间隔，默认是5s
        - 上面的两个参数都可以适当的调大，因为比起重启任务，等待JVM GC完成是可以忍受的
- 解决YARN队列资源不足导致的application失败
    - 现象：提交的任务需要的资源超过队列所拥有的的资源，会出现两种情况（具体出现哪种情况跟Hadoop版本、运维参数等有关系）：
        - YARN发现资源不足时，spark作业直接打印一行fail的log日志，直接失败
        - YARN发现资源不足时，作业就hang在那里，一直等待之前的作业完成，有足够的资源来执行
    - 解决方案：
        - 1.在J2EE平台限制每次只能提交一个作业，只允许有一个作业同时运行
        - 2.采用一种简单的调度区分方式，比如分配两个队列，一个专门用来运行需要较长时间才能跑完的spark作业，另一个队列则用来运行短时间的作业
        - 3.如果每次只允许一个作业运行，那么就应该把队列的资源用满，尽可能快速的跑完作业
> 生产环境，给spark使用的yarn队列的情况：500G 200个cpu core
- 解决各种序列化导致的报错
    - 可能报错的三种情况
        - 算子函数中，如果使用到外部自定义类型的变量，就需要该变量是可序列化的
        - 如果要将自定义的类型作为RDD的元素类型，那么该类型也需要是可序列化的，比如 JavaRDD<Integer, Student> 那么Student就需要可序列化
        - 不在前两种情况下，使用一些第三方的不支持序列化的类型 比如 Connection con = aRDD.foreach(...) 而Connection是不支持序列化的
- 解决算子函数返回null导致的问题
    - 在返回时，不返回null，比如用-99代替，之后通过filter过滤，再使用coalesce使分区更紧凑点
- 解决yarn-client模式导致的网卡流量激增问题
    - 产生原因：yarn-client提交作业后，driver是在本地机器上的，需要频繁的与各个executor通信（task的启动消息、task的执行统计消息、task的运行状态、shuffle的输出结果），通信消息特别多、通信频率特别高，就可能会导致本地的网卡流量激增
    - yarn-client的使用场景：一般只在测试环境中使用，只是偶尔使用，好处是：有详细的log日志，便于调试、开发、优化。实际在生产环境的话都是使用yarn-cluster模式
- 解决yarn-cluster模式的JVM栈内存溢出问题
    - 跟yarn-client的区别：
        - yarn-client是运行在本地机器上的，而yarn-cluster是运行在集群的某一个NodeManager上
        - yarn-client是本地机器负责spark作业的调度，所以网卡流量会激增
        - 本地模式通常跟集群不在同一个机房，yarn-cluster性能相对更好
    - 现象：
        - 运行一些包含有sparkSQL的作用的时候，可能会碰到yarn-client模式下，可以正常运行，但是在yarn-cluster可能无法提交运行，会报PermGen（永久代）的内存溢出OOM
        - 这是因为client模式下，使用的是spark客户端的默认配置，永久代的内存是128M。而cluster模式下是没有经过配置的，默认是82M
        - 所以在sparksql特别复杂，需要的内存超过82M而小于128M的时候就会出现上述问题
    - 解决方法：设置永久代的内存大小
        - spark-submit脚本中加上配置： ```--conf spark.driver.extraJavaOptions="-XX:PermSize=128M -XX:MaxPermSize=256M"```
> spark SQL 要注意：当有大量的or语句的时候，spark内部在处理or语句是递归的，超出JVM栈深度限制的话就会导致栈内存溢出
- 错误的持久化方式以及checkpoint的使用
    - userRDD.cache() 这样子使用是错误的，应该 userRDD = userRDD.cache()
    - 对RDD进行checkpoint的时候，会持久化一份数据到容错的文件系统上（比如HDFS）
        - 好处在于：cache失败（比如丢失数据）的时候，可以使用上checkpoint的数据而不用重新计算所有的过程，提高了作业的可靠性
        - 弊端在于：进行checkpoint的时候，需要写入HDFS是会消耗性能的
    - checkpoint原理
        - 1.在代码中，用SparkContext设置一个checkpoint目录，在容错文件系统上的一个目录
        - 2.在代码中，需要进行checkpoint的RDD，执行RDD.checkpoint()
        - 3.RDDCheckpointData（spark内部的API）会接管RDD，标记为marked for checkpoint，准备进行checkpoint
        - 4.job运行完，会调用一个finalRDD.doCheckpoint()方法，顺着rdd lineage，回溯扫描，发现标记为checkpoint的rdd会进行二次标记:inProcessCheckpoint，表示正在接受checkpoint操作
        - 5.job执行完之后，就会启动一个内部的新job，去将标识为inProcessCheckpoint的rdd数据都写入HDFS中（如果之前的RDD是cache过的，会从缓存中取数据，如果没有就重新计算在checkpoint）
        - 6.将checkpoint过的rdd之前的依赖rdd改成CheckpointRDD*，强制改变rdd的lineage，后面如果rdd的cache获取失败，就直接通过他上游的Checkpoint去HDFS获取checkpoint的数据
```
// 使用方法
sc.checkpointFile("hdfs://");
...
rdd.persist();
rdd.checkpoint();
```

### 三、数据倾斜
发生原因
- TODO：总结尽可能多的场景

问题定位思路：
- 1.到程序里找哪些地方会产生shuffle，比如groupByKey()算子、countByKey()、join等
- 2.看log里面哪行代码导致了OOM异常，或者任务执行到了那个stage，看那个stage的task执行得特别慢，就需要去查看对应的代码位置

解决方案：
- 1.聚合源数据
    - hive跟spark相比，是很适合在凌晨做离线计算，比如大规模加工处理（一般消耗的时间都比较长）
    - 所以对于导致数据倾斜的key，可以优先考虑是否可以在hive中先聚合处理。这样在spark就不需要做聚合操作了，也就不需要shuffle
    - 场景：
        - 1.把key跟其他字段凭借成一个字符串，这样可能一个key对只对应一条数据了  TODO：合理吗？这种方法
        - 2.把spark需要的聚合逻辑用hive实现，也就是用hive实现spark一样的功能，但是这样其实没有真正解决问题，而是把数据倾斜的问题移到了hive中
        - 3.还有一种情况，就是考虑考虑是否可以先放宽聚合的粒度。比如按城市、日期聚合。现在先只对城市聚合
- 2.过滤导致倾斜的key
    - 如果某个导致数据倾斜的key对业务没那么重要，不统计计算也没有太多影响，就可以考虑把这个key过滤掉
- 3.提高shuffle操作reduce并行度
    - 操作方法：使用shuffle相关算子比如countByKey、reduceByKey的时候，传入一个调节reduce并行度的参数
    - 治标不治本的方法，没有从根本上解决问题，只是尽可能缓解
    - 如果数据倾斜导致OOM，那么提高并行度以后不报OOM，能够运行，但是还是很慢，其实就不应该用这种方法了。也就是说OOM的数据倾斜，还是要选其他方法

- 4.使用随机Key实现双重聚合
    - 也就是给原来的key加上随机数，做聚合操作以后，需要再聚合一次完成统计目标
    - 使用场景：groupByKey/reduceByKey （join就不适合用这种方法）
- 5.将reduce join转为map join
    - 适用场景：其中一个rdd是比较小的，而且内存足够存放这个小RDD（注意spark中做map join是需要把小RDD做成广播变量的！跟hive的map join不一样）
    - 遇到数据倾斜的问题优先考虑这种方法
- 6.使用sample采样倾斜key单独进行join（两次join）
    - 把倾斜的key单独跟另一个RDD进行join的时候，能够把这个key分散到多个task跟，比起所有key去跟RDDjoin，更不容易发生数据倾斜
    - 不适用的场景：但导致数据倾斜的key特别多的时候，当初拉出来做join就得不偿失了，需要用下面第7种方式
    - 这种方式还有进一步优化：
        - 假如aRDD导致数据倾斜的key是 xx，过滤出xx所对应的数据xxRDD，那么把另一个RDD也过滤出xx假设为xx2RDD
        - xx2RDD假如只有一条数据，那么对这条数据分别把100个随机数作为前缀把数据扩散为100条。之后对xxRDD的key也分别加上100以内的随机数
        - 这样xxRDD.join(xx2RDD)也可以加快数据
- 7.使用随机数以及扩容表进行join
    - 把其中一个RDD进行扩容，比如扩大10倍：使用flatMap算子把每一条数据通过10个随机数扩大为10条
    - 这种方式只能说是缓解数据倾斜，并不能完全解决，
    - 局限性：因为两个RDD都可能比较大，所以不能扩大任意倍