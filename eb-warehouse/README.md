## 电商数仓项目


【需要思考的】
- 项目技术如何选型
    - 数据传输：**Flume、Kafka、Sqoop**、Logstash、DataX
    - 数据存储：**mysql、HDFS**、HBase、Redis、Mongodb
    - 数据计算：**Hive、Tez、Spark**、Flink、Storm
    - 数据查询（即席查询）：Presto、Druid、Impala、Kylin
- 框架版本如何选型
- 服务器使用物理机还是云主机
- 如何确认集群规模
> 这些都是一个架构师需要思考的事情



【选型】

框架 | Apache版本 | CDH版本（5.12.1）
--- | --- | ---
Hadoop | 2.7.2 | 2.6.0
Flume | 1.7.0 | 1.6.0 （实际用2.0以上）
Kafka | 0.11.02 | 
Kafka Manager | 1.3.3.22 | 
Hive | 1.2.1 | 1.1.0 
Sqoop | 1.4.6 | 1.4.6
Azkaban | 2.5.0 | 
Oozie |  | 4.1.0
Zookeeper | 3.4.10 | 
Presto | 0.189
Impala | | 2.9.0