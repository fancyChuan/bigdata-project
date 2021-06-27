## 大数据在线教育项目


### 实时需求
创建需要的topic
```
kafka-topics.sh --zookeeper hadoop102:2181/kafka --create --replication-factor 2 --partitions 6  --topic edu_qz_log
kafka-topics.sh --zookeeper hadoop102:2181/kafka --create --replication-factor 2 --partitions 6  --topic edu_page_topic
kafka-topics.sh --zookeeper hadoop102:2181/kafka --create --replication-factor 2 --partitions 6  --topic edu_register_topic
kafka-topics.sh --zookeeper hadoop102:2181/kafka --create --replication-factor 2 --partitions 6  --topic edu_course_learn


```