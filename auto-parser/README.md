## 变量自动化实时解析

基本流程：
- 监控mysql变量配置表    up_varialbes
- 更新变动信息到解析配置表 credit_var_input
- 访问mysql 获取row key
- 访问HBase，得到决策引擎的入参、出参数据
- spark解析变量，把解析结果写入hive
- 实时计算部分：
    - 车贷日志使用Flume推到Kafka
    - 从kafka消费数据，使用Spark Streaming解析
    
使用spark的原因：
- 单机解析时间过长，需要并行处理
- 
