## 日志生成模块

### 前置准备
使用Nginx配置负载均衡：[nginx.conf](conf/nginx.conf)

准备一个群起日志收集服务的脚本：[shop-logger.sh](bin/shop-logger.sh)
这个脚本放到 `/home/appuser/forlearn/shop-flink/rt_applog/bin` 下面

### 运行步骤
1、启动Nginx
```
/opt/modules/nginx/sbin/nginx
```
2、启动日志收集服务
```
# 到hadoop101 hadoop102 hadoop103三台机器上分别执行
java -jar /home/appuser/forlearn/shop-flink/rt_applog/shop-logger-0.0.1-SNAPSHOT.jar

# 也可以使用群起脚本
/home/appuser/forlearn/shop-flink/bin/shop-logger.sh start
```

3、启动模拟数据生成的程序
```
# 注意需要在 application.yml所在的目录执行下面的命令
java -jar /home/appuser/forlearn/shop-flink/rt_applog/gmall2020-mock-log-2020-12-18.jar
```

4、启动kafka消费者查看数据
```
kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic ods_base_log
```