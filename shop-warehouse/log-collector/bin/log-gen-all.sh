#! /bin/bash
# 同时在两台机器上运行
for i in s01 s02
do
    ssh $i "java -classpath /opt/module/log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar com.atguigu.appclient.AppMain $1 $2 > /data/logs/test.log &"
done
