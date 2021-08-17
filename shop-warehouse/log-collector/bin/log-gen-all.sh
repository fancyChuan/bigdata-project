#! /bin/bash
# 同时在两台机器上运行
for i in hadoop102 hadoop103
do
    echo ---------生成日志
    ssh $i "java -classpath /home/appuser/forlearn/shop-wh/log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar cn.fancychuan.appclient.AppMain $1 $2  >/dev/null 2>&1 &"
done
