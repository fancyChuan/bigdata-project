#! /bin/bash
# 运行脚本
java -classpath /home/appuser/forlearn/shop-wh/log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar cn.fancychuan.appclient.AppMain $1 $2 >/dev/null 2>&1 &