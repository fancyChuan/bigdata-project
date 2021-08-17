#!/bin/bash
# $1 日期
# $2 模拟生成数据的延续
# $3 模拟生成数据的条数

today=`date +%Y-%m-%d`
echo "----- 修改日期为：$1 ---------"
sudo date -s $1 ;

echo "----- 修改日期成功，开始模拟生成事件日志 ---------"
java -classpath /home/appuser/forlearn/shop-wh/log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar cn.fancychuan.appclient.AppMain $2 $3 >/dev/null 2>&1

echo "------ 模拟生成数据完成，将时间改回当前日期 ---------"
sudo date -s $today ;