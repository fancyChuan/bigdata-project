#! /bin/bash

case $1 in
"start"){
        for i in hadoop102 hadoop103
        do
                echo " --------启动 $i 采集flume-------"
                ssh $i "nohup /usr/local/flume/bin/flume-ng agent --conf-file /home/appuser/forlearn/shop-wh/flumeconf --name a1 -Dflume.root.logger=INFO,LOGFILE >/home/appuser/forlearn/shop-wh/flumetest1 2>&1  &"
        done
};;
"stop"){
        for i in hadoop102 hadoop103
        do
                echo " --------停止 $i 采集flume-------"
                ssh $i "ps -ef | grep file-flume-kafka | grep -v grep |awk  '{print \$2}' | xargs kill"
        done

};;
esac
