#!/bin/bash
JAVA_BIN=/usr/local/jdk/bin/java
APPNAME=shop-logger-0.0.1-SNAPSHOT.jar
case $1 in
  "start")
    {
      for i in hadoop102 hadoop103 hadoop101
      do
          echo "========: $i==============="
          ssh $i "$JAVA_BIN -Xms32m -Xmx64m -jar /home/appuser/forlearn/shop-flink/rt_applog/$APPNAME >/dev/null 2>&1 &"
          done
    };;

  "stop")
    {
      for i in hadoop102 hadoop103 hadoop101
      do
          echo "========: $i==============="
          ssh $i "ps -ef|grep $APPNAME | grep -v grep|awk '{print \$2}'| xargs kill" >/dev/null 2>&1
      done
    };;
esac