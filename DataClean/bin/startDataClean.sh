#!/bin/bash
# 需要在/etc/profile中配置FLINK_HOME
flink run -m yarn-cluster \
-d \
-yqu default \
-ynm DataCleanJob \
-yn 2 \
-ys 2 \
-yjm 1024 \
-ytm 1024 \
-c xuwei.tech.DataClean \
/data/soft/jars/DataClean/DataClean-1.0-SNAPSHOT-jar-with-dependencies.jar

