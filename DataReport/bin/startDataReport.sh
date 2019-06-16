#!/bin/bash
flink run -m yarn-cluster \
-d \
-yqu default \
-ynm DataReportJob \
-yn 2 \
-ys 2 \
-yjm 1024 \
-ytm 1024 \
-c xuwei.tech.DataReport \
/data/soft/jars/DataReport/DataReport-1.0-SNAPSHOT-jar-with-dependencies.jar