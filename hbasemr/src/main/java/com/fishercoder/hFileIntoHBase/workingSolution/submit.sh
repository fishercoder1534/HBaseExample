#!/usr/bin/env bash

#scp the jar into your cluster gateway/master node and run the following script from there

spark-submit --master yarn \
--deploy-mode client --driver-memory 16G \
--executor-memory 18G --num-executors 40 --executor-cores 2 \
--conf spark.shuffle.compress=true --conf spark.sql.shuffle.partitions=200 \
--driver-java-options '-Dcom.amazonaws.services.s3.enableV4' \
--class com.fishercoder.hFileIntoHBase.workingSolution.HFileGenerator \
--verbose hbasemr-1.0-SNAPSHOT-all.jar --local false --latestdatehour ".csv" \
--source sample-prod --hbaseTargetTable nuggets \
--hfile true --shards 160 --dataSourceFormat csv