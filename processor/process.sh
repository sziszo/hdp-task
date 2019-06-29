#!/usr/bin/env bash

if  [[ "$#" -ne 2 ]]; then
    echo "Usage: process.sh input_folder table_name";
    exit 1;
fi

spark-submit --jars /usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar,/usr/hdp/current/phoenix-client/phoenix-client.jar --files /etc/spark2/conf/hbase-site.xml --class com.example.ProcessorDriver processor-1.0-SNAPSHOT.jar $1 $2
