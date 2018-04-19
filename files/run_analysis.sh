#!/bin/bash

result_path="/home/cluster/spam-processing/results"

#for targ in "AR-01" "AT-01" "AU-01" "BR-01" "BR-02" "CL-01" "HK-01" "NL-01" "NO-01" "US-02" "US-03" "UY-01";
for targ in "NO-01"
do
   #for c in "CountMessages" "CountIPs" "CountASes" "CountCCs" "CountURLs" "CountFrequentWords" "CountAttach"
   for c in "Top10"
   do
      ~/spark/bin/spark-submit --master spark://master:7077 \
         --jars ~/spam-processing/project/lib/jsoup-1.8.3.jar \
         --class $c \
         ~/spam-processing/target/scala-2.10/spam-processing_2.10-1.0.jar \
         hdfs://master:8022/spam_eng/$targ
   done

   #~/hadoop/bin/hdfs dfs -getmerge /$targ.msgs_protocol $result_path/$targ/msgs_protocol
   #~/hadoop/bin/hdfs dfs -getmerge /$targ.ips_protocol $result_path/$targ/ips_protocol
   #~/hadoop/bin/hdfs dfs -getmerge /$targ.ases_protocol $result_path/$targ/ases_protocol
   #~/hadoop/bin/hdfs dfs -getmerge /$targ.ccs_protocol $result_path/$targ/ccs_protocol

   #~/hadoop/bin/hdfs dfs -getmerge /$targ.attach $result_path/$targ/attach
   #~/hadoop/bin/hdfs dfs -getmerge /$targ.freqwords $result_path/$targ/freqwords.unique
   #~/hadoop/bin/hdfs dfs -getmerge /$targ.http_ips $result_path/$targ/http_ips
   #~/hadoop/bin/hdfs dfs -getmerge /$targ.urls $result_path/$targ/urls

   #~/hadoop/bin/hdfs dfs -getmerge /$targ.octet_ips_protocol $result_path/$targ/octet_ips_protocol
   #~/hadoop/bin/hdfs dfs -getmerge /$targ.octet_msgs_protocol $result_path/$targ/octet_msgs_protocol 
   #~/hadoop/bin/hdfs dfs -getmerge /$targ.octet_ases_protocol $result_path/$targ/octet_ases_protocol 
   #~/hadoop/bin/hdfs dfs -getmerge /$targ.octet_ccs_protocol $result_path/$targ/octet_ccs_protocol 

done
