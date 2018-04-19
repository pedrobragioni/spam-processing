#!/bin/bash

input_class=$1

for targ in "AR-01" "AT-01" "AU-01" "BR-01" "BR-02" "CL-01" "HK-01" "NL-01" \
   "NO-01" "US-02" "US-03" "UY-01"
do
   ~/spark/bin/spark-submit --master spark://master:7077 \
      --class $input_class \
      ~/spam-processing/target/scala-2.10/spam-processing_2.10-1.0.jar \
      hdfs://master:8022/spam_eng/$targ
done
