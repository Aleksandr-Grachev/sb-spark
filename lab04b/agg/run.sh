#!/bin/bash

export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export PYSPARK_PYTHON=/opt/miniconda/envs/2024/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/miniconda/envs/2024/bin/python


sbt package

/opt/spark-3.4.3/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3 \
    --class agg target/scala-2.12/agg_2.12-1.0.jar 