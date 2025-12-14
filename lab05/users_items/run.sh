#!/bin/bash

xport SPARK_DIST_CLASSPATH=$(hadoop classpath)
export PYSPARK_PYTHON=/opt/miniconda/envs/2024/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/miniconda/envs/2024/bin/python


sbt package
/opt/spark-3.4.3/bin/spark-submit --class users_items target/scala-2.12/users_items_2.12-1.0.jar 
