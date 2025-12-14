#!/bin/bash

export SPARK_DIST_CLASSPATH=$(hadoop classpath)
export PYSPARK_PYTHON=/opt/miniconda/envs/2024/bin/python
export PYSPARK_DRIVER_PYTHON=/opt/miniconda/envs/2024/bin/python


sbt package
/opt/spark-3.4.3/bin/spark-submit \
        --conf spark.users_items.input_dir=/user/aleksandr.grachev/visits \
        --conf spark.users_items.output_dir=/user/aleksandr.grachev/users-items \
        --conf spark.users_items.update=0 \
        --class users_items \
         target/scala-2.12/users_items_2.12-1.0.jar 
