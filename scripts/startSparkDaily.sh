#!/bin/bash

export HADOOP_CONF_DIR=/opt/software/hadoop-2.7.4/etc/hadoop

spark-submit --verbose --class es.batch.process.movies.BatchMovie --master local[*] --driver-memory 4G --executor-memory 4G /home/netkako/Proyectos/movies_tfm/movies/batch-process/target/batch-process-1.0-SNAPSHOT.jar DAILY

