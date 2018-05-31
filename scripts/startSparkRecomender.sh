#!/bin/bash

export HADOOP_CONF_DIR=/opt/software/hadoop-2.7.4/etc/hadoop

spark-submit --verbose --class es.batch.process.movies.RecomenderMovie --master local[*] --driver-memory 6G --executor-memory 6G /home/netkako/Proyectos/movies_tfm/movies/batch-process/target/batch-process-1.0-SNAPSHOT.jar
