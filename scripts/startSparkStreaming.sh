#!/bin/bash

export HADOOP_CONF_DIR=/opt/software/hadoop-2.7.4/etc/hadoop

spark-submit --verbose --class es.streaming.kafka.StreamingKafkaConsumer --master local[*] --driver-memory 512m --executor-memory  512m /home/netkako/Proyectos/movies_tfm/movies/streaming-process/target/streaming-process-1.0-SNAPSHOT.jar
