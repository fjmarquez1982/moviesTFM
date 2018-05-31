#!/bin/bash
echo Script para arrancar procesos

echo 1 -- Arrancando zookeper......

cd /opt/software/kafka_2.11-0.10.2.1

bin/zookeeper-server-start.sh -daemon config/zookeeper.properties

sleep 1s

echo 2 -- Arrancando kafka server

bin/kafka-server-start.sh -daemon config/server.properties

sleep 5s

echo 3 -- Creando los topics

bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic valorations-topic --partitions 1 --replication-factor 1
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic movies-topic --partitions 1 --replication-factor 1
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic valorations-druid-topic --partitions 1 --replication-factor 1
sleep 1s

echo 4 -- Arrancando HDFS

start-dfs.sh

sleep 10s

echo 5 -- Arrancando Druid

cd /opt/software/druid-0.12.0
bin/coordinator.sh start
sleep 2s
bin/historical.sh start
sleep 2s
bin/broker.sh start
sleep 2s
bin/overlord.sh start
sleep 2s
bin/middleManager.sh start
sleep 2s

echo 6 -- Arrancando grafana
sudo /bin/systemctl start grafana-server

echo Procesos Arrancados

