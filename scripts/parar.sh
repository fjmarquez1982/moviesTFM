#!/bin/bash
echo Script para parar procesos

echo parando kafka server
cd /opt/software/kafka_2.11-0.10.2.1
bin/kafka-server-stop.sh

sleep 2s

echo parando HDFS

stop-dfs.sh

sleep 2s

echo parando Druid

cd /opt/software/druid-0.12.0
bin/coordinator.sh stop
sleep 2s
bin/historical.sh stop
sleep 2s
bin/broker.sh stop
sleep 2s
bin/overlord.sh stop
sleep 2s
bin/middleManager.sh stop
sleep 2s

echo parando zookeper......

cd /opt/software/kafka_2.11-0.10.2.1

bin/zookeeper-server-stop.sh
echo Procesos parados

