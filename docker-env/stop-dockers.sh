#!/bin/bash
docker kill master nifi slave1 slave2 slave3 hbase grafana rabbitmq influxdb
docker rm nifi master slave1 slave2 slave3 hbase grafana rabbitmq influxdb
docker network rm apache_network
