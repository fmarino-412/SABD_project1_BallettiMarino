#!/bin/bash
docker kill master nifi slave1 slave2 slave3 hbase influxdb grafana
docker rm nifi master slave1 slave2 slave3 hbase influxdb grafana
docker network rm apache_network
