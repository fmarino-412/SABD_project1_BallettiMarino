#!/bin/bash
docker network create --driver bridge apache_network
docker run -p 9880:9880 -d --network=apache_network -v nifi_conf:/opt/conf -v nifi_state:/data/nifi/state -e NIFI_WEB_HTTP_PORT='9880' --name=nifi apache/nifi
docker run -t -i -p 9864:9864 -d --network=apache_network --name=slave1 effeerre/hadoop
docker run -t -i -p 9863:9864 -d --network=apache_network --name=slave2 effeerre/hadoop
docker run -t -i -p 9862:9864 -d --network=apache_network --name=slave3 effeerre/hadoop
docker run -t -i -p 9870:9870 --network=apache_network --name=master effeerre/hadoop
