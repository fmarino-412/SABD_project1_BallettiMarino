#!/bin/bash
docker kill master nifi slave1 slave2 slave3
docker rm nifi master slave1 slave2 slave3
docker network rm apache_network
