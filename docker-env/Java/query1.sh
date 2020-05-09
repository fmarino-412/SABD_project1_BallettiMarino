#!/bin/bash
docker run --rm --network=apache_network -v "$PWD":/usr/src/sabd -w /usr/src/sabd java:8-jdk-alpine java -jar SABD_project1_BallettiMarino-1.0.jar