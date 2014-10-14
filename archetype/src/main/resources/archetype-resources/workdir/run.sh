#!/bin/bash

set -e

coordinator     --workerVmOptions "-ea -server -Xms2G -Xmx2G -XX:+PrintGC -XX:+HeapDumpOnOutOfMemoryError" \
                --clientHzFile      client-hazelcast.xml \
                --hzFile            conf/hazelcast.xml \
                --clientWorkerCount 2 \
                --memberWorkerCount 2 \
                --workerClassPath   '../target/*.jar' \
                --duration          5m \
                test.properties

provisioner --download

