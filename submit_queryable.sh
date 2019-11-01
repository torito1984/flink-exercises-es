#!/bin/bash

FLINK_HOME=<YOUR FLINK LOCATION HERE>
JOBMANAGER_HOST=127.0.0.1:8081
JOB_MAIN=iot.basics.queryable.QueryableCustomerServiceJob
$FLINK_HOME/flink run -m $JOBMANAGER_HOST -c $JOB_MAIN ./target/flink-exercises-0.1.jar
