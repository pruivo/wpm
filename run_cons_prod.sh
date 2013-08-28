#!/bin/bash

WORKING_DIR=`cd $(dirname $0); pwd`

${WORKING_DIR}/consumer.sh $1 && ${WORKING_DIR}/producer.sh $1

exit 0;
