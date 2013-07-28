#!/bin/bash

ENV_WORKING_DIR=`cd $(dirname $0); pwd`

CLASSPATH=${ENV_WORKING_DIR}:${ENV_WORKING_DIR}/lib/:${ENV_WORKING_DIR}/config
for jar in `ls ${ENV_WORKING_DIR}/lib/*.jar`; do
    CLASSPATH="${CLASSPATH}:${jar}"
done

EXEC_MAIN=eu.cloudtm.wpm.main.Main
D_VARS="-Djava.net.preferIPv4Stack=true"

append_d_var() {
    D_VARS="$D_VARS $1"
}
clean() {
    find ${ENV_WORKING_DIR}/log -type f -name '*.log' -exec rm -f {} \;
    find ${ENV_WORKING_DIR}/log -type f -name '*.ack' -exec rm -f {} \;
    find ${ENV_WORKING_DIR}/log -type f -name '*.zip' -exec rm -f {} \;
    find ${ENV_WORKING_DIR}/log -type f -name '*.check' -exec rm -f {} \;
    find ${ENV_WORKING_DIR}/log -type f -name '*.ready' -exec rm -f {} \;
    find ${ENV_WORKING_DIR}/log -type f -name '*.csv' -exec rm -f {} \;
    echo "Clean done!";
}

pid() {
    PID=`ps -fU $USER | grep ${EXEC_MAIN} | grep $1 | grep -v grep | awk '{print $2}'`;
}

kill-wpm-module() {
    pid $1;
    if [ -z "${PID}" ]; then
        echo "WPM module '$1' is not running";
    else
        echo "Killing WPM module '$1' with PID=${PID}";
        kill ${PID};
    fi
}

start-wpm-module() {
    echo "Starting WPM module '$1'";
    echo "# Started at $(date)" > $2;
    nohup java -cp ${CLASSPATH} ${D_VARS}  ${EXEC_MAIN} $1 >> $2 2>&1 &
    pid $1
    echo "Started WPM module '$1' with PID=${PID}. log file is $2"
}
