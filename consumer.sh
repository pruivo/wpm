#!/bin/bash

WORKING_DIR=`cd $(dirname $0); pwd`

. ${WORKING_DIR}/environment.sh
MODULE=consumer
LOG_FILE="consumer.log"

start_consumer() {
    clean
    append_d_var -Djavax.net.ssl.trustStore=config/serverkeys
    append_d_var -Djavax.net.ssl.trustStorePassword=cloudtm
    append_d_var -Djavax.net.ssl.keyStore=config/serverkeys
    append_d_var -Djavax.net.ssl.keyStorePassword=cloudtm
    start-wpm-module ${MODULE} ${LOG_FILE};
}
case $1 in
    start)
        start_consumer
        ;;
    stop)
        kill-wpm-module ${MODULE} $2;
        ;;
    status)
        pid ${MODULE};
        if [ -z "${PID}" ]; then
            echo "WPM module '${MODULE}' is not running";
        else
            echo "WPM module '${MODULE}' is running. PID=${PID}"
        fi
        ;;
    restart)
        kill-wpm-module ${MODULE};
        sleep 2s
        clean_log_files ${LOG_FILE}
        start_consumer
        ;;
    clean)
        clean_log_files ${LOG_FILE}
        ;;
    *)
        echo "Unknown command $1";
        echo "usage $0 [start|stop|status|restart|clean]"
        ;;
esac

exit 0;
