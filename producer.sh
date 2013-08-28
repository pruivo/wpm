#!/bin/bash

WORKING_DIR=`cd $(dirname $0); pwd`

. ${WORKING_DIR}/environment.sh
MODULE=producer
LOG_FILE="producer.log"

start_producer() {
    start-wpm-module ${MODULE} ${LOG_FILE};
}
case $1 in
    start)
        pid ${MODULE};
        start_producer
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
        start_producer
        ;;
    clean)
        clean_log_files ${LOG_FILE}
        ;;
    *)
        echo "Unknown command $1";
        echo "usage $0 [start|stop|status|restart]"
        ;;
esac

exit 0;
