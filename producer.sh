#!/bin/bash

WORKING_DIR=`cd $(dirname $0); pwd`

. ${WORKING_DIR}/environment.sh
MODULE=producer

start_producer() {
    start-wpm-module ${MODULE} producer.log;
}
case $1 in
    start)
        pid ${MODULE};
        if [ -z "${PID}" ]; then
            start_producer
        else
            echo "WPM module '${MODULE}' is already running. PID=${PID}"
        fi
        ;;
    stop)
        kill-wpm-module ${MODULE};
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
        start_producer
        ;;
    *)
        echo "Unknown command $1";
        echo "usage $0 [start|stop|status|restart]"
        ;;
esac

exit 0;
