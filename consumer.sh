#!/bin/bash

WORKING_DIR=`cd $(dirname $0); pwd`

. ${WORKING_DIR}/environment.sh
MODULE=consumer

start_consumer() {
    clean
    append_d_var -Djavax.net.ssl.trustStore=config/serverkeys
    append_d_var -Djavax.net.ssl.trustStorePassword=cloudtm
    append_d_var -Djavax.net.ssl.keyStore=config/serverkeys
    append_d_var -Djavax.net.ssl.keyStorePassword=cloudtm
    start-wpm-module ${MODULE} consumer.log;
}
case $1 in
    start)
        pid ${MODULE};
        if [ -z "${PID}" ]; then
            start_consumer
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
        start_consumer
        ;;
    *)
        echo "Unknown command $1";
        echo "usage $0 [start|stop|status|restart]"
        ;;
esac

exit 0;
