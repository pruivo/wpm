#!/bin/bash

WORKING_DIR=`cd $(dirname $0); pwd`

. ${WORKING_DIR}/environment.sh
MODULE=logService

start_log_service() {
    clean
    append_d_var -Djavax.net.ssl.trustStore=config/serverkeys
    append_d_var -Djavax.net.ssl.trustStorePassword=cloudtm
    append_d_var -Djavax.net.ssl.keyStore=config/serverkeys
    append_d_var -Djavax.net.ssl.keyStorePassword=cloudtm
    append_d_var -Dbind.address=127.0.0.1
    start-wpm-module ${MODULE} logService.log;
}
case $1 in
    start)
        pid ${MODULE};
        if [ -z "${PID}" ]; then
            start_log_service
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
        start_log_service
        ;;
    *)
        echo "Unknown command $1";
        echo "usage $0 [start|stop|status|restart]"
        ;;
esac

exit 0;
