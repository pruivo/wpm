#!/bin/bash

WPM_CLASS_PATH=.:wpm.jar:lib/*:config
EXEC_MAIN=eu.cloudtm.wpm.main.Main
MODULE=producer

java -cp ${WPM_CLASS_PATH} -Djava.net.preferIPv4Stack=false ${EXEC_MAIN} ${MODULE}




