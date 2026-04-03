#!/bin/bash

set -x

# a serverPort should end with 0
# Then the last digits is used for additional ports (debug, JMX, etc)
serverPort=8080

# Given ApplicationPidFileWriter, we have access to the running app through its PID in a file
# https://www.baeldung.com/spring-boot-shutdown
kill -9 `cat spring_${serverPort}.pid`