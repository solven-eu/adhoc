#!/bin/bash

if [ -z "${JAVA_HOME}" ];
then
  echo "\$JAVA_HOME is empty"
  # One may go for `JAVA_HOME=".../jdk-21.0.8+9-jre/"` to default the JRE_HOME
  exit 121
else
  echo "\$JAVA_HOME=$JAVA_HOME"
fi

# https://stackoverflow.com/questions/856881/how-to-activate-jmx-on-my-jvm-for-access-with-jconsole
# https://stackoverflow.com/questions/58696093/when-does-jvm-start-to-omit-stack-traces
# https://stackoverflow.com/questions/138511/what-are-java-command-line-options-to-set-to-allow-jvm-to-be-remotely-debugged

# a serverPort should end with 0
# Then the last digits is used for additional ports (debug, JMX, etc)
serverPort=8080

logFolder="./logs/"

# https://docs.spring.io/spring-boot/api/java/org/springframework/boot/context/ApplicationPidFileWriter.html
# The PID will be written in given file, enaling `shutdown` to kill based on PID
pidFile="-Dspring.pid.file=spring_${serverPort}.pid"

# -XX:HeapDumpPath=${logFolder}/heapdumps/
heapDumpOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${logFolder}/heapdumps/"

# %p will be replaced by PID
gcLogOptions="-Xlog:gc=debug:file=${logFolder}/gclogs/jvm_gc_%p.log"

# This scripts activate debug by default
echo "Java debug is open on port=${serverPort + 2}"
javaDebugOptions="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$((serverPort + 2))"

nohup $JAVA_HOME/bin/java \
  -Dserver.port=$serverPort \
  ${pidFile} \
  -Xmx8G -Xms8G \
  $heapDumpOptions  \
  -XX:-OmitStackTraceInFastThrow \
  $gcLogOptions \
  -Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.port=$((serverPort + 1)) \
  -Dcom.sun.management.jmxremote.rmi.port=$((serverPort + 1)) \
  -Dcom.sun.management.jmxremote.local.only=false \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  ${javaDebugOptions} \
  -jar 2-servers/server-with-adhoc-1.13.0-SNAPSHOT-fatjar.jar \
  > ${logFolder}/nohup_${serverPort}.log 2>&1 &