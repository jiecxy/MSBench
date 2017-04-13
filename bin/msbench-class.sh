#!/usr/bin/env bash
#
#Start MSBench class,includes master and worker(writer or reader)
#
#
#
#first check binding name, then set classpath, final execu
#

usage="Usage: msbench-class.sh  <msbench-class> <args...>"

#load classpath and other conf,before call config.sh, pass the SYS to it
SYS=$1
shift
. "${MSBENCH_HOME}/bin/msbench-config.sh"


# Attempt to find the available JAVA, if JAVA_HOME not set
if [ -z "$JAVA_HOME" ]; then
  JAVA_PATH=$(which java 2>/dev/null)
  if [ "x$JAVA_PATH" != "x" ]; then
    JAVA_HOME=$(dirname "$(dirname "$JAVA_PATH" 2>/dev/null)")
  fi
fi

# If JAVA_HOME still not set, error
if [ -z "$JAVA_HOME" ]; then
  echo "[ERROR] Java executable not found. Exiting."
  exit 1;
fi

JAVA_OPTS="-Dmsbench.logs.dir=${MSBENCH_HOME}/logs -Dlog4j.configuration=file:${MSBENCH_HOME}/conf/log4j.properties"

# About to run MSB
#echo "$JAVA_HOME/bin/java $JAVA_OPTS -classpath '$CLASSPATH' $@"
#echo "$JAVA_HOME/bin/java $JAVA_OPTS -classpath '...' $@"

# Run MSB
"$JAVA_HOME/bin/java" $JAVA_OPTS -classpath "$CLASSPATH" "$@"
