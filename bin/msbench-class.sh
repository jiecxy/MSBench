#!/usr/bin/env bash
#
#Start MSBench class,includes master and worker(writer or reader)
#
# Environment Variable Prerequisites
#
#   JAVA_HOME       (Optional) Must point at your Java Development Kit
#                   installation.  If empty, this script tries use the
#                   available java executable.
#
#   JAVA_OPTS       (Optional) Java runtime options used when any command
#                   is executed.
#
#first check binding name, then set classpath, final execu
#

usage="Usage: msbench-class.sh  <msbench-class> <args...>"

#load classpath and other conf
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


# About to run MSB
echo "$JAVA_HOME/bin/java $JAVA_OPTS -classpath $CLASSPATH $@"

# Run MSB
"$JAVA_HOME/bin/java" $JAVA_OPTS -classpath "$CLASSPATH" "$@"
