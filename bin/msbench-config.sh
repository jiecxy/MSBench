#!/usr/bin/env bash
# Environment Variable Prerequisites
#
#   Do not set the variables in this script. Instead put them into a script
#   setenv.sh in MSB_HOME/bin to keep your customizations separate.
#
#   MSBENCH_HOME       (Optional) MSB installation directory.  If not set
#                   this script will use the parent directory of where this
#                   script is run from.
#
#
# included in all the MSBENCH scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# symlink and absolute path should rely on MSBENCH_HOME to resolve
if [ -z "${MSBENCH_HOME}" ]; then
    export MSBENCH_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

export MSBENCH_CONF_DIR="${MSBENCH_CONF_DIR:-"${MSBENCH_HOME}/conf"}"
#echo "msbench_conf_dir is $MSBENCH_CONF_DIR"
# Find binding information
#echo "get binding info: grep ^$SYS: $MSBENCH_HOME/bin/bindings.properties -m 1"
BINDING_LINE=$(grep "^$SYS:" "$MSBENCH_HOME/bin/bindings.properties" -m 1)

if [ -z "$BINDING_LINE" ] ; then
    echo "[ERROR] The specified binding '$2' was not found.  Exiting."
    exit 1;
fi

# Get binding name and class
BINDING_NAME=$(echo "$BINDING_LINE" | cut -d':' -f1)
BINDING_CLASS=$(echo "$BINDING_LINE" | cut -d':' -f2)

# Some bindings have multiple versions that are managed in the same directory.
#   They are noted with a '-' after the binding name.
#   (e.g. cassandra-7 & cassandra-8)
BINDING_DIR=$(echo "$BINDING_NAME" | cut -d'-' -f1)
#echo "Binding dir is $BINDING_DIR"

# The 'basic' binding is core functionality
if [ "$BINDING_NAME" = "basic" ] ; then
    BINDING_DIR=core
fi


# Add Top level conf to classpath
if [ -z "$CLASSPATH" ] ; then
    CLASSPATH="$MSBENCH_HOME/conf"
else
    CLASSPATH="$CLASSPATH:$MSBENCH_HOME/conf"
fi

# Core libraries
for f in "$MSBENCH_HOME"/lib/*.jar ; do
if [ -r "$f" ] ; then
    CLASSPATH="$CLASSPATH:$f"
fi
done

# Core libraries in core module
for f in "$MSBENCH_HOME"/core/lib/*.jar ; do
    if [ -r "$f" ] ; then
        CLASSPATH="$CLASSPATH:$f"
    fi
done

# Database conf dir
if [ -r "$MSBENCH_HOME"/"$BINDING_DIR"-binding/conf ] ; then
    CLASSPATH="$CLASSPATH:$MSBENCH_HOME/$BINDING_DIR/conf"
fi

# Database libraries
for f in "$MSBENCH_HOME"/"$BINDING_DIR"-binding/lib/*.jar ; do
    if [ -r "$f" ] ; then
        CLASSPATH="$CLASSPATH:$f"
    fi
done

#echo "the class path is $CLASSPATH"

export BINDING_NAME
export BINDING_CLASS
export BINDING_DIR
export CLASSPATH

