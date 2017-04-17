#!/usr/bin/env bash
#
# FILE: msbench.sh
#
# DESCRIPTION: Start a messaging system benchmark work.
#

__ScriptName=$(basename $0)
__BaseDir=$(cd $(dirname $0)"/.."; pwd)

#-----------------------------------------------------------------------
# FUNCTION: usage
# DESCRIPTION:  Display usage information.
#-----------------------------------------------------------------------
usage() {
    cat << EOT
Usage :  ${__ScriptName} [OPTIONS]
  Start a MSBench test work from given options.

Options:
  -h                    [ REQUIRED ] Display this message
  -sys                  [ REQUIRED ] System system to be test (include basic | kafka | dl | pulsar). eg: -sys basic
  -sn                   [ REQUIRED ] Number of streams. eg: -sn 1
  -name                 [ REQUIRED ] Name prefix of streams. eg: -name test
  -tr                   [ REQUIRED ] Run time (seconds). eg: -tr 10
  -hosts                [ REQUIRED ] Indicate the available host name. eg: -hosts 1.1.1.1,2.2.2.2

  -w                    Number of writer per stream. eg: -w 1
  -wcf                  Config file name for writer of specific message system client, it should be put in ${__BaseDir}/conf/. eg: -wcf writer.conf
  -ms                   Message Size (Byte). eg: -ms 1024
  -sync                 Indicate write mode is sync, if not set, default is Async.
  -tp                   Initial throughput (messages/sec).
  -ftp                  Final throughput (messages/sec).
  -ctp                  Throughput change every interval (messages/sec).
  -ctps                 Change interval (seconds).
  -rtpl                 Random throughput list. Throughput periodically change by the list.

            Writer has four throughput strategies:
                1. NoLimit. eg: -tp -1
                2. Constant. eg: -tp 1000
                3. GradualChange. eg: -tp 1000 -ftp 2000 -ctp 100 -ctps 5
                4. GivenRandomChangeList. eg: -rtpl 1000,3000,2000,5000 -ctps 5

  -r                    Number of reader per stream. eg: -r 1
  -from                 Mode of read. 0: read from head, -1: read from tail. eg: -from 0
  -d                    The delay time (seconds) before reader to start, run time for reader is (tr - d), if not set, default is 0. eg: -d 10
  -rcf                  Config file name for reader of specific message system client, it should be put in ${__BaseDir}/conf/. eg: -rcf writer.conf

Example:
    $ ./$__ScriptName  -sys basic -sn 1 -name topic -w 1 -sync -r 2 -from -1 -ms 100 -tp 1000 -tr 1800 -hosts node_a,node_b -rcf read.config -wcf write.config
EOT
}   # ----------  end of function usage  ----------

if [ $# -eq 0 ]; then usage; exit 1; fi
SYS=
PREFIX=
NUMBER=
SYNC=""
SIZE=
DURATION=
W=
R=
RMODE=
TP=
FTP=
CTP=
CTPS=
RTPL=
HOSTS=
RCF=
WCF=
D=

#set user and passwd used by expect
if [ -z "${MSBENCH_HOME}" ]; then
  export MSBENCH_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

while [ "$1" != "" ]; do
  #echo "the arg remains $#, first is $1"
  case $1 in
    -sys)
      if [ -n "$2" ]; then
        SYS="$2"
        shift 2
        continue
      else
        echo "ERROR: '-sys' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -sn)
      if [ -n "$2" ]; then
        NUMBER="$2"
        shift 2
        continue
      else
        echo "ERROR: '-sn' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -name)
      if [ -n "$2" ]; then
        PREFIX="$2"
        shift 2
        continue
      else
        echo "ERROR: '-name' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -tp)
      if [ -n "$2" ]; then
        TP="-tp $2"
        shift 2
        continue
      else
        echo "ERROR: '-tp' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -ftp)
      if [ -n "$2" ]; then
          FTP="-ftp $2"
          shift 2
          continue
      fi
      ;;
    -ctp)
      if [ -n "$2" ]; then
          CTP="-ctp $2"
          shift 2
          continue
      fi
      ;;
    -ctps)
      if [ -n "$2" ]; then
          CTPS="-ctps $2"
          shift 2
          continue
      fi
      ;;
    -rtpl)
      if [ -n "$2" ]; then
          RTPL="-rtpl $2"
          shift 2
          continue
      fi
      ;;
    -sync)
        SYNC="-sync"
        shift
        continue
        ;;
    -ms)
      if [ -n "$2" ]; then
        SIZE="$2"
        shift 2
        continue
      else
        echo "ERROR: '-ms' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -r)
      if [ -n "$2" ]; then
        R="$2"
        shift 2
        continue
      else
        echo "ERROR: '-r' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -w)
      if [ -n "$2" ]; then
        W="$2"
        shift 2
        continue
      else
        echo "ERROR: '-w' requires a non-empty option argument."
        exit 1
      fi
      ;;
     -from)
      if [ -n "$2" ]; then
        RMODE="$2"
        shift 2
        continue
      else
        echo "ERROR: '-from' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -tr)
      if [ -n "$2" ]; then
        DURATION="$2"
        shift 2
        continue
      else
        echo "ERROR: '-tr' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -hosts)
      if [ -n "$2" ]; then
        HOSTS="$2"
        shift 2
        continue
      else
        echo "ERROR: '-name' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -w)
      if [ -n "$2" ]; then
        W="$2"
        shift 2
        continue
      else
        echo "ERROR: '-w' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -d)
      if [ -n "$2" ]; then
        D="$2"
        shift 2
        continue
      else
        echo "ERROR: '-d' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -rcf)
      if [ -n "$2" ]; then
        RCF="$2"
        shift 2
        continue
      else
        echo "ERROR: '-rcf' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -wcf)
      if [ -n "$2" ]; then
        WCF="$2"
        shift 2
        continue
      else
        echo "ERROR: '-wcf' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -h|-\?|--help)
      usage; exit 1;;
    --)              # End of all options.
      shift
      break
      ;;
    -?*)
      echo "WARN: Unknown option (ignored): $1"
      ;;
    *)
      echo "break out the loop"
      break
  esac
  shift
done

# arguments w r
if [ -z $W ] && [ -z $R ]; then
    echo "ERROR: -w or -r is required!"
    exit
fi

# argument hosts
if [ -z $HOSTS ]; then
    echo "ERROR: -hosts is required!"
    exit
fi

# argument tr
if [ -z $DURATION ]; then
    echo "ERROR: -tr is required!"
    exit
fi

READER_DURATION=${DURATION}
# argument d
if [ ! -z $D ]; then
    if [ ! $(expr ${DURATION} - ${D}) -gt 0 ]; then
        echo "ERROR: -tr should be greater than -d"
        exit
    else
        READER_DURATION=$(expr ${DURATION} - ${D})
    fi
fi

. "${MSBENCH_HOME}/bin/msbench-config.sh"


MASTERCLASS="cn.ac.ict.msbench.MSBClient"

IPLIST=${HOSTS//,/' '}
HOSTNUM=$(echo $IPLIST | wc -w)

# select a master to run
# if local ip is in the hosts list, select local machine as master, else random select one
LOCALIP=$(hostname -i)
LOCALNAME=$(hostname)
MASTERIP=127.0.0.1
location=$(($RANDOM % $HOSTNUM))
i=0
for ip in $IPLIST
do
    if [ $i -eq $location ]; then
        MASTERIP=$ip
    fi
    if [ "$ip"x = "$LOCALIP"x -o "$ip"x = "$LOCALNAME"x ]; then
        MASTERIP=$LOCALIP
        break
    fi
    i=$(($i+1))
done

# Relative conf path
RWCF=${WCF}
RRCF=${RCF}
# record the conf file absolute path
WCF=${MSBENCH_CONF_DIR}/${WCF}
RCF=${MSBENCH_CONF_DIR}/${RCF}
# echo "msbench reader config path is ${RCF}"
# todo execute other shell file in shell file internal

if [ "$MSBENCH_MASTER_PORT" = "" ]; then
    MSBENCH_MASTER_PORT=6789
fi

JAVA_OPTS="-Dmsbench.logs.dir=${MSBENCH_HOME}/logs -Dlog4j.configuration=file:${MSBENCH_HOME}/conf/log4j.properties"


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


# By default disable strict host key checking
if [ "$MSBENCH_SSH_OPTS" = "" ]; then
  MSBENCH_SSH_OPTS="-o StrictHostKeyChecking=no"
fi
# Start Master
#   -tr 1000 -M 1.1.1.1:9999 -P master -w 1 -r 1 -sn 1 -name topic

CMD=
# About to run MSB Master
if [ "$LOCALIP" = "$MASTERIP" ]; then
    CMD="${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS  -home ${MSBENCH_HOME} -tr $DURATION -M ${MASTERIP}:${MSBENCH_MASTER_PORT} -P master -sn $NUMBER -name $PREFIX"
else
    #expect ssh.exp $USER $PASSWD $MASTERIP
    CMD="ssh $MSBENCH_SSH_OPTS $MASTERIP source .bash_profile; \${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -home ${MSBENCH_HOME} -tr $DURATION \
        -M ${MASTERIP}:${MSBENCH_MASTER_PORT} -P master -sn $NUMBER -name $PREFIX"
fi
if [ ! -z $W ]; then
    CMD=${CMD}" -w $W"
fi
if [ ! -z $R ]; then
    CMD=${CMD}" -r $R"
fi
##########################################################################################
#echo $CMD
$CMD &


# Start Workers
#
IPARRY=
i=0
for IP in $IPLIST; do
    IPARRY[$i]=$IP
    if [ $IP != $LOCALIP ]; then
        if [ ! -z $RWCF ]; then
            ssh $MSBENCH_SSH_OPTS "$IP" "source .bash_profile; scp $LOCALIP:${WCF} \${MSBENCH_HOME}/conf"
        fi
        if [ ! -z $RRCF ]; then
            ssh $MSBENCH_SSH_OPTS "$IP" "source .bash_profile; scp $LOCALIP:${RCF} \${MSBENCH_HOME}/conf"
        fi
    fi
    i=$(($i+1))
done

IPSIZE=${#IPARRY[@]}
j=0
while [ $j -lt $NUMBER ]; do
    if [ ! -z $W ]; then
        i=0
        while [ $i -lt $W ]; do
            #use module to choose ip
            IP=${IPARRY[$((${i}%${IPSIZE}))]}
            #use ssh in remote host
            CMD=
            if [ $IP != $LOCALIP ]; then
                CMD="ssh $MSBENCH_SSH_OPTS $IP source .bash_profile; \${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -tr $DURATION -M ${MASTERIP}:${MSBENCH_MASTER_PORT} \
                        -home \${MSBENCH_HOME} -P writer -W $IP -sys $BINDING_CLASS -sname ${PREFIX}$j -ms $SIZE  $TP $FTP $CTP $CTPS $RTPL $SYNC"
                if [ ! -z $RWCF ]; then
                    CMD=${CMD}" -cf \${MSBENCH_HOME}/conf/${RWCF}"
                fi
            else
            #echo "start writer in local machine"
                CMD="${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -tr $DURATION -M ${MASTERIP}:${MSBENCH_MASTER_PORT} -home ${MSBENCH_HOME} -P writer -W $IP \
                        -sys $BINDING_CLASS -sname ${PREFIX}$j -ms $SIZE $TP $FTP $CTP $CTPS $RTPL $SYNC"
                if [ ! -z $RWCF ]; then
                    CMD=${CMD}" -cf ${WCF}"
                fi
            fi
            ##########################################################################################
            #echo $CMD
            $CMD &
            i=$(($i+1))
        done
    fi

    if [ ! -z $R ]; then
        i=0
        while [ $i -lt $R ]; do
            #assign from the end
            LOCATE=$((${IPSIZE}-1-$((${i}%${IPSIZE}))))
            IP=${IPARRY[$LOCATE]}
            #use ssh in remote host
            CMD=
            if [ $IP != $LOCALIP ]; then
                CMD="ssh $MSBENCH_SSH_OPTS $IP source .bash_profile; \${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -home ${MSBENCH_HOME} -tr $READER_DURATION \
                    -M ${MASTERIP}:${MSBENCH_MASTER_PORT} -P reader -W $IP -sys $BINDING_CLASS -sname ${PREFIX}$j -from $RMODE"
                if [ ! -z $RRCF ]; then
                    CMD=${CMD}" -cf \${MSBENCH_HOME}/conf/${RRCF}"
                fi
            else
                #echo "start reader in local machine"
                CMD="${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -home ${MSBENCH_HOME} -tr $READER_DURATION -M ${MASTERIP}:${MSBENCH_MASTER_PORT} -P reader -W $IP \
                    -sys $BINDING_CLASS -sname ${PREFIX}$j -from $RMODE"
                if [ ! -z $RRCF ]; then
                    CMD=${CMD}" -cf ${RCF}"
                fi
            fi
            if [ ! -z $D ]; then
                CMD=${CMD}" -d ${D}"
            fi
            ##########################################################################################
            #echo $CMD
            $CMD &
            i=$(($i+1))
        done
    fi
    j=$(($j+1))
done


