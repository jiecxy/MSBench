#!/usr/bin/env bash
#
# FILE: msbench.sh
#
# DESCRIPTION: Start a messaging system benchmark work.
#
#
#
#   WARNING!!! MSBench home must be located in a directory path that doesn't
#   contain spaces.
#
#        www.shellcheck.net was used to validate this script
#
#
#Args：
#-sys: 要测试的系统，需要提供用户自己根据具体系统实现MS接口的类路径
#-tr: 测试时间长度（单位：秒）
#-hosts：client所在节点列表
#-cf: 需要传入各个系统客户端的配置文件(config file)
#
#-sn：流的数量
#-name: 流名字的前缀
#
#-w: 每个流的writer进程数量
#-ms: 每条消息大小
#-tp: 写入速度(每秒写入多少条消息)，初始速率，若未设置下面用于变速的参数则一直为此恒定速率；当为-1时则不限定速率，其他变化参数将冲突
#-ftp: 变速下最后的达到速率(final tp)
#-ctp: 每次变化多大的速率(change tp)
#-ctps: 每隔多久变化一次，单位秒(change tp seconds)
#-rtpl: 指定速率变化的list，速率按照这个list周期变化(random tp list)，例-rtpl  10,20,15,30,1；此参数也需要配合cpts参数；与其他变化参数将冲突
#-sync: 同步，默认为异步
#注：速率控制和同步异步暂时只针对writer。速率控制策略有：恒定速率，不限速率，递增/减速率，随机变速。
#
#-r: 每个流的reader进程数量
#-from：0从头读；
#-1从尾读，即catchup read；
#-2不同进程从不同地方开始读（一个读，一个写，轮转）
#
# ./msbench.sh -sys [ basic | kafka | dl | pulsar] -sn 1 -name topic -w 1 -sync -r 2 -from -1 -ms 100 -tp 1000 -tr 1800 -hosts node_a,node_b
# -rcf read.config -wcf write.config -d delay

__ScriptVersion="2017.03.26"
__ScriptName="msbench.sh"

#-----------------------------------------------------------------------
# FUNCTION: usage
# DESCRIPTION:  Display usage information.
#-----------------------------------------------------------------------
usage() {
    cat << EOT

Usage :  ${__ScriptName} CFGFILE [OPTION] ...
  Start a MSBench test work from given options.

Options:
  -h, --help                    Display this message
  -V, --version                 Display script version
  -v, --verbose
  -sys, --sys=SYSTEM            System name to be test(include sample |  kafka | dl | pulsar)
  -sn, --streamnumber=NUMBER    Number of streams
  -name, --stream-prefix=PREFIX Name prefix of streams
  -w, --writer=NUMBER           Number of writer per stream
  -r, --reader=NUMBER           Number of reader per stream
  -from, --readmode=MODE        Mode of read

Exit status:
  0   if OK,
  !=0 if serious problems.

Example:
    $ ./$__ScriptName  -sys [ basic | kafka | dl | pulsar ] -sn 1 -name topic -w 1 -sync -r 2 -from -1 -ms 100
    -tp 1000 -tr 1800 -hosts node_a,node_b -rcf read.config -wcf write.config

Report bugs to yaoguangzhong@ict.ac.cn

EOT
}   # ----------  end of function usage  ----------

if [ $# -eq 0 ]; then usage; exit 1; fi
SYS=
PREFIX=
NUMBER=
SYNC=""
SIZE=
VERBOSE=false
DURATION=
W=
R=
RMODE=
VELOCITY=
VMODE=
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

#todo long options with '='
# set option values
#note, ctps arg should following tp or rtpl,use VMODE{-1,-2,-3,-4} to indicate four write mode
while [ "$1" != "" ]; do
  #echo "the arg remains $#, first is $1"
  case $1 in
    -sys|--system)
      if [ -n "$2" ]; then
        SYS="$2"
        shift 2
        continue
      else
        echo "ERROR: '-sys' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -sn|--stream-num)
      if [ -n "$2" ]; then
        NUMBER="$2"
        shift 2
        continue
      else
        echo "ERROR: '-sn' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -name|--prefix)
      if [ -n "$2" ]; then
        PREFIX="$2"
        shift 2
        continue
      else
        echo "ERROR: '-name' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -tp|--write-velocity)
      if [ -n "$2" ]; then
        if [ $2 -lt 0 ]; then
        VMODE=-1
        else
        VMODE=-2
        fi
        VELOCITY="$2"
        shift 2
        continue
      else
        echo "ERROR: '-tp' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -ftp)
      if [ -n "$2" ]; then
      VMODE=-3
      FTP="$2"
      shift 2
      continue
      fi
      ;;
    -ctp)
      if [ -n "$2" ]; then
      VMODE=-3
      CTP="$2"
      shift 2
      continue
      fi
      ;;
    -ctps)
      if [ -n "$2" ]; then
      if [ -n "$TP" ]; then
      VMODE=-3
      else
      VMODE=-4
      fi
      CTPS="$2"
      shift 2
      continue
      fi
      ;;
    -rtpl)
      if [ -n "$2" ]; then
      VMODE=-4
      RTPL="$2"
      shift 2
      continue
      fi
      ;;
    -sync)
        SYNC="-sync"
        shift
        continue
        ;;
    -ms|--message-size)
      if [ -n "$2" ]; then
        SIZE="$2"
        shift 2
        continue
      else
        echo "ERROR: '-ms' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -r|--reader-num)
#      echo "Info: '-r' set to $2."
      if [ -n "$2" ]; then
        R="$2"
        shift 2
        continue
      else
        echo "ERROR: '-r' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -w|--writer-num)
      if [ -n "$2" ]; then
#        echo "Info: '-w' set to $2."
        W="$2"
        shift 2
        continue
      else
        echo "ERROR: '-w' requires a non-empty option argument."
        exit 1
      fi
      ;;
     -from|--location)
      if [ -n "$2" ]; then
        RMODE="$2"
        shift 2
        continue
      else
        echo "ERROR: '-from' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -tr|--duration)
      if [ -n "$2" ]; then
        DURATION="$2"
        shift 2
        continue
      else
        echo "ERROR: '-tr' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -hosts|--hosts)
      if [ -n "$2" ]; then
        HOSTS="$2"
        shift 2
        continue
      else
        echo "ERROR: '-name' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -w|--writer-num)
      if [ -n "$2" ]; then
        W="$2"
        shift 2
        continue
      else
        echo "ERROR: '-w' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -d|--delay)
      if [ -n "$2" ]; then
        D="$2"
        shift 2
        continue
      else
        echo "ERROR: '-d' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -rcf|--reader-config)
      if [ -n "$2" ]; then
        RCF="$2"
        shift 2
        continue
      else
        echo "ERROR: '-rcf' requires a non-empty option argument."
        exit 1
      fi
      ;;
    -wcf|--writer-config)
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
    -v | --verbose )
        VERBOSE=true;
        shift
        ;;
    -V | --version )
      echoinfo "$(basename "$0") -- version $__ScriptVersion";
      exit 1;;

    --)              # End of all options.
      shift
      break
      ;;
    -?*)
      echo "WARN: Unknown option (ignored): $1"
      ;;
    *)               # Default case: If no more options then break out of the loop. 全角字符在这里响应
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

# argument d
if [ ! -z $D ]; then
    READER_DURATION=${DURATION}
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
MASTERIP=127.0.0.1
location=$(($RANDOM % $HOSTNUM))
i=0
for ip in $IPLIST
do
    if [ $i -eq $location ]; then
        MASTERIP=$ip
    fi
    if [ "$ip"x = "$LOCALIP"x ]; then
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
                if [ $VMODE -eq -1 ]; then
                    CMD="ssh $MSBENCH_SSH_OPTS $IP source .bash_profile; \${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -tr $DURATION -M ${MASTERIP}:${MSBENCH_MASTER_PORT} \
                        -home \${MSBENCH_HOME} -P writer -W $IP -sys $BINDING_CLASS -sname ${PREFIX}$j -ms $SIZE  -tp -1 $SYNC"
                elif [ $VMODE -eq -2 ]; then
                    CMD="ssh $MSBENCH_SSH_OPTS $IP source .bash_profile; \${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -tr $DURATION -M ${MASTERIP}:${MSBENCH_MASTER_PORT}  \
                        -home \${MSBENCH_HOME} -P writer -W $IP -sys $BINDING_CLASS -sname ${PREFIX}$j -ms $SIZE  -tp $VELOCITY $SYNC"
                elif [ $VMODE -eq -3 ]; then
                    CMD="ssh $MSBENCH_SSH_OPTS $IP source .bash_profile; \${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -tr $DURATION -M ${MASTERIP}:${MSBENCH_MASTER_PORT}  \
                        -home \${MSBENCH_HOME} -P writer -W $IP -sys $BINDING_CLASS -sname ${PREFIX}$j -ms $SIZE  -tp $VELOCITY -ftp $FTP -ctp $CTP -ctps $CTPS $SYNC"
                elif [ $VMODE -eq -4 ]; then
                    CMD="ssh $MSBENCH_SSH_OPTS $IP source .bash_profile; \${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -tr $DURATION -M ${MASTERIP}:${MSBENCH_MASTER_PORT} \
                        -home \${MSBENCH_HOME} -P writer -W $IP -sys $BINDING_CLASS -sname ${PREFIX}$j -ms $SIZE  -rtpl $RTPL -ctps $CTPS $SYNC"
                fi
                if [ ! -z $RWCF ]; then
                    CMD=${CMD}" -cf \${MSBENCH_HOME}/conf/${RWCF}"
                fi
            else
            #echo "start writer in local machine"
                if [ $VMODE -eq -1 ]; then
                    CMD="${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -tr $DURATION -M ${MASTERIP}:${MSBENCH_MASTER_PORT} -home ${MSBENCH_HOME} -P writer -W $IP \
                        -sys $BINDING_CLASS -sname ${PREFIX}$j -ms $SIZE  -tp -1 $SYNC"
                elif [ $VMODE -eq -2 ]; then
                    CMD="${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -tr $DURATION -M ${MASTERIP}:${MSBENCH_MASTER_PORT} -home ${MSBENCH_HOME} -P writer -W $IP \
                        -sys $BINDING_CLASS -sname ${PREFIX}$j -ms $SIZE  -tp $VELOCITY $SYNC"
                elif [ $VMODE -eq -3 ]; then
                    CMD="${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -tr $DURATION -M ${MASTERIP}:${MSBENCH_MASTER_PORT} -home ${MSBENCH_HOME} -P writer -W $IP \
                        -sys $BINDING_CLASS -sname ${PREFIX}$j -ms $SIZE  -tp $VELOCITY -ftp $FTP -ctp $CTP -ctps $CTPS $SYNC"
                elif [ $VMODE -eq -4 ]; then
                    CMD="${MSBENCH_HOME}/bin/msbench-class.sh $SYS $MASTERCLASS -tr $DURATION -M ${MASTERIP}:${MSBENCH_MASTER_PORT} -home ${MSBENCH_HOME} -P writer -W $IP \
                        -sys $BINDING_CLASS -sname ${PREFIX}$j -ms $SIZE  -rtpl $RTPL -ctps $CTPS $SYNC"
                fi
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


