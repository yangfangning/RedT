#!/bin/bash

LOCAL_UNAME="$5"
USERNAME="$5"
HOSTS="$1"
PATHE="$2"
NODE_CNT="$3"
count=0
#客户端计数
mc=0
DC_COUNT="$6"
LATENCY="$7"
LATENCY_RANGE="$8"
for HOSTNAME in ${HOSTS}; do
    # scp cpu_monitor.sh ${USERNAME}@${HOSTNAME}:${PATHE}
    #删除同名文件
    ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${USERNAME}@${HOSTNAME} "rm -rf /tmp/${USERNAME}_* ${USERNAME} ${count}" &
done
# bash ntptime.sh

if [[ $DC_COUNT -ne 0 ]]
then
#清理网络规则
    bash clean_group_delay.sh
    bash set_group_delay.sh 0 $DC_COUNT
fi

#不等于0
if [[ $LATENCY -ne 0 ]] 
then
#修改网络延迟
    bash reset_group_delay.sh $LATENCY $LATENCY_RANGE
fi

for HOSTNAME in ${HOSTS}; do
    #SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 10m 10m gdb -batch -ex \"run\" -ex \"bt\" --args ./rundb -nid${count} >> results.out 2>&1 | grep -v ^\"No stack.\"$"
    #大于等于
    if [ $count -ge $NODE_CNT ]; then
        SCRIPT="source /etc/profile;env SCHEMA_PATH=\"$2\" timeout -k 15m 15m ${PATHE}runcl -nid${count} > ${PATHE}clresults${count}.out 2>&1"
        echo "${HOSTNAME}: runcl ${count}"
    else
        SCRIPT="source /etc/profile;env SCHEMA_PATH=\"$2\" timeout -k 15m 15m ${PATHE}rundb -nid${count} > ${PATHE}dbresults${count}.out 2>&1"
        echo "${HOSTNAME}: rundb ${count}"
        # ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${USERNAME}@${HOSTNAME} "source /etc/profile;bash ${PATHE}cpu_monitor.sh ${USERNAME} ${count}" &
        let mc=mc+1
    fi
    ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${USERNAME}@${HOSTNAME} "${SCRIPT}" &
    count=`expr $count + 1`
done

sleep 60
OLD_IFS="$IFS"
IFS=" "
HOSTLIST=($HOSTS)
IFS="$OLD_IFS"
scp wkdbperf.sh ${USERNAME}@${HOSTLIST[0]}:${PATHE}
#检测性能数据，并输出到指定文件夹中
ssh ${USERNAME}@${HOSTLIST[0]} "bash ${PATHE}/wkdbperf.sh $4"

#大于0时，等待进程pids完成
let count=count+mc
while [ $count -gt 0 ]
do
    wait $pids
    count=`expr $count - 1`
done


bash reset_group_delay.sh 0 0