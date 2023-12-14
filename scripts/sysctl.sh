#!/bin/bash
# CAUTION: use root or sudo to execute this script.
# This script is used to setup kernel variables for DingoDB.

# setup kernel parameters

NUM_FILE=1048576
NUM_PROC=4194304

nr_open=$(sysctl fs.nr_open|awk '{print $3}')
echo "nr_open="${nr_open}

if [ ${nr_open} -lt ${NUM_FILE} ]
then
    echo "try to increase nr_open"
    echo "fs.nr_open = ${NUM_FILE}" >>  /etc/sysctl.conf
    sysctl -p
    nr_open=$(sysctl fs.nr_open|awk '{print $3}')
    echo "new nr_open="${nr_open}

    if [ ${nr_open} -lt ${NUM_FILE} ]
    then
        echo "increase faild, exit!"
        exit -1
    fi
fi

file_max=$(sysctl fs.file-max|awk '{print $3}')
echo "file-max="${file_max}

if [ ${file_max} -lt ${NUM_FILE} ]
then
    echo "try to increase file-max"
    echo "fs.file-max = ${NUM_FILE}" >>  /etc/sysctl.conf
    sysctl -p
    file_max=$(sysctl fs.file-max|awk '{print $3}')
    echo "new file-max="${file_max}

    if [ ${file_max} -lt ${NUM_FILE} ]
    then
        echo "increase faild, exit!"
        exit -1
    fi
fi

pid_max=$(sysctl kernel.pid_max|awk '{print $3}')
echo "pid_max="${pid_max}

if [ ${pid_max} -lt ${NUM_PROC} ]
then
    echo "try to increase pid_max"
    echo "kernel.pid_max = ${NUM_PROC}" >>  /etc/sysctl.conf
    sysctl -p
    pid_max=$(sysctl kernel.pid_max|awk '{print $3}')
    echo "new pid_max="${pid_max}

    if [ ${pid_max} -lt ${NUM_PROC} ]
    then
        echo "increase faild, exit!"
        exit -1
    fi
fi

echo "start to tun hugepage to madvise"
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled

# setup limits.conf
echo "start to setup limits.d/90-dingo.conf"
echo "* - nofile ${NUM_FILE}"  >  /etc/security/limits.d/90-dingo.conf
echo "* - nproc  ${NUM_PROC}"  >> /etc/security/limits.d/90-dingo.conf
echo "Please re-login to make the new limits.conf to take effect!"

ulimit -n ${NUM_FILE}
ulimit -u ${NUM_PROC}

echo "ulimit -n "$(ulimit -n)
echo "ulimit -u "$(ulimit -u)

if [ $(ulimit -n) -lt ${NUM_FILE} ]
then
    echo "set ulimit -n failed, please check limits.conf"
    exit -1
fi

if [ $(ulimit -u) -lt ${NUM_PROC} ]
then
    echo "set ulimit -u failed, please check limits.conf"
    exit -1
fi

