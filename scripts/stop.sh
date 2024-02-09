#!/bin/bash
# The ulimit is setup in start_program, the paramter of ulimit is:
#   ulimit -n 1048576
#   ulimit -u 4194304
#   ulimit -c unlimited
# If set ulimit failed, please use root or sudo to execute sysctl.sh to increase kernal limit.

mydir="${BASH_SOURCE%/*}"
if [[ ! -d "$mydir" ]]; then mydir="$PWD"; fi
. $mydir/shflags

DEFINE_string role 'store' 'server role'
DEFINE_integer server_num 3 'server number'
DEFINE_integer force 0 'use kill -9 to stop'
DEFINE_integer use_pgrep 0 'use pgrep to get pid'

# parse the command-line
FLAGS "$@" || exit 1
eval set -- "${FLAGS_ARGV}"

echo "role: ${FLAGS_role}"

BASE_DIR=$(dirname $(cd $(dirname $0); pwd))
DIST_DIR=$BASE_DIR/dist

SERVER_NUM=${FLAGS_server_num}

source $mydir/deploy_func.sh

for ((i=1; i<=$SERVER_NUM; ++i)); do
    if [ ${FLAGS_use_pgrep} -ne 0 ]; then
        echo "use_pgrep > 0, use pgrep to get pid"
        process_no=$(pgrep -f "${BASE_DIR}.*dingodb_server.*${FLAGS_role}" -U `id -u` | xargs)

        if [ "${process_no}" != "" ]; then
            echo "pid to kill: ${process_no}"
            if [ ${FLAGS_force} -eq 0 ]
            then
                kill ${process_no}
            else
                kill -9 ${process_no}
            fi
        else
            echo "not exist ${FLAGS_role} process"
        fi
    else
        echo "use PID file to get pid"

        program_dir=$BASE_DIR/dist/${FLAGS_role}${i}
        pid_file=${program_dir}/log/pid

        # Check if the PID file exists
        if [ -f "$pid_file" ]; then
            # Read the PID from the file
            pid=$(<"$pid_file")

            # Check if the PID is a number
            if [[ "$pid" =~ ^[0-9]+$ ]]; then
                # Kill the process with the specified PID
                if [ ${FLAGS_force} -eq 0 ]
                then
                    echo "Killing process with PID $pid"
                    kill ${pid}
                else
                    echo "Killing -9 process with PID $pid"
                    kill -9 ${pid}
                fi
            else
                echo "Invalid PID in the PID file: $pid"
            fi
        else
            echo "PID file not found: $pid_file"
        fi
    fi

done

echo "Finish..."
