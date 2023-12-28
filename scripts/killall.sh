set -x
ps -aux | grep runcl | awk '{print $2}' | xargs kill -9 2>/dev/null 1>/dev/null
ps -aux | grep rundb | awk '{print $2}' | xargs kill -9 2>/dev/null 1>/dev/null

for i in $(seq 2 5)
do
    if [[ $i -ne 5 ]] 
    then
        ssh 172.20.242.$i "ps -aux | grep rundb | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
        ssh 172.20.242.$i "ps -aux | grep runcl | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
        ssh 172.20.242.$i "rm -rf /data/core/*" 2>/dev/null 1>/dev/null
        ssh 172.20.242.$i "rm -rf /home/core/*" 2>/dev/null 1>/dev/null
        ssh 172.20.242.$i "rm -rf /home/ibtest/core*" 2>/dev/null 1>/dev/null
    fi
done
