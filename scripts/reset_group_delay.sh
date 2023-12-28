set -x
DELAY=${1}
RANGE=${2:-0}
for i in $(seq 2 5)
do
    # if [[ $i -ne 19 ]] 
    # then
    if [[ $RANGE -eq 0 ]]
    then 
        ssh 172.20.242.$i "sudo tc qdisc change dev eth0 parent 1:5 handle 50: netem delay ${DELAY}ms"
    else
        ssh 172.20.242.$i "sudo tc qdisc change dev eth0 parent 1:5 handle 50: netem delay ${DELAY}ms ${RANGE}ms distribution normal"
    fi
    # ssh 172.20.242.$i "sudo tc qdisc change dev eth0 parent 1:5 handle 50: netem delay ${DELAY}ms"
    # fi
done
