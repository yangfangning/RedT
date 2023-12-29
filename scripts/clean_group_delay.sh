set -x
for i in $(seq 2 4)
do
    # if [[ $i -ne 19 ]] 
    # then
    ssh 172.20.242.$i "sudo tc qdisc del dev eth0 root"
    # fi
done
