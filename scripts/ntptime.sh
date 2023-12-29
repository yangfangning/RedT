set -x
for i in $(seq 2 4)
do
    ssh 172.20.242.$i "sudo ntpdate 172.20.242.4" 2>/dev/null 1>/dev/null
done

