set -x
for i in $(seq 10 17)
do
    ssh 172.20.242.$i "sudo ntpdate 172.20.242.12" 2>/dev/null 1>/dev/null
done

