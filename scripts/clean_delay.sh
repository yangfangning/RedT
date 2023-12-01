set -x
for i in $(seq 144 147)
do
    ssh 10.77.110.$i "sudo tc qdisc del root dev em1 2>/dev/null"
done
# 这段代码的作用是通过SSH连接到一系列IP地址，然后在每个连接的主机上执行一个命令。代码的步骤如下： 
 
# 1.  set -x ：这是一个调试命令，用于在执行每个命令之前显示命令本身和其输出。 
 
# 2.  for i in $(seq 144 147) ：这是一个循环语句，从144到147之间的数字会被依次赋值给变量 i 。 
 
# 3.  do ：表示循环开始。 
 
# 4.  ssh 10.77.110.$i "sudo tc qdisc del root dev em1 2>/dev/null" ：这是一个SSH命令，用于远程连接到IP地址为 10.77.110.$i 的主机，并在该主机上执行命令 sudo tc qdisc del root dev em1 2>/dev/null 。这个命令的作用是删除网络接口 em1 上的根流量控制队列。 
 
# 5.  done ：表示循环结束。 
 
# 所以，这段代码的目的是通过SSH连接到一系列IP地址，并在每个主机上删除网络接口上的根流量控制队列。