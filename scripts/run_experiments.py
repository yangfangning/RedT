#!/usr/bin/python

import os,sys,datetime,re
import shlex
import subprocess
from experiments import *
from helper import *
from run_config import *
import glob
import time

# 当前时间
now = datetime.datetime.now()
# 格式化当前时间
strnow=now.strftime("%Y%m%d-%H%M%S")

#返回上一级目录
os.chdir('..')

#获得系统路径
PATH=os.getcwd()

result_dir = PATH + "/results/" + strnow + '/'
perf_dir = result_dir + 'perf/'

cfgs = configs

#执行
execute = True
#远程执行
remote = False
#istc或者vcloud
cluster = None
#没啥用
skip = False

#测试列表，存的是ycsb或者tpcc
exps=[]
arg_cluster = False
#没啥用
merge_mode = False
#设置数据中心？
set_dc = False
#数据中心数量
dc_count = 0
perfTime = 60
fromtimelist=[]
totimelist=[]
cpu_usage_index=0
set_latency = 0
#数据中心间延迟
latency = 0
#延迟波动范围
latency_range = 0

#调用这个程序的参数不得少于两个
if len(sys.argv) < 2:
     sys.exit("Usage: %s [-exec/-e/-noexec/-ne] [-c cluster] experiments\n \
            -exec/-e: compile and execute locally (default)\n \
            -noexec/-ne: compile first target only \
            -c: run remote on cluster; possible values: istc, vcloud\n \
            " % sys.argv[0])

for arg in sys.argv[1:]:
    if arg == "--help" or arg == "-h":
        sys.exit("Usage: %s [-exec/-e/-noexec/-ne] [-skip] [-c cluster] experiments\n \
                -exec/-e: compile and execute locally (default)\n \
                -noexec/-ne: compile first target only \
                -skip: skip any experiments already in results folder\n \
                -c: run remote on cluster; possible values: istc, vcloud\n \
                " % sys.argv[0])
    if arg == "--exec" or arg == "-e":
        execute = True
    elif arg == "--noexec" or arg == "-ne":
        execute = False
    elif arg == "--skip":
        skip = True
    elif arg == "-c":
        remote = True
        arg_cluster = True
    elif arg == '-m':
        merge_mode = True
    elif arg == '--dc':
        set_dc = True
    elif set_dc:
        dc_count = arg
        set_dc = False
    elif arg == '-l':
        set_latency = 1
    elif set_latency == 1:  # set delay
        latency = arg
        set_latency = 2
    elif set_latency == 2:  # set delay range
        latency_range = arg
        set_latency = 0
    elif arg_cluster:
        cluster = arg
        arg_cluster = False
    else:
        exps.append(arg)

for exp in exps:
    #fmt代表键，exper代表对应的值列表
    fmt,experiments = experiment_map[exp]()

    for e in experiments:
        #获得上述函数中设置的configs的值
        cfgs = get_cfgs(fmt,e)
        if remote:
            cfgs["TPORT_TYPE"], cfgs["TPORT_TYPE_IPC"], cfgs["TPORT_PORT"] = "tcp", "false", 6222
        output_f = get_outfile_name(cfgs, fmt)
        output_dir = output_f + "/"
        output_f += strnow

        #打开config文件，将里面的每一行放进lines，将cfgs中的配置写进config文件中
        f = open("config.h", 'r')
        lines = f.readlines()
        f.close()
        with open("config.h", 'w') as f_cfg:
            for line in lines:
                found_cfg = False
                for c in cfgs:
                    found_cfg = re.search("#define " + c + "\t", line) or re.search("#define " + c + " ", line)
                    if found_cfg:
                        f_cfg.write("#define " + c + " " + str(cfgs[c]) + "\n")
                        break
                if not found_cfg:
                    f_cfg.write(line)

        cmd = "make clean; make deps; make -j16"
        print (cmd)
        os.system(cmd)
        if not execute:
            exit()

        if execute:
            #创建目录
            cmd = "mkdir -p {}".format(perf_dir)
            print (cmd)
            os.system(cmd)
            #复制config文件到result_dir目录下，并重命名为output_f.cfg
            cmd = "cp config.h {}{}.cfg".format(result_dir,output_f)
            print (cmd)
            os.system(cmd)

            if remote:
                if cluster == 'istc':
                    machines_ = istc_machines
                    uname = istc_uname
                    cfg_fname = "istc_ifconfig.txt"
                elif cluster == 'vcloud':
                    machines_ = vcloud_machines
                    uname = vcloud_uname
                    uname2 = username
                    cfg_fname = "vcloud_ifconfig.txt"
                else:
                    assert(False)
                # machines = machines_[:(cfgs["NODE_CNT"]+1)]
                #获取服务器加客户端数量的机器数量
                machines = machines_[:(cfgs["NODE_CNT"]+cfgs["CLIENT_NODE_CNT"])]
                #将机器名称输出到ifconfig文件中
                with open("ifconfig.txt", 'w') as f_ifcfg:
                    for m in machines:
                        f_ifcfg.write(m + "\n")

                if cfgs["WORKLOAD"] == "TPCC":
                    files = ["rundb", "runcl", "ifconfig.txt"]
                elif cfgs["WORKLOAD"] == "YCSB":
                    files = ["rundb", "runcl", "ifconfig.txt"]
                
                # if cfgs["WORKLOAD"] == "TPCC":
                #     files = ["rundb", "runcl", "ifconfig.txt", "./benchmarks/TPCC_short_schema.txt", "./benchmarks/TPCC_full_schema.txt"]
                # elif cfgs["WORKLOAD"] == "YCSB":
                #     files = ["rundb", "runcl", "ifconfig.txt", "benchmarks/YCSB_schema.txt"]
                #生成一个迭代器，里面是机器和文件的所有组合，将文件复制到远程机器上
                for m, f in itertools.product(machines, files):
                    if cluster == 'istc':
                        cmd = 'scp {}/{} {}.csail.mit.edu:/{}/'.format(PATH, f, m, uname)
                    elif cluster == 'vcloud':
                        #在远程服务器上杀死运行中的名为 "rundb" 和 "runcl" 的进程
                        os.system('bash scripts/kill.sh {}'.format(m))
                        cmd = 'scp {}/{} {}:/{}'.format(PATH, f, m, uname)
                    print (cmd)
                    os.system(cmd)
                    # if cluster == 'istc':
                    #     cmd = 'ssh {}.csail.mit.edu:/{}/'.format(PATH, f, m, uname)
                    # elif cluster == 'vcloud':
                    #     os.system('./scripts/kill.sh {}'.format(m))
                    #     cmd = 'ssh {}/{} {}:/{}'.format(PATH, f, m, uname)
                #部署config文件
                print("Deploying: {}".format(output_f))
                os.chdir('./scripts')
                if cluster == 'istc':
                    cmd = 'bash deploy.sh \'{}\' /{}/ {}'.format(' '.join(machines), uname, cfgs["NODE_CNT"])
                elif cluster == 'vcloud':
                    #join后形成的数组，只node_cnt数量的运行rundb，其他运行rundb
                    cmd = 'bash vcloud_deploy.sh \'{}\' /{}/ {} {} {} {} {} {}'.format(' '.join(machines), uname, cfgs["NODE_CNT"], perfTime, uname2, dc_count, latency, latency_range)
                print (cmd)
                fromtimelist.append(str(int(time.time())) + "000")
                os.system(cmd)
                totimelist.append(str(int(time.time())) + "000")
                perfip = machines[0]
                #将这个文件复制到远程机器上
                cmd = "scp getFlame.sh {}:/{}/".format(perfip, uname)
                print (cmd)
                os.system(cmd)
                #在远程执行，这个sh脚本会检查是否存在pref report进程，没有花性能图
                cmd = 'ssh {} "bash /{}/getFlame.sh"'.format(perfip, uname)
                print (cmd)
                os.system(cmd)
                #将上面画的图，复制到本地，并重命名
                cmd = "scp {}:/{}/perf.svg {}{}.svg".format(perfip, uname, perf_dir, output_f)
                print (cmd)
                os.system(cmd)
                os.chdir('..')
                # cpu_usage_path=PATH + "/results/" + strnow + '/cpu_usage_' + str(cpu_usage_index)
                # cpu_usage_avg_path = PATH + "/results/" + strnow + '/cpu_usage_avg'
                # os.mkdir(cpu_usage_path)
                cpu_usage_index+=1
                for m, n in zip(machines, range(len(machines))):
                    if cluster == 'istc':
                        cmd = 'scp {}.csail.mit.edu:/{}/results.out {}{}_{}.out'.format(m,uname,result_dir,n,output_f)
                        print (cmd)
                        os.system(cmd)
                    elif cluster == 'vcloud':
                        #运行rundb和runcl后，会将输出输出到这个文件，将这个文件复制到本地并重命名
                        cmd = 'scp {}:/{}/dbresults{}.out results/{}/{}_{}.out'.format(m,uname,n,strnow,n,output_f)
                        print (cmd)
                        os.system(cmd)
                        # cmd = 'scp {}:/tmp/{}* {}/'.format(m,uname2,cpu_usage_path)
                        # print (cmd)
                        # os.system(cmd)

            else:
                nnodes = cfgs["NODE_CNT"]
                nclnodes = cfgs["NODE_CNT"]
                pids = []
                print("Deploying: {}".format(output_f))
                for n in range(nnodes+nclnodes):
                    if n < nnodes:
                        cmd = "./rundb -nid{}".format(n)
                    else:
                        cmd = "./runcl -nid{}".format(n)
                    print(cmd)
                    cmd = shlex.split(cmd)
                    ofile_n = "{}{}_{}.out".format(result_dir,n,output_f)
                    ofile = open(ofile_n,'w')
                    p = subprocess.Popen(cmd,stdout=ofile,stderr=ofile)
                    pids.insert(0,p)
                for n in range(nnodes + nclnodes):
                    pids[n].wait()

#获取输入的参数，并去重后升序排列
#并发控制方法
    al = []
    for e in experiments:
        al.append(e[2])
    al = sorted(list(set(al)))
#线程数量
    tcnt = []
    for e in experiments:
        tcnt.append(e[-2])
    # for e in experiments:
    #     tcnt.append(e[-6])
    tcnt = sorted(list(set(tcnt)))
#cross_dc_perc，数据中心间事务比率
    cocnt = []
    for e in experiments:
        cocnt.append(e[-1])
    cocnt = sorted(list(set(cocnt)))
#线程数量
    sk = []
    for e in experiments:
        sk.append(e[-2])
    sk = sorted(list(set(sk)))
#cross_dc_perc，数据中心间事务比率  
    dcp = []
    for e in experiments:
        dcp.append(e[-1])
    dcp = sorted(list(set(dcp)))
#cross_dc_perc，数据中心间事务比率   
    nd = []
    for e in experiments:
        nd.append(e[-1])
    nd = sorted(list(set(nd)))
#元组写事务的比率tup_write_perc
    wr = []
    for e in experiments:
        wr.append(e[4])
    wr = sorted(list(set(wr)))
#node-cnt
    cn = []
    for e in experiments:
        cn.append(e[1])
    cn = sorted(list(set(cn)))
#skew倾斜率
    ld = []
    for e in experiments:
        ld.append(e[-3])
    ld = sorted(list(set(ld)))
#元组写事务的比率tup_write_perc
    part = []
    for e in experiments:
        part.append(e[4])
    part = sorted(list(set(part)))
#事务写事务的比率txn_write_perc
    dc = []
    for e in experiments:
        dc.append(e[5])
    dc = sorted(list(set(dc)))
#cross_dc_perc，数据中心间事务比率
    tpcc_ld = []
    for e in experiments:
        tpcc_ld.append(e[-1])
    tpcc_ld = sorted(list(set(tpcc_ld)))
#cross_dc_perc，数据中心间事务比率
    ccnt = []
    for e in experiments:
        ccnt.append(e[-1])
    ccnt = sorted(list(set(ccnt)))

    cmd = ''
    os.chdir('./scripts')
    if exp == 'ycsb_skew':
        cmd='bash result.sh -a ycsb_skew -n {} -c {} -s {} -t {}'.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in sk]), strnow)
    elif exp == 'ycsb_tapir_skew':
        cmd='bash result.sh -a ycsb_tapir_skew -n {} -c {} -s {} -t {}'.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in sk]), strnow)
    elif exp == 'ycsb_early_skew':
        cmd='bash result.sh -a ycsb_early_skew -n {} -c {} -s {} -t {}'.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in sk]), strnow)
    elif exp == 'ycsb_cross_dc':
        cmd='bash result.sh -a ycsb_cross_dc -n {} -c {} -dc {} -t {} '.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in dcp]), strnow)
    elif exp == 'ycsb_network_delay':
        cmd='bash result.sh -a ycsb_network_delay -n {} -c {} -nd {} -t {} '.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in nd]), strnow)
    elif exp == 'ycsb_network_delay2':
        cmd='bash result.sh -a ycsb_network_delay -n {} -c {} -nd {} -t {} '.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in nd]), strnow)
    elif exp == 'ycsb_writes':
        cmd='bash result.sh -a ycsb_writes -n {} -c {} --wr {} -t {}'.format(cn[0], ','.join([str(x) for x in al]), ','.join([str(x) for x in wr]), strnow)
    elif exp == 'ycsb_tapir_cross_dc':
        cmd='bash result.sh -a ycsb_tapir_cross_dc -n {} -c {} -dc {} -t {} '.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in dcp]), strnow)
    elif exp == 'ycsb_early_cross_dc':
        cmd='bash result.sh -a ycsb_early_cross_dc -n {} -c {} -dc {} -t {} '.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in dcp]), strnow)
    elif exp == 'ycsb_tapir_network_delay':
        cmd='bash result.sh -a ycsb_tapir_network_delay -n {} -c {} -nd {} -t {} '.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in nd]), strnow)
    elif exp == 'ycsb_tapir_network_delay2':
        cmd='bash result.sh -a ycsb_tapir_network_delay -n {} -c {} -nd {} -t {} '.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in nd]), strnow)
    elif exp == 'ycsb_early_network_delay':
        cmd='bash result.sh -a ycsb_early_network_delay -n {} -c {} -nd {} -t {} '.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in nd]), strnow)
    elif exp == 'ycsb_early_network_delay2':
        cmd='bash result.sh -a ycsb_early_network_delay -n {} -c {} -nd {} -t {} '.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in nd]), strnow)
    elif exp == 'ycsb_early_writes':
        cmd='bash result.sh -a ycsb_tapir_writes -n {} -c {} --wr {} -t {}'.format(cn[0], ','.join([str(x) for x in al]), ','.join([str(x) for x in wr]), strnow)
    elif exp == 'ycsb_tapir_writes':
        cmd='bash result.sh -a ycsb_tapir_writes -n {} -c {} --wr {} -t {}'.format(cn[0], ','.join([str(x) for x in al]), ','.join([str(x) for x in wr]), strnow)
    elif 'ycsb_scaling' in exp:
        cmd='bash result.sh -a ycsb_scaling -n {} -c {} -t {} --ft {} --tt {}'.format(','.join([str(x) for x in cn]), ','.join([str(x) for x in al]), strnow, ','.join(fromtimelist), ','.join(totimelist))
    elif 'ycsb_scaling_tcp' in exp:
        cmd='bash result.sh -a ycsb_scaling_tcp -n {} -c {} -t {} --ft {} --tt {}'.format(','.join([str(x) for x in cn]), ','.join([str(x) for x in al]), strnow, ','.join(fromtimelist), ','.join(totimelist))
    elif 'ycsb_scaling_two_sided' in exp:
        cmd='bash result.sh -a ycsb_scaling_two_sided -n {} -c {} -t {} --ft {} --tt {}'.format(','.join([str(x) for x in cn]), ','.join([str(x) for x in al]), strnow, ','.join(fromtimelist), ','.join(totimelist))
    elif 'ycsb_scaling_one_sided' in exp:
        cmd='bash result.sh -a ycsb_scaling_one_sided -n {} -c {} -t {} --ft {} --tt {}'.format(','.join([str(x) for x in cn]), ','.join([str(x) for x in al]), strnow, ','.join(fromtimelist), ','.join(totimelist))
    elif 'ycsb_scaling_coroutine' in exp:
        cmd='bash result.sh -a ycsb_scaling_coroutine -n {} -c {} -t {} --ft {} --tt {}'.format(','.join([str(x) for x in cn]), ','.join([str(x) for x in al]), strnow, ','.join(fromtimelist), ','.join(totimelist))
    elif 'ycsb_scaling_dbpa' in exp:
        cmd='bash result.sh -a ycsb_scaling_dbpa -n {} -c {} -t {} --ft {} --tt {}'.format(','.join([str(x) for x in cn]), ','.join([str(x) for x in al]), strnow, ','.join(fromtimelist), ','.join(totimelist))
    elif 'ycsb_scaling_all' in exp:
        cmd='bash result.sh -a ycsb_scaling_all -n {} -c {} -t {} --ft {} --tt {}'.format(','.join([str(x) for x in cn]), ','.join([str(x) for x in al]), strnow, ','.join(fromtimelist), ','.join(totimelist))
    elif 'tpcc_scaling' in exp:
        cmd='bash result.sh -a tpcc_scaling -n {} -c {} -t {}'.format(','.join([str(x) for x in cn]), ','.join([str(x) for x in al]), strnow)
    elif 'ycsb_stress' in exp:
        cmd='bash result.sh -a ycsb_stress -n {} -c {} -s {} -l {} -t {}'.format(str(cn[0]), ','.join([str(x) for x in al]), str(sk[0]), ','.join([str(x) for x in ld]), strnow)
    elif 'tpcc_stress' in exp:
        cmd='bash result.sh -a tpcc_stress -n {} -c {} -l {} -t {}'.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in tpcc_ld]), strnow)
    elif 'tpcc_cstress' in exp:
        cmd='bash result.sh -a tpcc_stress_ctx -n {} -c {} -l {} -C {} -t {} --ft {} --tt {}'.format(str(cn[0]), ','.join([str(x) for x in al]), ','.join([str(x) for x in ld]), ','.join([str(x) for x in ccnt]), strnow, ','.join(fromtimelist), ','.join(totimelist))
    elif 'ycsb_thread' in exp:
        cmd='bash result.sh -a ycsb_thread -n {} -c {} -t {} -T {}'.format(str(cn[0]), ','.join([str(x) for x in al]), strnow, ','.join([str(x) for x in tcnt]))
    elif 'ycsb_tapir_thread' in exp:
        cmd='bash result.sh -a ycsb_tapir_thread -n {} -c {} -t {} -T {}'.format(str(cn[0]), ','.join([str(x) for x in al]), strnow, ','.join([str(x) for x in tcnt]))
    elif 'tpcc_thread' in exp:
        cmd='bash result.sh -a tpcc_thread -n {} -c {} -t {} -T {}'.format(str(cn[0]), ','.join([str(x) for x in al]), strnow, ','.join([str(x) for x in tcnt]))
    elif 'ycsb_partitions' in exp:
        cmd='bash result.sh -a ycsb_partitions -n {} -c {} -t {} -P {}'.format(str(cn[0]), ','.join([str(x) for x in al]), strnow, ','.join([str(x) for x in part]))
    elif 'ycsb_tapir_partitions' in exp:
        cmd='bash result.sh -a ycsb_partitions -n {} -c {} -t {} -P {}'.format(str(cn[0]), ','.join([str(x) for x in al]), strnow, ','.join([str(x) for x in part]))
    elif 'ycsb_early_partitions' in exp:
        cmd='bash result.sh -a ycsb_partitions -n {} -c {} -t {} -P {}'.format(str(cn[0]), ','.join([str(x) for x in al]), strnow, ','.join([str(x) for x in part]))
    elif 'ycsb_dcs' in exp:
        cmd='bash result.sh -a ycsb_dcs -n {} -c {} -t {} -D {}'.format(str(cn[0]), ','.join([str(x) for x in al]), strnow, ','.join([str(x) for x in dc]))
    elif 'ycsb_tapir_dcs' in exp:
        cmd='bash result.sh -a ycsb_dcs -n {} -c {} -t {} -D {}'.format(str(cn[0]), ','.join([str(x) for x in al]), strnow, ','.join([str(x) for x in dc]))
    elif 'ycsb_early_dcs' in exp:
        cmd='bash result.sh -a ycsb_dcs -n {} -c {} -t {} -D {}'.format(str(cn[0]), ','.join([str(x) for x in al]), strnow, ','.join([str(x) for x in dc]))
    elif 'ycsb_coroutine' in exp:
        cmd='bash result.sh -a ycsb_coroutine -n {} -c {} -t {} -CO {}'.format(str(cn[0]), ','.join([str(x) for x in al]), strnow, ','.join([str(x) for x in cocnt]))
    print (cmd)
    os.system(cmd)
    print (cmd)

    cmd=''
    os.chdir('../draw')
    if exp == 'ycsb_skew':
       cmd='./deneva-plot.sh -a ycsb_skew -c {} -t {}'.format(','.join([str(x) for x in al]), strnow)
    elif exp == 'ycsb_cross_dc':
       cmd='./deneva-plot.sh -a ycsb_cross_dc -c {} -t {}'.format(','.join([str(x) for x in al]), strnow)
    elif exp == 'ycsb_writes':
       cmd='./deneva-plot.sh -a ycsb_writes -c {} -t {}'.format(','.join([str(x) for x in al]), strnow)
    elif exp == 'ycsb_tapir_skew':
           cmd='./deneva-plot.sh -a ycsb_tapir_skew -c {} -t {}'.format(','.join([str(x) for x in al]), strnow)
    elif exp == 'ycsb_tapir_cross_dc':
       cmd='./deneva-plot.sh -a ycsb_tapir_cross_dc -c {} -t {}'.format(','.join([str(x) for x in al]), strnow)
    elif exp == 'ycsb_tapir_writes':
       cmd='./deneva-plot.sh -a ycsb_tapir_writes -c {} -t {}'.format(','.join([str(x) for x in al]), strnow)
    elif 'ycsb_scaling' in exp:
       cmd='./deneva-plot.sh -a ycsb_scaling -c {} -t {}'.format(','.join([str(x) for x in al]), strnow)
    elif 'tpcc_scaling' in exp:
       cmd='./deneva-plot.sh -a tpcc_scaling2 -c {} -t {}'.format(','.join([str(x) for x in al]), strnow)
    elif 'ycsb_stress' in exp:
       cmd='./deneva-plot.sh -a ycsb_stress -c {} -t {}'.format(','.join([str(x) for x in al]), strnow)
    elif 'tpcc_stress' in exp:
       cmd='./deneva-plot.sh -a tpcc_stress -c {} -t {}'.format(','.join([str(x) for x in al]), strnow)
    elif 'tpcc_cstress' in exp:
       cmd='./deneva-plot-his.sh -a tpcc_cstress -c {} -t {}'.format(','.join([str(x) for x in al]), strnow)
    print (cmd)
    os.system(cmd)
