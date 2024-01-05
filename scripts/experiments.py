import itertools
from paper_plots import *
# Experiments to run and analyze
# Go to end of file to fill in experiments
SHORTNAMES = {
    "CLIENT_NODE_CNT" : "CN",
    "CLIENT_THREAD_CNT" : "CT",
    "CLIENT_REM_THREAD_CNT" : "CRT",
    "CLIENT_SEND_THREAD_CNT" : "CST",
    "NODE_CNT" : "N",
    "THREAD_CNT" : "T",
    "COROUTINE_CNT" : "CO",
    "REM_THREAD_CNT" : "RT",
    "SEND_THREAD_CNT" : "ST",
    "CC_ALG" : "",
    "WORKLOAD" : "",
    "MAX_TXN_PER_PART" : "TXNS",
    "MAX_TXN_IN_FLIGHT" : "TIF",
    "PART_PER_TXN" : "PPT",
    "TUP_READ_PERC" : "TRD",
    "TUP_WRITE_PERC" : "TWR",
    "TXN_READ_PERC" : "RD",
    "TXN_WRITE_PERC" : "WR",
    "ZIPF_THETA" : "SKEW",
    "MSG_TIME_LIMIT" : "BT",
    "MSG_SIZE_MAX" : "BS",
    "DATA_PERC":"D",
    "ACCESS_PERC":"A",
    "PERC_PAYMENT":"PP",
    "MPR":"MPR",
    "REQ_PER_QUERY": "RPQ",
    "MODE":"",
    "PRIORITY":"",
    "ABORT_PENALTY":"PENALTY",
    "STRICT_PPT":"SPPT",
    "NETWORK_DELAY":"NDLY",
    "NETWORK_DELAY_TEST":"NDT",
    "REPLICA_CNT":"RN",
    "SYNTH_TABLE_SIZE":"TBL",
    "ISOLATION_LEVEL":"LVL",
    "YCSB_ABORT_MODE":"ABRTMODE",
    "NUM_WH":"WH",
    "CLV":"CLV",
}

fmt_title=["NODE_CNT","CC_ALG","ACCESS_PERC","TXN_WRITE_PERC","PERC_PAYMENT","MPR","MODE","MAX_TXN_IN_FLIGHT","SEND_THREAD_CNT","REM_THREAD_CNT","THREAD_CNT","COROUTINE_CNT","TXN_WRITE_PERC","TUP_WRITE_PERC","ZIPF_THETA","NUM_WH"]

##############################
# PLOTS
##############################
#dta_target_algos=['DLI_BASE','DLI_MVCC_OCC','DLI_MVCC_BASE','DLI_OCC','MAAT']#['DLI_MVCC_OCC','DLI_DTA','TIMESTAMP','WAIT_DIE']#['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP','OCC','DLI_MVCC_OCC','DLI_OCC','DLI_BASE','DLI_MVCC_BASE','DLI_DTA']
#dta_target_algos=['NO_WAIT', 'MVCC', 'CALVIN', 'MAAT']
#dta_target_algos=['DLI_DTA3']
#dta_target_algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP','OCC','DLI_MVCC_OCC','DLI_BASE','DLI_MVCC_BASE']
dta_target_algos=['TIMESTAMP']
# tpcc load
#tpcc_loads = ['50', '100', '200', '500', '1000', '2000', '5000']
tpcc_loads = ['50', '100', '200', '500', '1000', '2000', '5000']
# ycsb load
ycsb_loads = ['50', '100', '200', '500', '1000', '2000', '5000']

def pps_scaling():
    wl = 'PPS'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,tif] for tif,n,cc in itertools.product(load,nnodes,nalgos)]
    return fmt,exp

def ycsb_tapir_thread():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    #nnodes = [1,2,4,8,16,32]
    nnodes = [4]
    tapir = ['true']
    # algos=['CALVIN','MAAT','MVCC','NO_WAIT','TIMESTAMP','WAIT_DIE']
    algos=['NO_WAIT']
    base_table_size=1048576
    # base_table_size=1048576*8
    #base_table_size=2097152*8
    txn_write_perc = [0.2]
    tup_write_perc = [0.2]
    load = [10000]
    tcnt = [1,2,4,6,8,10,12,14,16,18,20]
    # tcnt = [6]
    ctcnt = [4]
    scnt = [1]
    rcnt = [1]
    skew = [0.2]
    #skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,sk,thr,cthr,sthr,rthr,sthr,rthr] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo,ir in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos,tapir)]
    #txn_write_perc = [0.0]
    #skew = [0.0]
    #exp = exp + [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

def ycsb_coroutine():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    #nnodes = [1,2,4,8,16,32]
    nnodes = [4]
    # algos=['CALVIN','MAAT','MVCC','NO_WAIT','TIMESTAMP','WAIT_DIE']
    algos=['NO_WAIT']
    base_table_size=1048576
    # base_table_size=1048576*8
    #base_table_size=2097152*8
    txn_write_perc = [0.2]
    tup_write_perc = [0.2]
    load = [20000]
    tcnt = [24]
    ctcnt = [4]
    cocnt = [2,3,4,5,6,7,8,9,10,12,14,16,18]
    scnt = [1]
    rcnt = [1]
    skew = [0.2]
    #skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT","COROUTINE_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr,cothr] for cothr,thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(cocnt,tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    #txn_write_perc = [0.0]
    #skew = [0.0]
    #exp = exp + [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

def ycsb_scaling():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    nnodes = [3,6,9,12]
    # nnodes = [12]
    tapir=['false']
    early=['false']
    algos = ['NO_WAIT']
    # algos = ['CALVIN']
    # base_table_size=262144*10
    base_table_size=1048576
    # base_table_size=2097152*8
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    # load = [80]
    tcnt = [20]
    ctcnt = [1]
    scnt = [1]
    rcnt = [1]
    skew = [0.2]
    # skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT","CENTER_CNT","USE_TAPIR","EARLY_PREPARE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,thr*n,sk,thr,cthr,sthr,rthr,sthr,rthr,3,ir,er] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,n,algo,ir,er in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,nnodes,algos,tapir,early)]
    return fmt,exp

def ycsb_scaling_early():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    nnodes = [3,6,9,12]
    # nnodes = [9,12]
    # nnodes = [3,6,9]
    tapir=['false']
    early=['true']
    algos = ['NO_WAIT']
    # algos = ['CALVIN']
    # base_table_size=262144*10
    base_table_size=1048576
    # base_table_size=2097152*8
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    # load = [80]
    tcnt = [20]
    ctcnt = [1]
    scnt = [1]
    rcnt = [1]
    skew = [0.2]
    # skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT","CENTER_CNT","USE_TAPIR","EARLY_PREPARE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,thr*n,sk,thr,cthr,sthr,rthr,sthr,rthr,3,ir,er] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,n,algo,ir,er in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,nnodes,algos,tapir,early)]
    return fmt,exp


def ycsb_scaling_l():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    # nnodes = [9,12,15]
    nnodes = [12]
    # nnodes = [3,6,9,12,15]
    # algos = ['CALVIN']
    algos = ['WOUND_WAIT']
    # base_table_size=262144*10
    base_table_size=1048576
    # base_table_size=2097152*8
    txn_write_perc = [0.0]
    tup_write_perc = [0.0]
    load = [20000]
    tcnt = [8]
    ctcnt = [2]
    scnt = [1]
    rcnt = [1]
    skew = [0.0]
    # skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    #txn_write_perc = [0.0]
    #skew = [0.0]
    #exp = exp + [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

def ycsb_scaling_m():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    # nnodes = [9,12,15]
    # nnodes = [9]
    nnodes = [3,6,9,12,15]
    algos = ['WOUND_WAIT']
    # base_table_size=262144*10
    base_table_size=1048576
    # base_table_size=2097152*8
    txn_write_perc = [0.2]
    tup_write_perc = [0.2]
    load = [20000]
    tcnt = [8]
    ctcnt = [2]
    scnt = [1]
    rcnt = [1]
    skew = [0.5]
    # skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    #txn_write_perc = [0.0]
    #skew = [0.0]
    #exp = exp + [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

def ycsb_scaling_h():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    # nnodes = [9,12,15]
    # nnodes = [2]
    nnodes = [3,6,9,12,15]
    algos = ['WOUND_WAIT']
    # base_table_size=262144*10
    base_table_size=1048576
    # base_table_size=2097152*8
    txn_write_perc = [0.2]
    tup_write_perc = [0.2]
    load = [20000]
    tcnt = [8]
    ctcnt = [2]
    scnt = [1]
    rcnt = [1]
    skew = [0.8]
    # skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr,sthr,rthr,sthr,rthr] for thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    #txn_write_perc = [0.0]
    #skew = [0.0]
    #exp = exp + [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp


def ycsb_scaling1():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    nnodes = [1,2,3,4,5]
    algos=['MAAT','MVCC','TIMESTAMP','OCC']
    base_table_size=1048576*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    ctcnt = [4]
    skew = [0.5]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr] for thr,cthr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,ctcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

def ecwc():
    wl = 'YCSB'
    nnodes = [2]
    algos=['NO_WAIT','WAIT_DIE','MVCC','CALVIN','TIMESTAMP','MAAT']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp


def ycsb_scaling_abort():
    wl = 'YCSB'
    nnodes = [1,2,4,8,16,32,64]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000,12000]
    load = [10000]
    tcnt = [4]
    skew = [0.6,0.7]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","YCSB_ABORT_MODE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,'true'] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

def ycsb_tapir_cross_dc():
    wl = 'YCSB'
    nnodes = [8]
    algos=['NO_WAIT']
    tapir=['true']
    early=['false']
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [320]
    tcnt = [40]  #THREAD_CNT
    skew = [0.2]
    # cross_dc_perc = [1] 
    cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT","CROSS_DC_TXN_PERC"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr,cro_dc_perc] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,tapir,early)]
    return fmt,exp

def ycsb_early_cross_dc():
    wl = 'YCSB'
    nnodes = [8]
    algos=['NO_WAIT']
    tapir=['false']
    early=['true']
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [320]
    tcnt = [40]  #THREAD_CNT
    skew = [0.2]
    # cross_dc_perc = [1] 
    cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT","CROSS_DC_TXN_PERC"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr,cro_dc_perc] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,tapir,early)]
    return fmt,exp

#thr, txn_wr_perc,    tup_wr_perc,   ld,  n,     sk, algo,  cro_dc_perc,  net_del,       ir,  er
#tcnt,txn_write_perc, tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,network_delay,tapir,early
#wl, ,base_table_size*n,txn_wr_perc,ld,ir,er,sk,cro_dc_perc,net_del


def ycsb_tapir_network_delay():
    wl = 'YCSB'
    nnodes = [8]
    algos=['NO_WAIT']
    tapir=['true']
    early=['false']
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [320]
    tcnt = [40]  #THREAD_CNT
    skew = [0.2]
    cross_dc_perc = [1.0]
    # network_delay = ['50000000UL','100000000UL','150000000UL','200000000UL','250000000UL','300000000UL','350000000UL','400000000UL','450000000UL','500000000UL'] 
    # network_delay = ['50000000UL','100000000UL','150000000UL','200000000UL','300000000UL','450000000UL'] 
    # network_delay = ['250000000UL','300000000UL','350000000UL','400000000UL','450000000UL','500000000UL'] 
    network_delay = ['0UL'] 
    # cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT","CROSS_DC_TXN_PERC", "NETWORK_DELAY"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr,cro_dc_perc,net_del] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,net_del,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,network_delay,tapir,early)]
    return fmt,exp

def ycsb_tapir_network_delay2():
    wl = 'YCSB'
    nnodes = [8]
    algos=['NO_WAIT']
    tapir=['true']
    early=['false']
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [320]
    tcnt = [40]  #THREAD_CNT
    skew = [0.2]
    cross_dc_perc = [0]
    # network_delay = ['50000000UL','100000000UL','150000000UL','200000000UL','250000000UL','300000000UL','350000000UL','400000000UL','450000000UL','500000000UL'] 
    # network_delay = ['50000000UL','100000000UL','150000000UL','200000000UL','300000000UL','450000000UL'] 
    # network_delay = ['250000000UL','300000000UL','350000000UL','400000000UL','450000000UL','500000000UL'] 
    network_delay = ['0UL'] 
    # cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT","CROSS_DC_TXN_PERC", "NETWORK_DELAY"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr,cro_dc_perc,net_del] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,net_del,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,network_delay,tapir,early)]
    return fmt,exp


def ycsb_network_delay2():
    wl = 'YCSB'
    nnodes = [8]
    algos=['NO_WAIT']
    tapir=['false']
    early=['false']
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [320]
    tcnt = [40]  #THREAD_CNT
    skew = [0.2]
    cross_dc_perc = [0.0]
    # network_delay = ['50000000UL','100000000UL','150000000UL','200000000UL','250000000UL','300000000UL','350000000UL','400000000UL','450000000UL','500000000UL'] 
    network_delay = ['0UL'] 
    # cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT","CROSS_DC_TXN_PERC", "NETWORK_DELAY"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr,cro_dc_perc,net_del] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,net_del,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,network_delay,tapir,early)]
    return fmt,exp

def ycsb_early_network_delay():
    wl = 'YCSB'
    nnodes = [8]
    algos=['NO_WAIT']
    tapir=['false']
    early=['true']
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [320]
    tcnt = [40]  #THREAD_CNT
    skew = [0.2]
    cross_dc_perc = [1.0]
    # network_delay = ['50000000UL','100000000UL','150000000UL','200000000UL','250000000UL','300000000UL','350000000UL','400000000UL','450000000UL','500000000UL'] 
    network_delay = ['0UL'] 
    # cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT","CROSS_DC_TXN_PERC", "NETWORK_DELAY"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr,cro_dc_perc,net_del] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,net_del,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,network_delay,tapir,early)]
    return fmt,exp

def ycsb_early_network_delay2():
    wl = 'YCSB'
    nnodes = [8]
    algos=['NO_WAIT']
    tapir=['false']
    early=['true']
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [320]
    tcnt = [40]  #THREAD_CNT
    skew = [0.2]
    cross_dc_perc = [0]
    # network_delay = ['50000000UL','100000000UL','150000000UL','200000000UL','250000000UL','300000000UL','350000000UL','400000000UL','450000000UL','500000000UL'] 
    network_delay = ['0UL'] 
    # cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT","CROSS_DC_TXN_PERC", "NETWORK_DELAY"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr,cro_dc_perc,net_del] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,net_del,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,network_delay,tapir,early)]
    return fmt,exp

def ycsb_tapir_skew():
    wl = 'YCSB'
    nnodes = [8]

    algos=['NO_WAIT']
    tapir=['true']
    early=['false']
    # base_table_size=1048576*10
    base_table_size=1048576    
    #base_table_size=2097152*8

    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [320]

    tcnt = [40]  #THREAD_CNT

    # skew = [0.0,0.2,0.4,0.6,0.65,0.7,0.75,0.8,0.85,0.9]
    # skew = [0.25,0.55,0.65,0.75]
    skew = [0.5]
    # skew = [0.0,0.1,0.2,0.3,0.4,0.5]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,tapir,early)]
    return fmt,exp


def ycsb_early_skew():
    wl = 'YCSB'
    nnodes = [8]

    algos=['NO_WAIT']
    tapir=['false']
    early=['true']
    # base_table_size=1048576*10
    base_table_size=1048576    
    #base_table_size=2097152*8

    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [320] #node_cnt*tcnt

    tcnt = [40]  #THREAD_CNT

    # skew = [0.0,0.2,0.4,0.5,0.6,0.65,0.7,0.75,0.8,0.85,0.9]
    # skew = [0.0,0.2,0.4,0.5]
    skew = [0.6,0.65,0.7,0.75,0.8,0.85,0.9]

    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,tapir,early)]
    return fmt,exp

def ycsb_tapir_writes():
    wl = 'YCSB'
    nnodes = [8]

    algos=['NO_WAIT']
    tapir=['true']
    early=['false']
    base_table_size=1048576
    # txn_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    txn_write_perc = [1]
    # txn_write_perc = [0.0]
    tup_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    load = [320]
    tcnt = [40]
    skew = [0.2]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,tapir,early)]
    return fmt,exp


def ycsb_early_writes():
    wl = 'YCSB'
    nnodes = [8]

    algos=['NO_WAIT']
    tapir=['false']
    early=['true']
    base_table_size=1048576
    # txn_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    txn_write_perc = [1]
    # tup_write_perc = [0.0]
    tup_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    load = [320]
    tcnt = [40]
    skew = [0.2]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,tapir,early)]
    return fmt,exp


def isolation_levels():
    wl = 'YCSB'
    nnodes = [1,2,4,8,16,32,64]
    algos=['NO_WAIT']
    levels=["READ_UNCOMMITTED","READ_COMMITTED","SERIALIZABLE","NOLOCK"]
    base_table_size=2097152*8
    load = [10000]
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    skew = [0.6,0.7]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","ISOLATION_LEVEL","MAX_TXN_IN_FLIGHT","ZIPF_THETA"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,level,ld,sk] for txn_wr_perc,tup_wr_perc,algo,sk,ld,n,level in itertools.product(txn_write_perc,tup_write_perc,algos,skew,load,nnodes,levels)]
    return fmt,exp

# def ycsb_partitions():
#     wl = 'YCSB'
#     nnodes = [16]
#     algos=['NO_WAIT']
#     # load = [10000,12000]
#     load = [80]
#     nparts = [2,6,8]
#     # nparts = [2,4,6,8]
#     base_table_size= 1048576
#     txn_write_perc = [1]
#     tup_write_perc = [0.5]
#     tcnt = [10]
#     skew = [0.2]
#     rpq =  16
#     fmt = ["WORKLOAD","NODE_CNT","CC_ALG","REQ_PER_QUERY","PART_PER_TXN","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","ONLY_ONE_HOME","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
#     exp = [[wl,n,algo,rpq,p,base_table_size*n,tup_wr_perc,txn_wr_perc,'false',ld,sk,thr,1] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts)]
#     return fmt,exp


def ycsb_tapir_partitions():
    wl = 'YCSB'
    # nnodes = [15]
    nnodes = [12]
    algos=['NO_WAIT']
    tapir=['true']
    early=['false']
    # load = [10000,12000]
    load = [240]
    # nparts = [6]
    nparts = [2,3,4,5,6]
    base_table_size= 1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    tcnt = [20]
    skew = [0.2]
    rpq =  10
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","REQ_PER_QUERY","PART_PER_TXN","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","CENTER_CNT","USE_TAPIR","EARLY_PREPARE","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,n,algo,rpq,p,base_table_size*n,tup_wr_perc,txn_wr_perc,4,ir,er,ld,sk,thr,1] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts,tapir,early)]
    return fmt,exp

def ycsb_early_partitions():
    wl = 'YCSB'
    # nnodes = [15]
    nnodes = [12]
    algos=['NO_WAIT']
    tapir=['false']
    early=['true']
    load = [240]
    # nparts = [6]
    nparts = [2,3,4,5,6]
    base_table_size= 1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    tcnt = [20]
    skew = [0.2]
    rpq =  10
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","REQ_PER_QUERY","PART_PER_TXN","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","CENTER_CNT","USE_TAPIR","EARLY_PREPARE","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,n,algo,rpq,p,base_table_size*n,tup_wr_perc,txn_wr_perc,4,ir,er,ld,sk,thr,1] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts,tapir,early)]
    return fmt,exp


def ycsb_tapir_dcs():
    wl = 'YCSB'
    # nnodes = [15]
    nnodes = [12]
    algos=['NO_WAIT']
    tapir=['true']
    early=['false']
    # load = [10000,12000]
    load = [240]
    # nparts = [6]
    nparts = [6]
    ndcs = [2,3,4,5,6]
    base_table_size= 1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    tcnt = [20]
    skew = [0.2]
    rpq =  12
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","REQ_PER_QUERY","PART_PER_TXN","DC_PER_TXN","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","CENTER_CNT","USE_TAPIR","EARLY_PREPARE","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,n,algo,rpq,p,dc,base_table_size*n,tup_wr_perc,txn_wr_perc,6,ir,er,ld,sk,thr,0] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p,dc,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts,ndcs,tapir,early)]
    return fmt,exp

def ycsb_early_dcs():
    wl = 'YCSB'
    # nnodes = [15]
    nnodes = [12]
    algos=['NO_WAIT']
    tapir=['false']
    early=['true']
    # load = [10000,12000]
    load = [240]
    # nparts = [6]
    nparts = [6]
    ndcs = [2,3,4,5,6]
    base_table_size= 1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    tcnt = [20]
    skew = [0.2]
    rpq =  12
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","REQ_PER_QUERY","PART_PER_TXN","DC_PER_TXN","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","CENTER_CNT","USE_TAPIR","EARLY_PREPARE","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,n,algo,rpq,p,dc,base_table_size*n,tup_wr_perc,txn_wr_perc,6,ir,er,ld,sk,thr,0] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p,dc,ir,er in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts,ndcs,tapir,early)]
    return fmt,exp

def ycsb_partitions_distr():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    load = [10000]
    nparts = [2,4,6,8,10,12,14,16]
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    tcnt = [4]
    skew = [0.6]
    rpq =  16
    fmt = ["WORKLOAD","REQ_PER_QUERY","PART_PER_TXN","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,rpq,p,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,1] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts)]
    return fmt,exp

def tpcc_scaling():
    wl = 'TPCC'
    nnodes = [4]
    # nnodes = [16,20]
    # nnodes = [4,8,12,16,20]
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
    nalgos=['NO_WAIT']
    npercpay=[1.0]
    # npercpay=[1.0]
    wh=16
    # wh=64
    load = [80]
    tcnt = [10]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","CLIENT_NODE_CNT","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,1,tif,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling_n():
    wl = 'TPCC'
    nnodes = [3,6,9,12]
    tapir=['false']
    early=['false']
    # nnodes = [16,20]
    # nnodes = [4,8,12,16,20]
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
    nalgos=['NO_WAIT']
    npercpay=[0.0]
    # npercpay=[1.0]
    wh=16
    # wh=64
    load = [320]
    tcnt = [20]
    ctcnt = [1]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","CLIENT_NODE_CNT","USE_TAPIR","EARLY_PREPARE","CENTER_CNT","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,1,ir,er,3,thr*n,thr,cthr] for thr,cthr,tif,pp,n,cc,ir,er in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos,tapir,early)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp
def tpcc_scaling_p():
    wl = 'TPCC'
    nnodes = [3,6,9,12]
    tapir=['false']
    early=['false']
    # nnodes = [3,6,9,12,15]
    nalgos=['NO_WAIT']

    npercpay=[1.0]
    # npercpay=[1.0]
    wh=16
    # wh=64
    load = [20000]
    tcnt = [20]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","CLIENT_NODE_CNT","USE_TAPIR","EARLY_PREPARE","CENTER_CNT","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,1,ir,er,3,thr*n,thr,cthr] for thr,cthr,tif,pp,n,cc,ir,er in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos,tapir,early)]
    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp
def tpcc_scaling_n_early():
    wl = 'TPCC'
    nnodes = [3,6,9,12]
    tapir=['false']
    early=['true']
    # nnodes = [16,20]
    # nnodes = [4,8,12,16,20]
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
    nalgos=['NO_WAIT']
    npercpay=[0.0]
    # npercpay=[1.0]
    wh=16
    # wh=64
    load = [240]
    tcnt = [20]
    ctcnt = [1]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","CLIENT_NODE_CNT","USE_TAPIR","EARLY_PREPARE","CENTER_CNT","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,1,ir,er,3,thr*n,thr,cthr] for thr,cthr,tif,pp,n,cc,ir,er in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos,tapir,early)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp
def tpcc_scaling_p_early():
    wl = 'TPCC'
    nnodes = [3,6,9,12]
    tapir=['false']
    early=['true']
    # nnodes = [3,6,9,12,15]
    nalgos=['NO_WAIT']

    npercpay=[1.0]
    # npercpay=[1.0]
    wh=16
    # wh=64
    load = [20000]
    tcnt = [20]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","CLIENT_NODE_CNT","USE_TAPIR","EARLY_PREPARE","CENTER_CNT","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,1,ir,er,3,thr*n,thr,cthr] for thr,cthr,tif,pp,n,cc,ir,er in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos,tapir,early)]
    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_cstress():
    wl = 'TPCC'
    nnodes = [1]
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
    nalgos=['MVCC']
    #nalgos=['NO_WAIT']
    npercpay=[0.0]
    # npercpay=[0.0]
    wh=128
    # wh=64
    # load = [1000,2000,3000,4000,5000]
    load = [5000]
    tcnt = [16]
    ctcnt = [16]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]
#
    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_cstress1():
    wl = 'TPCC'
    nnodes = [1]
    # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
    nalgos=['MAAT','MVCC','TIMESTAMP','OCC','DLI_DTA3','DLI_OCC']
    #nalgos=['NO_WAIT']
    npercpay=[0.0]
    # npercpay=[0.0]
    wh=128
    # wh=64
    # load = [1000,2000,3000,4000,5000]
    load = [5000]
    tcnt = [16]
    ctcnt = [16]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_cstress2():
    wl = 'TPCC'
    nnodes = [1]
    nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
    npercpay=[0.0]
    # npercpay=[0.0]
    wh=128
    # wh=64
    # load = [10000,20000,30000,40000,50000]
    load = [5000]
    #load = [1000]
    tcnt = [16]
    ctcnt = [16]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp
# def tpcc_cstress2():
#     wl = 'YCSB'
#     nnodes = [2]
#     # nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
#     # npercpay=[0.0]
#     base_table_size=2097152*8
#     txn_write_perc = [0.5]
#     tup_write_perc = [0.5]
#     skew = [0.6]
#     # wh=64
#     # load = [10000,20000,30000,40000,50000]
#     load = [10000,20000]
#     tcnt = [16]
#     ctcnt = [16]
#     # fmt = ["WORKLOAD","NODE_CNT","CC_ALG","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
#     # exp = [[wl,n,cc,wh*n,tif,thr,cthr] for thr,cthr,tif,n,cc in itertools.product(tcnt,ctcnt,load,nnodes,nalgos)]

#     fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT"]
#     exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,cthr] for thr,cthr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,ctcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,nalgos)]
#     # wh=4
#     # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
#     return fmt,exp
def tpcc_cstress3():
    wl = 'TPCC'
    nnodes = [1]
    nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
    #nalgos=['NO_WAIT']
    npercpay=[0.0]
    # npercpay=[0.0]
    wh=128
    # wh=64
    load = [1000,2000,3000,4000,5000]
    #load = [1000]
    tcnt = [60]
    ctcnt = [60]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr] for thr,cthr,tif,pp,n,cc in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos)]

    # wh=4
    # exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling1():
    wl = 'TPCC'
    nnodes = [1,2]
    nalgos=dta_target_algos
    npercpay=[0.0]
    wh=128
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_stress1():
    wl = 'TPCC'
    nnodes = [1]
    nalgos=dta_target_algos
    npercpay=[0.0]
    wh=128
    load = tpcc_loads
    #load = tpcc_loads
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling_debug():
    wl = 'TPCC'
    nnodes = [1,2]
    nalgos=['NO_WAIT']
    npercpay=[1.0]
    wh=32
    load = [20000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling2():
    wl = 'TPCC'
    nnodes = [1,2]
    # nnodes = [4]
    nalgos=dta_target_algos
    npercpay=[1.0]
    wh=128
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_stress2():
    wl = 'TPCC'
    nnodes = [1]
    # nnodes = [4]
    #nalgos=['TIMESTAMP']
    nalgos=dta_target_algos
    npercpay=[1.0]
    wh=128
    load = tpcc_loads
    #load = tpcc_loads
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling3():
    wl = 'TPCC'
    nnodes = [1,2]
    # nnodes = [4]
    nalgos=dta_target_algos
    npercpay=[0.5]
    wh=128
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_dist_ratio():
    wl = 'TPCC'
    nnodes = [16]
    nalgos=['WAIT_DIE','MAAT','MVCC','TIMESTAMP','OCC']
    npercpay=[1.0]
    wh=32
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp


def tpcc_scaling_whset():
    wl = 'TPCC'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
    npercpay=[0.0,0.5,1.0]
    wh=128
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH"]
    exp = [[wl,n,cc,pp,wh] for pp,n,cc in itertools.product(npercpay,nnodes,nalgos)]
    wh=256
    exp = exp + [[wl,n,cc,pp,wh] for pp,n,cc in itertools.product(npercpay,nnodes,nalgos)]
    return fmt,exp

def ycsb_skew_abort_writes():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    base_table_size=2097152*8
    txn_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","YCSB_ABORT_MODE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,'true'] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_skew_abort():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.825,0.85,0.875,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","YCSB_ABORT_MODE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,'true'] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp


def ycsb_partitions_abort():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    load = [10000]
    nparts = [1,2,4,6,8,10,12,14,16]
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    tcnt = [4]
    skew = [0.6]
    rpq =  16
    fmt = ["WORKLOAD","REQ_PER_QUERY","PART_PER_TXN","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT","YCSB_ABORT_MODE"]
    exp = [[wl,rpq,p,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,1,'true'] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts)]
    return fmt,exp

def network_sweep():
    wl = 'YCSB'
    nalgos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','TIMESTAMP','CALVIN']
    algos=['CALVIN']
# Network delay in ms
    ndelay=[0,0.05,0.1,0.25,0.5,0.75,1,1.75,2.5,5,7.5,10,17.5,25,50]
    ndelay = [int(n*1000000) for n in ndelay]
    nnodes = [2]
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6]
    base_table_size=2097152*8
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","NETWORK_DELAY_TEST","NETWORK_DELAY","SET_AFFINITY"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,"true",d,"false"] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,d,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,ndelay,nalgos)]
    return fmt,exp
def ycsb_thread():
    wl = 'YCSB'
    #nnodes = [1,2,4,8,16,32,64]
    #nnodes = [1,2,4,8,16,32]
    nnodes = [3]
    tapir = ['false']
    # algos=['CALVIN','MAAT','MVCC','NO_WAIT','TIMESTAMP','WAIT_DIE']
    algos=['MV_NO_WAIT']
    clvs=['CLV1']
    base_table_size=1048576
    # base_table_size=1048576*8
    #base_table_size=2097152*8
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [5,7,8,10]
    # tcnt = [6]
    ctcnt = [4]
    scnt = [1]
    rcnt = [1]
    skew = [0.5]
    #skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","CLV","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","ZIPF_THETA","THREAD_CNT","CLIENT_THREAD_CNT","SEND_THREAD_CNT","REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","CLIENT_REM_THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,clv,txn_wr_perc,ld,ir,sk,thr,cthr,sthr,rthr,sthr,rthr] for algo,thr,cthr,sthr,rthr,txn_wr_perc,tup_wr_perc,sk,ld,n,ir,clv in itertools.product(algos,tcnt,ctcnt,scnt,rcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,tapir,clvs)]
    #txn_write_perc = [0.0]
    #skew = [0.0]
    #exp = exp + [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp



#测试数据中心间事务比率的影响
def ycsb_cross_dc1():
    wl = 'YCSB'
    nnodes = [3]
    algos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    tapir=['false']
    early=['false']
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [320]
    tcnt = [7]  #THREAD_CNT
    skew = [0.2]
    # cross_dc_perc = [0] 
    cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT","CROSS_DC_TXN_PERC","CLV"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr,cro_dc_perc,clv] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,ir,er,clv in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,tapir,early,clvs)]
    return fmt,exp




def ycsb_dcs():
    wl = 'YCSB'
    # nnodes = [15]
    nnodes = [3]
    algos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    tapir=['false']
    early=['false']
    # load = [10000,12000]
    load = [240]
    # nparts = [6]
    nparts = [3]
    ndcs = [2,3]
    base_table_size= 1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    tcnt = [20]
    skew = [0.2]
    rpq =  12
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","REQ_PER_QUERY","PART_PER_TXN","DC_PER_TXN","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","CENTER_CNT","USE_TAPIR","EARLY_PREPARE","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT","CLV"]
    exp = [[wl,n,algo,rpq,p,dc,base_table_size*n,tup_wr_perc,txn_wr_perc,6,ir,er,ld,sk,thr,0,clv] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p,dc,ir,er,clv in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts,ndcs,tapir,early,clvs)]
    return fmt,exp

def tpcc_thread():
    wl = 'TPCC'
    #nnodes = [1,2,4,8,16,32,64]
    #nnodes = [1,2,4,8,16,32]
    nnodes = [3]
    algos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']

    npercpay=[0.0]
    # npercpay=[1.0]
    wh=40
    #base_table_size=2097152*8
    load = [10000]
    # tcnt = [4]
    tcnt = [4,8,12,16,20,24,28,32,36,40]
    ctcnt = [3]
    #skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","CLV"]
    exp = [[wl,n,cc,pp,thr*n,tif,thr,cthr,clv] for cc,thr,cthr,tif,pp,n,clv in itertools.product(algos,tcnt,ctcnt,load,npercpay,nnodes,clvs)]
    #txn_write_perc = [0.0]
    #skew = [0.0]
    #exp = exp + [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp
def ycsb_writes1():
    wl = 'YCSB'
    nnodes = [3]
    algos=['MV_NO_WAIT','MV_WOUND_WAIT']
    tapir=['false']
    early=['false']
    clvs=['CLV1','CLV2','CLV3']
    base_table_size=1048576
    # txn_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    txn_write_perc = [1]
    # txn_write_perc = [0.0]
    tup_write_perc = [0.0,0.5,0.9]
    load = [10000]
    tcnt = [10]
    skew = [0.2]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","CLV","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,clv,txn_wr_perc,ld,ir,er,sk,thr] for algo,thr,txn_wr_perc,tup_wr_perc,ld,n,sk,ir,er,clv in itertools.product(algos,tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,tapir,early,clvs)]
    return fmt,exp

def ycsb_skew1():
    wl = 'YCSB'
    nnodes = [3]
    algos=['MV_NO_WAIT','MV_WOUND_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    tapir=['false']
    early=['false']
    
    # base_table_size=1048576*10
    base_table_size=1048576
    #base_table_size=2097152*8

    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [10000] #node_cnt*tcnt

    tcnt = [10]  #THREAD_CNT
    #skew = [0.2]
    skew = [0.0,0.5,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","CLV","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,clv,txn_wr_perc,ld,ir,er,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,algo,sk,ir,er,clv in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,algos,skew,tapir,early,clvs)]
    return fmt,exp


def ycsb_hot1():
    wl = 'YCSB'
    nnodes = [3]
    algos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    tapir=['false']
    early=['false']
    hot = 'HOT'
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.3]
    # tup_write_perc = [1]
    load = [10000]
    tcnt = [10]  #THREAD_CNT
    acess_perc = [0.0,0.5,1.0]
    cross_dc_perc = [1.0]
    # network_delay = ['50000000UL','100000000UL','150000000UL','200000000UL','250000000UL','300000000UL','350000000UL','400000000UL','450000000UL','500000000UL'] 
    network_delay = ['0UL'] 
    # cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ACCESS_PERC","THREAD_CNT","CROSS_DC_TXN_PERC", "NETWORK_DELAY","CLV","SKEW_METHOD"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,acess_perc,thr,cro_dc_perc,net_del,clv,hot] for thr,txn_wr_perc,tup_wr_perc,ld,n,acess_perc,algo,cro_dc_perc,net_del,ir,er,clv in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,acess_perc,algos,cross_dc_perc,network_delay,tapir,early,clvs)]
    return fmt,exp

def ycsb_cross_dc1():
    wl = 'YCSB'
    nnodes = [3]
    algos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    tapir=['false']
    early=['false']
    hot = 'HOT'
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    # tup_write_perc = [1]
    load = [10000]
    tcnt = [10]  #THREAD_CNT
    acess_perc = [0.0]
    mpr = [0.0,0.5,0.9]
    # network_delay = ['50000000UL','100000000UL','150000000UL','200000000UL','250000000UL','300000000UL','350000000UL','400000000UL','450000000UL','500000000UL'] 
    network_delay = ['0UL'] 
    # cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ACCESS_PERC","THREAD_CNT","MPR", "NETWORK_DELAY","CLV","SKEW_METHOD"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,acess_perc,thr,mpr,net_del,clv,hot] for thr,txn_wr_perc,tup_wr_perc,ld,n,acess_perc,algo,mpr,net_del,ir,er,clv in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,acess_perc,algos,mpr,network_delay,tapir,early,clvs)]
    return fmt,exp

def tpcc_cross_dc1():
    wl = 'TPCC'
    nnodes = [3]
    nalgos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    npercpay=[0.0]
    wh=64
    mpr = [0.0,0.5,0.9]
    # wh=64
    # load = [10000,20000,30000,40000,50000]
    load = [10000]
    #load = [1000]
    tcnt = [10]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","CLV","MPR"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr,clv,mpr] for thr,cthr,tif,pp,n,cc,clv,mpr in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos,clvs,mpr)]
    return fmt,exp


def tpcc_wh1():
    wl = 'TPCC'
    nnodes = [3]
    nalgos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    npercpay=[0.0]
    wh=[10,40,100]
    # wh=64
    # load = [10000,20000,30000,40000,50000]
    load = [10000]
    #load = [1000]
    tcnt = [10]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","CLV"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr,clv] for thr,cthr,tif,pp,n,cc,clv,wh in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos,clvs,wh)]
    return fmt,exp

def tpcc_neworder_payment1():
    wl = 'TPCC'
    nnodes = [3]
    nalgos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    npercpay=[0.0,0.5,0.9]
    wh=64
    # wh=64
    # load = [10000,20000,30000,40000,50000]
    load = [10000]
    #load = [1000]
    tcnt = [10]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","CLV"]
    exp = [[wl,n,cc,pp,wh*n,tif,thr,cthr,clv] for thr,cthr,tif,pp,n,cc,clv in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos,clvs)]
    return fmt,exp
def ycsb_writes():
    wl = 'YCSB'
    nnodes = [3]

    algos=['MV_NO_WAIT','MV_WOUND_WAIT']
    tapir=['false']
    early=['false']
    clvs=['CLV1','CLV2','CLV3']
    base_table_size=1048576
    # txn_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    txn_write_perc = [1]
    # txn_write_perc = [0.0]
    tup_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    load = [10000]
    tcnt = [10]
    skew = [0.2]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","CLV","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,clv,txn_wr_perc,ld,ir,er,sk,thr] for algo,thr,txn_wr_perc,tup_wr_perc,ld,n,sk,ir,er,clv in itertools.product(algos,tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,tapir,early,clvs)]
    return fmt,exp

def ycsb_skew():
    wl = 'YCSB'
    nnodes = [3]
    algos=['MV_NO_WAIT','MV_WOUND_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    tapir=['false']
    early=['false']
    
    # base_table_size=1048576*10
    base_table_size=1048576
    #base_table_size=2097152*8

    txn_write_perc = [1]
    tup_write_perc = [0.5]
    load = [10000] #node_cnt*tcnt

    tcnt = [10]  #THREAD_CNT
    #skew = [0.2]
    skew = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","CLV","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,clv,txn_wr_perc,ld,ir,er,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,algo,sk,ir,er,clv in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,algos,skew,tapir,early,clvs)]
    return fmt,exp

def ycsb_network_delay():
    wl = 'YCSB'
    nnodes = [3]
    algos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    tapir=['false']
    early=['false']
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    # tup_write_perc = [1]
    load = [10000]
    tcnt = [10]  #THREAD_CNT
    skew = [0.2]
    cross_dc_perc = [1.0]
    # network_delay = ['50000000UL','100000000UL','150000000UL','200000000UL','250000000UL','300000000UL','350000000UL','400000000UL','450000000UL','500000000UL'] 
    network_delay = ['0UL'] 
    # cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ZIPF_THETA","THREAD_CNT","CROSS_DC_TXN_PERC", "NETWORK_DELAY","CLV"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,sk,thr,cro_dc_perc,net_del,clv] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo,cro_dc_perc,net_del,ir,er,clv in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos,cross_dc_perc,network_delay,tapir,early,clvs)]
    return fmt,exp

def ycsb_hot():
    wl = 'YCSB'
    nnodes = [3]
    algos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    tapir=['false']
    early=['false']
    hot = 'HOT'
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    # tup_write_perc = [1]
    load = [10000]
    tcnt = [10]  #THREAD_CNT
    acess_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    cross_dc_perc = [1.0]
    # network_delay = ['50000000UL','100000000UL','150000000UL','200000000UL','250000000UL','300000000UL','350000000UL','400000000UL','450000000UL','500000000UL'] 
    network_delay = ['0UL'] 
    # cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ACCESS_PERC","THREAD_CNT","CROSS_DC_TXN_PERC", "NETWORK_DELAY","CLV","SKEW_METHOD"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,acess_perc,thr,cro_dc_perc,net_del,clv,hot] for thr,txn_wr_perc,tup_wr_perc,ld,n,acess_perc,algo,cro_dc_perc,net_del,ir,er,clv in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,acess_perc,algos,cross_dc_perc,network_delay,tapir,early,clvs)]
    return fmt,exp

def ycsb_cross_dc():
    wl = 'YCSB'
    nnodes = [3]
    algos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    tapir=['false']
    early=['false']
    hot = 'HOT'
    base_table_size=1048576
    txn_write_perc = [1]
    tup_write_perc = [0.5]
    # tup_write_perc = [1]
    load = [10000]
    tcnt = [10]  #THREAD_CNT
    acess_perc = [0.5]
    mpr = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    # network_delay = ['50000000UL','100000000UL','150000000UL','200000000UL','250000000UL','300000000UL','350000000UL','400000000UL','450000000UL','500000000UL'] 
    network_delay = ['0UL'] 
    # cross_dc_perc = [0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0] 


    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","USE_TAPIR","EARLY_PREPARE","ACCESS_PERC","THREAD_CNT","MPR", "NETWORK_DELAY","CLV","SKEW_METHOD"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,ir,er,acess_perc,thr,mpr,net_del,clv,hot] for thr,txn_wr_perc,tup_wr_perc,ld,n,acess_perc,algo,mpr,net_del,ir,er,clv in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,acess_perc,algos,mpr,network_delay,tapir,early,clvs)]
    return fmt,exp

def tpcc_cross_dc():
    wl = 'TPCC'
    nnodes = [3]
    nalgos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    npercpay=[0.0]
    wh=64
    mpr = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    # wh=64
    # load = [10000,20000,30000,40000,50000]
    load = [10000]
    #load = [1000]
    tcnt = [10]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","CLV","MPR"]
    exp = [[wl,n,cc,pp,wh,tif,thr,cthr,clv,mpr] for thr,cthr,tif,pp,n,cc,clv,mpr in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos,clvs,mpr)]
    return fmt,exp


def tpcc_wh():
    wl = 'TPCC'
    nnodes = [3]
    nalgos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    npercpay=[0.0]
    wh=[10,20,30,40,50,60,70,80,100]
    # wh=64
    # load = [10000,20000,30000,40000,50000]
    load = [10000]
    #load = [1000]
    tcnt = [10]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","CLV"]
    exp = [[wl,n,cc,pp,wh,tif,thr,cthr,clv] for thr,cthr,tif,pp,n,cc,clv,wh in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos,clvs,wh)]
    return fmt,exp

def tpcc_neworder_payment():
    wl = 'TPCC'
    nnodes = [3]
    nalgos=['MV_WOUND_WAIT','MV_NO_WAIT']
    clvs=['CLV1','CLV2','CLV3']
    npercpay=[0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    wh=64
    # wh=64
    # load = [10000,20000,30000,40000,50000]
    load = [10000]
    #load = [1000]
    tcnt = [10]
    ctcnt = [4]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT","THREAD_CNT","CLIENT_THREAD_CNT","CLV"]
    exp = [[wl,n,cc,pp,wh,tif,thr,cthr,clv] for thr,cthr,tif,pp,n,cc,clv in itertools.product(tcnt,ctcnt,load,npercpay,nnodes,nalgos,clvs)]
    return fmt,exp
##############################
# END PLOTS
##############################

experiment_map = {
    'ycsb_writes': ycsb_writes,
    'ycsb_skew': ycsb_skew,
    'ycsb_network_delay': ycsb_network_delay,
    'ycsb_hot': ycsb_hot,
    'ycsb_cross_dc': ycsb_cross_dc,
    'tpcc_cross_dc': tpcc_cross_dc,
    'tpcc_wh': tpcc_wh,
    'tpcc_neworder_payment': tpcc_neworder_payment,

    'ycsb_writes1': ycsb_writes1,
    'ycsb_skew1': ycsb_skew1,
    'ycsb_network_delay1': ycsb_network_delay1,
    'ycsb_hot1': ycsb_hot1,
    'ycsb_cross_dc1': ycsb_cross_dc1,
    'tpcc_cross_dc1': tpcc_cross_dc1,
    'tpcc_wh1': tpcc_wh1,
    'tpcc_neworder_payment1': tpcc_neworder_payment1,

    'pps_scaling': pps_scaling,
    'ycsb_thread': ycsb_thread,
    'ycsb_coroutine':ycsb_coroutine,
    'ycsb_scaling': ycsb_scaling,
    'ycsb_scaling_early': ycsb_scaling_early,
    'ycsb_scaling1': ycsb_scaling1,
    'ycsb_scaling_abort': ycsb_scaling_abort,
    'ycsb_scaling_l': ycsb_scaling_l,
    'ycsb_scaling_m': ycsb_scaling_m,
    'ycsb_scaling_h': ycsb_scaling_h,
    'ppr_ycsb_scaling_abort': ycsb_scaling_abort,
    'ppr_ycsb_scaling_abort_plot': ppr_ycsb_scaling_abort_plot,  
    'ycsb_early_skew':ycsb_early_skew,
    'ycsb_early_cross_dc':ycsb_early_cross_dc,
    'ycsb_early_writes':ycsb_early_writes,
    'ycsb_network_delay2': ycsb_network_delay2,
    'ycsb_early_network_delay':ycsb_early_network_delay,
    'ycsb_early_network_delay2':ycsb_early_network_delay2,
    'isolation_levels': isolation_levels,
    'ycsb_tapir_partitions':ycsb_tapir_partitions,
    'ycsb_early_partitions':ycsb_early_partitions,
    'ycsb_partitions_abort': ycsb_partitions_abort,
    'ycsb_dcs':ycsb_dcs,
    'ycsb_tapir_dcs':ycsb_tapir_dcs,
    'ycsb_early_dcs':ycsb_early_dcs,
    'ppr_ycsb_partitions_abort': ycsb_partitions_abort,
    'ppr_ycsb_partitions_abort_plot': ppr_ycsb_partitions_abort_plot,
    'tpcc_scaling': tpcc_scaling,
    'tpcc_thread': tpcc_thread,
    'tpcc_cstress':tpcc_cstress,
    'tpcc_cstress1':tpcc_cstress1,
    'tpcc_cstress2':tpcc_cstress2,
    'tpcc_cstress3':tpcc_cstress3,
    'tpcc_scaling_debug': tpcc_scaling_debug,
    'tpcc_scaling1': tpcc_scaling1,
    'tpcc_scaling2': tpcc_scaling2,
    'tpcc_scaling3': tpcc_scaling3,
    'tpcc_scaling_n':tpcc_scaling_n,
    'tpcc_scaling_p':tpcc_scaling_p,
    'tpcc_scaling_n_early':tpcc_scaling_n_early,
    'tpcc_scaling_p_early':tpcc_scaling_p_early,
    'tpcc_stress1': tpcc_stress1,
    'tpcc_stress2': tpcc_stress2,
    'tpcc_dist_ratio': tpcc_dist_ratio,
    'tpcc_scaling_whset': tpcc_scaling_whset,
    'ycsb_skew_abort': ycsb_skew_abort,
    'ppr_pps_scaling': pps_scaling,
    'ppr_pps_scaling_plot': ppr_pps_scaling_plot,
    'ppr_ycsb_scaling': ycsb_scaling,
    'ppr_ycsb_scaling_plot': ppr_ycsb_scaling_plot,
    'ecwc': ecwc,
    'ppr_ecwc': ecwc,
    'ppr_ecwc_plot': ppr_ecwc_plot,
    'ppr_ycsb_skew': ycsb_skew,
    'ppr_ycsb_skew_plot': ppr_ycsb_skew_plot,
    'ppr_ycsb_skew_abort': ycsb_skew_abort,
    'ppr_ycsb_skew_abort_plot': ppr_ycsb_skew_abort_plot,
    'ppr_ycsb_writes': ycsb_writes,
    'ppr_ycsb_writes_plot': ppr_ycsb_writes_plot,
    'ppr_ycsb_partitions_plot': ppr_ycsb_partitions_plot,
    'ppr_isolation_levels': isolation_levels,
    'ppr_isolation_levels_plot': ppr_isolation_levels_plot,
    'ppr_tpcc_scaling': tpcc_scaling,
    'ppr_tpcc_scaling_plot': ppr_tpcc_scaling_plot,
    'network_sweep' : network_sweep,
    'ppr_network' : network_sweep,
    'ppr_network_plot' : ppr_network_plot,
    'ycsb_tapir_thread': ycsb_tapir_thread,
    'ycsb_tapir_cross_dc': ycsb_tapir_cross_dc,
    'ycsb_tapir_network_delay':ycsb_tapir_network_delay,
    'ycsb_tapir_network_delay2':ycsb_tapir_network_delay2,
    'ycsb_tapir_skew':ycsb_tapir_skew,
    'ycsb_tapir_writes':ycsb_tapir_writes,
}


# Default values for variable configurations
configs = {
    "NODE_CNT" : 3,
    "CENTER_CNT": 3,
    "THREAD_CNT": 7,
    "REPLICA_CNT": 0,
    "REPLICA_TYPE": "AP",
    "REM_THREAD_CNT": 1,
    "SEND_THREAD_CNT": 1,
    "CLIENT_NODE_CNT" : 1,
    "CLIENT_THREAD_CNT" : 4,
    "CLIENT_REM_THREAD_CNT" : 1,
    "CLIENT_SEND_THREAD_CNT" : 1,
    "MAX_TXN_PER_PART" : 10000,
    "WORKLOAD" : "YCSB",
    "CC_ALG" : "WAIT_DIE",
    "MPR" : 1.0,
    "TPORT_TYPE":"IPC",
    "TPORT_PORT":"18000",
    "PART_CNT": "NODE_CNT",
    "PART_PER_TXN": 2,
    "DC_PER_TXN": 2,
    "MAX_TXN_IN_FLIGHT": 10000,
    "NETWORK_DELAY": '100000000UL',
    "COROUTINE_CNT": 4,
    "ONLY_ONE_HOME": 'false',
    "NETWORK_DELAY_TEST": 'false',
    "DONE_TIMER": "1 * 20 * BILLION // ~1 minutes",
    "WARMUP_TIMER": "1 * 10 * BILLION // ~1 minutes",
    "SEQ_BATCH_TIMER": "5 * 1 * MILLION // ~5ms -- same as CALVIN paper",
    "BATCH_TIMER" : "0",
    "PROG_TIMER" : "10 * BILLION // in s",
    "NETWORK_TEST" : "false",
    "ABORT_PENALTY": "10 * 1000000UL   // in ns.",
    "ABORT_PENALTY_MAX": "5 * 100 * 1000000UL   // in ns.",
    "MSG_TIME_LIMIT": "0",
    "MSG_SIZE_MAX": 4096,
    "TXN_WRITE_PERC":0.2,
    "PRIORITY":"PRIORITY_ACTIVE",
    "TWOPL_LITE":"false",
    "USE_TAPIR":"false",
    "EARLY_PREPARE":"false",
#YCSB
    "INIT_PARALLELISM" : 1,
    "TUP_WRITE_PERC":0.2,
    "ZIPF_THETA":0.3,
    "ACCESS_PERC":0.03,
    "DATA_PERC": 100,
    "REQ_PER_QUERY": 10,
    "SYNTH_TABLE_SIZE":"65536",
    "CROSS_DC_TXN_PERC":1,
#TPCC
    "MPR":1.0,
    "NUM_WH": 32,
    "PERC_PAYMENT":0.0,
    "DEBUG_DISTR":"false",
    "DEBUG_ALLOC":"false",
    "DEBUG_RACE":"false",
    "MODE":"NORMAL_MODE",
    "SHMEM_ENV":"false",
    "STRICT_PPT":1,
    "SET_AFFINITY":"true",
    "LOGGING":"false",
    "SERVER_GENERATE_QUERIES":"false",
    "SKEW_METHOD":"ZIPF",
    "ENVIRONMENT_EC2":"false",
    "YCSB_ABORT_MODE":"false",
    "LOAD_METHOD": "LOAD_MAX",
    "ISOLATION_LEVEL":"SERIALIZABLE"
}

