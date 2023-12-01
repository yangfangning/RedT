import re, sys

summary = {}


def get_summary(sfile):
    with open(sfile, 'r') as f:
        for line in f:
            if 'summary' in line:
                results = re.split(',', line.rstrip('\n')[10:])
                for r in results:
                    (name, val) = re.split('=', r)
                    val = float(val)
                    if name not in summary.keys():
                        summary[name] = [val]
                    else:
                        summary[name].append(val)


for arg in sys.argv[1:]:
    get_summary(arg)
names = summary.keys()

# sfile = '/home/ljy/all-deneva/results/20220527-112657/0_NO_WAIT_TIF-20000_N-4_SYNTH_TABLE_SIZE-4194304_T-4_TWR-0.5_WR-0.5_YCSB_SKEW-0.3_20220527-112657.out'
# get_summary(sfile)

# a, b, c = 0, 0, 0
# if 'tput' in summary:
#     a = sum(summary['tput'])
# if 'total_txn_abort_cnt' in summary and 'total_txn_commit_cnt' in summary and summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0] != 0:
#     b = summary['total_txn_abort_cnt'][0] / (summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0])
# if 'remote_txn_commit_cnt' in summary and 'remote_txn_abort_cnt' in summary and 'total_txn_commit_cnt' in summary and 'total_txn_abort_cnt' in summary and summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0] != 0:
#     c = (summary['remote_txn_commit_cnt'][0] + summary['remote_txn_abort_cnt'][0]) / (
#             summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0])

# print a, b, c

a, b, c, d, f = 0, 0, 0, 0, 0
# e, g, h = 0, 0, 0
if 'tput' in summary:
    a = sum(summary['tput'])
if 'total_txn_abort_cnt' in summary and 'total_txn_commit_cnt' in summary and summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0] != 0:
    b = sum(summary['total_txn_abort_cnt']) / (sum(summary['total_txn_commit_cnt']) + sum(summary['total_txn_abort_cnt']))
    #b = summary['total_txn_abort_cnt'][0] / (summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0])
if 'lock_retry_cnt' in summary:
    c = sum(summary['lock_retry_cnt'])
if 'read_retry_cnt' in summary:
    d = sum(summary['read_retry_cnt'])
if 'avg_trans_total_run_time' in summary:
    e = sum(summary['avg_trans_total_run_time']) 
if 'avg_trans_commit_total_run_time' in summary:
    i = sum(summary['avg_trans_commit_total_run_time']) 
if 'remote_txn_cnt' in summary:
    g = sum(summary['remote_txn_cnt']) 
if 'txn_cnt' in summary:
    h = sum(summary['txn_cnt']) 
if 'worker_oneside_cnt' in summary and 'total_txn_commit_cnt' in summary and summary['total_txn_commit_cnt'][0] + summary['total_txn_abort_cnt'][0] != 0:
    f = sum(summary['worker_oneside_cnt']) / (sum(summary['total_txn_commit_cnt']) + sum(summary['total_txn_abort_cnt']))
if 'trans_read_write_count' in summary:
    j = sum(summary['trans_read_write_count']) 
if 'trans_fin_count' in summary:
    k = sum(summary['trans_fin_count']) 
if 'trans_read_write_time' in summary:
    m = sum(summary['trans_read_write_time']) 
if 'trans_fin_time' in summary:
    n = sum(summary['trans_fin_time'])
if 'max_num_msgs_rw' in summary:
    o = max(summary['max_num_msgs_rw']) 
if 'max_num_msgs_prep' in summary:
    s = max(summary['max_num_msgs_prep']) 
if 'max_num_msgs_commit' in summary:
    t = max(summary['max_num_msgs_commit']) 
if 'avg_num_msgs_rw' in summary:
    z = sum(summary['avg_num_msgs_rw']) 
if 'avg_num_msgs_prep' in summary:
    p = sum(summary['avg_num_msgs_prep']) 
if 'avg_num_msgs_commit' in summary:
    q = sum(summary['avg_num_msgs_commit']) 
if 'avg_num_rts_commit' in summary:
    u = sum(summary['avg_num_rts_commit'])

print a, b, e, i, j, k, m, n, o, s, t, z, p, q, u
# f, g / h
# print a, b, f, g, h