/* Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: hongyaozhao@tencent.com
 *
 */
//假设有一个全局结构，里面存着单个服务器上的,好像存全局不行，在事务表里实现，这里面还要改造，加入prepared时间，每当有一个事务返回回应，就会更新prepared时间，当最后一个分布式子事务回应后会更新提交时间戳。
#include "txn.h"
#include "row.h"
#include "manager.h"
#include "ssi.h"
#include "row_ssi.h"
#include "mem_alloc.h"

void Row_ssi::init(row_t * row) {
    _row = row;
    si_read_lock = NULL;
    write_lock = NULL;

    owner = NULL;

    prereq_mvcc = NULL;
    readhis = NULL;
    writehis = NULL;
    readhistail = NULL;
    writehistail = NULL;
    blatch = false;
    latch = (pthread_mutex_t *) mem_allocator.alloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(latch, NULL);
    whis_len = 0;
    rhis_len = 0;
    commit_lock = 0;
    preq_len = 0;
}

row_t * Row_ssi::clear_history(TsType type, ts_t ts) {
    SSIHisEntry ** queue;
    SSIHisEntry ** tail;
    switch (type) {
    case R_REQ:
        queue = &readhis;
        tail = &readhistail;
        break;
    case W_REQ:
        queue = &writehis;
        tail = &writehistail;
        break;
    default:
        assert(false);
    }
    SSIHisEntry * his = *tail;
    SSIHisEntry * prev = NULL;
    row_t * row = NULL;
    while (his && his->prev && his->prev->ts < ts) {
        prev = his->prev;
        assert(prev->ts >= his->ts);
        if (row != NULL) {
            row->free_row();
            mem_allocator.free(row, sizeof(row_t));
        }
        row = his->row;
        his->row = NULL;
        return_his_entry(his);
        his = prev;
        if (type == R_REQ) rhis_len --;
        else whis_len --;
    }
    *tail = his;
    if (*tail) (*tail)->next = NULL;
    if (his == NULL) *queue = NULL;
    return row;
}

SSIReqEntry * Row_ssi::get_req_entry() {
    return (SSIReqEntry *) mem_allocator.alloc(sizeof(SSIReqEntry));
}

void Row_ssi::return_req_entry(SSIReqEntry * entry) {
    mem_allocator.free(entry, sizeof(SSIReqEntry));
}

SSIHisEntry * Row_ssi::get_his_entry() {
    return (SSIHisEntry *) mem_allocator.alloc(sizeof(SSIHisEntry));
}

void Row_ssi::return_his_entry(SSIHisEntry * entry) {
    if (entry->row != NULL) {
        entry->row->free_row();
        mem_allocator.free(entry->row, sizeof(row_t));
    }
    mem_allocator.free(entry, sizeof(SSIHisEntry));
}

void Row_ssi::buffer_req(TsType type, TxnManager * txn)
{
    SSIReqEntry * req_entry = get_req_entry();
    assert(req_entry != NULL);
    req_entry->txn = txn;
    req_entry->ts = txn->get_start_timestamp();
    req_entry->starttime = get_sys_clock();
    if (type == P_REQ) {
        preq_len ++;
        STACK_PUSH(prereq_mvcc, req_entry);
    }
}

// for type == R_REQ
//     debuffer all non-conflicting requests
// for type == P_REQ
//   debuffer the request with matching txn.
SSIReqEntry * Row_ssi::debuffer_req( TsType type, TxnManager * txn) {
    SSIReqEntry ** queue = &prereq_mvcc;
    SSIReqEntry * return_queue = NULL;

    SSIReqEntry * req = *queue;
    SSIReqEntry * prev_req = NULL;
    if (txn != NULL) {
        assert(type == P_REQ);
        while (req != NULL && req->txn != txn) {
            prev_req = req;
            req = req->next;
        }
        assert(req != NULL);
        if (prev_req != NULL)
            prev_req->next = req->next;
        else {
            assert( req == *queue );
            *queue = req->next;
        }
        preq_len --;
        req->next = return_queue;
        return_queue = req;
    }
    return return_queue;
}

void Row_ssi::insert_history(ts_t ts, TxnManager * txn, row_t * row)
{
    SSIHisEntry * new_entry = get_his_entry();
    new_entry->ts = ts;
    new_entry->txn = txn->get_txn_id();
    new_entry->row = row;
    if (row != NULL) {
        whis_len ++;
    } else {
        rhis_len ++;
    }
    SSIHisEntry ** queue = (row == NULL)?
        &(readhis) : &(writehis);
    SSIHisEntry ** tail = (row == NULL)?
        &(readhistail) : &(writehistail);
    SSIHisEntry * his = *queue;
    while (his != NULL && ts < his->ts) {
        his = his->next;
    }

    if (his) {
        LIST_INSERT_BEFORE(his, new_entry,(*queue));
    } else
        LIST_PUT_TAIL((*queue), (*tail), new_entry);
}

SSILockEntry * Row_ssi::get_entry() {
    SSILockEntry * entry = (SSILockEntry *)
        mem_allocator.alloc(sizeof(SSILockEntry));
    entry->type = LOCK_NONE;
    entry->txn = 0;

    return entry;
}

void Row_ssi::get_lock(lock_t type, TxnManager * txn) {
    SSILockEntry * entry = get_entry();
    entry->type = type;
    entry->start_ts = get_sys_clock();
    entry->txn = txn->get_txn_id();
    if (type == DLOCK_SH)
        STACK_PUSH(si_read_lock , entry);
    if (type == DLOCK_EX)
        STACK_PUSH(write_lock , entry);
    if (type == LOCK_COM)
        commit_lock = txn->get_txn_id();
}

void Row_ssi::release_lock(lock_t type, TxnManager * txn) {
    if (type == DLOCK_SH) {
        SSILockEntry * read = si_read_lock;
        SSILockEntry * pre_read = NULL;
        while (read != NULL) {
            if (read->txn == txn->get_txn_id()) {
                assert(read != NULL);
                if (pre_read != NULL)
                    pre_read->next = read->next;
                else {
                    assert( read == si_read_lock );
                    si_read_lock = read->next;
                }
                read->next = NULL;
            }
            pre_read = read;
            read = read->next;
        }
    }
    if (type == DLOCK_EX) {
        SSILockEntry * write = write_lock;
        SSILockEntry * pre_write = NULL;
        while (write != NULL) {
            if (write->txn == txn->get_txn_id()) {
                assert(write != NULL);
                if (pre_write != NULL)
                    pre_write->next = write->next;
                else {
                    assert( write == write_lock );
                    write_lock = write->next;
                }
                write->next = NULL;
            }
            pre_write = write;
            write = write->next;
        }
    }
    if (type == LOCK_COM) {
        if (commit_lock == txn->get_txn_id()) commit_lock = 0;
    }
}

// RC Row_ssi::can_lock(lock_t type, TxnManager * txn, ts_t start_ts){

//     if(type ==R_REQ){//对于读来说，要么rock要么终止（超时终止），在读之前会用读时间戳更新全局读时间戳
//         //判断事务这行上有没有写锁
//         if(owner!=NULL && owner->txn.start != runing){//如果事务上锁且上锁事务不是在运行状态，就要判断是否需要等待事务状态，
//             //有锁
//             //判断要读哪一个数据，如果读的那个行版本不是最新的，通过写历史的头来判断

//         }else{
//             SSIHisEntry * whis = writehis;
//             while (whis != NULL && whis->ts > start_ts) {
//                 whis = whis->next;
//             }
//         }
//     }else if (type == W_REQ)
//     {
//         //创建一个新行
//     }
    

// }

RC Row_ssi::access(TxnManager * txn, TsType type, row_t * row) {
    RC rc = RCOK;
    ts_t start_ts = txn->get_start_timestamp();
    uint64_t starttime = get_sys_clock();
    txnid_t txnid = txn->get_txn_id();
    if (g_central_man) {
        glob_manager.lock_row(_row);
    } else {
        pthread_mutex_lock(latch);
    }
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
    if (type == R_REQ) {//读一定是可以读到的，就是可能会等待前面事务确定
        //判断是否可以上锁，可以上锁就上锁，否则lock_for_read
        //can_lock(DLOCK_SH, txn, start_ts)

        // Add the si-read lock
        get_lock(DLOCK_SH, txn);
        // Traverse the whole write lock
        SSILockEntry * write = write_lock;
        while (write != NULL) {
            if (write->txn == txnid) {
                write = write->next;
                continue;
            }
            inout_table.set_inConflict(txn->get_thd_id(), write->txn, txnid);
            inout_table.set_outConflict(txn->get_thd_id(), txnid, write->txn);
            DEBUG("ssi read the write_lock in %ld out %ld\n",write->txn, txnid);
            write = write->next;
        }
        // Read the row遍历版本进行查找读哪一个
        rc = RCOK;
        SSIHisEntry * whis = writehis;
        while (whis != NULL && whis->ts > start_ts) {
            whis = whis->next;
        }
        row_t * ret = (whis == NULL) ? _row : whis->row;
        txn->cur_row = ret;
        assert(strstr(_row->get_table_name(), ret->get_table_name()));
        // Iterate over a version that is newer than the one you are currently reading.
        whis = writehis;
        while (whis != NULL && whis->ts > start_ts) {
            whis = whis->next;
        }
        // Check to see if two RW dependencies exist.
        // Add RW dependencies
        whis = writehis;
        while (whis != NULL && whis->ts > start_ts) {
            bool out = inout_table.get_outConflict(txn->get_thd_id(),whis->txn);
            if (out) { //! Abort
                rc = Abort;
                DEBUG("ssi txn %ld read the write_commit in %ld abort, whis_ts %ld current_start_ts %ld\n",txnid, whis->txn, inout_table.get_commit_ts(txn->get_thd_id(), whis->txn), start_ts);
                goto end;             
            }
            inout_table.set_inConflict(txn->get_thd_id(), whis->txn, txnid);
            inout_table.set_outConflict(txn->get_thd_id(), txnid, whis->txn);
            DEBUG("ssi read the write_commit in %ld out %ld\n",whis->txn, txnid);
            whis = whis->next;
        }
    } else if (type == P_REQ) {
        // Add the write lock
        get_lock(DLOCK_EX, txn);
        // Traverse the whole read his
        SSILockEntry * si_read = si_read_lock;
        while (si_read != NULL) {
            if (si_read->txn == txnid) {
                si_read = si_read->next;
                continue;
            }
            if (inout_table.get_state(txn->get_thd_id(), si_read->txn) == SSI_COMMITTED &&
                inout_table.get_commit_ts(txn->get_thd_id(), si_read->txn) > start_ts) {
                bool in = inout_table.get_inConflict(txn->get_thd_id(), si_read->txn);
                if (in) { //! Abort
                    rc = Abort;
                    DEBUG("ssi txn %ld write the read_commit in %ld abort, rhis_ts %ld current_start_ts %ld\n",txnid, si_read->txn, inout_table.get_commit_ts(txn->get_thd_id(), si_read->txn), start_ts);
                    goto end;
                }
            }
            si_read = si_read->next;
        }
        // Traverse the whole read lock
        si_read = si_read_lock;
        while (si_read != NULL) {
            if (si_read->txn == txnid) {
                si_read = si_read->next;
                continue;
            }
            if (inout_table.get_state(txn->get_thd_id(), si_read->txn) == SSI_ABORTED) {
                si_read = si_read->next;
                continue;
            }
            if (inout_table.get_state(txn->get_thd_id(), si_read->txn) == SSI_COMMITTED &&
                inout_table.get_commit_ts(txn->get_thd_id(), si_read->txn) <= start_ts) {
                si_read = si_read->next;
                continue;
            }
            inout_table.set_outConflict(txn->get_thd_id(), si_read->txn, txnid);
            inout_table.set_inConflict(txn->get_thd_id(), txnid, si_read->txn);
            DEBUG("ssi write the si_read_lock out %ld in %ld\n", si_read->txn, txnid);
            si_read = si_read->next;
        }

        if (preq_len < g_max_pre_req){
            DEBUG("buf P_REQ %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
            buffer_req(P_REQ, txn);
            rc = RCOK;
        } else  {
            rc = Abort;
        }
    } else if (type == W_REQ) {
        ts_t ts = txn->get_commit_timestamp();
        rc = RCOK;
        release_lock(DLOCK_EX, txn);
        release_lock(LOCK_COM, txn);
        //TODO: here need to consider whether need to release the si-read lock.
        // release_lock(DLOCK_SH, txn);

        // the corresponding prewrite request is debuffered.
        insert_history(ts, txn, row);
        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
        SSIReqEntry * req = debuffer_req(P_REQ, txn);
        assert(req != NULL);
        return_req_entry(req);
    } else if (type == XP_REQ) {
        release_lock(DLOCK_EX, txn);
        release_lock(LOCK_COM, txn);
        //TODO: here need to consider whether need to release the si-read lock.
        release_lock(DLOCK_SH, txn);

        DEBUG("debuf %ld %ld\n",txn->get_txn_id(),_row->get_primary_key());
        SSIReqEntry * req = debuffer_req(P_REQ, txn);
        assert (req != NULL);
        return_req_entry(req);
    } else {
        assert(false);
    }

    if (rc == RCOK) {
        if (whis_len > g_his_recycle_len || rhis_len > g_his_recycle_len) {
            ts_t t_th = glob_manager.get_min_ts(txn->get_thd_id());
            if (readhistail && readhistail->ts < t_th) {
                clear_history(R_REQ, t_th);
            }
            // Here is a tricky bug. The oldest transaction might be
            // reading an even older version whose timestamp < t_th.
            // But we cannot recycle that version because it is still being used.
            // So the HACK here is to make sure that the first version older than
            // t_th not be recycled.
            if (whis_len > 1 && writehistail->prev->ts < t_th) {
                row_t * latest_row = clear_history(W_REQ, t_th);
                if (latest_row != NULL) {
                    assert(_row != latest_row);
                    _row->copy(latest_row);
                }
            }
        }
    }
end:
    uint64_t timespan = get_sys_clock() - starttime;
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;

    if (g_central_man) {
        glob_manager.release_row(_row);
     } else {
        pthread_mutex_unlock(latch);
     }

    return rc;
}

RC Row_ssi::validate_last_commit(TxnManager * txn) {
    RC rc = RCOK;
    SSIHisEntry *  whis = writehis;
    ts_t start_ts = txn->get_start_timestamp();
    if (g_central_man) {
        glob_manager.lock_row(_row);
    } else {
        pthread_mutex_lock(latch);
    }
    // INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);

    if (commit_lock != 0 && commit_lock != txn->get_txn_id()) {
        DEBUG("si last commit lock %ld, %ld\n",commit_lock, txn->get_txn_id());
        rc = Abort;
        goto last_commit_end;
    }
    get_lock(LOCK_COM, txn);
    // Iterate over a version that is newer than the one you are currently reading.
    while (whis != NULL && whis->ts < start_ts) {
        whis = whis->next;
    }
    if (whis != NULL) {
        DEBUG("si last commit whis %ld, %ld, %ld\n",whis->ts, start_ts, txn->get_txn_id());
        release_lock(LOCK_COM, txn);
        rc = Abort;
    }    
last_commit_end:
    if (g_central_man) {
        glob_manager.release_row(_row);
    } else {
        pthread_mutex_unlock(latch);
    }
    return rc;
}
