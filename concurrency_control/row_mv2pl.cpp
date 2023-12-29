
/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_mv2pl.h"
#include "mem_alloc.h"

//初始化TODO
void Row_mv2pl::init(row_t *row) { 
	_row = row;
    owner = NULL;
    waiters_head = NULL;
    waiters_tail = NULL;
    waiters_read_head = NULL;
    waiters_read_tail = NULL;
    retire_head = NULL;

    waiter_cnt = 0;
    whis_len = 0;
    max_retire_cts = 0;
    max_cts = 0;
    writehis = NULL;
    writehistail = NULL;

    latch = new pthread_mutex_t;
    pthread_mutex_init(latch, NULL);

    blatch = false;

}


void Row_mv2pl::release_2pl_hisentry(Mv2plhisEntry* entry) {
    if (entry->row != NULL) {
        entry->row->free_row();
        mem_allocator.free(entry->row, sizeof(row_t));
    }
    mem_allocator.free(entry, sizeof(Mv2plhisEntry));
}

void Row_mv2pl::release_2pl_hisentry2(Mv2plhisEntry* entry) {
    mem_allocator.free(entry, sizeof(Mv2plhisEntry));
}



//获得一个新的元组历史结构
Mv2plhisEntry * Row_mv2pl::creat_new_hisentry() {
	return (Mv2plhisEntry *) mem_allocator.alloc(sizeof(Mv2plhisEntry));
}
//获得一个新的事务元组
Mv2plEntry * Row_mv2pl::create_2pl_entry() {
    return (Mv2plEntry *) mem_allocator.alloc(sizeof(Mv2plEntry)); 
}

void Row_mv2pl::release_2pl_entry(Mv2plEntry *entry) {
    mem_allocator.free(entry, sizeof(Mv2plEntry));
}

//插入新的元组历史结构，这里应该有创建这个历史记录的事务id或者指针
void Row_mv2pl::insert_history(ts_t ts, TxnManager * txn, row_t *row) {
	Mv2plhisEntry * new_entry = creat_new_hisentry();
	new_entry->ts = ts;
    new_entry->txn = txn;
	new_entry->row = row;
    new_entry->commited = false;
    whis_len ++;
    LIST_PUT_HEAD(writehistail, writehis, new_entry);
}


RC Row_mv2pl::access(TxnManager * txn, lock_t type, row_t * row) {
    DEBUG_T("txn %ld access \n",txn->get_txn_id());
    RC rc = RCOK;
    ts_t start_ts = txn->get_start_timestamp();
    uint64_t starttime = get_sys_clock();
    txnid_t txnid = txn->get_txn_id();
	//上线程锁
    if (g_central_man) {
        glob_manager.lock_row(_row);
    } else {
        pthread_mutex_lock(latch);
    }
	//统计相关信息的，等到需要实验时解决
    INC_STATS(txn->get_thd_id(), trans_access_lock_wait_time, get_sys_clock() - starttime);
	//查看是否有拥有者
    bool conflict = (owner != NULL);
	//如果是读操作
    if (type == DLOCK_SH) {//读一定是可以读到的，就是可能会等待前面事务确定
        //先推高本地最大读时间戳，由于本地最大最大读时间戳一定式是小于当前时间的，所以后来的写事物的本地提交时间戳一定大于这个读时间戳，所以本地时间戳不保存了
		//找到版本链最新数据
        DEBUG_T("txn %ld access read\n",txn->get_txn_id());
        Mv2plhisEntry * whis =  writehistail;
        if(conflict) {
            //有上锁事务，
            if (whis == NULL || whis->ts < start_ts){//可能能读到未提交的数据
                //当本地时间戳没确定时直接跳过，说明处于运行状态，当确定后还小于开始时间戳，等待，本地提交时间戳大于开始时间戳的话说明读不到新建的版本，跳过，对于终止的事务，终止时是状态先确定还是
                if(owner->txn->get_prepare_timestamp() <= start_ts){
                    DEBUG_T("txn %ld need wait %ld co/xp\n", txn->get_txn_id(),owner->txn->get_txn_id());
                    Mv2plEntry * entry = create_2pl_entry();
                    entry->start_ts = start_ts;
                    entry->txn = txn;
                    entry->type = type;
                    Mv2plEntry * en;
                    //标记需要等待多少行
                    //txn->lock_ready = false;
                    ATOM_CAS(txn->lock_ready,1,0);
                    txn->incr_lr();
                    LIST_PUT_TAIL(waiters_read_head, waiters_read_tail, entry); 
                    waiter_cnt ++;
                    rc = WAIT;
                    goto final;
                }
            }
        }
        //只能读到可见的数据，快照读判断读哪一个数据
        while (whis != NULL && whis->ts > start_ts) {
            whis = whis->next;
        }
        row_t * ret = (whis == NULL) ? _row : whis->row;//这里有隐患，当读事务是非常老的事务，但是写版本已经被清理了，可能会去读最开始的版本
        //增加依赖，当读的是一个未提交的数据且不为空
        DEBUG_T("txn %ld begin read\n", txn->get_txn_id());
#if CLV == CLV2 || CLV == CLV3
        if( whis && !whis->commited){
            //读的数据不为空,且没提交增加依赖
            //前驱事务加依赖
            ONCONFLICT * entry = whis->txn->creat_on_entry();
            entry->txn_id = txn->get_txn_id();
            entry->abort_cnt = txn->abort_cnt;
            entry->next = NULL;
            if(whis->txn->onconflicthead){
              whis->txn->onconflicttail->next = entry;
              whis->txn->onconflicttail = entry;
            }else{
              whis->txn->onconflicttail = entry;
              whis->txn->onconflicthead = entry;
            }
            //后继事务加依赖
            assert(txn->inconflict >= 0);
            ATOM_CAS(txn->prep_ready,true,false);
            txn->incr_pr();
            DEBUG_T("txn %ld add inconflict %ld \n", txn->get_txn_id(),whis->txn->get_txn_id());
        }
#endif        
        txn->cur_row = ret;
    } else if (type == DLOCK_EX) {//如果是写操作
        //最大提交时间戳在提交时确定，可见但未提交的不能算，因为有回滚的可能
        DEBUG_T("txn %ld access write\n",txn->get_txn_id());
        if( start_ts < max_retire_cts){
            rc = Abort;
            txn->set_rc(rc);
            DEBUG_T("txn %ld abort because ts little \n", txn->get_txn_id());
#if CLV == CLV2 || CLV == CLV3 
            ATOM_CAS(txn->prep_ready, false, true);
            ATOM_CAS(txn->need_prep_cont, true, false);
            ATOM_CAS(txn->inconflict,txn->inconflict,-1);
#endif
            goto final;
        }
        if (conflict){
#if CC_ALG == MV_WOUND_WAIT
            //已经进入提交阶段的不会被wound，只会对正在运行的事务wound
            bool canwound = true;
            //如果请求锁的事务的开始时间戳大于拥有者的时间戳，那么说明应该等待，否则wound
            if (start_ts > owner->start_ts){
                canwound = false;
            }
            //如果事务可以wound且没有进入其他状态，就放入等待队列中，然后从等待队列中取出。如果不可以wound，就加入等待队列中.如果可以wound但是进入了其他状态，就回滚
            if(canwound && ATOM_CAS(owner->txn->txn_state, RUNNING, WOUNDED)){
                owner->txn->set_rc(Abort);
                DEBUG_T("txn %ld wound by %ld \n", owner->txn->get_txn_id(), txn->get_txn_id());                  
                release_2pl_entry(owner);
                owner = NULL;
                Mv2plEntry *entry = create_2pl_entry();
                entry->start_ts = start_ts;
                entry->txn = txn;
                entry->type = type;
                Mv2plEntry * en;
                ATOM_CAS(txn->lock_ready,1,0);
                txn->incr_lr();
                //这里是将事务插入等待者队列中的合适未知，等待者队列中，最新的事务在最前，
                en = waiters_head;
                while (en != NULL && start_ts > en->start_ts) {
                    en = en->next;
                }
                if (en) {
                    LIST_INSERT_BEFORE(en, entry,waiters_head); 
                } else {
                    LIST_PUT_TAIL(waiters_head, waiters_tail, entry); 
                }
                waiter_cnt ++;
                while (waiters_head){ 
                    waiter_cnt --;    
                    LIST_GET_HEAD(waiters_head,waiters_tail,entry);
                    if(entry->txn->get_rc() == Abort){
                        if(entry->txn->decr_lr() == 0) {
                            if(ATOM_CAS(entry->txn->lock_ready,false,true)) {
                                DEBUG_T("txn %ld need cont run because rc = abort \n", entry->txn->get_txn_id());
                                txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(), entry->txn->get_batch_id());//唤醒事务，等待者上位，重新执行事务，
                            }         
                        }
                        release_2pl_entry(entry);
                        continue;
                    }else{
                        owner = entry;
                        row_t * ret = (writehistail == NULL) ? _row : writehistail->row;
                        txn->cur_row = ret;
                        Mv2plhisEntry * whis =  writehistail;
#if CLV == CLV2 || CLV == CLV3
                        if( whis && !whis->commited){
                            //读的数据不为空,且没提交增加依赖
                            //前驱事务加依赖
                            ONCONFLICT * conflict = whis->txn->creat_on_entry();
                            conflict->txn_id = entry->txn->get_txn_id();
                            conflict->abort_cnt = entry->txn->abort_cnt;
                            conflict->next = NULL;
                            if(whis->txn->onconflicthead){
                            whis->txn->onconflicttail->next = conflict;
                            whis->txn->onconflicttail = conflict;
                            }else{
                            whis->txn->onconflicttail = conflict;
                            whis->txn->onconflicthead = conflict;
                            }
                            //后继事务加依赖
                            assert(owner->txn->inconflict >= 0);
                            ATOM_CAS(owner->txn->prep_ready,true,false);
                            owner->txn->incr_pr();
                            DEBUG_T("txn %ld add inconflict %ld \n", owner->txn->get_txn_id(),whis->txn->get_txn_id());
                        }
#endif
                    break;
                    } 
                } 
                while (waiters_read_head){
                    LIST_GET_HEAD(waiters_read_head, waiters_read_tail, entry);
                    //统计信息
                    uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
                    entry->txn->twopl_wait_start = 0;
                    entry->txn->txn_stats.cc_block_time += timespan;
                    entry->txn->txn_stats.cc_block_time_short += timespan;
                    INC_STATS(txn->get_thd_id(), twopl_wait_time, timespan);
                    //设置唤醒后事务的要获取的行数据，快照读数据
                    ts_t ts = entry->start_ts;
                    Mv2plhisEntry * whis =  writehistail;
                    while (whis != NULL && whis->ts > ts) {
                        whis = whis->next;
                    }
                    entry->txn->cur_row = (whis == NULL) ? _row : whis->row;
                    if (entry->txn->decr_lr() == 0) {
                        if (ATOM_CAS(entry->txn->lock_ready, false, true)) {
                            DEBUG_T("txn %ld need cont run read\n", entry->txn->get_txn_id());
                            txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(),entry->txn->get_batch_id());  // 唤醒事务，等待者上位，重新执行事务，
                        }
                    }
                    release_2pl_entry(entry);
                    waiter_cnt --;
                }
                if(!owner){
                  rc = Abort;
                }else if(txn == owner->txn){
                    owner->txn->decr_lr();
                    ATOM_CAS(owner->txn->lock_ready, false, true);
                    DEBUG_T("txn %ld wound become owner \n", txn->get_txn_id());
                    rc = RCOK;
                }else{
                    DEBUG_T("txn %ld wound but other tx run \n", txn->get_txn_id());
                    if(owner->txn->decr_lr() == 0) {
                        if(ATOM_CAS(owner->txn->lock_ready,false,true)) {
                            DEBUG_T("txn %ld need cont run \n", owner->txn->get_txn_id());
                            txn_table.restart_txn(txn->get_thd_id(), owner->txn->get_txn_id(), owner->txn->get_batch_id());//唤醒事务，等待者上位，重新执行事务，
                        }         
                    }
                    rc = WAIT;
                }   
            }else if (!canwound){
                Mv2plEntry * entry = create_2pl_entry();
                entry->start_ts = start_ts;
                entry->txn = txn;
                entry->type = type;
                Mv2plEntry * en;
                //wound后需要等待拥有者终止，等待唤醒
                //txn->lock_ready = false;
                ATOM_CAS(txn->lock_ready,1,0);
                DEBUG_T("txn %ld need wait %ld co wound\n", txn->get_txn_id(),owner->txn->get_txn_id());
                txn->incr_lr();
                //这里是将事务插入等待者队列中的合适未知，等待者队列中，最新的事务在最前，
                en = waiters_head;
                while (en != NULL && start_ts > en->start_ts) {
                    en = en->next;
                }
                if (en) {
                    LIST_INSERT_BEFORE(en, entry,waiters_head); 
                } else {
                    LIST_PUT_TAIL(waiters_head, waiters_tail, entry); 
                }
                waiter_cnt ++;
                rc = WAIT;
            }else{
                DEBUG_T("txn %ld need abort because owner come other \n", txn->get_txn_id(),owner->txn->get_txn_id());
                rc = Abort;
                txn->set_rc(rc);
#if CLV == CLV2 || CLV == CLV3 
                ATOM_CAS(txn->prep_ready, false, true);
                ATOM_CAS(txn->need_prep_cont, true, false);
                ATOM_CAS(txn->inconflict,txn->inconflict,-1);
#endif
            }

#endif          

#if CC_ALG == MV_NO_WAIT

            rc = Abort;
            txn->set_rc(rc);
#if CLV == CLV2 || CLV == CLV3 
            ATOM_CAS(txn->prep_ready, false, true);
            ATOM_CAS(txn->need_prep_cont, true, false);
            ATOM_CAS(txn->inconflict,txn->inconflict,-1);
#endif
            DEBUG_T("abort %ld , %ld %ld %lx\n", txn->get_txn_id(), txn->get_batch_id(),
            _row->get_primary_key(), (uint64_t)_row);
            goto final;
#endif
//未实现
#if CC_ALG == MV_DL_DETECT
#endif        
        }else{
            //行上目前没有上写锁的事务，那么上锁的时候就要考虑是否能上锁了，要判断，当前事务与退休者的尾部的提交时间戳的大小，如果大于的话，成为拥有者，否则，回滚
            DEBUG_T("no owner, txn %ld is owner\n", txn->get_txn_id());     
            Mv2plEntry * entry = create_2pl_entry();
            entry->type = type;
            entry->start_ts = start_ts;
            entry->txn = txn;
            owner = entry;
            row_t * ret = (writehistail == NULL) ? _row : writehistail->row;
            txn->cur_row = ret;
#if CLV == CLV2 || CLV == CLV3
                Mv2plhisEntry * whis = writehistail;
                if( whis && !whis->commited){       
                    ONCONFLICT * conflict = whis->txn->creat_on_entry();
                    conflict->txn_id = txn->get_txn_id();
                    conflict->abort_cnt = txn->abort_cnt;
                    conflict->next = NULL;
                    if(whis->txn->onconflicthead){
                        whis->txn->onconflicttail->next = conflict;
                        whis->txn->onconflicttail = conflict;
                    }else{
                        whis->txn->onconflicttail = conflict;
                        whis->txn->onconflicthead = conflict;
                    }
                    //后继事务加依赖
                    assert(owner->txn->inconflict >= 0);
                    ATOM_CAS(owner->txn->prep_ready,true,false);
                    owner->txn->incr_pr();
                    DEBUG_T("txn %ld inconflict %ld num: %d \n", owner->txn->get_txn_id(),whis->txn->get_txn_id(), owner->txn->inconflict);
                }           
#endif            
        }  
    }
final:
    uint64_t curr_time = get_sys_clock();
    uint64_t timespan = curr_time - starttime;
    if (rc == WAIT && txn->twopl_wait_start == 0) {
        txn->twopl_wait_start = curr_time;
    }
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;

    INC_STATS(txn->get_thd_id(),twopl_getlock_time,timespan);
    INC_STATS(txn->get_thd_id(),twopl_getlock_cnt,1);     
    if (g_central_man) glob_manager.release_row(_row);
    else pthread_mutex_unlock( latch );
    return rc;
}

//提交时释放锁和唤醒与解除依赖，根据不同的类型进行不同的回滚操作，在等待者队列中，拥有者，退休者（也就是级联回滚的情况）
void Row_mv2pl::lock_release(TxnManager * txn, lock_t type){

    uint64_t starttime = get_sys_clock();
    if (g_central_man)
        glob_manager.lock_row(_row);
    else {
        pthread_mutex_lock(latch);
        }
    //有可能出现级联回滚情况
    if(type == XP1){
        DEBUG_T("txn %ld abort\n", txn->get_txn_id());
        Mv2plEntry * retire = owner;

#if CC_ALG == MV_WOUND_WAIT && CLV == CLV1
        if(owner == NULL || txn->get_txn_id() != owner->txn->get_txn_id()){
          DEBUG_T("txn %ld not owner because wound\n", txn->get_txn_id());
        }else{
          owner = NULL;
          release_2pl_entry(retire);
        }
#else
#if CLV == CLV2 || CLV == CLV3
        //判断回滚的事务是否是拥有者，是的话不用清理历史了，否则要清理历史
        //不是拥有者
        if (owner == NULL || txn->get_txn_id() != owner->txn->get_txn_id()){
            DEBUG_T("级联终止 %ld\n");
            DEBUG_T("txn %ld not owner\n", txn->get_txn_id());
            Mv2plhisEntry * entry = writehistail;
            //找到要清理的版本，要清理的版本，要满足事务id能对上
            while (entry != NULL && entry->txn->get_txn_id() != txn->get_txn_id() && !entry->commited) {
                //说明这个事务在这行上写的历史已经被清理，不需要进行其他操作了，也不需要唤醒等待者
                entry = entry->next;
            }
            //如果不为空且id能对上
            if (entry != NULL && entry->txn->get_txn_id() == txn->get_txn_id()){
                DEBUG_T("cleanr history\n");     
                //历史没有被清理，找到了这个事务，解除依赖，并将后续历史给清理了
                max_retire_cts = max_cts;
                if (entry->next == NULL){
                  writehistail = NULL;
                  writehis = NULL;
                }else{
                  writehistail = entry->next;
                }
                Mv2plhisEntry * prev;
                while(entry != NULL){
                  prev = entry;
                  entry = entry->prev;
                  release_2pl_hisentry2(prev);
                  whis_len--;//历史长度减一
                }
                //要清理的还在历史里时，拥有者应该也要被清理，但是此时拥有者还没有加上依赖，所以，需要将其设为回滚
                owner->txn->set_rc(Abort);
                DEBUG_T("txn %ld need abort\n", owner->txn->get_txn_id());
                ATOM_CAS(owner->txn->inconflict,owner->txn->inconflict,-1);
                ATOM_CAS(owner->txn->prep_ready, false, true);
                ATOM_CAS(owner->txn->need_prep_cont, true, false);
                owner = NULL;
                release_2pl_entry(retire);
            }
        }else{//是拥有者，删除拥有者
            owner = NULL;
            release_2pl_entry(retire);
        }
#else
        owner = NULL;
        release_2pl_entry(retire);
#endif
#endif
        //当在历史版本中的回滚，或者拥有者回滚时
        if  (owner == NULL){
            DEBUG_T("wait run\n");
            //唤醒等待者
            Mv2plEntry * entry;
            //对于读事务，直接获取队尾的上存的行就行了，因为他们等待的原因是事务不确定能不能读到
            while (waiters_read_head){
                LIST_GET_HEAD(waiters_read_head, waiters_read_tail, entry);
                //统计信息
                uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
                entry->txn->twopl_wait_start = 0;
                entry->txn->txn_stats.cc_block_time += timespan;
                entry->txn->txn_stats.cc_block_time_short += timespan;
                INC_STATS(txn->get_thd_id(), twopl_wait_time, timespan);
                //设置唤醒后事务的要获取的行数据，快照读数据
                ts_t ts = entry->start_ts;
                Mv2plhisEntry * whis =  writehistail;
                while (whis != NULL && whis->ts > ts) {
                    whis = whis->next;
                }
                entry->txn->cur_row = (whis == NULL) ? _row : whis->row;
                if (entry->txn->decr_lr() == 0) {
                    if (ATOM_CAS(entry->txn->lock_ready, false, true)) {
                        DEBUG_T("txn %ld need cont run read\n", entry->txn->get_txn_id());
                        txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(),entry->txn->get_batch_id());  // 唤醒事务，等待者上位，重新执行事务，
                    }
                }
                release_2pl_entry(entry);
                waiter_cnt --;
            }
            
            while (waiters_head){   
                waiter_cnt --;   
                LIST_GET_HEAD(waiters_head,waiters_tail,entry);
                //统计信息
                if(entry->txn->get_rc() == Abort){
                    if(entry->txn->decr_lr() == 0) {
                        if(ATOM_CAS(entry->txn->lock_ready,false,true)) {
                            DEBUG_T("txn %ld need cont run write\n", entry->txn->get_txn_id());
                            txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(), entry->txn->get_batch_id());//唤醒事务，等待者上位，重新执行事务，
                        }         
                    }
                    release_2pl_entry(entry);
                    continue;
                }else{
                    uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
                    entry->txn->twopl_wait_start = 0;
                    entry->txn->txn_stats.cc_block_time += timespan;
                    entry->txn->txn_stats.cc_block_time_short += timespan;
                    INC_STATS(txn->get_thd_id(),twopl_wait_time,timespan);

                    owner = entry;
                    //设置唤醒后事务的要获取的行数据
                    entry->txn->cur_row = (writehistail == NULL) ? _row : writehistail->row;
#if CLV == CLV2 || CLV == CLV3
                    if( writehistail && !writehistail->commited){
                        //读的数据不为空,且没提交增加依赖
                        //前驱事务加依赖
                        ONCONFLICT * conflict = writehistail->txn->creat_on_entry();
                        conflict->txn_id = entry->txn->get_txn_id();
                        conflict->abort_cnt = entry->txn->abort_cnt;
                        conflict->next = NULL;
                        if(writehistail->txn->onconflicthead){
                        writehistail->txn->onconflicttail->next = conflict;
                        writehistail->txn->onconflicttail = conflict;
                        }else{
                        writehistail->txn->onconflicttail = conflict;
                        writehistail->txn->onconflicthead = conflict;
                        }
                        //后继事务加依赖
                        assert(entry->txn->inconflict >= 0);
                        ATOM_CAS(entry->txn->prep_ready,true,false);
                        entry->txn->incr_pr();
                        DEBUG_T("txn %ld add inconflict %ld \n", entry->txn->get_txn_id(),writehistail->txn->get_txn_id());
                    }
#endif
                    if(entry->txn->decr_lr() == 0) {
                        if(ATOM_CAS(entry->txn->lock_ready,false,true)) {
                            DEBUG_T("txn %ld need cont run \n", entry->txn->get_txn_id());
                            txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(), entry->txn->get_batch_id());//唤醒事务，等待者上位，重新执行事务，
                        }         
                    }
                    break;
                }       
            }
        }
    }else{//如果是提交操作，说明已经退休过了，只需要解除依赖就可以了
#if CLV == CLV2 || CLV == CLV3
        //将他写的历史版本的标志设为true
        assert(retire_head);
        assert(retire_head->txn->get_txn_id() == txn->get_txn_id());
        retire_head->commited = true;
        retire_head = retire_head->prev;
#endif
        max_cts = txn->get_commit_timestamp();
        clear_history(txn);
    }
    

    uint64_t curr_time = get_sys_clock();
    uint64_t timespan = curr_time - starttime;
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;

    INC_STATS(txn->get_thd_id(),twopl_getlock_time,timespan);
    INC_STATS(txn->get_thd_id(),twopl_getlock_cnt,1);     
    if (g_central_man) glob_manager.release_row(_row);
    else pthread_mutex_unlock( latch );
}
//退休函数，可见操作后，将事务从拥有者变为退休者，只有写操作才会调用，将等待队列中的读事务全部重新执行，写事务只能唤醒一个
void Row_mv2pl::retire(TxnManager * txn, row_t * row) {  
    DEBUG_T("txn %ld retire in %lx\n", txn->get_txn_id(),(uint64_t)row);
    //这里要唤醒等待者
    uint64_t starttime = get_sys_clock();
    if (g_central_man)
        glob_manager.lock_row(_row);
    else {
        //TODO这里改为拥有者持续时间
        uint64_t mtx_wait_starttime = get_sys_clock();
        pthread_mutex_lock( latch );
        INC_STATS(txn->get_thd_id(),mtx[18],get_sys_clock() - mtx_wait_starttime);
    }

    max_retire_cts = txn->get_commit_timestamp();//加到插入历史数据里吧
    insert_history(max_retire_cts, txn , row);
#if CLV == CLV2 || CLV == CLV3
    assert(writehistail);
    assert(!writehistail->commited);
    if(!retire_head){
        retire_head = writehistail;
    }
#endif
    Mv2plEntry * entry;
    Mv2plEntry * retire = owner;
    int ex_num = 0;
    //拥有者可见后，唤醒等待者，对于不同的并发控制方法，唤醒等待者的方法不同
    //对于nowait来说，没有等待者唤醒，对于wound_wait来说，将等待者的头叫出来，然后判断是否可以成为拥有者，不能直接终止，能就成为拥有者，对于wait_die来说，退休意味着可以提交，那么等待者中全部都是时间戳小于拥有者的，等待者中的事务全部都要回滚
    //只实现wound_wait了
#if CC_ALG == MV_WAIT_DIE//所有等待者全部终止，总感觉不靠谱，先不弄了
#endif 
#if CC_ALG == MV_DL_DETECT//未实现
#endif 

    while (waiters_read_head){
        LIST_GET_HEAD(waiters_read_head, waiters_read_tail, entry );
        uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
        entry->txn->twopl_wait_start = 0;
        entry->txn->txn_stats.cc_block_time += timespan;
        entry->txn->txn_stats.cc_block_time_short += timespan;
        INC_STATS(txn->get_thd_id(), twopl_wait_time, timespan);
        //设置唤醒后事务的要获取的行数据
        ts_t ts = entry->start_ts;
        Mv2plhisEntry * whis =  writehistail;
        while (whis != NULL && whis->ts > ts) {
            whis = whis->next;
        }
        entry->txn->cur_row = (whis == NULL) ? _row : whis->row;
        //这里唤醒后应该加依赖的
#if CLV == CLV2 || CLV == CLV3
        if( whis && !whis->commited){
            //读的数据不为空,且没提交增加依赖
            //前驱事务加依赖
            ONCONFLICT * conflict = whis->txn->creat_on_entry();
            conflict->txn_id = entry->txn->get_txn_id();
            conflict->abort_cnt = entry->txn->abort_cnt;
            conflict->next = NULL;
            if(whis->txn->onconflicthead){
              whis->txn->onconflicttail->next = conflict;
              whis->txn->onconflicttail = conflict;
            }else{
              whis->txn->onconflicttail = conflict;
              whis->txn->onconflicthead = conflict;
            }
            //后继事务加依赖
            //assert(entry->txn->inconflict >= 0);
            ATOM_CAS(entry->txn->prep_ready,true,false);
            entry->txn->incr_pr();
            DEBUG_T("txn %ld add inconflict %ld \n", entry->txn->get_txn_id(),whis->txn->get_txn_id());
        }
#endif 
        if (entry->txn->decr_lr() == 0) {
            DEBUG_T("txn %ld need cont run because retire\n", entry->txn->get_txn_id());
            if (ATOM_CAS(entry->txn->lock_ready, false, true)) {
                txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(),entry->txn->get_batch_id());  // 唤醒事务，等待者上位，重新执行事务，
            }
        }
        release_2pl_entry(entry);
        waiter_cnt --;
    }
#if CC_ALG == MV_WOUND_WAIT//取出头部放入拥有者，如果不能放入    
    while (waiters_head && ex_num < 1){
        //事务为终止的话，也需要唤醒吧，不然不就一直没人去管了吗
        waiter_cnt --;
        LIST_GET_HEAD(waiters_head,waiters_tail,entry);
        if (entry->start_ts < max_retire_cts || entry->txn->get_rc() == Abort) {
            entry->txn->set_rc(Abort);
#if CLV == CLV2 || CLV == CLV3
            ATOM_CAS(entry->txn->prep_ready, false, true);
            ATOM_CAS(entry->txn->need_prep_cont, true, false);
            ATOM_CAS(entry->txn->inconflict,entry->txn->inconflict,-1);
#endif
            if(entry->txn->decr_lr() == 0) {
                if(ATOM_CAS(entry->txn->lock_ready,false,true)) {
                    DEBUG_T("txn %ld need cont run because retire\n", entry->txn->get_txn_id());
                    txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(), entry->txn->get_batch_id());//唤醒事务，等待者上位，重新执行事务，
                }
            }
            release_2pl_entry(entry);
        }else{
            ex_num++;
            uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
            entry->txn->twopl_wait_start = 0;
            entry->txn->txn_stats.cc_block_time += timespan;
            entry->txn->txn_stats.cc_block_time_short += timespan;
            INC_STATS(txn->get_thd_id(),twopl_wait_time,timespan);
            owner = entry;
            //设置唤醒后事务的要获取的行数据
            entry->txn->cur_row = writehistail->row;
#if CLV == CLV2 || CLV == CLV3
            if( writehistail && !writehistail->commited){
                //读的数据不为空,且没提交增加依赖
                //前驱事务加依赖
                ONCONFLICT * conflict = writehistail->txn->creat_on_entry();
                conflict->txn_id = entry->txn->get_txn_id();
                conflict->abort_cnt = entry->txn->abort_cnt;
                conflict->next = NULL;
                if(writehistail->txn->onconflicthead){
                writehistail->txn->onconflicttail->next = conflict;
                writehistail->txn->onconflicttail = conflict;
                }else{
                writehistail->txn->onconflicttail = conflict;
                writehistail->txn->onconflicthead = conflict;
                }
                //后继事务加依赖
                //assert(entry->txn->inconflict >= 0);
                ATOM_CAS(entry->txn->prep_ready,true,false);
                entry->txn->incr_pr();
                DEBUG_T("txn %ld add inconflict %ld \n", entry->txn->get_txn_id(),writehistail->txn->get_txn_id());
            }
#endif
            if(entry->txn->decr_lr() == 0) {
                if(ATOM_CAS(entry->txn->lock_ready,false,true)) {
                    DEBUG_T("txn %ld need cont run \n", entry->txn->get_txn_id());
                    txn_table.restart_txn(txn->get_thd_id(), entry->txn->get_txn_id(), entry->txn->get_batch_id());//唤醒事务，等待者上位，重新执行事务，
                }
            }
        }    
    }
    //nowait直接不进入while循环
#endif 
    if(ex_num < 1){
        owner = NULL;
    }
    release_2pl_entry(retire);    
    if (g_central_man) glob_manager.release_row(_row);
    else pthread_mutex_unlock( latch );
 
}
void Row_mv2pl::clean_wait(TxnManager * txn, lock_t type){
    DEBUG_T("txn %ld clean_wait\n", txn->get_txn_id());
    //这里要唤醒等待者
    if (g_central_man)
        glob_manager.lock_row(_row);
    else {   
        pthread_mutex_lock( latch );
    }
    Mv2plEntry * en;
    Mv2plEntry * entry;
    int num = 0;
    if (type == DLOCK_EX) {
      if (txn->lock_ready_cnt == 0) {
        DEBUG_T("txn %ld already cont run\n", txn->get_txn_id());
        if (owner && owner->txn->get_txn_id() == txn->get_txn_id()) {
          DEBUG_T("owner %ld need clean\n", txn->get_txn_id());
          // 唤醒后续等待者
          while (waiters_head) {
            waiter_cnt--;
            LIST_GET_HEAD(waiters_head, waiters_tail, entry);
            // 统计信息
            if (entry->txn->get_rc() == Abort) {
              if (entry->txn->decr_lr() == 0) {
                if (ATOM_CAS(entry->txn->lock_ready, false, true)) {
                  DEBUG_T("txn %ld need cont run because clean wait\n", entry->txn->get_txn_id());
                  txn_table.restart_txn(
                      txn->get_thd_id(), entry->txn->get_txn_id(),
                      entry->txn->get_batch_id());  // 唤醒事务，等待者上位，重新执行事务，
                }
              }
              release_2pl_entry(entry);
              continue;
            } else {
              num++;
              uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
              entry->txn->twopl_wait_start = 0;
              entry->txn->txn_stats.cc_block_time += timespan;
              entry->txn->txn_stats.cc_block_time_short += timespan;
              INC_STATS(txn->get_thd_id(), twopl_wait_time, timespan);

              owner = entry;
              // 设置唤醒后事务的要获取的行数据
              entry->txn->cur_row = (writehistail == NULL) ? _row : writehistail->row;
#if CLV == CLV2 || CLV == CLV3
                if( writehistail && !writehistail->commited){
                    //读的数据不为空,且没提交增加依赖
                    //前驱事务加依赖
                    ONCONFLICT * conflict = writehistail->txn->creat_on_entry();
                    conflict->txn_id = entry->txn->get_txn_id();
                    conflict->abort_cnt = entry->txn->abort_cnt;
                    conflict->next = NULL;
                    if(writehistail->txn->onconflicthead){
                    writehistail->txn->onconflicttail->next = conflict;
                    writehistail->txn->onconflicttail = conflict;
                    }else{
                    writehistail->txn->onconflicttail = conflict;
                    writehistail->txn->onconflicthead = conflict;
                    }
                    //后继事务加依赖
                    assert(entry->txn->inconflict >= 0);
                    ATOM_CAS(entry->txn->prep_ready,true,false);
                    entry->txn->incr_pr();
                    DEBUG_T("txn %ld add inconflict %ld \n", entry->txn->get_txn_id(),writehistail->txn->get_txn_id());
                }
#endif
              if (entry->txn->decr_lr() == 0) {
                if (ATOM_CAS(entry->txn->lock_ready, false, true)) {
                  DEBUG_T("txn %ld need cont run because clean wait\n", entry->txn->get_txn_id());
                  txn_table.restart_txn(
                      txn->get_thd_id(), entry->txn->get_txn_id(),
                      entry->txn->get_batch_id());  // 唤醒事务，等待者上位，重新执行事务，
                }
              }
              break;
            }
          }
          if (num == 0) {
            owner = NULL;
          }
        }
      } else {
        en = waiters_head;
        while (en) {
          if (en->txn->get_txn_id() == txn->get_txn_id()) {
            break;
          }
          en = en->next;
        }
        if (en) {
          LIST_REMOVE_HT(en, waiters_head, waiters_tail);
        }
      }
    } else {
      if (txn->lock_ready_cnt == 0) {
        DEBUG_T("txn %ld already cont run\n", txn->get_txn_id());
      } else {
        en = waiters_read_head;
        while (en) {
          if (en->txn->get_txn_id() == txn->get_txn_id()) {
            break;
          }
          en = en->next;
        }
        if (en) {
          LIST_REMOVE_HT(en, waiters_read_head, waiters_read_tail);
        }
      }
    }
    if (g_central_man) glob_manager.release_row(_row);
    else pthread_mutex_unlock( latch );
}


//ts是从每个线程池中的最小时间戳中的最小值，这个最小值在事务开始的时候执行更新，用事务的开始时间戳来更新.
void Row_mv2pl::clear_history(TxnManager * txn){   
    if (whis_len > g_his_recycle_len ) {
        ts_t t_th = glob_manager.get_min_ts(txn->get_thd_id());
        // Here is a tricky bug. The oldest transaction might be 
        // reading an even older version whose timestamp < t_th.
        // But we cannot recycle that version because it is still being used.
        // So the HACK here is to make sure that the first version older than
        // t_th not be recycled.
        //清理历史，并且将原始行上的data变为最后最后一份数据的data
#if CLV == CLV1
        if (whis_len > 1 && writehis->prev->ts < t_th ) {
#else
        if (whis_len > 1 && writehis->prev->ts < t_th && writehis->commited) {
#endif
            row_t * latest_row = clear_history(t_th);
            if (latest_row != NULL) {
                assert(_row != latest_row);
                _row->copy(latest_row);
            }
        }
    }       
}


row_t * Row_mv2pl::clear_history(ts_t ts) {
	Mv2plhisEntry ** queue;
	Mv2plhisEntry ** tail;

    queue = &writehis;
    tail = &writehistail;
    
	Mv2plhisEntry * his = *queue;
	Mv2plhisEntry * prev = NULL;
	row_t * row = NULL;
#if CLV == CLV1
    while (his && his->prev && his->prev->ts < ts ) {
#else
    while (his && his->prev && his->prev->ts < ts && his->commited) {
#endif
		prev = his->prev;
		assert(prev->ts >= his->ts);
		if (row != NULL) {//这一步第一次循环一定不进入，这一步是为了将历史元组上的行清空
			row->free_row();
			mem_allocator.free(row, row_t::get_row_size(ROW_DEFAULT_SIZE));
		}
		row = his->row;//将row定义为这个最旧元组上的行
		his->row = NULL;//将其变为空？？？应该是释放元组上的行标记不会影响到真正的行
		release_2pl_hisentry(his);
		his = prev;
        whis_len--;
	}
	*queue = his;
    if (*queue) (*queue)->next = NULL;
    if (his == NULL) *tail = NULL;
	return row;
}