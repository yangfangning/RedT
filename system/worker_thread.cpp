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

#include "worker_thread.h"

#include "abort_queue.h"
#include "global.h"
#include "helper.h"
#include "logger.h"
#include "maat.h"
#include "manager.h"
#include "math.h"
#include "message.h"
#include "msg_queue.h"
#include "msg_thread.h"
#include "query.h"
#include "tpcc_query.h"
#include "txn.h"
#include "wl.h"
#include "work_queue.h"
#include "ycsb_query.h"
#include "maat.h"
#include "transport.h"
#include "routine.h"
#include <boost/bind.hpp>
#include "ssi.h"

void WorkerThread::setup() {
	if( get_thd_id() == 0) {
    send_init_done_to_all_nodes();
  }
  _thd_txn_id = 0;
}

void WorkerThread::statqueue(uint64_t thd_id, Message * msg, uint64_t starttime) {
  if (msg->rtype == RTXN_CONT ||
      msg->rtype == RQRY_RSP || msg->rtype == RACK_PREP  ||
      msg->rtype == RACK_FIN || msg->rtype == RTXN  ||
      msg->rtype == CL_RSP || msg->rtype == RACK_PREP_CONT) {
    uint64_t queue_time = get_sys_clock() - starttime;
		INC_STATS(thd_id,trans_local_process,queue_time);
  } else if (msg->rtype == RQRY || msg->rtype == RQRY_CONT ||
             msg->rtype == RFIN || msg->rtype == RPREPARE ||
             msg->rtype == RFWD){
    uint64_t queue_time = get_sys_clock() - starttime;
		INC_STATS(thd_id,trans_remote_process,queue_time);
  } else if (msg->rtype == CL_QRY || msg->rtype == CL_QRY_O) {
    uint64_t queue_time = get_sys_clock() - starttime;
    INC_STATS(thd_id,trans_process_client,queue_time);
  }
}

void WorkerThread::process(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  RC rc __attribute__ ((unused));

  DEBUG("%ld Processing %ld %d\n",get_thd_id(),msg->get_txn_id(),msg->get_rtype());
  assert(msg->get_rtype() == CL_QRY || msg->get_rtype() == CL_QRY_O || msg->get_txn_id() != UINT64_MAX);
  uint64_t starttime = get_sys_clock();
		switch(msg->get_rtype()) {
			case RPASS:
        //rc = process_rpass(msg);
				break;
			case RPREPARE:
        rc = process_rprepare(yield, msg, cor_id);
				break;
			case RFWD:
        rc = process_rfwd(yield, msg, cor_id);
				break;
			case RQRY:
        rc = process_rqry(yield, msg, cor_id);
				break;
			case RQRY_CONT:
        rc = process_rqry_cont(yield, msg, cor_id);
				break;
			case RQRY_RSP:
        rc = process_rqry_rsp(yield, msg, cor_id);
				break;
			case RFIN:
        rc = process_rfin(yield, msg, cor_id);
				break;
      case SET_CO_TS:
        rc = process_set_co_ts(yield, msg, cor_id);
				break;
			case RLOG:
        rc = process_rlog(yield, msg, cor_id);
				break;
			case RACK_LOG:
        rc = process_rack_log(yield, msg, cor_id);
				break;
			case RFIN_LOG:
        rc = process_rfin_log(yield, msg, cor_id);
				break;
      case RCO_LOG:
        rc = process_rco_log(yield, msg, cor_id);
        break;
      case RACK_CO_LOG:
        rc = process_rack_co_log(yield, msg, cor_id);
        break;
			case RACK_FIN_LOG:
        rc = process_rack_fin_log(yield, msg, cor_id);
				break;
			case RACK_PREP:
        rc = process_rack_prep(yield, msg, cor_id);
				break;
      case RACK_PRE_PREP:
        rc = process_rack_pre_prep(yield, msg, cor_id);
				break;
      case RACK_PREP_CONT:
        rc = process_rack_prep_cont(yield, msg, cor_id);
				break;
			case RACK_FIN:
        rc = process_rack_rfin(msg);
				break;
			case RTXN_CONT:
        rc = process_rtxn_cont(yield, msg, cor_id);
				break;
      case CL_QRY:
      case CL_QRY_O:
			case RTXN://1
#if CC_ALG == CALVIN
        rc = process_calvin_rtxn(yield, msg, cor_id);
#else
        rc = process_rtxn(yield, msg, cor_id);
#endif
				break;
			case LOG_FLUSHED:
        rc = process_log_flushed(msg);
				break;
			case LOG_MSG:
        rc = process_log_msg(msg);
				break;
			case LOG_MSG_RSP:
        rc = process_log_msg_rsp(msg);
				break;
			default:
        printf("Msg: %d\n",msg->get_rtype());
        fflush(stdout);
				assert(false);
				break;
		}
  statqueue(get_thd_id(), msg, starttime);
  uint64_t timespan = get_sys_clock() - starttime;
  INC_STATS(get_thd_id(),worker_process_cnt,1);

  INC_STATS(get_thd_id(),worker_process_time,timespan);
  INC_STATS(get_thd_id(),worker_process_cnt_by_type[msg->rtype],1);
  INC_STATS(get_thd_id(),worker_process_time_by_type[msg->rtype],timespan);
  DEBUG("%ld EndProcessing %d %ld\n",get_thd_id(),msg->get_rtype(),msg->get_txn_id());
}

void WorkerThread::check_if_done(RC rc) {
  if (txn_man->waiting_for_response()) return;
  if (rc == Commit) {
    txn_man->txn_stats.finish_start_time = get_sys_clock();
    commit();
  }
  if (rc == Abort) {
    txn_man->txn_stats.finish_start_time = get_sys_clock();
    abort();
  }
}

void WorkerThread::release_txn_man() {
  txn_table.release_transaction_manager(get_thd_id(), txn_man->get_txn_id(),
                                        txn_man->get_batch_id());
  txn_man = NULL;
}

void WorkerThread::calvin_wrapup(yield_func_t &yield, uint64_t cor_id) {
  txn_man->release_locks(yield, RCOK, cor_id);
  txn_man->commit_stats();
  DEBUG("(%ld,%ld) calvin ack to %ld\n", txn_man->get_txn_id(), txn_man->get_batch_id(),
        txn_man->return_id);
  if(txn_man->return_id == g_node_id) {
    work_queue.sequencer_enqueue(_thd_id,Message::create_message(txn_man,CALVIN_ACK));
  } else {
    msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, CALVIN_ACK),
                      txn_man->return_id);
  }
  release_txn_man();
}

// Can't use txn_man after this function提交之后都不会使用txnmanager函数
void WorkerThread::commit() {
  // printf("xxx txn %lu commit\n", txn_man->get_txn_id());
  DEBUG_T("COMMIT %ld -- %f\n", txn_man->get_txn_id(),
        (double)get_sys_clock() - run_starttime / BILLION);
  total_local_txn_commit++;
  total_num_msgs_rw += txn_man->num_msgs_rw;
  total_num_msgs_prep += txn_man->num_msgs_prep;
  total_num_msgs_commit += txn_man->num_msgs_commit;
  if(txn_man->num_msgs_rw > max_num_msgs_rw) max_num_msgs_rw = txn_man->num_msgs_rw;
  if(txn_man->num_msgs_prep > max_num_msgs_prep) max_num_msgs_prep = txn_man->num_msgs_prep;
  if(txn_man->num_msgs_commit > max_num_msgs_commit) max_num_msgs_commit = txn_man->num_msgs_commit;

  assert(txn_man);
  assert(IS_LOCAL(txn_man->get_txn_id()));

  uint64_t timespan = get_sys_clock() - txn_man->txn_stats.starttime;
  // printf("COMMIT %ld %f -- %f\n", txn_man->get_txn_id(),
        // simulation->seconds_from_start(get_sys_clock()), (double)timespan / BILLION);

  // ! trans total time
  uint64_t end_time = get_sys_clock();
  uint64_t timespan_short  = end_time - txn_man->txn_stats.restart_starttime;
  uint64_t two_pc_timespan  = end_time - txn_man->txn_stats.prepare_start_time;
  uint64_t finish_timespan  = end_time - txn_man->txn_stats.finish_start_time;
  uint64_t prepare_timespan = txn_man->txn_stats.finish_start_time - txn_man->txn_stats.prepare_start_time;
  INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
  INC_STATS(get_thd_id(), trans_prepare_count, 1);

  INC_STATS(get_thd_id(), trans_2pc_time, two_pc_timespan);
  INC_STATS(get_thd_id(), trans_finish_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_commit_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_total_run_time, timespan_short);
  INC_STATS(get_thd_id(), trans_commit_total_run_time, timespan_short);

  INC_STATS(get_thd_id(), trans_2pc_count, 1);
  INC_STATS(get_thd_id(), trans_finish_count, 1);
  INC_STATS(get_thd_id(), trans_commit_count, 1);
  INC_STATS(get_thd_id(), trans_total_count, 1);

  // Send result back to client
#if !SERVER_GENERATE_QUERIES
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,CL_RSP),txn_man->client_id);
    // printf("CL_RSP txn %ld client %d, is local %d?\n", txn_man->get_txn_id(), txn_man->client_id, IS_LOCAL(txn_man->get_txn_id()));
#endif
  // remove txn from pool
  // if(txn_man != NULL) release_txn_man();
  // Do not use txn_man after this
}

void WorkerThread::abort() {
  DEBUG_T("ABORT %ld -- %f\n", txn_man->get_txn_id(),
        (double)get_sys_clock() - run_starttime / BILLION);
  // TODO: TPCC Rollback here
	// printf("xxx txn %lu abort\n", txn_man->get_txn_id());
  ++txn_man->abort_cnt;
  txn_man->reset();

  uint64_t end_time = get_sys_clock();
  uint64_t timespan_short  = end_time - txn_man->txn_stats.restart_starttime;
  uint64_t two_pc_timespan  = end_time - txn_man->txn_stats.prepare_start_time;
  uint64_t finish_timespan  = end_time - txn_man->txn_stats.finish_start_time;
  uint64_t prepare_timespan = txn_man->txn_stats.finish_start_time - txn_man->txn_stats.prepare_start_time;
  INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
  INC_STATS(get_thd_id(), trans_prepare_count, 1);

  INC_STATS(get_thd_id(), trans_2pc_time, two_pc_timespan);
  INC_STATS(get_thd_id(), trans_finish_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_abort_time, finish_timespan);
  INC_STATS(get_thd_id(), trans_total_run_time, timespan_short);
  INC_STATS(get_thd_id(), trans_abort_total_run_time, timespan_short);

  INC_STATS(get_thd_id(), trans_2pc_count, 1);
  INC_STATS(get_thd_id(), trans_finish_count, 1);
  INC_STATS(get_thd_id(), trans_abort_count, 1);
  INC_STATS(get_thd_id(), trans_total_count, 1);
  #if WORKLOAD != DA //actually DA do not need real abort. Just count it and do not send real abort msg.
  uint64_t penalty =
      abort_queue.enqueue(get_thd_id(), txn_man->get_txn_id(), txn_man, txn_man->get_abort_cnt());
  // printf("abort txn %ld client %d, is local %d?\n", txn_man->get_txn_id(), txn_man->client_id, IS_LOCAL(txn_man->get_txn_id()));
  txn_man->txn_stats.total_abort_time += penalty;
  release_txn_man();
  #endif
}

TxnManager * WorkerThread::get_transaction_manager(Message * msg) {
#if CC_ALG == CALVIN
  TxnManager* local_txn_man =
      txn_table.get_transaction_manager(get_thd_id(), msg->get_txn_id(), msg->get_batch_id());
#else
  TxnManager * local_txn_man = txn_table.get_transaction_manager(get_thd_id(),msg->get_txn_id(),0);
#endif
  return local_txn_man;
}

char type2char(DATxnType txn_type)
{
  switch (txn_type)
  {
    case DA_READ:
      return 'R';
    case DA_WRITE:
      return 'W';
    case DA_COMMIT:
      return 'C';
    case DA_ABORT:
      return 'A';
    case DA_SCAN:
      return 'S';
    default:
      return 'U';
  }
}

void WorkerThread::no_routines() {
     _routines = new coroutine_func_t[1];

     _routines[0] = coroutine_func_t(bind(&WorkerThread::run, this, _1, 0));
     printf("Init coroutine succ\n");
}
RC WorkerThread::run(yield_func_t &yield, uint64_t cor_id) {

  tsetup();
  printf("Running WorkerThread %ld\n",_thd_id);

  uint64_t ready_starttime;
  uint64_t idle_starttime = 0;
  uint64_t get_msg_starttime = 0;

	while(!simulation->is_done()) {
    get_msg_starttime = get_sys_clock();
    txn_man = NULL;
    heartbeat();

    progress_stats();
    Message* msg;

  // DA takes msg logic

  // #define TEST_MSG_order
  #ifdef TEST_MSG_order
    while(1)
    {
      msg = work_queue.dequeue(get_thd_id());
      if (!msg) {
        if (idle_starttime == 0) idle_starttime = get_sys_clock();
        continue;
      }
      printf("s seq_id:%lu type:%c trans_id:%lu item:%c state:%lu next_state:%lu\n",
      ((DAClientQueryMessage*)msg)->seq_id,
      type2char(((DAClientQueryMessage*)msg)->txn_type),
      ((DAClientQueryMessage*)msg)->trans_id,
      static_cast<char>('x'+((DAClientQueryMessage*)msg)->item_id),
      ((DAClientQueryMessage*)msg)->state,
      (((DAClientQueryMessage*)msg)->next_state));
      fflush(stdout);
    }
  #endif

    msg = work_queue.dequeue(get_thd_id());
    uint64_t get_msg_endtime = get_sys_clock();
    INC_STATS(_thd_id,worker_idle_time, get_msg_endtime - get_msg_starttime);
    INC_STATS(_thd_id,worker_msg_time, get_msg_endtime - get_msg_starttime);
    if(!msg) {
      if (idle_starttime == 0) idle_starttime = get_sys_clock();
      //todo: add sleep 0.01ms
      continue;
    }
    simulation->last_da_query_time = get_sys_clock();
    if(idle_starttime > 0) {
      // INC_STATS(_thd_id,worker_idle_time,get_sys_clock() - idle_starttime);
      idle_starttime = 0;
    }
    //uint64_t starttime = get_sys_clock();
    DEBUG_T("worker run txn %ld type %d \n", msg->get_txn_id(),msg->get_rtype());
    if((msg->rtype != CL_QRY && msg->rtype != CL_QRY_O && msg->rtype != RLOG && msg->rtype != RFIN_LOG) || CC_ALG == CALVIN) {
      txn_man = get_transaction_manager(msg);//获取事务的管理器，没有就建一个
      if(msg->rtype == RACK_LOG || msg->rtype == RACK_FIN_LOG || msg->rtype == RACK_FIN || msg->rtype == RACK_PREP || msg->rtype == RACK_PREP_CONT || msg->rtype == SET_CO_TS || 
      msg->rtype == RACK_PRE_PREP
      //  || msg->rtype == RPREPARE
       ){ 
        // printf("txn %ld type %d check already commit\n", msg->get_txn_id(),msg->get_rtype());
        if(!txn_man || !(txn_man->txn) || !txn_man->query || 
        // txn_man->query->partitions_touched.size() == 0 || 
        txn_man->abort_cnt != msg->current_abort_cnt) {//txn already committed
          DEBUG_T("txn %ld type %d already commit, query?%d, partitions_touched?%d, abort cnt %d, msg abort cnt %d\n", msg->get_txn_id(),msg->get_rtype(), !txn_man->query, txn_man->query->partitions_touched.size() == 0 , txn_man->abort_cnt, msg->current_abort_cnt);
          continue;
        }
        else if (txn_man->txn_state == COMMITING && (msg->rtype == RACK_PREP || msg->rtype == RACK_PREP_CONT || msg->rtype == RACK_PRE_PREP)) {
          DEBUG_T("txn %ld type %d state %ld\n", msg->get_txn_id(),msg->get_rtype(), txn_man->txn_state);
          continue;
        }
        else if (txn_man->txn_state == COMMIT && (msg->rtype == RACK_FIN || msg->rtype == SET_CO_TS)) {
          DEBUG_T("txn %ld type %d state %ld\n", msg->get_txn_id(),msg->get_rtype(), txn_man->txn_state);
          continue;
        }
      }
      if(!txn_man->txn){
        // assert(msg->rtype == RACK_PREP || msg->rtype == RACK_FIN);
        printf("because no txn?\n");   
        // bool ready = txn_man->set_ready();
        // assert(ready);
        continue; // in majority, txn_man may has been destroyed when receive msgs
      } 

      if ((CC_ALG != CALVIN) && IS_LOCAL(txn_man->get_txn_id())) {
        if (msg->rtype != RTXN_CONT &&
            ((msg->rtype != RACK_PREP) || (txn_man->get_rsp_cnt() == 1))) {
          txn_man->txn_stats.work_queue_time_short += msg->lat_work_queue_time;
          txn_man->txn_stats.cc_block_time_short += msg->lat_cc_block_time;
          txn_man->txn_stats.cc_time_short += msg->lat_cc_time;
          txn_man->txn_stats.msg_queue_time_short += msg->lat_msg_queue_time;
          txn_man->txn_stats.process_time_short += msg->lat_process_time;
          /*
          if (msg->lat_network_time/BILLION > 1.0) {
            printf("%ld %d %ld -> %ld: %f %f\n",msg->txn_id, msg->rtype,
          msg->return_node_id,get_node_id() ,msg->lat_network_time/BILLION,
          msg->lat_other_time/BILLION);
          }
          */
          txn_man->txn_stats.network_time_short += msg->lat_network_time;
        }

      } else {
          txn_man->txn_stats.clear_short();
      }
      if (CC_ALG != CALVIN) {
        txn_man->txn_stats.lat_network_time_start = msg->lat_network_time;
        txn_man->txn_stats.lat_other_time_start = msg->lat_other_time;
      }
      txn_man->txn_stats.msg_queue_time += msg->mq_time;
      txn_man->txn_stats.msg_queue_time_short += msg->mq_time;
      msg->mq_time = 0;
      txn_man->txn_stats.work_queue_time += msg->wq_time;
      txn_man->txn_stats.work_queue_time_short += msg->wq_time;
      //txn_man->txn_stats.network_time += msg->ntwk_time;
      msg->wq_time = 0;
      txn_man->txn_stats.work_queue_cnt += 1;


      ready_starttime = get_sys_clock();

      bool ready;
      // if((msg->rtype == RQRY || msg->rtype == RPREPARE || msg->rtype == RFIN) && msg->current_abort_cnt > txn_man->txn_stats.abort_cnt){
      //   ready = false;
      // }
      // else 
      #if !USE_TAPIR && !EARLY_PREPARE
      if(msg->rtype == RFIN && !txn_man->finish_read_write) ready = false;
      else 
      ready = txn_man->unset_ready(); 
      #else
      ready = txn_man->unset_ready(); 
      #endif

      INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
      
      if(!ready) {
        // Return to work queue, end processing
        DEBUG_T("txn %ld type %d not ready\n", msg->get_txn_id(),msg->get_rtype());
        work_queue.enqueue(get_thd_id(),msg,true);
        continue;
      }
      txn_man->register_thread(this);
    }

    process(yield, msg, cor_id);

    // process(msg);  /// DA
    ready_starttime = get_sys_clock();
    if(txn_man) {
      bool ready = txn_man->set_ready();//为真时，表明ready为假，
      if (!ready) {
        DEBUG_T("txn %ld type %d set ready failed, ready %d\n", msg->get_txn_id(),msg->get_rtype(), txn_man->txn_ready);
        assert(ready);
      }
    }
    INC_STATS(get_thd_id(),worker_deactivate_txn_time,get_sys_clock() - ready_starttime);

    // delete message
    ready_starttime = get_sys_clock();
#if CC_ALG != CALVIN
    msg->release();
#endif
    INC_STATS(get_thd_id(),worker_release_msg_time,get_sys_clock() - ready_starttime);
	}
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

RC WorkerThread::process_set_co_ts(yield_func_t &yield, Message * msg, uint64_t cor_id) {
#if CLV == CLV3
  txn_man->set_commit_timestamp(((FinishMessage*)msg)->commit_timestamp);
  if(txn_man->get_rc() == RCOK){
    txn_man->retire(yield, cor_id);
    txn_man->finish_retire = true;
  }
#endif
  return RCOK;
}

RC WorkerThread::process_rfin(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG_T("RFIN %ld from %ld\n",msg->get_txn_id(),msg->return_node_id);
  assert(CC_ALG != CALVIN);
  M_ASSERT_V(!IS_LOCAL(msg->get_txn_id()), "RFIN local: %ld %ld/%d\n", msg->get_txn_id(),
             msg->get_txn_id() % g_node_cnt, g_node_id);
#if CC_ALG == MAAT || CC_ALG == MV_WOUND_WAIT || CC_ALG == MV_NO_WAIT
  txn_man->set_commit_timestamp(((FinishMessage*)msg)->commit_timestamp);
#endif
#if (CC_ALG == MV_WOUND_WAIT || CC_ALG == MV_NO_WAIT) && CLV == CLV3
  assert(txn_man->get_commit_timestamp() > 0);

#endif
  txn_man->abort_cnt = msg->current_abort_cnt;
  txn_man->set_rc(((FinishMessage*)msg)->rc);
  if(((FinishMessage*)msg)->rc == RCOK){
    assert(txn_man->get_rc() == RCOK);
  }
  if (txn_man->get_rc() == RCOK){
    txn_man->txn_state = COMMITING;
#if CLV == CLV2
    txn_man->retire(yield, cor_id);
    txn_man->finish_retire = true;
#endif   
  }

#if USE_REPLICA && !USE_TAPIR
  if(txn_man->get_local_log()){
    txn_man->log_replica(RFIN_LOG, GET_NODE_ID(msg->get_txn_id()));
    RC rc = WAIT_REM;
    return rc;
  }
#endif
  
  if(((FinishMessage*)msg)->rc == Abort) {
	  // printf("xxx txn %lu send rack_fin, rc = %d\n", txn_man->get_txn_id(), txn_man->get_rc());
    txn_man->abort(yield, cor_id);
    txn_man->reset();
    txn_man->reset_query();
    // printf("%d:%d send abort finish ack to %d\n", g_node_id, msg->get_txn_id(), GET_NODE_ID(msg->get_txn_id()));  
    msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN),
                      GET_NODE_ID(msg->get_txn_id()));
    return Abort;
  }

//now commit 
  txn_man->commit(yield, cor_id);
  assert(txn_man->finish_retire == true);
  //if(!txn_man->query->readonly() || CC_ALG == OCC)
  if (!((FinishMessage*)msg)->readonly || CC_ALG == MAAT || CC_ALG == OCC || USE_TAPIR || CC_ALG == NO_WAIT || CC_ALG == SSI || CC_ALG == MV_NO_WAIT || CC_ALG == MV_WOUND_WAIT)
#if TAPIR_DEBUG
    printf("%d:%d send commit finish ack to %d\n", g_node_id, msg->get_txn_id(), GET_NODE_ID(msg->get_txn_id()));
#endif
    msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN),
                      GET_NODE_ID(msg->get_txn_id()));
  // release_txn_man();
	// printf("xxx txn %lu send rack_fin, rc = %d\n", txn_man->get_txn_id(), txn_man->get_rc());

  return RCOK;
}

RC WorkerThread::process_rlog(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  RC rc = RCOK;
  DEBUG_T("RLOG %ld from %ld\n",msg->get_txn_id(),msg->return_node_id);
  // txn_man->abort_cnt = msg->current_abort_cnt;
  // txn_man->set_rc(rc);
#if USE_REPLICA
  // pthread_mutex_lock(&log_lock);
	// log_count ++;
	// log_content = log_count;
	// pthread_mutex_unlock(&log_lock);
#endif
  DEBUG_T("%d:%d send rack log to %d\n", g_node_id, msg->get_txn_id(), msg->return_node_id);
  Message * return_msg = Message::create_message(NULL,RACK_LOG,msg->get_txn_id());
  return_msg->current_abort_cnt = msg->current_abort_cnt;
  msg_queue.enqueue(get_thd_id(),return_msg,msg->return_node_id);
  return rc;
}

RC WorkerThread::process_rack_log(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  RC rc = RCOK;
  DEBUG_T("RACK_LOG %ld from %ld\n",msg->get_txn_id(),msg->return_node_id);
  int responses_left = txn_man->received_log_response(((AckMessage*)msg)->rc);
  // if(txn_man->get_return_node() == g_node_id){
  uint64_t prepare_message_timespan  = get_sys_clock() - txn_man->txn_stats.log_start_time;
  // INC_STATS(get_thd_id(), trans_prepare_log_message_time, prepare_message_timespan);
  // INC_STATS(get_thd_id(), trans_prepare_log_message_count, 1);
  // }
  assert(responses_left >=0);
#if MAJORITY
  if(responses_left == 1){//able to return with 2 confirm(local+one remote)
#else
  if(responses_left == 0){
#endif
    INC_STATS(get_thd_id(), trans_prepare_log_message_time, prepare_message_timespan);
    INC_STATS(get_thd_id(), trans_prepare_log_message_count, 1);


    //这个情况是远程的日志写完都返回prepare消息了，本地还没写完，的情况
		if(txn_man->get_return_node() == g_node_id){
      assert(IS_LOCAL(txn_man->get_txn_id()));    
      if(txn_man->get_rsp_cnt() > 0) return WAIT;//如果远程prepare还没回应完，等待

#if CC_ALG == MV_NO_WAIT || CC_ALG == MV_WOUND_WAIT
        if (ATOM_CAS(txn_man->prep_ready,false,false)){
          assert(txn_man->txn_state == PREPARE);
          ATOM_CAS(txn_man->need_prep_cont,false,true);
          return WAIT_REM;
        }
#endif
          
      //finish
      uint64_t finish_start_time = get_sys_clock();
			txn_man->txn_stats.finish_start_time = finish_start_time;
			uint64_t prepare_timespan  = finish_start_time - txn_man->txn_stats.prepare_start_time;
      
      INC_STATS(get_thd_id(), trans_logging_count, 1);
      // assert(txn_man->start_logging_time!=0);
      // printf("logging time %lu", get_sys_clock() - txn_man->start_logging_time);
      INC_STATS(get_thd_id(), trans_logging_time, get_sys_clock() - txn_man->start_logging_time);
      txn_man->start_fin_time = get_sys_clock();
			
			// INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
      // INC_STATS(get_thd_id(), trans_prepare_count, 1);
#if CC_ALG == MV_NO_WAIT || CC_ALG == MV_WOUND_WAIT
      rc = txn_man->get_rc();
      if(rc == Abort) {
        txn_man->send_finish_messages();
        txn_man->abort(yield, cor_id);
      }else{
#if CO_LOG
        txn_man->send_colog_messages();
        rc = WAIT_REM;
        return rc;
#endif
        txn_man->send_finish_messages();
        txn_man->txn_state = COMMITING;
        assert(txn_man->get_local_log());
        txn_man->log_replica(RFIN_LOG, g_node_id); 
        rc = WAIT_REM;
        return rc;
      }
#else
#if EARLY_PREPARE
      if(txn_man->get_rc()==Abort) return Abort;
      // if(txn_man->aborted) return Abort;
#else
      assert(txn_man->get_rc()==RCOK);
#endif
#if CO_LOG
      txn_man->send_colog_messages();
      rc = WAIT_REM;
      return rc;
#endif
      txn_man->send_finish_messages();
      assert(txn_man->get_local_log());
      txn_man->log_replica(RFIN_LOG, g_node_id); 
      rc = WAIT_REM;
      return rc;
#endif 

    }else{
      DEBUG_T("%d:%d send rack prep to %d\n", g_node_id, txn_man->get_txn_id(), txn_man->get_return_node());
#if CC_ALG == MV_WOUND_WAIT || CC_ALG == MV_NO_WAIT
#if CLV == CLV2 || CLV == CLV3
      //验证事务，写完prepare日志后验证
        if (ATOM_CAS(txn_man->prep_ready,false,false)){
          assert(txn_man->txn_state == PREPARE);
          ATOM_CAS(txn_man->need_prep_cont,false,true);
          return WAIT;
        }
#endif
#endif

#if EARLY_PREPARE
	    // printf("xxx txn %lu send rack_prep, rc = %d\n", txn_man->get_txn_id(), txn_man->get_rc());
      txn_man->finish_read_write = true;
#endif  
      msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man,RACK_PREP),txn_man->get_return_node());
    }    
  }
  return RCOK;
}

RC WorkerThread::process_rack_prep_cont(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  RC rc = RCOK;  
  assert(txn_man->need_prep_cont == false);
  //这里其实可以增加统计，日志写完后，等待了多长时间进行统计信息
    //这个情况是远程的日志写完都返回prepare消息了，本地的还不能开始提交的情况
    if(txn_man->has_local_write()){
       assert(txn_man->get_log_rsp_cnt() == 0 || txn_man->get_log_rsp_cnt() == 1 );
    }else{
      assert(txn_man->get_log_rsp_cnt() == 0);
    }
		if(txn_man->get_return_node() == g_node_id){
      assert(IS_LOCAL(txn_man->get_txn_id()));
      if(txn_man->get_rsp_cnt() > 0) return WAIT;//如果远程prepare还没回应完，等待  
      //finish
      uint64_t finish_start_time = get_sys_clock();
			txn_man->txn_stats.finish_start_time = finish_start_time;
			uint64_t prepare_timespan  = finish_start_time - txn_man->txn_stats.prepare_start_time;
      
      INC_STATS(get_thd_id(), trans_logging_count, 1);
      // assert(txn_man->start_logging_time!=0);
      // printf("logging time %lu", get_sys_clock() - txn_man->start_logging_time);
      INC_STATS(get_thd_id(), trans_logging_time, get_sys_clock() - txn_man->start_logging_time);
      txn_man->start_fin_time = get_sys_clock();
			
			// INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
      // INC_STATS(get_thd_id(), trans_prepare_count, 1);
      rc = txn_man->get_rc();
      if(rc == Abort) {
        txn_man->send_finish_messages();
        txn_man->abort(yield, cor_id);
      }else{
#if CO_LOG
        txn_man->send_colog_messages();
        rc = WAIT_REM;
        return rc;
#endif
        txn_man->send_finish_messages();
        txn_man->txn_state = COMMITING;
        if(txn_man->get_local_log()){
          txn_man->log_replica(RFIN_LOG, g_node_id); 
          rc = WAIT_REM;
          return rc;
        }
        return RCOK;
      }
    }else{
      msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man,RACK_PREP),txn_man->get_return_node());
      return RCOK;
    }    
}

RC WorkerThread::process_rfin_log(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  RC rc = RCOK;
  // txn_man->set_rc(rc);
  DEBUG_T("RFIN LOG %ld from %ld\n",msg->get_txn_id(),msg->return_node_id);
  // txn_man->abort_cnt = msg->current_abort_cnt;
#if USE_REPLICA
  // pthread_mutex_lock(&log_lock);
	// log_count ++;
	// log_content = log_count;
	// pthread_mutex_unlock(&log_lock);
#endif
  // printf("%d:%d send rack fin log to %d\n", g_node_id, msg->get_txn_id(), msg->return_node_id);
  Message * return_msg = Message::create_message(NULL,RACK_FIN_LOG,msg->get_txn_id());
  return_msg->current_abort_cnt = msg->current_abort_cnt;
  msg_queue.enqueue(get_thd_id(),return_msg,msg->return_node_id);
  return rc;
}

RC WorkerThread::process_rack_fin_log(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG_T("RACK FIN LOG %ld from %ld\n",msg->get_txn_id(),msg->return_node_id);
  int responses_left = txn_man->received_log_fin_response(((AckMessage*)msg)->rc);
  assert(responses_left >=0);

#if MAJORITY
  if(responses_left == 1){//able to return with 2 confirm(local+one remote)
#else
  if(responses_left == 0){
#endif

    bool is_local = txn_man->get_return_node() == g_node_id;
    RC txn_rc = txn_man->get_rc();
  
  	if(is_local){
      if(txn_rc == RCOK){
        txn_man->commit(yield, cor_id);
        assert(txn_man->finish_retire == true);
      }else{
        txn_man->abort(yield, cor_id);
      }
      if(txn_man->get_fin_rsp_cnt() > 0) return WAIT;
      //finish
      txn_man->txn_stats.twopc_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
      if(txn_rc == RCOK) {
        commit();
      }
      else abort();
    }else{
  	  // printf("xxx txn %lu send rack_fin, rc = %d\n", txn_man->get_txn_id(), txn_man->get_rc());
      if(txn_rc == RCOK){
        txn_man->commit(yield, cor_id);
        assert(txn_man->finish_retire == true);
      }else{
        txn_man->abort(yield, cor_id);
        txn_man->reset();
        txn_man->reset_query();
      }  
      msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man, RACK_FIN), txn_man->get_return_node());      
      // release_txn_man();
    }    
  }
  return RCOK;
}

RC WorkerThread::process_rack_pre_prep(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  RC rc = RCOK;
  int responses_left = 0;

  if (!txn_man || !(txn_man->txn) || !txn_man->query || txn_man->query->partitions_touched.size() == 0 || txn_man->abort_cnt != msg->current_abort_cnt) {
    DEBUG_T("RPREP_ACK skip %ld from %ld\n",msg->get_txn_id(),msg->get_return_id());
    return RCOK;
  }
  responses_left = txn_man->received_pre_response(((AckMessage*)msg)->rc);
  //计算最大提交时间戳，这里只有这种并发控制才有,本地的在txn中直接生成并计算了
#if CC_ALG == MV_NO_WAIT || CC_ALG == MV_WOUND_WAIT
  txn_man->set_max_prepare_timestamp(((AckMessage*)msg)->prepare_timestamp);
#endif
  assert(responses_left >= 0);
  if (responses_left > 0) return WAIT;

  if(txn_man->get_rc() != Abort) {
    //设置提交时间戳，发送消息
    txn_man->set_commit_timestamp(txn_man->max_prepare_timestamp);
    txn_man->send_co_ts_messages();
    txn_man->retire(yield, cor_id);
    txn_man->finish_retire = true;
  } 
  return rc;
}


RC WorkerThread::process_rack_prep(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG_T("RPREP_ACK %ld from %ld\n",msg->get_txn_id(),msg->get_return_id());
  RC rc = RCOK;
  int responses_left = 0;

  if (!txn_man || !(txn_man->txn) || !txn_man->query || txn_man->query->partitions_touched.size() == 0 || txn_man->abort_cnt != msg->current_abort_cnt) {
    DEBUG_T("RPREP_ACK skip %ld from %ld\n",msg->get_txn_id(),msg->get_return_id());
    return RCOK;
  }
  // if(!txn_man || !(txn_man->txn)){//txn already committed
  //   return rc;
  // }
#if TAPIR_DEBUG
  printf("%d receive prep rack messages from %d\n", txn_man->get_txn_id(), msg->return_node_id);
#endif
#if USE_TAPIR
  responses_left = txn_man->received_tapir_response(((AckMessage*)msg)->rc, msg->return_node_id);
  // if(responses_left < 0) {
  //   return RCOK;
  // }
  
  assert(responses_left >= 0);
  // if (responses_left == 0) printf("recive prepare %d message\n", txn_man->prepare_count);
#else
  responses_left = txn_man->received_response(((AckMessage*)msg)->rc);
  //计算最大提交时间戳，这里只有这种并发控制才有,本地的在txn中直接生成并计算了
#if CC_ALG == MV_NO_WAIT || CC_ALG == MV_WOUND_WAIT
	txn_man->set_max_prepare_timestamp(((AckMessage*)msg)->prepare_timestamp);
  //prepare发送的消息是终止的话，将本地的事务的相关设置设为该回滚时的情况
  if(((AckMessage*)msg)->rc == Abort){
    if(ATOM_CAS(txn_man->prep_ready, false, true)){
      ATOM_CAS(txn_man->inconflict, txn_man->inconflict, -1);
    }
  }
#endif
uint64_t prepare_message_timespan = get_sys_clock() - txn_man->txn_stats.prepare_start_time;
INC_STATS(get_thd_id(), trans_prepare_message_time, prepare_message_timespan);
INC_STATS(get_thd_id(), trans_prepare_message_count, 1);

assert(responses_left >= 0);
#endif
#if CC_ALG == MAAT
  // Integrate bounds
  uint64_t lower = ((AckMessage*)msg)->lower;
  uint64_t upper = ((AckMessage*)msg)->upper;
  if(lower > time_table.get_lower(get_thd_id(),msg->get_txn_id())) {
    time_table.set_lower(get_thd_id(),msg->get_txn_id(),lower);
  }
  if(upper < time_table.get_upper(get_thd_id(),msg->get_txn_id())) {
    time_table.set_upper(get_thd_id(),msg->get_txn_id(),upper);
  }
  DEBUG("%ld bound set: [%ld,%ld] -> [%ld,%ld]\n", msg->get_txn_id(), lower, upper,
        time_table.get_lower(get_thd_id(), msg->get_txn_id()),
        time_table.get_upper(get_thd_id(), msg->get_txn_id()));
  if(((AckMessage*)msg)->rc != RCOK) {
    time_table.set_state(get_thd_id(),msg->get_txn_id(),MAAT_ABORTED);
  }
#endif

#if CC_ALG == WOOKONG
  // Integrate bounds
  uint64_t lower = ((AckMessage*)msg)->lower;
  uint64_t upper = ((AckMessage*)msg)->upper;
  if(lower > wkdb_time_table.get_lower(get_thd_id(),msg->get_txn_id())) {
    wkdb_time_table.set_lower(get_thd_id(),msg->get_txn_id(),lower);
  }
  if(upper < wkdb_time_table.get_upper(get_thd_id(),msg->get_txn_id())) {
    wkdb_time_table.set_upper(get_thd_id(),msg->get_txn_id(),upper);
  }
  DEBUG("%ld bound set: [%ld,%ld] -> [%ld,%ld]\n",msg->get_txn_id(),lower,upper,wkdb_time_table.get_lower(get_thd_id(),msg->get_txn_id()),wkdb_time_table.get_upper(get_thd_id(),msg->get_txn_id()));
  if(((AckMessage*)msg)->rc != RCOK) {
    wkdb_time_table.set_state(get_thd_id(),msg->get_txn_id(),WKDB_ABORTED);
  }
#endif
#if CC_ALG == DTA || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
  // Integrate bounds
  uint64_t lower = ((AckMessage*)msg)->lower;
  uint64_t upper = ((AckMessage*)msg)->upper;
  if (lower > dta_time_table.get_lower(get_thd_id(), msg->get_txn_id())) {
    dta_time_table.set_lower(get_thd_id(), msg->get_txn_id(), lower);
  }
  if (upper < dta_time_table.get_upper(get_thd_id(), msg->get_txn_id())) {
    dta_time_table.set_upper(get_thd_id(), msg->get_txn_id(), upper);
  }
  DEBUG("%ld bound set: [%ld,%ld] -> [%ld,%ld]\n", msg->get_txn_id(), lower, upper,
        dta_time_table.get_lower(get_thd_id(), msg->get_txn_id()),
        dta_time_table.get_upper(get_thd_id(), msg->get_txn_id()));
  if(((AckMessage*)msg)->rc != RCOK) {
    dta_time_table.set_state(get_thd_id(), msg->get_txn_id(), DTA_ABORTED);
  }
#endif
#if CC_ALG == SILO
  uint64_t max_tid = ((AckMessage*)msg)->max_tid;
  txn_man->find_tid_silo(max_tid);
#endif

#if EARLY_PREPARE
  if(txn_man->get_rc() == Abort && !txn_man->aborted) {
    txn_man->send_finish_messages();
    if(txn_man->get_local_log()){
      txn_man->log_replica(RFIN_LOG, g_node_id); 
      // rc = WAIT_REM;
      // return rc;
    }else
      txn_man->abort(yield, cor_id);
  }
#endif

  if (responses_left > 0) return WAIT;
#if !USE_TAPIR
#if MAJORITY
  if (txn_man->get_log_rsp_cnt() > 1) return WAIT;
#else
  if (txn_man->get_log_rsp_cnt() > 0) return WAIT;  
#endif
#endif
//远程和本地日志都写完了，判断本地事务是否还有依赖存在,如果prepare回应的有回滚，那么不需要等待其依赖，直接回滚，否则等待其依赖解除
  // Done waiting
  if(txn_man->get_rc() == RCOK) {
    rc = txn_man->validate(yield, cor_id);//mv2pl没有这一步
  }


#if CC_ALG == MV_NO_WAIT || CC_ALG == MV_WOUND_WAIT
  if(txn_man->get_rc() == RCOK) {
    if (ATOM_CAS(txn_man->prep_ready,false,false)){
      assert(txn_man->txn_state == PREPARE);
      ATOM_CAS(txn_man->need_prep_cont,false,true);
      return WAIT_REM;
    }
  }
#endif
  assert(IS_LOCAL(txn_man->get_txn_id()));
  if(IS_LOCAL(txn_man->get_txn_id())) {
		INC_STATS(get_thd_id(), trans_logging_count, 1);
    // assert(txn_man->start_logging_time!=0);
    // printf("logging time %lu", get_sys_clock() - txn_man->start_logging_time);
		INC_STATS(get_thd_id(), trans_logging_time, get_sys_clock() - txn_man->start_logging_time);
		txn_man->start_fin_time = get_sys_clock();
	}
  uint64_t finish_start_time = get_sys_clock();
  txn_man->txn_stats.finish_start_time = finish_start_time;
  uint64_t prepare_timespan  = finish_start_time - txn_man->txn_stats.prepare_start_time;
  // INC_STATS(get_thd_id(), trans_prepare_time, prepare_timespan);
  // INC_STATS(get_thd_id(), trans_prepare_count, 1);

  if(rc == Abort || txn_man->get_rc() == Abort) {
    txn_man->set_rc(Abort);
    rc = Abort;
  }
#if USE_TAPIR
  // printf("prepare %d\n", txn_man->txn_state);
  if(txn_man->txn_state == PREPARE) {
    txn_man->send_finish_messages();
    txn_man->txn_state = COMMITING;
  } else {
    return rc;
  }
  if(rc == Abort) {
    txn_man->abort(yield, cor_id);
  }else{
    // if(txn_man->query->partitions_touched.size() != 0)
      txn_man->commit(yield, cor_id);
  }
  return rc;
#endif
//这里才对，这里和本地处理那里应该一样吧 
  if(rc == Abort) {
#if !EARLY_PREPARE
    txn_man->send_finish_messages();
    txn_man->abort(yield, cor_id);
#else
    // if(!txn_man->aborted) {
    //   txn_man->send_finish_messages();
    //   if(txn_man->get_local_log()){
    //     txn_man->log_replica(RFIN_LOG, g_node_id); 
    //     rc = WAIT_REM;
    //     return rc;
    //   }else
    //     txn_man->abort(yield, cor_id);
    // }
#endif
  } else {
#if CO_LOG
    txn_man->send_colog_messages();
    rc = WAIT_REM;
    return rc;
#endif   
    txn_man->send_finish_messages();
    txn_man->txn_state = COMMITING;
#if USE_REPLICA
    if(txn_man->get_local_log()){
      txn_man->log_replica(RFIN_LOG, g_node_id); 
      rc = WAIT_REM;
      return rc;
    }else{
      txn_man->commit(yield, cor_id);
      assert(txn_man->finish_retire == true);
    }
#else
    // if(txn_man->query->partitions_touched.size() != 0)
      txn_man->commit(yield, cor_id);
#endif
// #endif
  }
  return rc;
}

RC WorkerThread::process_rack_co_log(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  // printf("RCO_LOG_ACK %ld from %ld\n",msg->get_txn_id(),msg->get_return_id());
  RC rc = RCOK;
  int responses_left = 0;

  responses_left = txn_man->received_response(((AckMessage*)msg)->rc);

  assert(responses_left >= 0);

  
  if (responses_left > 0) return WAIT;
  // Done waiting
  txn_man->send_finish_messages();
  txn_man->txn_state = COMMITING;
#if CLV == CLV2
  //调用退休函数，此时事务回滚的情况只有日志写失败，但是不可能,这里不需要判断事务是否回滚，能走到这一步，一定能提交，那些直接finish回滚的是没有prepare阶段，所以finishi哪里需要判断是否回滚，然后确定是否退休，这里不需要
  txn_man->retire(yield,cor_id);
  txn_man->finish_retire = true;
#endif
#if USE_REPLICA
//写完本地日志在写finish日志，不然直接提交？？
    if(txn_man->get_local_log()){
      txn_man->log_replica(RFIN_LOG, g_node_id); 
      rc = WAIT_REM;
      return rc;
    }else{
      txn_man->commit(yield, cor_id);
      assert(txn_man->finish_retire == true);
    }
#else
    // if(txn_man->query->partitions_touched.size() != 0)
      txn_man->commit(yield, cor_id);
#endif
  return rc;
}
RC WorkerThread::process_rack_rfin(Message * msg) {
  DEBUG_T("RFIN_ACK %ld from %d\n",msg->get_txn_id(), msg->return_node_id);

  RC rc = RCOK;
  int responses_left = 0;
  if (!txn_man || !(txn_man->txn) || !txn_man->query || txn_man->query->partitions_touched.size() == 0 || txn_man->abort_cnt != msg->current_abort_cnt) {
    DEBUG_T("RPREP_ACK skip %ld from %ld\n",msg->get_txn_id(),msg->get_return_id());
    return RCOK;
  }
#if TAPIR_DEBUG
  printf("%d receive rfin rack messages from %d\n", txn_man->get_txn_id(), msg->return_node_id);
#endif
#if USE_TAPIR
  responses_left = txn_man->received_tapir_fin_response(((AckMessage*)msg)->rc, msg->return_node_id);
  assert(responses_left >= 0);
  // if(responses_left < 0) {
  //   return RCOK;
  // }
  // if (responses_left == 0) printf("%d recive commit %d message\n",txn_man->get_txn_id(), txn_man->commit_count);
#else
  responses_left = txn_man->received_fin_response(((AckMessage*)msg)->rc);
  assert(responses_left >=0);
  
#endif
  if (responses_left > 0) return WAIT;
#if !USE_TAPIR 
#if MAJORITY
  if (txn_man->get_log_fin_rsp_cnt() > 1) return WAIT;
#else
  if (txn_man->get_log_fin_rsp_cnt() > 0) return WAIT;
#endif
#endif
  // Done waiting
  txn_man->txn_state = COMMIT;
  INC_STATS(get_thd_id(), trans_fin_count, 1);
	INC_STATS(get_thd_id(), trans_fin_time, get_sys_clock() - txn_man->start_fin_time);
	// start_fin_time = get_sys_clock();
  txn_man->txn_stats.twopc_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
#if USE_TAPIR
  // printf("responses_left %d %d\n", responses_left, txn_man->txn_state);
  if(txn_man->txn_state != COMMIT) return rc;
  // if (responses_left == 0) printf("%d recive commit %d message\n",txn_man->get_txn_id(), txn_man->commit_count);
  if(txn_man->get_rc() == RCOK) {
#if TAPIR_DEBUG
  printf("%d commit\n", txn_man->get_txn_id());
#endif
      commit();
  } else {
#if TAPIR_DEBUG
  printf("%d abort\n", txn_man->get_txn_id());
#endif
      abort();
  }
  // txn_man->txn_state = 0;
#else
  if(txn_man->get_rc() == RCOK) {
      commit();
  } else {
      abort();
  }
#endif
  return rc;
}

RC WorkerThread::process_rqry_rsp(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG_T("RQRY_RSP %ld from %ld\n",msg->get_txn_id(),msg->return_node_id);
  assert(IS_LOCAL(msg->get_txn_id()));
#if TAPIR_DEBUG
  printf("%d receive rqry rsp messages from %d\n", txn_man->get_txn_id(), msg->return_node_id);
#endif

#if PARAL_SUBTXN == true
  if (!txn_man->query || txn_man->query->partitions_touched.size() == 0 || txn_man->abort_cnt != msg->current_abort_cnt) return RCOK;

  int responses_left = txn_man->received_response(((AckMessage*)msg)->rc);
  assert(responses_left >=0);
  
  if(!txn_man->aborted && ((QueryResponseMessage*)msg)->rc == Abort) {
    txn_man->start_abort(yield, cor_id);
  }

  if (responses_left > 0) return WAIT;
  //Done Waiting
  txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  INC_STATS(get_thd_id(), trans_read_write_time, get_sys_clock() - txn_man->start_rw_time);
  INC_STATS(get_thd_id(), trans_read_write_count, 1);
  // printf("enter prepare phase %ld\n", get_sys_clock() - txn_man->start_rw_time);
  // printf("read/write time %ld\n", get_sys_clock() - txn_man->start_rw_time);
  txn_man->start_logging_time = get_sys_clock();

  RC rc = txn_man->get_rc();
  if(rc == RCOK){

    rc = txn_man->start_commit(yield, cor_id);
  }
#else
  txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  
  if(((QueryResponseMessage*)msg)->rc == Abort) {
    txn_man->start_abort(yield, cor_id);
    return Abort;
  }
  RC rc = ((QueryResponseMessage*)msg)->rc;
  rc = txn_man->run_txn(yield, cor_id);
#endif

  check_if_done(rc);
  return rc;
}

RC WorkerThread::process_rqry(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG_T("RQRY %ld from %ld\n",msg->get_txn_id(),msg->return_node_id);
#if ONE_NODE_RECIEVE == 1 && defined(NO_REMOTE) && LESS_DIS_NUM == 10
#else
  M_ASSERT_V(!IS_LOCAL(msg->get_txn_id()), "RQRY local: %ld %ld/%d\n", msg->get_txn_id(),
             msg->get_txn_id() % g_node_cnt, g_node_id);
  assert(!IS_LOCAL(msg->get_txn_id()));
#endif
  RC rc = RCOK;

  msg->copy_to_txn(txn_man);

#if CC_ALG == MVCC 
  txn_table.update_min_ts(get_thd_id(),txn_man->get_txn_id(),0,txn_man->get_timestamp());
#endif

#if CC_ALG == SSI || CC_ALG == MV_NO_WAIT || CC_ALG == MV_WOUND_WAIT
    txn_table.update_min_ts(get_thd_id(),txn_man->get_txn_id(),0,txn_man->get_start_timestamp());
#endif

#if CC_ALG == MAAT
    time_table.init(get_thd_id(),txn_man->get_txn_id());
#endif
  txn_man->abort_cnt = msg->current_abort_cnt;
  rc = txn_man->run_txn(yield, cor_id);

  // Send response
  if(rc != WAIT){
	  // printf("xxx txn %lu send rack_prep, rc = %d\n", txn_man->get_txn_id(), txn_man->get_rc());
    txn_man->finish_read_write = true;
#if EARLY_PREPARE
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_PREP),txn_man->return_id);
#else
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RQRY_RSP),txn_man->return_id);
#endif
  }
  return rc;
}

RC WorkerThread::process_rqry_cont(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG_T("RQRY_CONT %ld from %ld\n",msg->get_txn_id(),msg->return_node_id);
  assert(!IS_LOCAL(msg->get_txn_id()));
  RC rc = txn_man->get_rc();
  if(rc != Abort){
    txn_man->run_txn_post_wait();
    rc = txn_man->run_txn(yield, cor_id);
  }
  // Send response
  if(rc != WAIT) {
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RQRY_RSP),txn_man->return_id);
  }
  return rc;
}


RC WorkerThread::process_rtxn_cont(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG_T("RTXN_CONT %ld\n",msg->get_txn_id());
  assert(IS_LOCAL(msg->get_txn_id()));

  txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  if(txn_man->get_rc() != Abort){
    txn_man->run_txn_post_wait();
  }
  RC rc = txn_man->run_txn(yield, cor_id);
  check_if_done(rc);
  return RCOK;
}

RC WorkerThread::process_rprepare(yield_func_t &yield, Message * msg, uint64_t cor_id) {
    DEBUG_T("RPREP %ld\n",msg->get_txn_id());
    RC rc = RCOK;
    txn_man->abort_cnt = msg->current_abort_cnt;
#if USE_REPLICA
#if USE_TAPIR
#if TAPIR_DEBUG
    printf("%d:%d send prepare ack to %d\n", g_node_id, msg->get_txn_id(), GET_NODE_ID(msg->get_txn_id()));
#endif
    pthread_mutex_lock(&log_lock);
    log_count ++;
    log_content = log_count;
    pthread_mutex_unlock(&log_lock);
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_PREP),msg->return_node_id);
    return rc;
#else
//生成本地提交时间戳
#if CC_ALG == MV_WOUND_WAIT || CC_ALG == MV_NO_WAIT
  txn_man->txn_state = PREPARE;
  if(txn_man->has_local_write()){
    txn_man->set_prepare_timestamp(get_next_ts());
//clv3验证
#if CLV == CLV3
    msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man,RACK_PRE_PREP),msg->return_node_id); 
#endif
    txn_man->log_replica(RLOG, msg->return_node_id);
    rc = WAIT_REM;
    return rc;
  }else{
    txn_man->set_prepare_timestamp(txn_man->get_start_timestamp());
#if CLV == CLV3
    msg_queue.enqueue(get_thd_id(), Message::create_message(txn_man,RACK_PRE_PREP),msg->return_node_id);
#endif
    if (ATOM_CAS(txn_man->prep_ready,false,false)){
      assert(txn_man->txn_state == PREPARE);
      ATOM_CAS(txn_man->need_prep_cont,false,true);
      return WAIT_REM;
    }
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_PREP),msg->return_node_id);
    return rc;
  }


#else
    txn_man->log_replica(RLOG, msg->return_node_id);
    rc = WAIT_REM;
    return rc;
#endif
#endif
#else
#if CC_ALG == TICTOC
    // Integrate bounds
    TxnManager * txn_man = txn_table.get_transaction_manager(get_thd_id(),msg->get_txn_id(),0);
    PrepareMessage* pmsg = (PrepareMessage*)msg;
    txn_man->_min_commit_ts = pmsg->_min_commit_ts;
    // txn_man->_min_commit_ts = txn_man->_min_commit_ts > qmsg->_min_commit_ts ?
    //                         txn_man->_min_commit_ts : qmsg->_min_commit_ts;
#endif
    // Validate transaction
    rc  = txn_man->validate(yield, cor_id);
    txn_man->set_rc(rc);
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man,RACK_PREP),msg->return_node_id);
    // Clean up as soon as abort is possible
    if(rc == Abort) {
      txn_man->abort(yield, cor_id);
    }
    return rc;
#endif
}

RC WorkerThread::process_rco_log(yield_func_t &yield, Message * msg, uint64_t cor_id) {
    // printf("rco log %ld, return_id %ld \n",msg->get_txn_id(), msg->return_node_id);
    RC rc = RCOK;
    msg_queue.enqueue(get_thd_id(),Message::create_message(txn_man, RACK_CO_LOG), msg->return_node_id);
    return rc;
}

uint64_t WorkerThread::get_next_txn_id() {
  uint64_t txn_id =
      (get_node_id() + get_thd_id() * g_node_cnt) + (g_thread_cnt * g_node_cnt * _thd_txn_id);
  ++_thd_txn_id;
  return txn_id;
}

RC WorkerThread::process_rtxn( yield_func_t &yield, Message * msg, uint64_t cor_id) {
  RC rc = RCOK;
  uint64_t txn_id = UINT64_MAX;
  bool is_cl_o = msg->get_rtype() == CL_QRY_O;
  if(msg->get_rtype() == CL_QRY || msg->get_rtype() == CL_QRY_O) {
    // This is a new transaction
    // Only set new txn_id when txn first starts
    #if WORKLOAD == DA
      msg->txn_id=((DAClientQueryMessage*)msg)->trans_id;
      txn_id=((DAClientQueryMessage*)msg)->trans_id;
    #else
      txn_id = get_next_txn_id();
      msg->txn_id = txn_id;
    #endif
    // Put txn in txn_table
    txn_man = txn_table.get_transaction_manager(get_thd_id(),txn_id,0);
    txn_man->register_thread(this);//生成事务的线程吗？？
    uint64_t ready_starttime = get_sys_clock();
    bool ready = txn_man->unset_ready();
    INC_STATS(get_thd_id(),worker_activate_txn_time,get_sys_clock() - ready_starttime);
    assert(ready);
    if (CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT) {
      #if WORKLOAD == DA //mvcc use timestamp
        if (da_stamp_tab.count(txn_man->get_txn_id())==0)
        {
          da_stamp_tab[txn_man->get_txn_id()]=get_next_ts();
          txn_man->set_timestamp(da_stamp_tab[txn_man->get_txn_id()]);
        }
        else
        txn_man->set_timestamp(da_stamp_tab[txn_man->get_txn_id()]);
      #else
      txn_man->set_timestamp(get_next_ts());
      #endif
    }
    txn_man->txn_stats.starttime = get_sys_clock();
    txn_man->txn_stats.restart_starttime = txn_man->txn_stats.starttime;
    msg->copy_to_txn(txn_man);
    DEBUG_T("START %ld %f %lu\n", txn_man->get_txn_id(),
          simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
    #if WORKLOAD==DA
      if(da_start_trans_tab.count(txn_man->get_txn_id())==0)
      {
        da_start_trans_tab.insert(txn_man->get_txn_id());
          INC_STATS(get_thd_id(),local_txn_start_cnt,1);
      }
    #else
      INC_STATS(get_thd_id(), local_txn_start_cnt, 1);
    #endif

  } else {
    txn_man->txn_stats.restart_starttime = get_sys_clock();
    DEBUG_T("RESTART %ld %f %lu\n", txn_man->get_txn_id(),
        simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
  }
    // Get new timestamps
  if(is_cc_new_timestamp()) {
  #if WORKLOAD==DA //mvcc use timestamp
    if(da_stamp_tab.count(txn_man->get_txn_id())==0)
    {
      da_stamp_tab[txn_man->get_txn_id()]=get_next_ts();
      txn_man->set_timestamp(da_stamp_tab[txn_man->get_txn_id()]);
    }
    else
      txn_man->set_timestamp(da_stamp_tab[txn_man->get_txn_id()]);
  #else
    txn_man->set_timestamp(get_next_ts());
  #endif
  }

#if CC_ALG == MVCC 
    txn_table.update_min_ts(get_thd_id(),txn_id,0,txn_man->get_timestamp());
#endif
#if CC_ALG == OCC || CC_ALG == SSI || CC_ALG == MV_WOUND_WAIT || CC_ALG == MV_NO_WAIT || CC_ALG == MV_DL_DETECT
  #if WORKLOAD==DA
    if(da_start_stamp_tab.count(txn_man->get_txn_id())==0)
    {
      da_start_stamp_tab[txn_man->get_txn_id()]=get_next_ts();
      txn_man->set_start_timestamp(da_start_stamp_tab[txn_man->get_txn_id()]);
    }
    else
      txn_man->set_start_timestamp(da_start_stamp_tab[txn_man->get_txn_id()]);
  #else
      txn_man->set_start_timestamp(get_next_ts());
      txn_table.update_min_ts(get_thd_id(),txn_man->get_txn_id(),0,txn_man->get_start_timestamp());
  #endif
#endif
#if CC_ALG == MAAT
  #if WORKLOAD==DA
  if(da_start_stamp_tab.count(txn_man->get_txn_id())==0)
  {
    da_start_stamp_tab[txn_man->get_txn_id()]=1;
    time_table.init(get_thd_id(), txn_man->get_txn_id());
    assert(time_table.get_lower(get_thd_id(), txn_man->get_txn_id()) == 0);
    assert(time_table.get_upper(get_thd_id(), txn_man->get_txn_id()) == UINT64_MAX);
    assert(time_table.get_state(get_thd_id(), txn_man->get_txn_id()) == MAAT_RUNNING);
  }
  #else
  time_table.init(get_thd_id(),txn_man->get_txn_id());
  assert(time_table.get_lower(get_thd_id(),txn_man->get_txn_id()) == 0);
  assert(time_table.get_upper(get_thd_id(),txn_man->get_txn_id()) == UINT64_MAX);
  assert(time_table.get_state(get_thd_id(),txn_man->get_txn_id()) == MAAT_RUNNING);
  #endif
#endif

  rc = init_phase();

  txn_man->txn_stats.init_complete_time = get_sys_clock();
  INC_STATS(get_thd_id(),trans_init_time, txn_man->txn_stats.init_complete_time - txn_man->txn_stats.restart_starttime);
  INC_STATS(get_thd_id(),trans_init_count, 1);
  if (rc != RCOK) return rc;
  #if WORKLOAD == DA
    printf("thd_id:%lu stxn_id:%lu batch_id:%lu seq_id:%lu type:%c rtype:%d trans_id:%lu item:%c laststate:%lu state:%lu next_state:%lu\n",
      this->_thd_id,
      ((DAClientQueryMessage*)msg)->txn_id,
      ((DAClientQueryMessage*)msg)->batch_id,
      ((DAClientQueryMessage*)msg)->seq_id,
      type2char(((DAClientQueryMessage*)msg)->txn_type),
      ((DAClientQueryMessage*)msg)->rtype,
      ((DAClientQueryMessage*)msg)->trans_id,
      static_cast<char>('x'+((DAClientQueryMessage*)msg)->item_id),
      ((DAClientQueryMessage*)msg)->last_state,
      ((DAClientQueryMessage*)msg)->state,
      ((DAClientQueryMessage*)msg)->next_state);
    fflush(stdout);
  #endif
  // Execute transaction
  if (is_cl_o) {
    rc = txn_man->send_remote_request();
  } else {
    rc = txn_man->run_txn(yield, cor_id);
  }
  check_if_done(rc);
  return rc;
}

RC WorkerThread::init_phase() {
  RC rc = RCOK;
  //m_query->part_touched[m_query->part_touched_cnt++] = m_query->part_to_access[0];
  return rc;
}


RC WorkerThread::process_log_msg(Message * msg) {
  assert(ISREPLICA);
  DEBUG_T("REPLICA PROCESS %ld\n",msg->get_txn_id());
  LogRecord * record = logger.createRecord(&((LogMessage*)msg)->record);
  logger.enqueueRecord(record);
  return RCOK;
}

RC WorkerThread::process_log_msg_rsp(Message * msg) {
  DEBUG_T("REPLICA RSP %ld\n",msg->get_txn_id());
  txn_man->repl_finished = true;
  if (txn_man->log_flushed) commit();
  return RCOK;
}

RC WorkerThread::process_log_flushed(Message * msg) {
  DEBUG("LOG FLUSHED %ld\n",msg->get_txn_id());
  if(ISREPLICA) {
    msg_queue.enqueue(get_thd_id(), Message::create_message(msg->txn_id, LOG_MSG_RSP),
                      GET_NODE_ID(msg->txn_id));
    return RCOK;
  }

  txn_man->log_flushed = true;
  if (g_repl_cnt == 0 || txn_man->repl_finished) commit();
  return RCOK;
}

RC WorkerThread::process_rfwd(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG("RFWD (%ld,%ld)\n",msg->get_txn_id(),msg->get_batch_id());
  txn_man->txn_stats.remote_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  assert(CC_ALG == CALVIN);
  int responses_left = txn_man->received_response(((ForwardMessage*)msg)->rc);
  assert(responses_left >=0);
  if(txn_man->calvin_collect_phase_done()) {
    assert(ISSERVERN(txn_man->return_id));
    RC rc = txn_man->run_calvin_txn(yield, cor_id);
    if(rc == RCOK && txn_man->calvin_exec_phase_done()) {
      calvin_wrapup(yield, cor_id);
      return RCOK;
    }
  }
  return WAIT;
}

RC WorkerThread::process_calvin_rtxn(yield_func_t &yield, Message * msg, uint64_t cor_id) {
  DEBUG("START %ld %f %lu\n", txn_man->get_txn_id(),
        simulation->seconds_from_start(get_sys_clock()), txn_man->txn_stats.starttime);
  assert(ISSERVERN(txn_man->return_id));
  txn_man->txn_stats.local_wait_time += get_sys_clock() - txn_man->txn_stats.wait_starttime;
  // Execute
  RC rc = txn_man->run_calvin_txn(yield, cor_id);
  // if((txn_man->phase==6 && rc == RCOK) || txn_man->active_cnt == 0 || txn_man->participant_cnt ==
  // 1) {
  if(rc == RCOK && txn_man->calvin_exec_phase_done()) {
    calvin_wrapup(yield, cor_id);
  }
  return RCOK;
}

bool WorkerThread::is_cc_new_timestamp() {
  return (CC_ALG == MVCC || CC_ALG == TIMESTAMP);
}

ts_t WorkerThread::get_next_ts() {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager.get_ts(get_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager.get_ts(get_thd_id());
		return _curr_ts;
	}
}
ts_t WorkerThread::co_get_next_ts(uint64_t cor_id) {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager.get_ts(get_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager.get_ts(get_thd_id());
    _curr_ts = _curr_ts * COROUTINE_CNT + cor_id;
		return _curr_ts;
	}
}
/*
bool WorkerThread::is_mine(Message* msg) {  //TODO:have some problems!
  if (((DAQueryMessage*)msg)->seq_id % THREAD_CNT == get_thd_id()) {
    return true;
}
  return false;
	}
*/
bool WorkerThread::is_mine(Message* msg) {  //TODO:have some problems!
  if (((DAQueryMessage*)msg)->trans_id == get_thd_id()) {
    return true;
  }
  return false;
}

void WorkerNumThread::setup() {
}

RC WorkerNumThread::run() {
  tsetup();
  printf("Running WorkerNumThread %ld\n",_thd_id);

  // uint64_t idle_starttime = 0;
  int i = 0;
	while(!simulation->is_done()) {
    progress_stats();

    uint64_t wq_size = work_queue.get_wq_cnt();
    uint64_t tx_size = work_queue.get_txn_cnt();
    uint64_t ewq_size = work_queue.get_enwq_cnt();
    uint64_t dwq_size = work_queue.get_dewq_cnt();

    uint64_t etx_size = work_queue.get_entxn_cnt();
    uint64_t dtx_size = work_queue.get_detxn_cnt();

    work_queue.set_detxn_cnt();
    work_queue.set_dewq_cnt();
    work_queue.set_entxn_cnt();
    work_queue.set_enwq_cnt();

    INC_STATS(_thd_id,work_queue_wq_cnt[i],wq_size);
    INC_STATS(_thd_id,work_queue_tx_cnt[i],tx_size);

    INC_STATS(_thd_id,work_queue_ewq_cnt[i],ewq_size);
    INC_STATS(_thd_id,work_queue_dwq_cnt[i],dwq_size);

    INC_STATS(_thd_id,work_queue_etx_cnt[i],etx_size);
    INC_STATS(_thd_id,work_queue_dtx_cnt[i],dtx_size);
    i++;
    sleep(1);
    printf("------------%d s-----------%s--\n", i, simulation->is_warmup_done()?"exec":"warm");
    // if(idle_starttime ==0)
    //   idle_starttime = get_sys_clock();

    // if(get_sys_clock() - idle_starttime > 1000000000) {
    //   i++;
    //   idle_starttime = 0;
    // }
    //uint64_t starttime = get_sys_clock();

	}
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}
