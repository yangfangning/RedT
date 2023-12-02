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

#include "global.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "txn.h"

#include "catalog.h"
#include "global.h"
#include "mem_alloc.h"
#include "row_lock.h"
#include "row_maat.h"
#include "row_mvcc.h"
#include "row_occ.h"
#include "row_ts.h"
#include "row_null.h"
#include "mem_alloc.h"
#include "manager.h"
#include "wl.h"
#include "row_ssi.h"
#include "row_mv2pl.h"

#define SIM_FULL_ROW true

int row_t::get_row_size(int tuple_size){
	int size = sizeof(row_t);
	return size;
} 

RC row_t::init(table_t *host_table, uint64_t part_id, uint64_t row_id) {
	part_info = true;
	_row_id = row_id;
	_part_id = part_id;
	this->table = host_table;
    table_idx = host_table->table_id;

	Catalog * schema = host_table->get_schema();
	tuple_size = schema->get_tuple_size();
    
	memset(table_name, 0, 15);
	memcpy(table_name, host_table->get_table_name(), strlen(host_table->get_table_name()));
	#if SIM_FULL_ROW
		data = (char *) mem_allocator.alloc(sizeof(char) * tuple_size);
	#else
		data = (char *) mem_allocator.alloc(sizeof(uint64_t) * 1);
	#endif

	return RCOK;
}

RC row_t::switch_schema(table_t *host_table) {
	this->table = host_table;
	return RCOK;
}

void row_t::init_manager(row_t * row) {
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE
	return;
#endif
	DEBUG_M("row_t::init_manager alloc \n");
#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == CALVIN || CC_ALG == WOUND_WAIT
	manager = (Row_lock *) mem_allocator.align_alloc(sizeof(Row_lock));
#elif CC_ALG == TIMESTAMP
	manager = (Row_ts *) mem_allocator.align_alloc(sizeof(Row_ts));
#elif CC_ALG == MVCC
	manager = (Row_mvcc *) mem_allocator.align_alloc(sizeof(Row_mvcc));
#elif CC_ALG == OCC
	manager = (Row_occ *) mem_allocator.align_alloc(sizeof(Row_occ));
#elif CC_ALG == MAAT
	manager = (Row_maat *) mem_allocator.align_alloc(sizeof(Row_maat));
#elif CC_ALG == CNULL
	manager = (Row_null *) mem_allocator.align_alloc(sizeof(Row_null));
#elif CC_ALG == SSI
    manager = (Row_ssi *) mem_allocator.align_alloc(sizeof(Row_ssi));
#elif CC_ALG == MV_WOUND_WAIT || CC_ALG == MV_NO_WAIT || CC_ALG == MV_DL_DETECT
    manager = (Row_mv2pl *) mem_allocator.align_alloc(sizeof(Row_mv2pl));
#endif

#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC
	manager->init(this);
#endif
}

table_t *row_t::get_table() { return table; }

Catalog *row_t::get_schema() { return get_table()->get_schema(); }

const char *row_t::get_table_name() { return get_table()->get_table_name(); };
uint64_t row_t::get_tuple_size() { return tuple_size; }

uint64_t row_t::get_field_cnt() { return get_schema()->field_cnt; }

void row_t::set_value(int id, void * ptr) {
	// table* table = get_table();
	int datasize = get_schema()->get_field_size(id);
	int pos = get_schema()->get_field_index(id);
	DEBUG("set_value pos %d datasize %d -- %lx\n", pos, datasize, (uint64_t)this);
#if SIM_FULL_ROW
	memcpy( &data[pos], ptr, datasize);
#else
	char d[tuple_size];
	memcpy( &d[pos], ptr, datasize);
#endif
}

void row_t::set_value(int id, void * ptr, int size) {
	int pos = get_schema()->get_field_index(id);
#if SIM_FULL_ROW
	memcpy( &data[pos], ptr, size);
#else
	char d[tuple_size];
	memcpy( &d[pos], ptr, size);
#endif
}

void row_t::set_value(const char * col_name, void * ptr) {
	uint64_t id = get_schema()->get_field_id(col_name);
	set_value(id, ptr);
}

SET_VALUE(uint64_t);
SET_VALUE(int64_t);
SET_VALUE(double);
SET_VALUE(UInt32);
SET_VALUE(SInt32);

GET_VALUE(uint64_t);
// GET_VALUE(int64_t);
void row_t::get_value(int col_id, int64_t & value) {
	int pos = get_schema()->get_field_index(col_id);
	DEBUG("get_value pos %d -- %lx\n",pos,(uint64_t)this); 
	assert(pos <= tuple_size);
	value = *(int64_t *)&data[pos];
}
GET_VALUE(double);
GET_VALUE(UInt32);
GET_VALUE(SInt32);

char * row_t::get_value(int id) {
	int pos __attribute__ ((unused));
	pos = get_schema()->get_field_index(id);
	DEBUG("get_value pos %d -- %lx\n",pos,(uint64_t)this);
#if SIM_FULL_ROW
	return &data[pos];
#else
	return data;
#endif
}

char * row_t::get_value(char * col_name) {
	uint64_t pos __attribute__ ((unused));
	pos = get_schema()->get_field_index(col_name);
#if SIM_FULL_ROW
	return &data[pos];
#else
	return data;
#endif
}

char *row_t::get_data() { return data; }

void row_t::set_data(char * data) {
	int ts = tuple_size;
#if SIM_FULL_ROW
	memcpy(this->data, data, ts);
#else
	char d[ts];
	memcpy(d, data, ts);
#endif
}
// copy from the src to this
void row_t::copy(row_t * src) {
    assert(src!=NULL);
    //printf("src->table_name = %s ; this->table_name = %s\n",src->table_name,this->table_name);
	assert(src->get_schema() == this->get_schema());
#if SIM_FULL_ROW
	set_data(src->get_data());
#else
	char d[tuple_size];
	set_data(d);
#endif
}
//将这一行中的data清空
void row_t::free_row() {
	DEBUG_M("row_t::free_row free\n");

#if SIM_FULL_ROW
	mem_allocator.free(data, sizeof(char) * get_tuple_size());
#else
	mem_allocator.free(data, sizeof(uint64_t) * 1);
#endif

}

RC row_t::get_lock(access_t type, TxnManager * txn) {
	RC rc = RCOK;
#if CC_ALG == CALVIN
	lock_t lt = (type == RD || type == SCAN)? DLOCK_SH : DLOCK_EX;
	rc = this->manager->lock_get(lt, txn);
#endif
	return rc;
}


RC row_t::remote_copy_row(row_t* remote_row, TxnManager * txn, Access *access) {
  RC rc = RCOK;
  uint64_t init_time = get_sys_clock();
  txn->cur_row = (row_t *) mem_allocator.alloc(row_t::get_row_size(remote_row->tuple_size));
  INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);
  
  uint64_t copy_time = get_sys_clock();
  memcpy((char*)txn->cur_row, (char*)remote_row, row_t::get_row_size(remote_row->tuple_size));
  access->data = txn->cur_row;
//   printf("remote_copy_row.cpp:286】table_name = %s operate_size = %ld tuple_size = %ld sizeof(row_t)=%d\n",txn->cur_row->table_name,row_t::get_row_size(remote_row->tuple_size),txn->cur_row->tuple_size,sizeof(row_t));
  //access->orig_row = txn->cur_row;

  INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
  return rc;
}
//读写操作从这进入
RC row_t::get_row(yield_func_t &yield,access_t type, TxnManager *txn, Access *access,uint64_t cor_id) {
  RC rc = RCOK;
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE
	access->data = this;
		return rc;
#endif
#if ISOLATION_LEVEL == NOLOCK
	access->data = this;
		return rc;
#endif

#if CC_ALG == CNULL
  	uint64_t init_time = get_sys_clock();
	txn->cur_row = (row_t *) mem_allocator.alloc(row_t::get_row_size(tuple_size));
	txn->cur_row->init(get_table(), get_part_id());
    INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);

	rc = this->manager->access(type,txn);

  	uint64_t copy_time = get_sys_clock();
	// txn->cur_row->copy(this);
	access->data = txn->cur_row;
  	INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
	goto end;
#endif
#if CC_ALG == MAAT
  uint64_t init_time = get_sys_clock();
  DEBUG_M("row_t::get_row MAAT alloc \n");
	txn->cur_row = (row_t *) mem_allocator.alloc(row_t::get_row_size(tuple_size));
	txn->cur_row->init(get_table(), get_part_id());
  INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);

  rc = this->manager->access(type,txn);

  uint64_t copy_time = get_sys_clock();
  txn->cur_row->copy(this);
	access->data = txn->cur_row;
	//assert(rc == RCOK);
  INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
	goto end;
#endif


#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == WOUND_WAIT
  	uint64_t init_time = get_sys_clock();
	//uint64_t thd_id = txn->get_thd_id();
	lock_t lt = (type == RD || type == SCAN) ? DLOCK_SH : DLOCK_EX; // ! this wrong !!
    INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);
	rc = this->manager->lock_get(lt, txn);
  	uint64_t copy_time = get_sys_clock();
	access->data = this;
	if (rc == RCOK) {
	} else if (rc == Abort) {
		// total_num_atomic_retry++;
	} else if (rc == WAIT) {
		ASSERT(CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT);
	}
  	INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
	goto end;
#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == SSI || CC_ALG == MV_NO_WAIT || CC_ALG == MV_WOUND_WAIT
  uint64_t init_time = get_sys_clock();
#if CC_ALG == TIMESTAMP
	DEBUG_M("row_t::get_row TIMESTAMP alloc \n");
	txn->cur_row = (row_t *) mem_allocator.alloc(row_t::get_row_size(tuple_size));
	txn->cur_row->init(get_table(), this->get_part_id());
	assert(txn->cur_row->get_schema() == this->get_schema());
#elif CC_ALG == MV_NO_WAIT || CC_ALG == MV_WOUND_WAIT
	INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);
	lock_t lt = (type == RD || type == SCAN) ? DLOCK_SH : DLOCK_EX; 
	rc = this->manager->access(txn, lt, NULL);
  	uint64_t copy_time = get_sys_clock();
	if (rc == RCOK) {
		access->data = txn->cur_row;
	} else if (rc == Abort) {
		// total_num_atomic_retry++;
	} else if (rc == WAIT) {
		rc = WAIT;
		INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
		goto end;
	}
	if (rc != Abort) {
		assert(access->data->get_data() != NULL);
		assert(access->data->get_table() != NULL);
		assert(access->data->get_schema() == this->get_schema());
		assert(access->data->get_table_name() != NULL);
	}
#else
	INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);
	uint64_t copy_time = get_sys_clock();
	// row_t * row;

	if (type == WR) {
		rc = this->manager->access(txn, P_REQ, NULL);
		if (rc != RCOK) goto end;
	}
	if ((type == WR && rc == RCOK) || type == RD || type == SCAN) {
		rc = this->manager->access(txn, R_REQ, NULL);

    	copy_time = get_sys_clock();

		if (rc == RCOK ) {
			access->data = txn->cur_row;
		} else if (rc == WAIT) {
			rc = WAIT;
			INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
			goto end;
		} else if (rc == Abort) {
		}
		if (rc != Abort) {
			assert(access->data->get_data() != NULL);
			assert(access->data->get_table() != NULL);
			assert(access->data->get_schema() == this->get_schema());
			assert(access->data->get_table_name() != NULL);
		}
	}
#endif
	//找到了！！！多版本，对于写操作，会创建一个新的行
	if (rc != Abort && (CC_ALG == MVCC || CC_ALG == SSI || CC_ALG == MV_NO_WAIT || CC_ALG == MV_WOUND_WAIT) && type == WR) {
		DEBUG_M("row_t::get_row MVCC alloc \n");
		row_t * newr = (row_t *) mem_allocator.alloc(row_t::get_row_size(tuple_size));
		newr->init(this->get_table(), get_part_id());
		newr->copy(access->data);
		access->data = newr;
	}
	INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
	goto end;
#elif CC_ALG == OCC
	// OCC always make a local copy regardless of read or write
  uint64_t init_time = get_sys_clock();
	DEBUG_M("row_t::get_row OCC alloc \n");
	txn->cur_row = (row_t *) mem_allocator.alloc(row_t::get_row_size(tuple_size));
	txn->cur_row->init(get_table(), get_part_id());
  INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);

	rc = this->manager->access(txn, R_REQ);

  uint64_t copy_time = get_sys_clock();
	access->data = txn->cur_row;
  INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
	goto end;
#elif CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == CALVIN
#if CC_ALG == HSTORE_SPEC
	if(txn_table.spec_mode) {
		DEBUG_M("row_t::get_row HSTORE_SPEC alloc \n");
		txn->cur_row = (row_t *) mem_allocator.alloc(row_t::get_row_size(tuple_size));
		txn->cur_row->init(get_table(), get_part_id());
		rc = this->manager->access(txn, R_REQ);
		access->data = txn->cur_row;
		goto end;
	}
#endif
	access->data = this;
	goto end;
#else
	assert(false);
#endif

end:
	return rc;
}



RC row_t::get_ts(uint64_t &orig_wts, uint64_t &orig_rts) {
	RC rc = RCOK;
	return rc;
}

RC row_t::get_row(access_t type, TxnManager * txn, row_t *& row, uint64_t &orig_wts, uint64_t &orig_rts) {
		RC rc = RCOK;
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE
		row = this;
		return rc;
#endif
	return rc;
}
// Return call for get_row if waiting
RC row_t::get_row_post_wait(access_t type, TxnManager * txn, row_t *& row) {
	RC rc = RCOK;
  uint64_t init_time = get_sys_clock();
	assert(CC_ALG == WAIT_DIE || CC_ALG == MVCC || CC_ALG == TIMESTAMP || CC_ALG == WOUND_WAIT || CC_ALG == SSI || CC_ALG == MV_WAIT_DIE || CC_ALG == MV_WOUND_WAIT || CC_ALG == MV_NO_WAIT);
#if CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT
	assert(txn->lock_ready);
	rc = RCOK;
	//ts_t endtime = get_sys_clock();
	row = this;
#elif CC_ALG == MV_WOUND_WAIT || CC_ALG == MV_WAIT_DIE || CC_ALG == MV_NO_WAIT
	assert(txn->lock_ready);
	row = txn->cur_row;
	assert(row->get_data() != NULL);
	assert(row->get_table() != NULL);
	assert(row->get_schema() == this->get_schema());
	assert(row->get_table_name() != NULL);
	if (type == WR) {
		row_t * newr = (row_t *) mem_allocator.alloc(row_t::get_row_size(tuple_size));
		newr->init(this->get_table(), get_part_id());
		INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);
		uint64_t copy_time = get_sys_clock();
			newr->copy(row);
			row = newr;
		INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
	}
#elif CC_ALG == MVCC || CC_ALG == TIMESTAMP || CC_ALG == SSI
	assert(txn->ts_ready);
	//INC_STATS(thd_id, time_wait, t2 - t1);
	row = txn->cur_row;

	assert(row->get_data() != NULL);
	assert(row->get_table() != NULL);
	assert(row->get_schema() == this->get_schema());
	assert(row->get_table_name() != NULL);
	if (( CC_ALG == MVCC || CC_ALG == SSI) && type == WR) {
		DEBUG_M("row_t::get_row_post_wait MVCC alloc \n");
		row_t * newr = (row_t *) mem_allocator.alloc(row_t::get_row_size(tuple_size));
		newr->init(this->get_table(), get_part_id());
    INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);
    uint64_t copy_time = get_sys_clock();
		newr->copy(row);
		row = newr;
    INC_STATS(txn->get_thd_id(), trans_cur_row_copy_time, get_sys_clock() - copy_time);
	}
#endif
  INC_STATS(txn->get_thd_id(), trans_cur_row_init_time, get_sys_clock() - init_time);
	return rc;
}
#if CC_ALG == MV_NO_WAIT || CC_ALG == MV_WOUND_WAIT
void row_t::retire(TxnManager *txn, row_t *row) { this->manager->retire(txn, row); }
#else
void row_t::retire(TxnManager *txn, row_t *row) {}
#endif
// the "row" is the row read out in get_row(). For locking based CC_ALG,
// the "row" is the same as "this". For timestamp based CC_ALG, the
// "row" != "this", and the "row" must be freed.
// For MVCC, the row will simply serve as a version. The version will be
// delete during history cleanup.
// For TIMESTAMP, the row will be explicity deleted at the end of access().
// (c.f. row_ts.cpp)
uint64_t row_t::return_row(RC rc, access_t type, TxnManager *txn, row_t *row) {
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE
	return 0;
#endif
#if ISOLATION_LEVEL == NOLOCK
	return 0;
#endif

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == CALVIN || CC_ALG == WOUND_WAIT
	assert (row == NULL || row == this || type == XP);
	if (CC_ALG != CALVIN && ROLL_BACK &&
			type == XP) {  // recover from previous writes. should not happen w/ Calvin
		this->copy(row);
	}
	this->manager->lock_release(txn);
	return 0;
#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == SSI
	// for RD or SCAN or XP, the row should be deleted.
	// because all WR should be companied by a RD
	// for MVCC RD, the row is not copied, so no need to free.
	if (CC_ALG == TIMESTAMP && (type == RD || type == SCAN)) {
		row->free_row();
			DEBUG_M("row_t::return_row TIMESTAMP free \n");
		mem_allocator.free(row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	}
	if (type == XP) {
		row->free_row();
			DEBUG_M("row_t::return_row XP free \n");
		mem_allocator.free(row, row_t::get_row_size(ROW_DEFAULT_SIZE));
		this->manager->access(txn, XP_REQ, NULL);
	} else if (type == WR) {
		assert (type == WR && row != NULL);
		assert (row->get_schema() == this->get_schema());
		RC rc = this->manager->access(txn, W_REQ, row);
		assert(rc == RCOK);
	}
	return 0;
#elif CC_ALG == MV_WOUND_WAIT || CC_ALG == MV_NO_WAIT

	if (type == RD || type == SCAN) {
		//什么也不干，读操作不需要释放访问的行，也不会有事务依赖于该事务
		//这里在clv1不用操作，因为不会依赖于任何事物，在clv2或者3，应该要有判断是否可以提交，或者这个活应该放到退休里去干
	}else if (type == XP) {//回滚操作的话，需要将事务获取的行释放，然后执行回滚操作
		row->free_row();
		DEBUG_M("row_t::return_row XP free \n");
		mem_allocator.free(row, row_t::get_row_size(ROW_DEFAULT_SIZE));
		this->manager->lock_release(txn, XP1);
	} else if (type == WR) {//提交的话，对这行调用提交操作
		assert (type == WR && row != NULL);
		assert (row->get_schema() == this->get_schema());
#if CLV == CLV1
		this->manager->retire(txn, row);
#endif
		this->manager->lock_release(txn, W);
	}
	return 0;
#elif CC_ALG == OCC
	assert (row != NULL);
	if (type == WR) manager->write(row, txn->get_end_timestamp());
	row->free_row();
	DEBUG_M("row_t::return_row OCC free \n");
	mem_allocator.free(row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	manager->release();
	return 0;
#elif CC_ALG == CNULL
	assert (row != NULL);
	if (rc == Abort) {
		manager->abort(type,txn);
	} else {
		manager->commit(type,txn);
	}

		row->free_row();
	DEBUG_M("row_t::return_row Maat free \n");
		mem_allocator.free(row, row_t::get_row_size(ROW_DEFAULT_SIZE));
	return 0;
#elif CC_ALG == MAAT
	if (row != NULL) {
		row->free_row();
	    DEBUG_M("row_t::return_row Maat free \n");
		mem_allocator.free(row, row_t::get_row_size(ROW_DEFAULT_SIZE));
    }
	return 0;
#elif CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
	assert (row != NULL);
	if (ROLL_BACK && type == XP) {// recover from previous writes.
		this->copy(row);
	}
	return 0;
#else
	assert(false);
#endif
}
