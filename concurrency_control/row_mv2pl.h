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

#ifndef ROW_MV2PL
#define ROW_MV2PL

class table_t;
class Catalog;
class TxnManager;


struct Mv2plEntry {
  lock_t type;//类型可能没有用了，因为只有写锁，没有读锁
  ts_t   start_ts;//这个也不知道有什么用,，这个改为这个事务的开始时间戳吧
  TxnManager * txn;
  Mv2plEntry * next;
  Mv2plEntry * prev;
};

struct Mv2plhisEntry {
  ts_t   ts;
  row_t * row;//这里面存的只有表相关信息，没有管理者这类东西，还有data，其实就相当于存的data，不能调用其他功能
  TxnManager * txn;
  Mv2plhisEntry * next;
  Mv2plhisEntry * prev;
  bool commited = false;
};
//这个待定，与上面冲突了有点
struct Mv2plLockEntry {
    lock_t type;
    ts_t   start_ts;
    // TxnManager * txn;
    txnid_t txn;
    Mv2plLockEntry * next;
    Mv2plLockEntry * prev;
};

class Row_mv2pl {
public:

  void init(row_t * row);
  //在这里完成的操作应该是看输入的是什么操作，当读时通过时间戳判断怎么读，当是写时判断事务需要等待，或者是否中止，当读操作时，返回正确的当前行，否则返回不能读，或者等待，对于lockforread怎么设计还需要考虑，对于
	RC access(TxnManager * txn, lock_t type, row_t * row);
  //释放写锁,这里其实可以分为很多，提交，终止，在什么阶段终止
  void lock_release(TxnManager * txn ,lock_t type);
  //退休者，也就是可见操作
  void retire(TxnManager * txn, row_t * row);
  
private:
  //需要实现的函数

  //等待者
  Mv2plEntry * waiters_head;
	Mv2plEntry * waiters_tail;

  Mv2plEntry * waiters_read_head;
  Mv2plEntry * waiters_read_tail;
  //拥有者
  Mv2plEntry * owner;
  // 退休者的头是最老的数据，尾是最新的数据
  Mv2plhisEntry * retire_head;
  Mv2plhisEntry * retire_tail;
  UInt32 waiter_cnt;
 
  ts_t max_retire_cts;//最大的退休时间，有可能变小，因为可能回滚导致退休的事务回滚
  ts_t max_cts; //行上最大提交时间戳
  //历史数据的指针,历史数据只有一个时，头和尾指向一个，头是最早的数据，尾是最新的数据
  Mv2plhisEntry * writehis;
	Mv2plhisEntry * writehistail;
  uint64_t whis_len;//记录有多少个历史数据，不包括最初的一个

  // 创建新的版本和释放历史版本，会被上面的clear_history调用
  Mv2plhisEntry* creat_new_hisentry();
  void release_2pl_hisentry(Mv2plhisEntry* entry);
  void release_2pl_hisentry2(Mv2plhisEntry* entry);
  // 获取事务元组和释放事务元组
	Mv2plEntry* create_2pl_entry();
	void release_2pl_entry(Mv2plEntry * entry);
  
  
  //进行历史回收
  row_t * clear_history(ts_t ts);
  void clear_history(TxnManager * txn);

  //插入历史数据
  void insert_history( ts_t ts, TxnManager * txn, row_t * row);
  
//下面的待定

  pthread_mutex_t * latch;
  bool blatch;
  row_t * _row;
};

#endif
