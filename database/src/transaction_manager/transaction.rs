use std::collections::HashMap;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use crate::buffer_manager::buffer::Buffer;
use crate::buffer_manager::buffer_mgr::BufferMgr;
use crate::file_manager::block_id::BlockId;
use crate::file_manager::file_mgr::FileMgr;
use crate::log_manager::log_mgr::LogMgr;
use crate::transaction_manager::concurrency_manager::concurrency_mgr::ConcurrencyMgr;
use crate::transaction_manager::recovery_manager::RecoveryMgr;

pub struct BufferList {
    buffers: HashMap<BlockId, Arc<Mutex<Buffer>>>,
    pins: Vec<BlockId>,
    bm: Arc<Mutex<BufferMgr>>,
}

impl BufferList {
    pub fn new(bm: Arc<Mutex<BufferMgr>>) -> Self {
        Self {
            buffers: HashMap::new(),
            pins: Vec::new(),
            bm,
        }
    }

    pub fn get_buffer(&self, blk: BlockId) -> Option<Arc<Mutex<Buffer>>> {
        self.buffers.get(&blk).map(|b| b.clone())
    }

    pub fn pin(&mut self, blk: BlockId) {
        {
            // lock the buffer manager
            let mut bm_ = self.bm.lock().unwrap();
            let buff = bm_.pin(blk.clone());
            self.buffers.insert(blk.clone(), buff);
        }
        self.pins.push(blk.clone());
    }

    pub fn unpin(&mut self, blk: BlockId) {
        let buff = self.buffers.get(&blk).map(|b| b.clone());
        match buff {
            Some(buf) => {
                {
                    // lock the buffer manager
                    let mut bm_ = self.bm.lock().unwrap();
                    bm_.unpin(buf);
                }
                self.pins.retain(|b| b != &blk); // remove blk from pins
                if !self.pins.contains(&blk) {
                    self.buffers.remove(&blk);
                }
            }
            None => return,
        }
    }

    fn unpin_all(&mut self) {
        for blk in self.pins.clone() {
            let buff = self.buffers.get(&blk).map(|b| b.clone());
            if let Some(buf) = buff {
                {
                    // lock the buffer manager
                    let mut bm_ = self.bm.lock().unwrap();
                    bm_.unpin(buf);
                }
            }
        }
        self.buffers.clear();
        self.pins.clear();
    }
}

static NEXT_TX_NUM: AtomicI32 = AtomicI32::new(0); // for generating transaction numbers
const END_OF_FILE: i32 = -1; // dummy page number for end of file

pub struct Transaction {
    recovery_mgr: RecoveryMgr,
    concur_mgr: ConcurrencyMgr,
    bm: Arc<Mutex<BufferMgr>>,
    fm: FileMgr,
    txnum: i32,
    my_buffers: BufferList,
}

pub struct TransactionForUndo<'a> {
    pub(crate) concur_mgr: &'a mut ConcurrencyMgr,
    pub(crate) my_buffers: &'a mut BufferList,
}

impl Transaction {
    pub fn new(fm: FileMgr, lm: Arc<Mutex<LogMgr>>, bm: Arc<Mutex<BufferMgr>>) -> Self {
        let txnum = Self::next_tx_number();
        Self {
            recovery_mgr: RecoveryMgr::new(txnum, lm.clone(), bm.clone()),
            concur_mgr: ConcurrencyMgr::new(),
            bm: bm.clone(),
            fm: fm.clone(),
            txnum,
            my_buffers: BufferList::new(bm.clone()),
        }
    }

    pub fn commit(&mut self) {
        self.recovery_mgr.commit();
        self.concur_mgr.release();
        self.my_buffers.unpin_all();
        println!("transaction {} committed", self.txnum);
    }

    pub fn roll_back(&mut self) {
        let mut tx_for_undo = TransactionForUndo {
            concur_mgr: &mut self.concur_mgr,
            my_buffers: &mut self.my_buffers,
        };
        self.recovery_mgr.rollback(&mut tx_for_undo);
        self.concur_mgr.release();
        self.my_buffers.unpin_all();
        println!("transaction {} rolled back", self.txnum);
    }

    pub fn recovery(&mut self) {
        let mut tx_for_undo = TransactionForUndo {
            concur_mgr: &mut self.concur_mgr,
            my_buffers: &mut self.my_buffers,
        };

        {
            // lock the buffer manager
            let bm_ = self.bm.lock().unwrap();
            bm_.flush_all(self.txnum);
        }

        self.recovery_mgr.recover(&mut tx_for_undo);
    }

    pub fn pin(&mut self, blk: BlockId) {
        self.my_buffers.pin(blk);
    }
    pub fn unpin(&mut self, blk: BlockId) {
        self.my_buffers.unpin(blk);
    }

    pub fn get_int(&mut self, blk: BlockId, offset: i32) -> i32 {
        self.concur_mgr.slock(blk.clone());
        let buff = self.my_buffers.get_buffer(blk.clone());
        match buff {
            Some(b) => {
                let get_from_block: i32;
                {
                    // lock the buffer
                    let mut b_ = b.lock().unwrap();
                    get_from_block = b_.contents().get_int(offset).unwrap().clone();
                }
                return get_from_block;
            }
            None => panic!("Transaction::get_int: failed to get buffer"),
        }
    }

    pub fn get_string(&mut self, blk: BlockId, offset: i32) -> String {
        self.concur_mgr.slock(blk.clone());
        let buff = self.my_buffers.get_buffer(blk.clone());
        match buff {
            Some(b) => {
                let get_from_block: String;
                {
                    // lock the buffer
                    let mut b_ = b.lock().unwrap();
                    get_from_block = b_.contents().get_string(offset).unwrap().clone();
                }
                return get_from_block;
            }
            None => panic!("Transaction::get_string: failed to get buffer"),
        }
    }

    pub fn set_int(&mut self, blk: BlockId, offset: i32, val: i32, ok_to_log: bool) {
        self.concur_mgr.xlock(blk.clone());
        let buff = self.my_buffers.get_buffer(blk.clone());
        let mut lsn = -1;
        match buff {
            Some(b) => {
                {
                    // lock the buffer
                    let mut b_ = b.lock().unwrap();
                    if ok_to_log {
                        lsn = self.recovery_mgr.set_int(&mut b_, offset, val);
                    }
                    let p = b_.contents();
                    p.set_int(offset, val);
                    b_.set_modified(self.txnum, lsn);
                }
            }
            None => panic!("Transaction::set_int: failed to get buffer"),
        }
    }

    pub fn set_string(&mut self, blk: BlockId, offset: i32, val: String, ok_to_log: bool) {
        self.concur_mgr.xlock(blk.clone());
        let buff = self.my_buffers.get_buffer(blk.clone());
        let mut lsn = -1;
        match buff {
            Some(b) => {
                {
                    // lock the buffer
                    let mut b_ = b.lock().unwrap();
                    if ok_to_log {
                        lsn = self.recovery_mgr.set_string(&mut b_, offset, val.clone());
                    }
                    let p = b_.contents();
                    p.set_string(offset, val);
                    b_.set_modified(self.txnum, lsn);
                }
            }
            None => panic!("Transaction::set_string: failed to get buffer"),
        }
    }

    pub fn size(&mut self, filename: String) -> i32 {
        let dummyblk = BlockId::new(filename.clone(), END_OF_FILE);
        self.concur_mgr.slock(dummyblk.clone());
        return self.fm.length(filename.clone()).unwrap();
    }

    pub fn append(&mut self, filename: String) -> BlockId {
        let dummyblk = BlockId::new(filename.clone(), END_OF_FILE);
        self.concur_mgr.xlock(dummyblk.clone());
        return self.fm.append(filename.clone()).unwrap();
    }

    pub fn block_size(&self) -> i32 {
        self.fm.block_size()
    }

    pub fn available_buffs(&self) -> i32 {
        let num: i32;
        {
            // lock the buffer manager
            let bm_ = self.bm.lock().unwrap();
            num = bm_.available().clone();
        }
        num
    }

    fn next_tx_number() -> i32 {
        NEXT_TX_NUM.fetch_add(1, Ordering::SeqCst);
        let tx_num: i32 = NEXT_TX_NUM.load(Ordering::SeqCst).clone();
        println!("nwxt transaction: {}", tx_num);
        tx_num
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use std::fs;
    use std::sync::{Arc, Mutex};

    use crate::common::integer;
    use crate::{
        buffer_manager::buffer_mgr::BufferMgr,
        file_manager::{block_id::BlockId, file_mgr::FileMgr, page::Page},
        log_manager::log_mgr::LogMgr,
    };

    fn setup(db_directory: String) {
        // delete db_directory if exists
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    fn teardown(db_directory: String) {
        // delete db_directory if exists
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    #[test]
    fn test_transaction() -> Result<()> {
        let db_directory = "./db/transactiontest".to_string();
        let log_file = "testfile".to_string();
        setup((&db_directory).to_string());

        let mut fm = FileMgr::new(db_directory.clone(), 400);
        let log_mgr = Arc::new(Mutex::new(
            LogMgr::new(fm.clone(), log_file.clone()).unwrap(),
        ));
        let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), log_mgr.clone(), 10)));

        // Test transaction on ok_to_log = false
        // when ok_to_log is false, the transaction does not write to log, only write value to disk
        {
            // create Transaction
            let mut tx1 = Transaction::new(fm.clone(), log_mgr.clone(), bm.clone());
            // check transaction number
            assert_eq!(tx1.txnum, 1);
            let blk = BlockId::new("testfile".to_string(), 1);
            tx1.pin(blk.clone());
            tx1.set_int(blk.clone(), 80, 123, false);
            tx1.set_string(blk, 40, "one".to_string(), false);
            tx1.commit();

            // check disk content
            let mut p = Page::new(fm.block_size());
            let blk = BlockId::new("testfile".to_string(), 1);
            fm.read(&blk, &mut p).unwrap();
            let n = p.get_int(80).unwrap();
            assert_eq!(n, 123);
            let s = p.get_string(40).unwrap();
            assert_eq!(s, "one".to_string());

            // check log content
            let mut p = Page::new(fm.block_size());
            let blk = BlockId::new("testfile".to_string(), 0);
            fm.read(&blk, &mut p).unwrap();
            let commit_offset = p.get_int(0).unwrap();
            let commit_log = p.get_bytes(commit_offset).unwrap();
            // commit log record | COMMIT (= 2) | txnum (= 1) |
            assert_eq!(commit_log, vec![0, 0, 0, 2, 0, 0, 0, 1]);
            let start_offset = commit_offset + commit_log.len() as i32 + integer::BYTES;
            let start_log = p.get_bytes(start_offset).unwrap();
            // start log record | START (= 1) | txnum (= 1) |
            assert_eq!(start_log, vec![0, 0, 0, 1, 0, 0, 0, 1]);
        }

        // Test transaction on ok_to_log = true
        // when ok_to_log is true, the transaction writes to log and disk
        {
            let mut tx2 = Transaction::new(fm.clone(), log_mgr.clone(), bm.clone());
            // check transaction number
            assert_eq!(tx2.txnum, 2);
            let blk = BlockId::new("testfile".to_string(), 1);
            tx2.pin(blk.clone());
            let ival = tx2.get_int(blk.clone(), 80);
            let sval = tx2.get_string(blk.clone(), 40);
            assert_eq!(ival, 123);
            assert_eq!(sval, "one".to_string());
            let new_ival = ival + 1;
            let new_sval = sval + "!";
            tx2.set_int(blk.clone(), 80, new_ival, true);
            tx2.set_string(blk.clone(), 40, new_sval.clone(), true);
            tx2.commit();

            // check disk content
            let mut p = Page::new(fm.block_size());
            let blk = BlockId::new("testfile".to_string(), 1);
            fm.read(&blk, &mut p).unwrap();
            let n = p.get_int(80).unwrap();
            assert_eq!(n, 124); // new value

            // Test log content
            let mut p = Page::new(fm.block_size());
            let blk = BlockId::new("testfile".to_string(), 0);
            fm.read(&blk, &mut p).unwrap();

            // check commit log
            let commit_offset = p.get_int(0).unwrap();
            let commit_log = p.get_bytes(commit_offset).unwrap();
            // commit log record | COMMIT (= 2) | txnum (= 2) |
            assert_eq!(commit_log, vec![0, 0, 0, 2, 0, 0, 0, 2]);

            // check set_string log
            let set_string_offset = commit_offset + commit_log.len() as i32 + integer::BYTES;
            let set_string_log = p.get_bytes(set_string_offset).unwrap();
            // set_string log record | SETSTRING (= 5) | txnum (= 2) | blk filename (= 8) | blk number (= 1) | offset (= 40) | val.length (= 3) | val (= "one") |
            // value is old value, not new value
            assert_eq!(
                set_string_log,
                vec![
                    0, 0, 0, 5, 0, 0, 0, 2, 0, 0, 0, 8, 116, 101, 115, 116, 102, 105, 108, 101, 0,
                    0, 0, 1, 0, 0, 0, 40, 0, 0, 0, 3, 111, 110, 101
                ]
            );

            // check set_int log
            let set_int_offset = set_string_offset + set_string_log.len() as i32 + integer::BYTES;
            let set_int_log = p.get_bytes(set_int_offset).unwrap();
            // set_int log record | SETINT (= 4) | txnum (= 2) | blk filename (= 8) | blk number (= 1) | offset (= 80) | val (= 123) |
            // value is old value, not new value
            assert_eq!(
                set_int_log,
                vec![
                    0, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0, 8, 116, 101, 115, 116, 102, 105, 108, 101, 0,
                    0, 0, 1, 0, 0, 0, 80, 0, 0, 0, 123
                ]
            );

            // check start log
            let start_offset = set_int_offset + set_int_log.len() as i32 + integer::BYTES;
            let start_log = p.get_bytes(start_offset).unwrap();
            // start log record | START (= 1) | txnum (= 2) |
            assert_eq!(start_log, vec![0, 0, 0, 1, 0, 0, 0, 2]);
        }

        // Test rollback
        {
            let mut tx3 = Transaction::new(fm.clone(), log_mgr.clone(), bm.clone());
            // check transaction number
            assert_eq!(tx3.txnum, 3);
            let blk = BlockId::new("testfile".to_string(), 1);
            tx3.pin(blk.clone());

            tx3.set_int(blk, 80, 160, true);
            tx3.roll_back();

            // check disk content
            let mut p = Page::new(fm.block_size());
            let blk = BlockId::new("testfile".to_string(), 1);
            fm.read(&blk, &mut p).unwrap();
            let n = p.get_int(80).unwrap();
            assert_eq!(n, 124); // old value

            // check log content
            let mut p = Page::new(fm.block_size());
            let blk = BlockId::new("testfile".to_string(), 0);
            fm.read(&blk, &mut p).unwrap();

            // check rollback log
            let rollback_offset = p.get_int(0).unwrap();
            let rollback_log = p.get_bytes(rollback_offset).unwrap();
            // rollback log record | ROLLBACK (= 3) | txnum (= 3) |
            assert_eq!(rollback_log, vec![0, 0, 0, 3, 0, 0, 0, 3]);

            // check set_int log
            let set_int_offset = rollback_offset + rollback_log.len() as i32 + integer::BYTES;
            let set_int_log = p.get_bytes(set_int_offset).unwrap();
            // set_int log record | SETINT (= 4) | txnum (= 3) | blk filename (= 8) | blk number (= 1) | offset (= 80) | val (= 124) |
            // value is new value, not old value
            assert_eq!(
                set_int_log,
                vec![
                    0, 0, 0, 4, 0, 0, 0, 3, 0, 0, 0, 8, 116, 101, 115, 116, 102, 105, 108, 101, 0,
                    0, 0, 1, 0, 0, 0, 80, 0, 0, 0, 124
                ]
            );

            // check start log
            let start_offset = set_int_offset + set_int_log.len() as i32 + integer::BYTES;
            let start_log = p.get_bytes(start_offset).unwrap();
            // start log record | START (= 1) | txnum (= 3) |
            assert_eq!(start_log, vec![0, 0, 0, 1, 0, 0, 0, 3]);
        }

        // Test get_int
        {
            let mut tx4 = Transaction::new(fm.clone(), log_mgr.clone(), bm.clone());
            // check transaction number
            assert_eq!(tx4.txnum, 4);
            let blk = BlockId::new("testfile".to_string(), 1);
            tx4.pin(blk.clone());
            let ival = tx4.get_int(blk.clone(), 80);
            assert_eq!(ival, 124);
        }

        teardown(db_directory.clone());
        Ok(())
    }
}
