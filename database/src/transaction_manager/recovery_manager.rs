pub mod check_point_record;
pub mod commit_record;
pub mod log_record;
pub mod log_record_item;
pub mod roll_back_record;
pub mod set_int_record;
pub mod set_string_record;
pub mod start_record;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use crate::buffer_manager::buffer::Buffer;
use crate::buffer_manager::buffer_mgr::BufferMgr;
use crate::log_manager::log_mgr::LogMgr;

use commit_record::CommitRecord;
use roll_back_record::RollBackRecord;
use set_int_record::SetIntRecord;
use set_string_record::SetStringRecord;
use start_record::StartRecord;

use log_record::{LogRecord, LogRecordType};

use super::transaction::TransactionForUndo;

// each transaction has its own recovery manager
pub struct RecoveryMgr {
    lm: Arc<Mutex<LogMgr>>,
    bm: Arc<Mutex<BufferMgr>>,
    txnum: i32,
}

impl RecoveryMgr {
    pub fn new(txnum: i32, lm: Arc<Mutex<LogMgr>>, bm: Arc<Mutex<BufferMgr>>) -> Self {
        // write START record to log
        {
            // lock the log manager
            let mut lm_ = lm.lock().unwrap();
            StartRecord::write_to_log(&mut lm_, txnum);
        }
        Self { lm, bm, txnum }
    }

    pub fn commit(&self) {
        // flush all buffer to disk (write-Ahead logging)
        {
            // lock the buffer manager
            let bm_ = self.bm.lock().unwrap();
            bm_.flush_all(self.txnum); // ここでもlogはflushされる？
        }
        // write COMMIT record to log and flush it to disk
        {
            // lock the log manager
            let mut lm_ = self.lm.lock().unwrap();
            let lsn = CommitRecord::write_to_log(&mut lm_, self.txnum);
            lm_.flush(lsn);
        }
    }

    pub fn rollback(&mut self, tx_for_undo: &mut TransactionForUndo) {
        self.do_roll_back(tx_for_undo);
        // flush all buffer
        {
            // lock the buffer manager
            let bm_ = self.bm.lock().unwrap();
            bm_.flush_all(self.txnum);
        }
        // write Rollback record to log and flush it to disk
        {
            // lock the log manager
            let mut lm_ = self.lm.lock().unwrap();
            let lsn = RollBackRecord::write_to_log(&mut lm_, self.txnum);
            lm_.flush(lsn);
        }
    }

    pub fn recover(&mut self, tx_for_undo: &mut TransactionForUndo) {
        self.do_recover(tx_for_undo);
        // flush all buffer
        {
            // lock the buffer manager
            let bm_ = self.bm.lock().unwrap();
            bm_.flush_all(self.txnum);
        }
    }

    pub fn set_int(&self, buff: &mut Buffer, offset: i32, _new_val: i32) -> i32 {
        let old_val = buff.contents().get_int(offset).unwrap();
        let blk = buff.block().unwrap();
        {
            // lock the log manager
            let mut lm_: std::sync::MutexGuard<'_, LogMgr> = self.lm.lock().unwrap();
            return SetIntRecord::write_to_log(&mut lm_, self.txnum, blk, offset, old_val);
        }
    }

    pub fn set_string(&self, buff: &mut Buffer, offset: i32, _new_val: String) -> i32 {
        let old_val = buff.contents().get_string(offset).unwrap();
        let blk = buff.block().unwrap();
        {
            // lock the log manager
            let mut lm_ = self.lm.lock().unwrap();
            return SetStringRecord::write_to_log(&mut lm_, self.txnum, blk, offset, old_val);
        }
    }

    pub fn do_roll_back(&mut self, tx_for_undo: &mut TransactionForUndo) {
        {
            // lock the log maanger
            let mut lm_ = self.lm.lock().unwrap();
            let mut iter = lm_.iterator();
            while let Some(bytes) = iter.next() {
                let rec = LogRecord::create_log_record(bytes);
                if let Some(txnum) = rec.tx_number() {
                    println!("rec: {:?}", rec);
                    if txnum == self.txnum {
                        match rec {
                            LogRecordType::START(_) => break, // arrived at the start record of this transaction
                            _ => {
                                rec.undo(tx_for_undo);
                            } // undo the log record
                        }
                    }
                }
            }
        }
    }

    pub fn do_recover(&mut self, tx_for_undo: &mut TransactionForUndo) {
        let mut finished_txs: HashSet<i32> = HashSet::new();
        {
            // lock the log manager
            let mut lm_ = self.lm.lock().unwrap();
            let mut iter = lm_.iterator();
            while let Some(bytes) = iter.next() {
                let rec = LogRecord::create_log_record(bytes);
                if let LogRecordType::CHECKPOINT(_) = rec {
                    return;
                }
                if let LogRecordType::COMMIT(_) | LogRecordType::ROLLBACK(_) = rec {
                    finished_txs.insert(rec.tx_number().unwrap());
                } else if !finished_txs.contains(&rec.tx_number().unwrap()) {
                    rec.undo(tx_for_undo);
                }
            }
        }
    }
}
