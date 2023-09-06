use std::fmt::Display;
use std::vec;

use crate::common::integer;
use crate::file_manager::page::Page;
use crate::log_manager::log_mgr::LogMgr;
use crate::transaction_manager::recovery_manager::log_record_item::{LogRecordItem, RecordType};
use crate::transaction_manager::transaction::TransactionForUndo;

/// data format:
/// |     4      |
/// | CHECKPOINT |
#[derive(Debug)]
pub struct CheckPointRecord {}

impl Display for CheckPointRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<CHECKPOINT>")
    }
}

impl CheckPointRecord {
    pub fn new() -> Self {
        Self {}
    }

    /// write a checkpoint record to the log and return its LSN
    pub fn write_to_log(lm: &mut LogMgr) -> i32 {
        let tpos = integer::BYTES;
        let rec = vec![0; tpos as usize];
        let mut p = Page::new_log(rec);
        p.set_int(0, RecordType::CHECKPOINT as i32);
        return lm.append(p.contents().into_vec());
    }
}

impl LogRecordItem for CheckPointRecord {
    fn op() -> RecordType {
        RecordType::CHECKPOINT
    }

    fn tx_number(&self) -> Option<i32> {
        None
    }

    fn undo(&self, _tx: &mut TransactionForUndo) {
        // do nothing
    }
}
