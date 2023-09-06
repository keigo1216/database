use std::fmt::Display;

use crate::common::integer;
use crate::file_manager::page::Page;
use crate::log_manager::log_mgr::LogMgr;
use crate::transaction_manager::recovery_manager::log_record_item::{LogRecordItem, RecordType};
use crate::transaction_manager::transaction::TransactionForUndo;

/// data format:
/// |      4       |   4   |
/// | START ( = 1) | txnum |
#[derive(Debug)]
pub struct StartRecord {
    txnum: i32,
}

impl Display for StartRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<START {}>", self.txnum)
    }
}

impl StartRecord {
    pub fn new(mut p: Page) -> Self {
        let tpos = integer::BYTES;
        let txnum = p
            .get_int(tpos)
            .expect("StartRecord::new: failed to get txnum");
        Self { txnum }
    }

    /// write a start record to the log and return its lsn
    pub fn write_to_log(lm: &mut LogMgr, txnum: i32) -> i32 {
        let tpos = integer::BYTES;
        let fpos = tpos + integer::BYTES;
        let rec = vec![0; fpos as usize];
        let mut p = Page::new_log(rec);
        p.set_int(0, RecordType::START as i32);
        p.set_int(tpos, txnum);
        return lm.append(p.contents().into_vec());
    }
}

impl LogRecordItem for StartRecord {
    fn op() -> RecordType {
        RecordType::START
    }

    fn tx_number(&self) -> Option<i32> {
        Some(self.txnum)
    }

    fn undo(&self, _tx: &mut TransactionForUndo) {
        // do nothing
    }
}
