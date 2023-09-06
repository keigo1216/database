use std::fmt::Display;
use std::vec;

use crate::common::integer;
use crate::file_manager::block_id::BlockId;
use crate::file_manager::page::Page;
use crate::log_manager::log_mgr::LogMgr;
use crate::transaction_manager::recovery_manager::log_record_item::{LogRecordItem, RecordType};
use crate::transaction_manager::transaction::TransactionForUndo;

/// data format:
/// |       4        |   4   |         4       |     n    |    4   |   4    |  4  |
/// | SETINT ( = 4 ) | txnum | filename.length | filename | blknum | offset | val |
#[derive(Debug)]
pub struct SetIntRecord {
    txnum: i32,
    offset: i32,
    val: i32,
    blk: BlockId,
}

impl Display for SetIntRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<SETINT {} {} {} {}>",
            self.txnum, self.blk, self.offset, self.val
        )
    }
}

impl SetIntRecord {
    pub fn new(mut p: Page) -> Self {
        let tpos = integer::BYTES;
        let txnum = p
            .get_int(tpos)
            .expect("SetIntRecord::new: failed to get txnum");
        let fpos = tpos + integer::BYTES;
        let filename = p
            .get_string(fpos)
            .expect("SetIntRecord::new: failed to get filename");
        let bpos = fpos + Page::max_length(filename.len() as i32);
        let blknum = p
            .get_int(bpos)
            .expect("SetIntRecord::new: failed to get blknum");
        let blk = BlockId::new(filename, blknum);
        let opos = bpos + integer::BYTES;
        let offset = p
            .get_int(opos)
            .expect("SetIntRecord::new: failed to get offset");
        let vpos = opos + integer::BYTES;
        let val = p
            .get_int(vpos)
            .expect("SetIntRecord::new: failed to get val");
        Self {
            txnum,
            offset,
            val,
            blk,
        }
    }

    /// write a setint record to the log and return its lsn
    pub fn write_to_log(lm: &mut LogMgr, txnum: i32, blk: BlockId, offset: i32, val: i32) -> i32 {
        let tpos = integer::BYTES;
        let fpos = tpos + integer::BYTES;
        let bpos = fpos + Page::max_length(blk.filename().len() as i32);
        let opos = bpos + integer::BYTES;
        let vpos = opos + integer::BYTES;
        let reclen = vpos + integer::BYTES;
        let rec = vec![0; reclen as usize];
        let mut p = Page::new_log(rec);
        p.set_int(0, RecordType::SETINT as i32);
        p.set_int(tpos, txnum);
        p.set_string(fpos, blk.filename());
        p.set_int(bpos, blk.number());
        p.set_int(opos, offset);
        p.set_int(vpos, val);
        return lm.append(p.contents().into_vec());
    }
}

impl LogRecordItem for SetIntRecord {
    fn op() -> RecordType {
        RecordType::SETINT
    }

    fn tx_number(&self) -> Option<i32> {
        Some(self.txnum)
    }

    fn undo(&self, tx: &mut TransactionForUndo) {
        // pin
        tx.my_buffers.pin(self.blk.clone());

        // set old value
        tx.concur_mgr.xlock(self.blk.clone());
        match tx.my_buffers.get_buffer(self.blk.clone()) {
            Some(buff) => {
                {
                    // get lock on buffer
                    let mut b = buff.lock().unwrap();
                    let p = b.contents();
                    p.set_int(self.offset, self.val);
                    b.set_modified(self.txnum, -1);
                }
            }
            None => {
                panic!("SetIntRecord::undo: failed to get buffer")
            }
        }

        // unpin
        tx.my_buffers.unpin(self.blk.clone());
    }
}
