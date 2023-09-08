use crate::file_manager::block_id::BlockId;
use crate::record_management::layout::Layout;
use crate::record_management::record_page::RecordPage;
use crate::record_management::rid::RID;
use crate::transaction_manager::transaction::Transaction;

pub struct TableScan {
    pub layout: Layout,
    rp: RecordPage,
    filename: String,
    current_slot: i32,
}

impl TableScan {
    pub fn new(tx: &mut Transaction, tblname: String, layout: Layout) -> Self {
        let filename = tblname.clone() + ".tbl";
        let blk = match tx.size(filename.clone()) {
            0 => tx.append(filename.clone()),
            _ => BlockId::new(filename.clone(), 0),
        };
        Self {
            layout: layout.clone(),
            rp: RecordPage::new(tx, blk, layout.clone()),
            filename,
            current_slot: -1,
        }
    }

    /// close the scan
    pub fn close(&self, tx: &mut Transaction) {
        tx.unpin(self.rp.block().clone());
    }

    /// move to the first record
    pub fn before_first(&mut self, tx: &mut Transaction) {
        self.move_to_block(tx, 0);
    }

    /// move to the next record
    /// current_slot will be -1 if there is no next record
    /// @return: false if there is no next record
    /// @return: true if there is next record
    pub fn next(&mut self, tx: &mut Transaction) -> bool {
        self.current_slot = self.rp.next_after(tx, self.current_slot);
        while self.current_slot < 0 {
            if self.at_last_block(tx) {
                return false;
            };
            self.move_to_block(tx, self.rp.block().number() + 1);
            self.current_slot = self.rp.next_after(tx, self.current_slot);
        }
        return true;
    }

    /// get int from current slot and field name
    /// @return: the int value
    pub fn get_int(&mut self, tx: &mut Transaction, field_name: &String) -> i32 {
        self.rp.get_int(tx, self.current_slot, field_name)
    }

    /// get string from current slot and field name
    /// @return: the string value
    pub fn get_string(&mut self, tx: &mut Transaction, field_name: &String) -> String {
        self.rp.get_string(tx, self.current_slot, field_name)
    }

    /// whether the field exists in the schema
    /// @return: true if the field exists
    /// @return: false if the field does not exist
    pub fn has_field(&self, field_name: &String) -> bool {
        self.layout.schema().has_field(field_name)
    }

    /// set int to current slot and field name
    /// @param val: the int value
    /// @return: the int value
    pub fn set_int(&mut self, tx: &mut Transaction, field_name: &String, val: i32) {
        self.rp.set_int(tx, self.current_slot, field_name, val);
    }

    /// set string to current slot and field name
    /// @param val: the string value
    /// @return: the string value
    pub fn set_string(&mut self, tx: &mut Transaction, field_name: &String, val: String) {
        self.rp.set_string(tx, self.current_slot, field_name, val);
    }

    /// allocate new record space
    pub fn insert(&mut self, tx: &mut Transaction) {
        self.current_slot = self.rp.insert_after(tx, self.current_slot);
        while self.current_slot < 0 {
            if self.at_last_block(tx) {
                self.move_to_new_block(tx);
            } else {
                self.move_to_block(tx, self.rp.block().number() + 1);
            }
            self.current_slot = self.rp.insert_after(tx, self.current_slot);
        }
    }

    /// delete current record
    pub fn delete(&mut self, tx: &mut Transaction) {
        self.rp.delete(tx, self.current_slot);
    }

    /// move to the RID recoerd
    pub fn move_to_rid(&mut self, tx: &mut Transaction, rid: RID) {
        self.close(tx);
        let blk = BlockId::new(self.filename.clone(), rid.block_number());
        self.rp = RecordPage::new(tx, blk, self.layout.clone());
        self.current_slot = rid.slot_number();
    }

    /// get the RID of current record
    pub fn get_rid(&self) -> RID {
        RID::new(self.rp.block().number(), self.current_slot)
    }

    /// move to the block at blknum and update current_slot to -1
    /// @param blknum: the block number
    fn move_to_block(&mut self, tx: &mut Transaction, blknum: i32) {
        self.close(tx);
        let blk = BlockId::new(self.filename.clone(), blknum);
        self.rp = RecordPage::new(tx, blk, self.layout.clone());
        self.current_slot = -1;
    }

    /// move to the new block (append new block) and update current_slot to -1
    fn move_to_new_block(&mut self, tx: &mut Transaction) {
        self.close(tx);
        let blk = tx.append(self.filename.clone());
        self.rp = RecordPage::new(tx, blk, self.layout.clone());
        self.current_slot = -1;
    }

    /// whether the current block is the last block
    fn at_last_block(&self, tx: &mut Transaction) -> bool {
        // tx.size means the number of blocks
        // rp.block().number() means the current block number
        // so if rp.block().number() = 1 then tx.size = 2
        return self.rp.block().number() == tx.size(self.filename.clone()) - 1;
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer_manager::buffer_mgr::BufferMgr;
    use crate::file_manager::file_mgr::FileMgr;
    use crate::log_manager::log_mgr::LogMgr;
    use crate::record_management::schema::Schema;
    use crate::transaction_manager::transaction::Transaction;

    use super::*;
    use anyhow::Result;
    use std::fs;
    use std::sync::{Arc, Mutex};

    fn setup() {
        // delete db_deirectory if exists
        if fs::metadata("./db/tablescantest".to_string()).is_ok() {
            fs::remove_dir_all("./db/tablescantest".to_string()).unwrap();
        }
    }

    #[test]
    fn test_table_scan() -> Result<()> {
        let db_directory = "./db/tablescantest".to_string();
        let log_file = "testfile".to_string();
        setup();

        let fm = FileMgr::new(db_directory.clone(), 400);
        let log_mgr = Arc::new(Mutex::new(
            LogMgr::new(fm.clone(), log_file.clone()).unwrap(),
        ));
        let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), log_mgr.clone(), 10)));
        let mut tx = Transaction::new(fm.clone(), log_mgr.clone(), bm.clone());
        let mut sch = Schema::new();
        sch.add_int_field("A".to_string());
        sch.add_string_field("B".to_string(), 9);
        let layout = Layout::new_from_schema(sch.clone());
        let mut ts = TableScan::new(&mut tx, "T".to_string(), layout.clone());

        ts.before_first(&mut tx);
        for i in 0..50 {
            // record must be following
            // if i = 0
            // | 00 00 00 01 | 00 00 00 00 |  00 00 00 04  | 73 65 63 30 00 00 00 00 00 00 |
            // | USED flag   |   "A" field | "B" field len |          "B" field            |
            // Be careful that the "B" field is 9 bytes
            ts.insert(&mut tx); // allocate new record space
            ts.set_int(&mut tx, &"A".to_string(), i);
            ts.set_string(
                &mut tx,
                &"B".to_string(),
                "rec".to_string() + &i.to_string(),
            );
        }
        // Check insert
        ts.before_first(&mut tx);
        let mut n = 0;
        while ts.next(&mut tx) {
            assert_eq!(ts.get_int(&mut tx, &"A".to_string()), n);
            assert_eq!(
                ts.get_string(&mut tx, &"B".to_string()),
                "rec".to_string() + &n.to_string()
            );
            n += 1;
        }

        // if ts.next return false, the ts.current_slot will be -1
        // so recall ts.next will return true because ts.current_slot is -1 means the first(?) slot
        // this assert! will be failed
        // assert!(!ts.next(&mut tx));

        // delete
        ts.before_first(&mut tx);
        while ts.next(&mut tx) {
            // record must be following
            // if i = 0
            // | 00 00 00 00 | 00 00 00 00 |  00 00 00 04  | 73 65 63 30 00 00 00 00 00 00 |
            // | EMPTY flag  |   "A" field | "B" field len |          "B" field            |
            ts.delete(&mut tx);
        }
        assert_eq!(ts.current_slot, -1);

        ts.close(&mut tx);
        tx.commit();
        Ok(())
    }
}
