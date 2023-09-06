use crate::file_manager::block_id::BlockId;
use crate::record_management::layout::Layout;
use crate::record_management::schema::Type;
use crate::transaction_manager::transaction::Transaction;

const EMPTY: i32 = 0;
const USED: i32 = 0;

/// Homogeneous: all records in same table have same file
/// unspanned: no split record
/// fixed-length: all records have same length
/// | empty/inuse | record 1 | empty/inuse | record 2 | empty/inuse | record 3 | ...
pub struct RecordPage {
    tx: Transaction,
    blk: BlockId,
    layout: Layout,
}

impl RecordPage {
    pub fn new(mut tx: Transaction, blk: BlockId, layout: Layout) -> Self {
        tx.pin(blk.clone());
        Self { tx, blk, layout }
    }

    pub fn get_int(&mut self, slot: i32, field_name: &String) -> i32 {
        let fldpos = self.offset(slot) + self.layout.offset(field_name);
        return self.tx.get_int(self.blk.clone(), fldpos);
    }

    pub fn get_string(&mut self, slot: i32, field_name: &String) -> String {
        let fldpos = self.offset(slot) + self.layout.offset(field_name);
        return self.tx.get_string(self.blk.clone(), fldpos).clone();
    }

    pub fn set_int(&mut self, slot: i32, field_name: &String, val: i32) {
        let fldpos = self.offset(slot) + self.layout.offset(field_name);
        self.tx.set_int(self.blk.clone(), fldpos, val, true);
    }

    pub fn set_string(&mut self, slot: i32, field_name: &String, val: String) {
        let fldpos = self.offset(slot) + self.layout.offset(field_name);
        self.tx.set_string(self.blk.clone(), fldpos, val, true);
    }

    pub fn delete(&mut self, slot: i32) {
        self.set_flag(slot, EMPTY);
    }

    /// return the first record setting empty/inuse flag to USED
    /// @return: the slot number of the record
    /// if no record is found, return -1
    /// @return: the slot number of the record
    pub fn next_after(&mut self, slot: i32) -> i32 {
        self.search_after(slot, USED)
    }

    /// search for the first empty record and set empty/inuse flag to USED and return the slot number
    /// @param slot: the slot number of the record
    /// if no empty record is found, return -1
    /// @return: the slot number of the record
    pub fn insert_after(&mut self, slot: i32) -> i32 {
        let new_slot = self.search_after(slot, EMPTY);
        if new_slot >= 0 {
            self.set_flag(new_slot, USED);
        }
        return new_slot;
    }

    /// delte the all records at blk
    /// set empty/inuse flag to empty
    /// set INTEGER to 0
    /// set VARCHAR to ""
    pub fn format(&mut self) {
        let mut slot = 0;
        while self.is_valid_slot(slot) {
            self.tx
                .set_int(self.blk.clone(), Self::offset(&self, slot), EMPTY, false);
            let sch = self.layout.schema();
            for field_name in sch.get_fields().iter() {
                let fldpot = self.offset(slot) + self.layout.offset(field_name);
                println!("field_name: {}, offset: {}", field_name, fldpot);
                match sch.get_type_(field_name).into() {
                    Type::INTEGER => self.tx.set_int(self.blk.clone(), fldpot, 0, false),
                    Type::VARCHAR => {
                        self.tx
                            .set_string(self.blk.clone(), fldpot, "".to_string(), false)
                    }
                }
            }
            slot += 1;
        }
    }

    /// set empty/inuse flag to indicate if a record is empty or inuse
    /// @param slot: the slot number of the record
    /// @param flag: EMPTY or USED
    fn set_flag(&mut self, slot: i32, flag: i32) {
        println!("slot: {:?}", slot);
        self.tx
            .set_int(self.blk.clone(), Self::offset(self, slot), flag, true);
    }

    /// return true if the slot is valid
    /// example
    /// block size = 400, slot size = 101
    /// slot 0: offset = 0, size = 101
    /// slot 1: offset = 101, size = 101 (offset(slot + 1) = 202 > 400)
    /// slot 2: offset = 202, size = 101 (offset(slot + 1) = 303 > 400)
    /// slot 3: offset = 303, size = 101 (offset(slot + 1) = 404 > 400)
    /// slot 4: offset = 404, size = 101 (offset(slot + 1) = 505 > 400) -> invalid
    /// @param slot: the slot number of the record
    fn is_valid_slot(&self, slot: i32) -> bool {
        self.offset(slot + 1) <= self.tx.block_size()
    }

    /// search for the first record (start from slot + 1) with the given flag
    /// @param slot: the slot number of the record
    /// @param flag: EMPTY or USED
    fn search_after(&mut self, mut slot: i32, flag: i32) -> i32 {
        slot += 1;
        while self.is_valid_slot(slot) {
            println!("slot: {}", slot);
            if self.tx.get_int(self.blk.clone(), Self::offset(self, slot)) == flag {
                return slot;
            }
            slot += 1;
        }
        return -1;
    }

    /// return slot size to calculate the starting location of a record slot
    fn offset(&self, slot: i32) -> i32 {
        slot * self.layout.slot_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::fs;
    use std::sync::{Arc, Mutex};

    use crate::buffer_manager::buffer_mgr::BufferMgr;
    use crate::file_manager::file_mgr::FileMgr;
    use crate::log_manager::log_mgr::LogMgr;
    use crate::record_management::schema::Schema;

    fn setup(db_directory: String) {
        // delete db_deirectory if exists
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    fn teardown() {}

    #[test]
    fn test_record_page() -> Result<()> {
        let db_directory = "./db/recordpagetest".to_string();
        let log_file = "testfile".to_string();
        setup(db_directory.clone());
        let fm = FileMgr::new(db_directory.clone(), 400);
        let log_mgr = Arc::new(Mutex::new(
            LogMgr::new(fm.clone(), log_file.clone()).unwrap(),
        ));
        let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), log_mgr.clone(), 10)));
        let mut tx: Transaction = Transaction::new(fm.clone(), log_mgr.clone(), bm.clone());
        let mut sch = Schema::new();
        sch.add_int_field("A".to_string());
        sch.add_string_field("B".to_string(), 9);
        let layout = Layout::new_from_schema(sch.clone());
        let blk = tx.append(log_file.clone());
        tx.pin(blk.clone());
        let mut rp = RecordPage::new(tx, blk.clone(), layout);
        rp.format();

        let mut slot = rp.insert_after(-1);
        // check insert_after
        assert_eq!(slot, 0);

        while slot >= 0 {
            let n = 50 + slot;
            rp.set_int(slot, &"A".to_string(), n);
            rp.set_string(slot, &"B".to_string(), format!("rec{}", n));
            slot = rp.insert_after(slot);
        }

        // Get record set UESD
        slot = rp.next_after(-1);

        while slot >= 0 {
            let n = 50 + slot;

            // check set_int and get_int
            assert_eq!(rp.get_int(slot, &"A".to_string()), n);
            // check set_string and get_string
            assert_eq!(rp.get_string(slot, &"B".to_string()), format!("rec{}", n));

            // check delete
            rp.delete(slot);
            let next_empty_slot = rp.search_after(slot - 1, EMPTY);
            assert_eq!(next_empty_slot, slot);

            slot = rp.next_after(slot);
        }

        rp.tx.unpin(blk.clone());
        rp.tx.commit();

        teardown();
        Ok(())
    }
}
