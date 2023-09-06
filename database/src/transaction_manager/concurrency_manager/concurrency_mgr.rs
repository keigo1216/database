use std::collections::HashMap;

use crate::file_manager::block_id::BlockId;
use crate::transaction_manager::concurrency_manager::lock_table::LockTable;

pub struct ConcurrencyMgr {
    locktbl: LockTable,
    locks: HashMap<BlockId, String>,
}

impl ConcurrencyMgr {
    pub fn new() -> Self {
        Self {
            locktbl: LockTable::new(),
            locks: HashMap::new(),
        }
    }

    pub fn slock(&mut self, blk: BlockId) {
        if let None = self.locks.get(&blk) {
            self.locktbl.slock(blk.clone());
            self.locks.insert(blk.clone(), "S".to_string());
        }
    }

    pub fn xlock(&mut self, blk: BlockId) {
        if !self.has_xlock(&blk) {
            self.slock(blk.clone());
            self.locktbl.xlock(blk.clone());
            self.locks.insert(blk.clone(), "X".to_string());
        }
    }

    pub fn release(&mut self) {
        for (blk, _) in self.locks.iter() {
            self.locktbl.unlock(blk.clone());
        }

        self.locks.clear();
    }

    fn has_xlock(&self, blk: &BlockId) -> bool {
        let locktype = self.locks.get(blk);
        match locktype {
            Some(l) => l == "X",
            None => false,
        }
    }
}
