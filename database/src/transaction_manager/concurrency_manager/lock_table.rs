use std::collections::HashMap;
use std::thread;
use std::time;

use crate::file_manager::block_id::BlockId;

use self::constants::MAX_TIME;

mod constants {
    pub const MAX_TIME: i64 = 10000;
}

pub struct LockTable {
    locks: HashMap<BlockId, i32>,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
    }

    // get slock
    pub fn slock(&mut self, blk: BlockId) {
        let timestamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        while self.has_xlock(&blk) && !Self::wait_too_long(timestamp) {
            thread::sleep(time::Duration::from_millis(10));
        }
        if self.has_xlock(&blk) {
            panic!("block {} is locked", blk);
        }
        let val = self.get_lock_val(&blk);
        self.locks.insert(blk, val + 1);
    }

    /// get xlock
    pub fn xlock(&mut self, blk: BlockId) {
        let timestamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        while self.has_other_slocks(&blk) && !Self::wait_too_long(timestamp) {
            thread::sleep(time::Duration::from_millis(10));
        }
        if self.has_other_slocks(&blk) {
            panic!("block {} is locked", blk);
        }
        self.locks.insert(blk, -1);
    }

    pub fn unlock(&mut self, blk: BlockId) {
        let val = self.get_lock_val(&blk);
        if val > 1 {
            self.locks.insert(blk, val - 1);
        } else {
            self.locks.remove(&blk);
        }
    }

    /// if the block is slocked by other transactions, return true
    fn has_other_slocks(&self, blk: &BlockId) -> bool {
        self.get_lock_val(blk) > 1
    }

    pub(crate) fn has_xlock(&self, blk: &BlockId) -> bool {
        return self.get_lock_val(blk) < 0;
    }

    fn wait_too_long(starttime: i64) -> bool {
        let now = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        now - starttime > MAX_TIME
    }

    fn get_lock_val(&self, blk: &BlockId) -> i32 {
        if let Some(i_val) = self.locks.get(&blk) {
            *i_val
        } else {
            0
        }
    }
}
