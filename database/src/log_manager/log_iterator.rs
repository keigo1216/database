use crate::file_manager::block_id::BlockId;
use crate::file_manager::file_mgr::FileMgr;
use crate::file_manager::page::Page;
use std::iter::Iterator;

pub struct LogIterator {
    fm: FileMgr,
    blk: BlockId,
    p: Page,
    current_pos: i32,
    boundary: i32,
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let has_next = self.current_pos < self.fm.block_size() || self.blk.number() > 0;
        if !has_next {
            return None;
        }
        if self.current_pos == self.fm.block_size() {
            if self.blk.number() == 0 {
                panic!("the end of the log has been reached");
            }
            self.blk = BlockId::new(self.blk.filename(), self.blk.number() - 1);
            self.move_to_block();
        }
        let rec = self
            .p
            .get_bytes(self.current_pos)
            .expect("Error reading bytes");
        self.current_pos += rec.len() as i32 + std::mem::size_of::<i32>() as i32;
        Some(rec)
    }
}

impl LogIterator {
    pub fn new(mut fm: FileMgr, blk: BlockId) -> Self {
        let mut p = Page::new_log(vec![0; fm.block_size() as usize]);
        fm.read(&blk, &mut p).expect("Error reading block");
        let boundary = p.get_int(0).expect("Error reading boundary");
        let current_pos = boundary;
        Self {
            fm,
            blk,
            p,
            current_pos,
            boundary,
        }
    }

    #[allow(dead_code)]
    pub fn has_next(&mut self) -> bool {
        self.current_pos < self.fm.block_size() || self.blk.number() > 0
    }

    fn move_to_block(&mut self) {
        self.fm
            .read(&self.blk, &mut self.p)
            .expect("Error reading block");
        self.boundary = self.p.get_int(0).expect("Error reading boundary");
        self.current_pos = self.boundary;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    use crate::file_manager::block_id::BlockId;
    use crate::file_manager::file_mgr::FileMgr;
    use crate::file_manager::page::Page;
    use std::fs;

    #[test]
    fn test_log_iterator() -> Result<()> {
        // setup file manager
        let db_directory = "./db/logtest";

        // delete ./db/logtest
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }

        let mut fm = FileMgr::new(db_directory.to_string(), 20);
        let blk = BlockId::new("testfile".to_string(), 0);
        let mut p = Page::new(fm.block_size());
        p.set_int(0, 16);
        fm.write(&blk, &mut p).expect("Error writing block on Test");

        // test new
        {
            let mut log_iter = LogIterator::new(fm.clone(), blk.clone());
            assert_eq!(log_iter.current_pos, 16);
            assert_eq!(log_iter.boundary, 16);
            assert_eq!(log_iter.p.get_bytes(0).unwrap(), vec![0; 16]);
        }

        // delete ./db/logtest
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }

        Ok(())
    }
}
