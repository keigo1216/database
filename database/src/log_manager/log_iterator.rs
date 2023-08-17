use crate::file_manager::block_id::BlockId;
use crate::file_manager::file_mgr::FileMgr;
use crate::file_manager::page::Page;
use std::iter::Iterator;

struct LogIterator {
    fm: FileMgr,
    blk: BlockId,
    p: Page,
    current_pos: i32,
    boundary: i32,
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos == self.fm.block_size() {
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