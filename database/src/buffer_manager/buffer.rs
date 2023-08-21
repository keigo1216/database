use crate::file_manager::block_id::BlockId;
use crate::file_manager::file_mgr::FileMgr;
use crate::file_manager::page::Page;
use crate::log_manager::log_mgr::LogMgr;

#[derive(Clone)]
pub struct Buffer {
    fm: FileMgr,
    lm: LogMgr,
    contents: Page,
    blk: Option<BlockId>,
    pub pins: i32,
    txnum: i32,
    lsn: i32,
}

impl Buffer {
    pub fn new(fm: FileMgr, lm: LogMgr) -> Self {
        let contents = Page::new(fm.block_size());
        Self {
            fm,
            lm,
            contents,
            blk: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        }
    }

    /// return the associated page
    pub fn contents(&mut self) -> &mut Page {
        &mut self.contents
    }

    pub fn block(&self) -> Option<BlockId> {
        self.blk.clone()
    }

    /// calling this method if the page associated with the buffer has been modified
    pub fn set_modified(&mut self, txnum: i32, lsn: i32) {
        self.txnum = txnum;
        if lsn >= 0 {
            self.lsn = lsn
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> i32 {
        self.txnum
    }

    pub fn assign_to_block(&mut self, b: BlockId) {
        self.flush();
        self.fm
            .read(&b, &mut self.contents)
            .expect("assign_to_block: read error");
        self.blk = Some(b);
        self.pins = 0;
    }

    pub fn flush(&mut self) {
        if self.txnum >= 0 {
            self.lm.flush(self.lsn);
            match self.blk.clone() {
                Some(blk) => self.fm.write(&blk, &mut self.contents).unwrap(),
                None => panic!("Buffer is not assigned to block"),
            }
            self.txnum = -1;
        }
    }

    pub fn pin(&mut self) {
        self.pins += 1;
    }

    pub fn unpin(&mut self) {
        self.pins -= 1;
    }
}
