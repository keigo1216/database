use crate::file_manager::block_id::BlockId;
use crate::file_manager::file_mgr::FileMgr;
use crate::file_manager::page::Page;
use crate::file_manager::FileManagerError;
use crate::log_manager::log_iterator::LogIterator;

pub struct LogMgr {
    fm: FileMgr,
    log_file: String,
    log_page: Page,
    current_blk: BlockId,
    latest_lsn: i32,
    last_saved_lsn: i32,
}

impl LogMgr {
    /// Creates a new log manager for the specified log file.
    /// If the log file does not yet exist, it is created with an empty first block.
    /// and set the block size to that of the specified file manager.
    pub fn new(mut fm: FileMgr, log_file: String) -> Result<Self, FileManagerError> {
        let mut log_page = Page::new_log(vec![0; fm.block_size() as usize]);
        let log_size = fm.length(log_file.clone())?;
        let current_blk;
        if log_size == 0 {
            // log file does not exist
            match Self::append_new_block(&mut fm, &log_file, &mut log_page) {
                // create new block (appended block size to top of block) and set as current block
                Ok(blk) => current_blk = blk,
                Err(e) => return Err(e),
            }
        } else {
            current_blk = BlockId::new(log_file.clone(), log_size - 1); // set current block to last block in log file
            match fm.read(&current_blk, &mut log_page) {
                Ok(_) => (),
                Err(e) => return Err(e),
            }
        }
        Ok(Self {
            fm,
            log_file,
            log_page,
            current_blk,
            latest_lsn: 0,
            last_saved_lsn: 0,
        })
    }

    /// add record to the log and return LSN (identifier of the log record)
    /// algorithm:
    /// 1. get remaining space in current block
    /// 2. get bytes needed to store logrec. add 4 bytes for size of logrec
    /// 3. if not enough space in current block
    ///    a. create new block (appended block size to top of block) and set as current block
    ///    b. get remaining space in current block
    /// 4. get position to write logrec, backlog from end of block
    /// 5. write logrec to log page
    /// 6. update remaining space in current block
    /// 7. update latest LSN
    /// @param logrec: log record
    pub fn append(&mut self, logrec: Vec<u8>) -> i32 {
        let mut boundary = self.log_page.get_int(0).expect("io Error"); // get remaining space in current block
        let recsize = logrec.len() as i32;
        let bytes_needed = recsize + std::mem::size_of::<i32>() as i32; // get bytes needed to store logrec. add 4 bytes for size of logrec

        if boundary - bytes_needed < std::mem::size_of::<i32>() as i32 {
            // if not enough space in current block
            self.flush_page();
            self.current_blk = Self::append_new_block(&mut self.fm, &self.log_file, &mut self.log_page)
                .expect("append new block error"); // create new block (appended block size to top of block) and set as current block
            boundary = self.log_page.get_int(0).expect("io Error"); // get remaining space in current block
        }

        let recpos = boundary - bytes_needed; // get position to write logrec, backlog from end of block
        self.log_page.set_bytes(recpos, logrec); // write logrec to log page (first 4 bytes is size of logrec)
        self.log_page.set_int(0, recpos); // update remaining space in current block
        self.latest_lsn += 1; // increment latest LSN
        self.latest_lsn
    }

    /// Ensures that the log record corresponding to the specified LSN has been written to disk.
    /// If the LSN corresponds to a log record that was never written to the log buffer, then the log record is written to disk.
    pub fn flush(&mut self, lsn: i32) {
        if lsn >= self.last_saved_lsn {
            self.flush_page();
        }
    }


    /// flush all log records to disk and return iterator to read log records
    pub fn iterator(&mut self) -> LogIterator {
        self.flush_page();
        LogIterator::new(self.fm.clone(), self.current_blk.clone())
    }

    fn append_new_block(
        fm: &mut FileMgr,
        log_file: &String,
        log_page: &mut Page,
    ) -> Result<BlockId, FileManagerError> {
        let blk = match fm.append(log_file.clone()) {
            Ok(blk) => blk,
            Err(e) => return Err(e),
        };
        // write block size to top of log page
        log_page.set_int(0, fm.block_size());
        match fm.write(&blk, log_page) {
            Ok(_) => Ok(blk),
            Err(e) => Err(e),
        }
    }

    /// Ensures that the log record corresponding to the specified LSN has been written to disk.
    /// All earlier log records will also be written to disk.
    fn flush_page(&mut self) {
        self.fm
            .write(&self.current_blk, &mut self.log_page)
            .unwrap();
        self.last_saved_lsn = self.latest_lsn;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    use crate::file_manager::file_mgr::FileMgr;
    use crate::log_manager::log_mgr::LogMgr;
    use std::fs::{self, File, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;

    #[test]
    fn test_log_mgr() -> Result<()> {
        let db_directory = "./db/logtest".to_string();
        let log_file = "testfile".to_string();
        // delete ./db/logtest
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }

        // test new 
        {
            let mut fm = FileMgr::new(db_directory.to_string(), 20);
            // log_size == 0
            let log_mgr = LogMgr::new(fm.clone(), log_file.clone()).unwrap();
            assert_eq!(log_mgr.current_blk, BlockId::new(log_file.clone(), 0));
            assert_eq!(log_mgr.latest_lsn, 0);
            assert_eq!(log_mgr.last_saved_lsn, 0);

            // check set block size
            let mut p = Page::new(fm.block_size());
            fm.read(&log_mgr.current_blk, &mut p).expect("test_log_mgr: read error");
            assert_eq!(p.get_int(0).unwrap(), fm.block_size());
        }

        // test new
        {
            let fm = FileMgr::new(db_directory.to_string(), 20);
            // log_size == 2
            let mut path = PathBuf::from(db_directory.clone());
            path.push(log_file.clone());
            File::create(path.clone()).unwrap(); // create file
            let mut file = OpenOptions::new().write(true).read(true).open(path.clone()).unwrap();
            file.write_all(&vec![0; 40]).unwrap();

            let log_mgr = LogMgr::new(fm.clone(), log_file.clone()).unwrap();
            assert_eq!(log_mgr.current_blk, BlockId::new(log_file.clone(), 1));

            // delete file
            fs::remove_file(path.clone()).unwrap();
        }

        // test append
        {
            let fm = FileMgr::new(db_directory.to_string(), 20);
            let mut log_mgr = LogMgr::new(fm.clone(), log_file.clone()).unwrap();
            let logrec = vec![1, 2, 3, 4];
            let lsn = log_mgr.append(logrec.clone());
            assert_eq!(lsn, 1);
            assert_eq!(log_mgr.latest_lsn, 1);
            assert_eq!(log_mgr.last_saved_lsn, 0);

            // check log page
            assert_eq!(log_mgr.log_page.get_int(0).unwrap(), fm.block_size() - 8);
            assert_eq!(log_mgr.log_page.get_bytes(fm.block_size() - 8).unwrap(), vec![1, 2, 3, 4]);
        }

        // test append (not enough space in current block)
        {
            let fm = FileMgr::new(db_directory.to_string(), 20);
            let mut log_mgr = LogMgr::new(fm.clone(), log_file.clone()).unwrap();
            let logrec = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
            log_mgr.append(logrec.clone());
            let lsn = log_mgr.append(logrec.clone());
            assert_eq!(lsn, 2);
            assert_eq!(log_mgr.latest_lsn, 2);
            assert_eq!(log_mgr.last_saved_lsn, 1);

            // check log page
            assert_eq!(log_mgr.log_page.get_int(0).unwrap(), fm.block_size() - 13);
            assert_eq!(log_mgr.log_page.get_bytes(fm.block_size() - 13).unwrap(), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        }

        Ok(())
    }
}