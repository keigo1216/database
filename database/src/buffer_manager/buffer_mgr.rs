use crate::buffer_manager::buffer::Buffer;
use crate::file_manager::block_id::BlockId;
use crate::file_manager::file_mgr::FileMgr;
use crate::log_manager::log_mgr::LogMgr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;

mod constants {
    pub const MAX_TIME: i64 = 10000;
}

/// this class manages the pinning and unpinning of buffers to blocks
/// this instance is created during system startup and each database has one instance
#[derive(Debug)]
pub struct BufferMgr {
    buffer_pool: Vec<Arc<Mutex<Buffer>>>,
    num_available: i32,
}

impl BufferMgr {
    pub fn new(fm: FileMgr, lm: Arc<Mutex<LogMgr>>, numbuffs: i32) -> Self {
        let mut buffer_pool = Vec::new();
        for _ in 0..numbuffs {
            buffer_pool.push(Arc::new(Mutex::new(Buffer::new(fm.clone(), lm.clone()))));
        }
        let num_available = numbuffs;
        Self {
            buffer_pool,
            num_available,
        }
    }

    /// returns the number of available buffers
    pub fn available(&self) -> i32 {
        return self.num_available;
    }

    /// flush disk contents corresponding to transaction number
    /// @param txnum transaction number
    pub fn flush_all(&self, txnum: i32) {
        for buffer in self.buffer_pool.iter() {
            let mut b_ = buffer.lock().unwrap();
            if b_.modifying_tx() == txnum {
                b_.flush()
            }
        }
    }

    /// unpin the page
    // to do: convert synchronize method
    pub fn unpin(&mut self, buff: Arc<Mutex<Buffer>>) {
        let mut buff_ = buff.lock().unwrap();
        buff_.unpin();
        if !buff_.is_pinned() {
            self.num_available += 1;
            // to do: notify threads waiting on buffer
        }
    }

    /// returns a buffer object pinned to a page containing the blk
    /// @param blk block id
    /// @return buffer object
    // to do: convert synchronize method
    pub fn pin(&mut self, blk: BlockId) -> Arc<Mutex<Buffer>> {
        let timestamp = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let mut buff = self.try_to_pin(blk.clone());
        while buff.is_none() && !self.wait_too_long(timestamp) {
            thread::sleep(time::Duration::from_millis(10));
            buff = self.try_to_pin(blk.clone());
        }
        match buff {
            Some(b) => b,
            None => panic!("buffer pool has no available buffers"),
        }
    }

    fn wait_too_long(&self, starttime: i64) -> bool {
        let now = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        now - starttime > constants::MAX_TIME
    }

    fn try_to_pin(&mut self, blk: BlockId) -> Option<Arc<Mutex<Buffer>>> {
        let buff = match self.find_existing_buffer(blk.clone()) {
            Some(b) => Some(b), // if buffer exists, return it
            None => {
                // otherwise, choose unpinned buffer
                let buff = self.choose_unpinned_buffer();
                match buff {
                    Some(b) => {
                        {
                            let mut b_ = b.lock().unwrap();
                            b_.assign_to_block(blk);
                        }
                        Some(b)
                    }
                    None => None,
                }
            }
        };
        match buff {
            Some(b) => {
                {
                    let mut b_ = b.lock().unwrap();
                    if !b_.is_pinned() {
                        self.num_available -= 1;
                    }
                    b_.pin();
                }
                Some(b)
            }
            None => None,
        }
    }

    fn find_existing_buffer(&self, blk: BlockId) -> Option<Arc<Mutex<Buffer>>> {
        for buffer in self.buffer_pool.iter() {
            let buf = buffer.lock().unwrap();
            if let Some(b) = buf.block() {
                if b.equals(&blk) {
                    return Some(buffer.clone());
                }
            }
        }
        return None;
    }

    fn choose_unpinned_buffer(&self) -> Option<Arc<Mutex<Buffer>>> {
        for buffer in self.buffer_pool.iter() {
            let buf = buffer.lock().unwrap();
            if !buf.is_pinned() {
                return Some(buffer.clone());
            }
        }
        return None;
    }

    pub fn print_pin(&self) {
        for (i, buffer) in self.buffer_pool.iter().enumerate() {
            let buf = buffer.lock().unwrap();
            println!("buffer {} is pinned: {}", i, buf.pins);
        }
    }
}
