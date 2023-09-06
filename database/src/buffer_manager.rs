pub mod buffer;
pub mod buffer_mgr;

#[cfg(test)]
mod tests {
    use crate::buffer_manager::buffer_mgr::BufferMgr;
    use crate::file_manager::block_id::BlockId;
    use crate::file_manager::file_mgr::FileMgr;
    use crate::file_manager::page::Page;
    use crate::log_manager::log_mgr::LogMgr;
    use std::fs;
    use std::sync::{Arc, Mutex};

    fn setup(db_directory: String) -> () {
        // delete db_directory if exists
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    #[test]
    fn test_buffer() {
        let db_directory = "./db/buffertest".to_string();
        let log_file = "testfile".to_string();
        setup(db_directory.clone());

        let mut fm = FileMgr::new(db_directory.clone(), 20);
        let log_mgr = Arc::new(Mutex::new(
            LogMgr::new(fm.clone(), log_file.clone()).unwrap(),
        )); //  create testfile and 0 padding for 400 bytes
        let mut bm = BufferMgr::new(fm.clone(), log_mgr.clone(), 3); // 3 buffers

        let buffer1 = bm.pin(BlockId::new("testfile".to_string(), 1)); // pin block 1
        {
            let mut buffer1_ = buffer1.lock().unwrap();
            let p = buffer1_.contents(); // get page asoociated with buffer
            let n = p.get_int(10).expect("get_int error: "); // read i32 from page at offset 80
            p.set_int(10, n + 1);
            buffer1_.set_modified(1, 0); // mark buffer as modified
        }
        bm.unpin(buffer1); // unpin buffer
        let buffer2 = bm.pin(BlockId::new("testfile".to_string(), 2)); // flush buffer1 to disk
        let _ = bm.pin(BlockId::new("testfile".to_string(), 3));
        let _ = bm.pin(BlockId::new("testfile".to_string(), 4));

        bm.unpin(buffer2); // None get dirty buffer2, so it does not need to be flushed
        let buffer2 = bm.pin(BlockId::new("testfile".to_string(), 1));
        {
            let mut buffer2_ = buffer2.lock().unwrap();
            let p2 = buffer2_.contents();
            p2.set_int(10, 9999); // not flushed to disk
            buffer2_.set_modified(1, 0);
        }

        // test disk content
        let mut p = Page::new(fm.block_size());
        let blk = BlockId::new("testfile".to_string(), 1);
        fm.read(&blk, &mut p).unwrap();
        let n = p.get_int(10).unwrap();
        assert_eq!(n, 1); // buffer1 is flushed to disk

        // flush buffer2 to disk and test disk content
        bm.flush_all(1);
        fm.read(&blk, &mut p).unwrap();
        let n = p.get_int(10).unwrap();
        assert_eq!(n, 9999); // buffer2 is flushed to disk

        // bm.unpin(buffer2);
    }

    #[test]
    fn test_buffer_mgr() {
        let db_directory = "./db/buffertest".to_string();
        let log_file = "testfile".to_string();
        setup(db_directory.clone());

        let fm = FileMgr::new(db_directory.clone(), 20);
        let log_mgr = Arc::new(Mutex::new(
            LogMgr::new(fm.clone(), log_file.clone()).unwrap(),
        )); //  create testfile and 0 padding for 400 bytes
        let mut bm = BufferMgr::new(fm.clone(), log_mgr.clone(), 6); // 3 buffers
        assert_eq!(bm.available(), 6);
        let mut buff = vec![None; 6];
        buff[0] = Some(bm.pin(BlockId::new("testfile".to_string(), 0)));
        buff[1] = Some(bm.pin(BlockId::new("testfile".to_string(), 1)));
        buff[2] = Some(bm.pin(BlockId::new("testfile".to_string(), 2)));
        bm.unpin(buff[1].clone().unwrap());
        assert_eq!(bm.available(), 4);
        buff[1] = None;
        buff[3] = Some(bm.pin(BlockId::new("testfile".to_string(), 0)));
        buff[4] = Some(bm.pin(BlockId::new("testfile".to_string(), 1)));
        assert_eq!(bm.available(), 3);
        bm.unpin(buff[2].clone().unwrap());
        buff[2] = None;
        buff[5] = Some(bm.pin(BlockId::new("testfile".to_string(), 3)));

        assert_eq!(
            buff[0].clone().unwrap().lock().unwrap().block().unwrap(),
            BlockId::new("testfile".to_string(), 0)
        );
        assert_eq!(
            buff[3].clone().unwrap().lock().unwrap().block().unwrap(),
            BlockId::new("testfile".to_string(), 0)
        );
        assert_eq!(
            buff[4].clone().unwrap().lock().unwrap().block().unwrap(),
            BlockId::new("testfile".to_string(), 1)
        );
        assert_eq!(
            buff[5].clone().unwrap().lock().unwrap().block().unwrap(),
            BlockId::new("testfile".to_string(), 3)
        );
    }
}
