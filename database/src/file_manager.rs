pub mod block_id;
pub mod file_mgr;
pub mod page;

#[derive(Debug)]
pub enum FileManagerError {
    FileOpenError,
    FileNotFound,
    FileCreateError,
    ReadBlockError(block_id::BlockId),
    WriteBlockError(block_id::BlockId),
    AppendBlockError(block_id::BlockId),
}

#[cfg(test)]
mod tests {
    use super::{block_id::BlockId, file_mgr::FileMgr, page::Page};
    use std::fs;

    fn teardown(path: String) {
        // delete path
        if fs::metadata(path.clone()).is_ok() {
            fs::remove_dir_all(path.clone()).unwrap();
        }
    }

    #[test]
    pub fn test_file_manager() {
        let path = "./db/filetest";
        let block_size = 400;
        let mut fm = FileMgr::new(path.to_string(), block_size);
        let mut blk = BlockId::new("testfile".to_string(), 0);

        let pos1 = 88;
        let mut p1 = Page::new(fm.block_size());
        p1.set_string(pos1, "abcdefghijklm".to_string());

        let size = Page::max_length("abcdefghijklm".len() as i32);
        let pos2 = pos1 + size;
        p1.set_int(pos2, 345);

        let _ = fm.write(&mut blk, &mut p1);

        let mut p2 = Page::new(fm.block_size());

        let _ = fm.read(&blk, &mut p2);

        assert_eq!(p2.get_string(pos1).unwrap(), "abcdefghijklm");
        assert_eq!(p2.get_int(pos2).unwrap(), 345);

        teardown("./db".to_string());
    }
}
