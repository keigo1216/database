use crate::file_manager::block_id::BlockId;
use crate::file_manager::page::Page;
use crate::file_manager::FileManagerError;
use bytebuffer::ByteBuffer;
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::BufRead;
use std::io::BufReader;
use std::io::Seek;
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct FileMgr {
    db_directory: String,
    block_size: i32,
    is_new: bool,
    open_files: HashSet<String>,
}

impl FileMgr {
    pub fn new(db_directory: String, block_size: i32) -> Self {
        let is_new = !match fs::metadata(db_directory.clone()) {
            Ok(_) => true,
            Err(_) => false,
        };

        // create directory if not exists
        if is_new {
            fs::create_dir_all(db_directory.clone()).unwrap();
        }

        // remove any leftover temporary tables
        for entry in fs::read_dir(db_directory.clone()).unwrap() {
            let entry = entry.unwrap();
            let file_path = entry.path();
            let file_name = file_path.file_name().unwrap().to_string_lossy().to_string();

            if file_name.starts_with("temp") {
                if file_path.is_dir() {
                    fs::remove_dir_all(file_path).unwrap();
                } else if file_path.is_file() {
                    fs::remove_file(file_path).unwrap();
                }
            }
        }

        let open_files = HashSet::new();

        Self {
            db_directory,
            block_size,
            is_new,
            open_files,
        }
    }

    /// Read the contents of the file at the given path and position in the given p.
    /// Writes contents to the given p.byte_buffer.
    /// @param blk the block to be read
    /// @param p the page to be written to
    pub fn read(&mut self, blk: &BlockId, p: &mut Page) -> Result<(), FileManagerError> {
        // if blk.filename() is already in open_files, then we don't need to create again
        let filename = self.get_file(blk.filename().clone());
        let mut file = match filename {
            Some(f) => {
                // file already exists
                match OpenOptions::new().read(true).open(self.get_path(f)) {
                    Ok(file) => file,
                    Err(e) => panic!("file_mgr.rs: read: {:?}", e),
                }
            }
            None => {
                // file does not exist
                match OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(self.get_path(blk.filename().clone()))
                {
                    Ok(file) => {
                        self.open_files.insert(blk.filename().clone());
                        file
                    }
                    Err(e) => panic!("file_mgr.rs: read: {:?}", e),
                }
            }
        };

        let offset = (blk.number() * self.block_size) as u64;
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        let mut buf_reader = BufReader::with_capacity(self.block_size as usize, file);
        let byte_array = buf_reader.fill_buf().unwrap();
        p.set_byte_buffer(ByteBuffer::from_vec(byte_array.to_vec()));
        Ok(())
    }

    /// Write the contents of the given p to the file at the given path and position.
    /// Writes contents from the given p.byte_buffer.
    /// @param blk the block to be written
    /// @param p the page to be written
    pub fn write(&mut self, blk: &BlockId, p: &mut Page) -> Result<(), FileManagerError> {
        // if blk.filename() is already in open_files, then we don't need to create again
        let filename = self.get_file(blk.filename().clone());
        let mut file = match filename {
            Some(f) => {
                // file already exists
                match OpenOptions::new().write(true).open(self.get_path(f)) {
                    Ok(file) => file,
                    Err(e) => panic!("file_mgr.rs: read: {:?}", e),
                }
            }
            None => {
                // file does not exist
                match OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(self.get_path(blk.filename().clone()))
                {
                    Ok(file) => {
                        self.open_files.insert(blk.filename().clone());
                        file
                    }
                    Err(e) => panic!("file_mgr.rs: read: {:?}", e),
                }
            }
        };

        let offset = (blk.number() * self.block_size) as u64;
        file.seek(std::io::SeekFrom::Start(offset)).unwrap();
        file.write_all_at(&mut p.contents().into_vec(), offset)
            .unwrap();
        Ok(())
    }

    /// Appends a new block to the end of the specified file with 0 padding.
    /// @param filename the name of the file
    pub fn append(&mut self, filename: String) -> Result<BlockId, FileManagerError> {
        match self.get_file(filename.clone()) {
            Some(f) => {
                match OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(self.get_path(f))
                {
                    Ok(file) => file,
                    Err(e) => panic!("file_mgr.rs: append: {:?}", e),
                }
            }
            None => {
                match OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(self.get_path(filename.clone()))
                {
                    Ok(file) => {
                        self.open_files.insert(filename.clone());
                        file
                    }
                    Err(e) => panic!("file_mgr.rs: append: {:?}", e),
                }
            }
        };
        let newblknum = match fs::metadata(self.get_path(filename.clone())) {
            Ok(metadata) => {
                ((metadata.len() + (self.block_size - 1) as u64) / self.block_size as u64) as i32
            }
            Err(_) => return Err(FileManagerError::FileNotFound),
        };
        let blk = BlockId::new(filename.clone(), newblknum);
        match OpenOptions::new()
            .write(true)
            .open(self.get_path(filename.clone()))
        {
            Ok(file) => {
                let offset = (blk.number() * self.block_size) as u64;
                match file.write_all_at(&mut vec![0; self.block_size as usize], offset) {
                    Ok(_) => Ok(blk),
                    Err(_) => Err(FileManagerError::AppendBlockError(blk.clone())),
                }
            }
            Err(_) => Err(FileManagerError::FileOpenError),
        }
    }

    /// Returns the number of blocks in the specified file.
    /// If the file does not exist, then a new file is created and returned 0.
    /// @param filename the name of the file
    /// @return the number of blocks in the file
    pub fn length(&mut self, filename: String) -> Result<i32, FileManagerError> {
        match self.get_file(filename.clone()) {
            Some(_) => {}
            None => {
                match OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(self.get_path(filename.clone()))
                {
                    Ok(_) => {
                        self.open_files.insert(filename.clone());
                    }
                    Err(e) => panic!("file_mgr.rs: length: {:?}", e),
                }
            }
        };
        match fs::metadata(self.get_path(filename.clone())) {
            Ok(metadata) => Ok((metadata.len() as f64 / self.block_size as f64).ceil() as i32),
            Err(_) => Err(FileManagerError::FileNotFound),
        }
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn block_size(&self) -> i32 {
        self.block_size
    }

    /// Returns the path to the file with the given filename.
    /// If the file does not exist, return None.
    /// @param filename the name of the file
    fn get_file(&mut self, filename: String) -> Option<String> {
        // if file is already open, return filename
        if self.open_files.contains(&filename) {
            return Some(filename);
        } else {
            return None;
        }
    }

    /// Returns the path to the file with the given filename.
    /// Add directory to filename.
    /// @param filename the name of the file
    fn get_path(&self, filename: String) -> String {
        // create path to file
        let mut path = PathBuf::from(self.db_directory.clone());
        path.push(filename);

        path.to_str().unwrap().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::fs;
    use std::fs::File;
    use std::io::{Read, Write};

    fn setup(db_directory: String) -> () {
        // delete db_directory if exists
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    fn teardown(db_directory: String) -> () {
        // delete db_directory if exists
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    #[test]
    pub fn test_file_mgr() -> Result<()> {
        let db_directory = "./test_db".to_string();

        // test fn new
        {
            setup(db_directory.clone());
            // create some files in db_directory
            fs::create_dir(db_directory.clone()).unwrap();
            fs::create_dir(db_directory.clone() + "/temp1").unwrap(); // create directory
            fs::create_dir(db_directory.clone() + "/temp2").unwrap(); // create directory
            File::create(db_directory.clone() + "/temp3.txt").unwrap(); // create file

            let _file_mgr = FileMgr::new(db_directory.clone(), 400);

            // check if db_directory exists
            assert_eq!(fs::metadata(db_directory.clone()).is_ok(), true);
            // check temp* files are deleted
            assert_eq!(fs::metadata(db_directory.clone() + "/temp1").is_ok(), false);
            assert_eq!(fs::metadata(db_directory.clone() + "/temp2").is_ok(), false);
            assert_eq!(
                fs::metadata(db_directory.clone() + "/temp3.txt").is_ok(),
                false
            );
            teardown(db_directory.clone());
        }

        // test fn read
        {
            setup(db_directory.clone());

            let mut file_mgr = FileMgr::new(db_directory.clone(), 20);
            let mut page = Page::new(20);

            // create file
            let mut file = File::create(db_directory.clone() + "/test.txt").unwrap();
            file.write_all("Hello World!. My Name is hogehoge.".as_bytes())
                .unwrap();

            // read block
            let block_id = BlockId::new("test.txt".to_string(), 0);
            let result = file_mgr.read(&block_id, &mut page);

            match result {
                Ok(_) => (),
                Err(FileManagerError::FileOpenError) => panic!("FileOpenError"),
                Err(FileManagerError::ReadBlockError(_)) => panic!("ReadBlockError"),
                Err(_) => panic!("Unknown Error"),
            }

            assert_eq!(
                page.contents().into_vec(),
                "Hello World!. My Name is hogehoge."
                    .to_string()
                    .into_bytes()
                    .iter()
                    .take(20)
                    .cloned()
                    .collect::<Vec<u8>>()
            );

            teardown(db_directory.clone());
        }

        // test fn write
        {
            setup(db_directory.clone());

            let mut file_mgr = FileMgr::new(db_directory.clone(), 20);
            let mut page = Page::new_log("Hello World!".as_bytes().to_vec());

            // write block
            let block_id = BlockId::new("test.txt".to_string(), 0);
            let result = file_mgr.write(&block_id, &mut page);

            match result {
                Ok(_) => (),
                Err(FileManagerError::FileOpenError) => panic!("FileOpenError"),
                Err(FileManagerError::WriteBlockError(_)) => panic!("WriteBlockError"),
                Err(_) => panic!("Unknown Error"),
            }

            // check if file is written
            let mut file = File::open(db_directory.clone() + "/test.txt").unwrap();
            let mut contents = String::new();
            file.read_to_string(&mut contents).unwrap();
            assert_eq!(contents, "Hello World!");

            teardown(db_directory.clone());
        }

        // test fn append
        {
            setup(db_directory.clone());

            let message = "H".repeat(20);
            let block_size = 30;
            let mut file_mgr = FileMgr::new(db_directory.clone(), block_size);

            // create file
            let mut file = File::create(db_directory.clone() + "/test.txt").unwrap();
            file.write_all(message.as_bytes()).unwrap();

            // append block
            let result = file_mgr.append("test.txt".to_string());

            match result {
                Ok(_) => (),
                Err(FileManagerError::FileOpenError) => panic!("FileOpenError"),
                Err(FileManagerError::FileNotFound) => panic!("FileNotFound"),
                Err(FileManagerError::AppendBlockError(_)) => panic!("AppendBlockError"),
                Err(_) => panic!("Unknown Error"),
            }

            // check if file is written
            let mut file = File::open(db_directory.clone() + "/test.txt").unwrap();
            let mut contents = String::new();
            file.read_to_string(&mut contents).unwrap();
            let result = message.to_string()
                + &"\0".repeat(
                    (block_size as usize - message.len() % block_size as usize)
                        % block_size as usize
                        + block_size as usize,
                );
            assert_eq!(contents, result);

            teardown(db_directory.clone());
        }

        // test fn length
        {
            setup(db_directory.clone());

            let mut file_mgr = FileMgr::new(db_directory.clone(), 20);
            let mut page = Page::new_log("H".repeat(30).as_bytes().to_vec());

            // write block
            let block_id = BlockId::new("test.txt".to_string(), 0);
            let result = file_mgr.write(&block_id, &mut page);
            assert!(result.is_ok());

            let length = file_mgr.length("test.txt".to_string());
            match length {
                Ok(len) => assert_eq!(len, 2),
                Err(FileManagerError::FileOpenError) => panic!("FileOpenError"),
                Err(FileManagerError::FileNotFound) => panic!("FileNotFound"),
                Err(_) => panic!("Unknown Error"),
            }
            assert!(length.is_ok());

            teardown(db_directory.clone());
        }

        Ok(())
    }
}
