use std::fs::{self, File};
use std::fs::OpenOptions;
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use bytebuffer::ByteBuffer;

use crate::file_manager::page::Page;
use crate::file_manager::block_id::BlockId;
use crate::file_manager::FileManagerError;

pub struct FileMgr {
    db_directory: String,
    block_size: i32,
    is_new: bool,
}

impl FileMgr {
    pub fn new(db_directory: String, block_size: i32) -> Self {
        let is_new = !match fs::metadata(db_directory.clone()) {
            Ok(_) => true,
            Err(_) => false
        };

        // create directory if not exists
        if is_new {
            fs::create_dir(db_directory.clone()).unwrap();
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

        Self {
            db_directory,
            block_size,
            is_new,
        }
    }

    pub fn read(&self, blk: &BlockId, p: &mut Page) -> Result<(), FileManagerError> {
        match OpenOptions::new().read(true).open(blk.filename()) { // open file
            Ok(file) => {
                let offset = (blk.number() * self.block_size) as u64;
                let mut byte_buffer = p.contents().into_vec();
                match file.read_exact_at(&mut byte_buffer, offset) { // read block
                    Ok(_) => {
                        p.set_byte_buffer(ByteBuffer::from_vec(byte_buffer));
                        Ok(())
                    },
                    Err(_) => Err(FileManagerError::ReadBlockError(blk.clone())),
                }
            },
            Err(_) => Err(FileManagerError::FileOpenError),
        }
    }

    pub fn write(&self, blk: &BlockId, p: &mut Page) -> Result<(), FileManagerError> {
        match OpenOptions::new().write(true).create(true).open(blk.filename()){ // open file
            Ok(file) => {
                let offset = (blk.number() * self.block_size) as u64;
                match file.write_all_at(&mut p.contents().into_vec(), offset) { // write to file
                    Ok(_) => Ok(()),
                    Err(_) => Err(FileManagerError::WriteBlockError(blk.clone())),
                }
            },
            Err(_) => Err(FileManagerError::FileOpenError),
        }
    } 

    pub fn append(&self, filename: String) -> Result<BlockId, FileManagerError> {
        let newblknum = match fs::metadata(filename.clone()) {
            Ok(metadata) => ((metadata.len() +(self.block_size - 1) as u64) / self.block_size as u64) as i32,
            Err(_) => return Err(FileManagerError::FileNotFound)
        };
        let blk = BlockId::new(filename, newblknum);
        match OpenOptions::new().write(true).open(blk.filename()) {
            Ok(file) => {
                let offset = (blk.number() * self.block_size) as u64;
                match file.write_all_at(&mut vec![0; self.block_size as usize], offset) {
                    Ok(_) => Ok(blk),
                    Err(_) => Err(FileManagerError::AppendBlockError(blk.clone())),
                }
            },
            Err(_) => Err(FileManagerError::FileOpenError),
        }
    }

    pub fn length(&self, filename: String) -> Result<i32, FileManagerError> {
        match fs::metadata(filename) {
            Ok(metadata) => Ok(((metadata.len() +(self.block_size - 1) as u64) / self.block_size as u64) as i32),
            Err(_) => Err(FileManagerError::FileNotFound)
        }
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn block_size(&self) -> i32 {
        self.block_size
    }

    pub fn get_file(&self, filename: String) -> Result<File, FileManagerError> {
        // create path to file
        let mut path = PathBuf::from(self.db_directory.clone());
        path.push(filename);

        match OpenOptions::new().read(true).write(true).open(path) {
            Ok(file) => Ok(file),
            Err(_) => Err(FileManagerError::FileNotFound),
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::fs;
    use std::fs::File;
    use std::io::{Write, Read};

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
            assert_eq!(fs::metadata(db_directory.clone() + "/temp3.txt").is_ok(), false);
            teardown(db_directory.clone());
        }

        // test fn read
        {
            setup(db_directory.clone());
            
            let file_mgr = FileMgr::new(db_directory.clone(), 20);
            let mut page = Page::new(20);

            // create file
            let mut file = File::create(db_directory.clone() + "/test.txt").unwrap();
            file.write_all("Hello World!. My Name is hogehoge.".as_bytes()).unwrap();

            // read block
            let block_id = BlockId::new(db_directory.clone() + "/test.txt", 0);
            let result = file_mgr.read(&block_id, &mut page);

            match result {
                Ok(_) => (),
                Err(FileManagerError::FileOpenError) => panic!("FileOpenError"),
                Err(FileManagerError::ReadBlockError(_)) => panic!("ReadBlockError"),
                Err(_) => panic!("Unknown Error"),
            }

            assert_eq!(page.contents().into_vec(), "Hello World!. My Name is hogehoge.".to_string().into_bytes().iter().take(20).cloned().collect::<Vec<u8>>());

            teardown(db_directory.clone());
        }

        // test fn write
        {
            setup(db_directory.clone());

            let file_mgr = FileMgr::new(db_directory.clone(), 20);
            let mut page = Page::new_log("Hello World!".as_bytes().to_vec());
            
            // create file
            File::create(db_directory.clone() + "/test.txt").unwrap();

            // write block
            let block_id = BlockId::new(db_directory.clone() + "/test.txt", 0);
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
            let file_mgr = FileMgr::new(db_directory.clone(), block_size);
            
            // create file
            let mut file = File::create(db_directory.clone() + "/test.txt").unwrap();
            file.write_all(message.as_bytes()).unwrap();

            // append block
            let result = file_mgr.append(db_directory.clone() + "/test.txt");

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
            let result = message.to_string() + &"\0".repeat((block_size as usize - message.len() % block_size as usize) % block_size as usize + block_size as usize);
            assert_eq!(contents, result);

            teardown(db_directory.clone()); 
        }

        Ok(())
    }
}