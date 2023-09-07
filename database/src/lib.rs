use std::sync::{Arc, Mutex};

pub mod buffer_manager;
pub mod common;
pub mod file_manager;
pub mod log_manager;
pub mod metadata_management;
pub mod record_management;
pub mod transaction_manager;

pub struct SimpleDB {
    _log_file: String,
    _block_size: i32,
    _num_buffer: i32,
    file_mgr: file_manager::file_mgr::FileMgr,
    log_mgr: Arc<Mutex<log_manager::log_mgr::LogMgr>>,
    buffer_mgr: Arc<Mutex<buffer_manager::buffer_mgr::BufferMgr>>,
}

impl SimpleDB {
    pub fn new(_log_file: String, _block_size: i32, _num_buffer: i32) -> Self {
        let file_mgr = file_manager::file_mgr::FileMgr::new("./db".to_string(), _block_size);
        let log_mgr = Arc::new(Mutex::new(
            log_manager::log_mgr::LogMgr::new(file_mgr.clone(), _log_file.clone()).unwrap(),
        ));
        let buffer_mgr = Arc::new(Mutex::new(buffer_manager::buffer_mgr::BufferMgr::new(
            file_mgr.clone(),
            log_mgr.clone(),
            _num_buffer,
        )));

        return Self {
            _log_file,
            _block_size,
            _num_buffer,
            file_mgr,
            log_mgr,
            buffer_mgr,
        };
    }

    pub fn new_tx(&self) -> transaction_manager::transaction::Transaction {
        return transaction_manager::transaction::Transaction::new(
            self.file_mgr.clone(),
            self.log_mgr.clone(),
            self.buffer_mgr.clone(),
        );
    }
}
