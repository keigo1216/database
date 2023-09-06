use crate::file_manager::page::Page;
use crate::transaction_manager::recovery_manager::log_record_item::LogRecordItem;
use crate::transaction_manager::recovery_manager::log_record_item::RecordType;
use crate::transaction_manager::recovery_manager::{
    check_point_record::CheckPointRecord, commit_record::CommitRecord,
    roll_back_record::RollBackRecord, set_int_record::SetIntRecord,
    set_string_record::SetStringRecord, start_record::StartRecord,
};
use crate::transaction_manager::transaction::TransactionForUndo;

pub struct LogRecord {}

#[derive(Debug)]
pub enum LogRecordType {
    CHECKPOINT(CheckPointRecord),
    START(StartRecord),
    COMMIT(CommitRecord),
    ROLLBACK(RollBackRecord),
    SETINT(SetIntRecord),
    SETSTRING(SetStringRecord),
}

impl LogRecordType {
    pub fn tx_number(&self) -> Option<i32> {
        match self {
            LogRecordType::CHECKPOINT(c) => c.tx_number(),
            LogRecordType::START(s) => s.tx_number(),
            LogRecordType::COMMIT(c) => c.tx_number(),
            LogRecordType::ROLLBACK(r) => r.tx_number(),
            LogRecordType::SETINT(s) => s.tx_number(),
            LogRecordType::SETSTRING(s) => s.tx_number(),
        }
    }

    pub fn undo(&self, tx: &mut TransactionForUndo) {
        match self {
            LogRecordType::CHECKPOINT(c) => c.undo(tx),
            LogRecordType::START(s) => s.undo(tx),
            LogRecordType::COMMIT(c) => c.undo(tx),
            LogRecordType::ROLLBACK(r) => r.undo(tx),
            LogRecordType::SETINT(s) => s.undo(tx),
            LogRecordType::SETSTRING(s) => s.undo(tx),
        }
    }
}

impl LogRecord {
    pub fn create_log_record(bytes: Vec<u8>) -> LogRecordType {
        let mut p = Page::new_log(bytes);
        let log_record = RecordType::from(
            p.get_int(0)
                .expect("Logrecord::create_log_record: failed to get record type"),
        );
        match log_record {
            RecordType::CHECKPOINT => LogRecordType::CHECKPOINT(CheckPointRecord::new()),
            RecordType::START => LogRecordType::START(StartRecord::new(p)),
            RecordType::COMMIT => LogRecordType::COMMIT(CommitRecord::new(p)),
            RecordType::ROLLBACK => LogRecordType::ROLLBACK(RollBackRecord::new(p)),
            RecordType::SETINT => LogRecordType::SETINT(SetIntRecord::new(p)),
            RecordType::SETSTRING => LogRecordType::SETSTRING(SetStringRecord::new(p)),
        }
    }
}
