use crate::transaction_manager::transaction::TransactionForUndo;

pub enum RecordType {
    CHECKPOINT,
    START,
    COMMIT,
    ROLLBACK,
    SETINT,
    SETSTRING,
}

impl From<i32> for RecordType {
    fn from(i: i32) -> Self {
        match i {
            0 => RecordType::CHECKPOINT,
            1 => RecordType::START,
            2 => RecordType::COMMIT,
            3 => RecordType::ROLLBACK,
            4 => RecordType::SETINT,
            5 => RecordType::SETSTRING,
            _ => panic!("RecordType::from: invalid i32"),
        }
    }
}

pub trait LogRecordItem {
    fn op() -> RecordType;
    fn tx_number(&self) -> Option<i32>;
    fn undo(&self, tx: &mut TransactionForUndo) -> ();
}
