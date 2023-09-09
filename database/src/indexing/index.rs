use crate::common::Constant;
use crate::{record_management::rid::RID, transaction_manager::transaction::Transaction};

pub trait Index {
    fn before_first(&mut self, search_key: Constant, tx: &mut Transaction);
    fn next(&mut self, tx: &mut Transaction) -> bool;
    fn get_data_rid(&mut self, tx: &mut Transaction) -> RID;
    fn insert(&mut self, val: Constant, data_rid: RID, tx: &mut Transaction);
    fn delete(&mut self, val: Constant, data_rid: RID, tx: &mut Transaction);
    fn close(&mut self, tx: &mut Transaction);
}
