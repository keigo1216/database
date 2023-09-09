use std::collections::HashMap;

use crate::metadata_management::index_mgr::IndexMgr;
use crate::metadata_management::state_mgr::StatMgr;
use crate::metadata_management::table_mgr::TableMgr;
use crate::metadata_management::view_mgr::ViewMgr;
use crate::record_management::layout::Layout;
use crate::record_management::schema::Schema;
use crate::transaction_manager::transaction::Transaction;

use super::index_mgr::IndexInfo;
use super::state_mgr::StateInfo;

pub struct MetadataMgr {
    table_mgr: TableMgr,
    view_mgr: ViewMgr,
    stat_mgr: StatMgr,
    index_mgr: IndexMgr,
}

impl MetadataMgr {
    pub fn new(is_new: bool, tx: &mut Transaction) -> Self {
        let tx = tx;
        let table_mgr = TableMgr::new(is_new, tx);
        let view_mgr = ViewMgr::new(is_new, table_mgr.clone(), tx);
        let stat_mgr = StatMgr::new(table_mgr.clone(), tx);
        let index_mgr = IndexMgr::new(is_new, table_mgr.clone(), stat_mgr.clone(), tx);
        Self {
            table_mgr,
            view_mgr,
            stat_mgr,
            index_mgr,
        }
    }

    pub fn create_table(&mut self, tblname: String, sch: Schema, tx: &mut Transaction) {
        self.table_mgr.create_table(tblname, sch, tx);
    }

    pub fn get_layout(&mut self, tblname: String, tx: &mut Transaction) -> Layout {
        self.table_mgr.get_layout(tblname, tx)
    }

    pub fn create_view(&mut self, viewname: String, tx: &mut Transaction, viewdef: String) {
        self.view_mgr.create_view(viewname, viewdef, tx);
    }

    pub fn get_view_def(&mut self, viewname: String, tx: &mut Transaction) -> Option<String> {
        self.view_mgr.get_view_def(viewname, tx)
    }

    pub fn create_index(
        &mut self,
        idxname: String,
        tblname: String,
        fldname: String,
        tx: &mut Transaction,
    ) {
        self.index_mgr.create_index(idxname, tblname, fldname, tx);
    }

    pub fn get_index_info(
        &mut self,
        tblname: String,
        tx: &mut Transaction,
    ) -> HashMap<String, IndexInfo> {
        self.index_mgr.get_index_info(tblname, tx)
    }

    pub fn get_stat_info(
        &mut self,
        tblname: String,
        layout: Layout,
        tx: &mut Transaction,
    ) -> StateInfo {
        self.stat_mgr.get_stat_info(tblname, layout, tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        record_management::{schema::Type, table_scan::TableScan},
        SimpleDB,
    };
    use anyhow::Result;
    use std::fs;

    fn setup() {
        let db_directory = "./db".to_string();
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    fn teardown() {
        let db_directory = "./db".to_string();
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    #[test]
    fn test_metadata_mgr() -> Result<()> {
        setup();
        let db = SimpleDB::new("metadatamgrtest".to_string(), 400, 8);
        let mut tx = db.new_tx();
        let mut mdm = MetadataMgr::new(true, &mut tx);

        let mut sch = Schema::new();
        sch.add_int_field("A".to_string());
        sch.add_string_field("B".to_string(), 9);

        mdm.create_table("MyTable".to_string(), sch, &mut tx);
        let layout = mdm.get_layout("MyTable".to_string(), &mut tx);
        let _size = layout.slot_size();
        let sch2 = layout.schema();
        // check sch == sch2
        assert_eq!(sch2.get_type_(&"A".to_string()), Type::INTEGER.into());
        assert_eq!(sch2.get_type_(&"B".to_string()), Type::VARCHAR.into());
        assert_eq!(sch2.get_length(&"B".to_string()), 9);

        let mut ts = TableScan::new(&mut tx, "MyTable".to_string(), layout.clone());
        for i in 0..50 {
            ts.insert(&mut tx);
            ts.set_int(&mut tx, &"A".to_string(), i);
            ts.set_string(&mut tx, &"B".to_string(), format!("rec{}", i));
        }
        let si = mdm.get_stat_info("MyTable".to_string(), layout, &mut tx);
        assert_eq!(si.distinct_values(&"A".to_string()), 1 + 50 / 3);
        assert_eq!(si.distinct_values(&"B".to_string()), 1 + 50 / 3);
        assert_eq!(si.records_output(), 50);
        assert_eq!(si.blocks_accessed(), 3); // record size is 21 bytes, block size is 400 bytes, so 400 / 21 = 19.05 -> 19 records per block

        let view_def = "select A from MyTable".to_string();
        mdm.create_view("ViewA".to_string(), &mut tx, view_def.clone());
        let view_def2 = mdm.get_view_def("ViewA".to_string(), &mut tx);
        assert_eq!(view_def, view_def2.unwrap());

        mdm.create_index(
            "indexA".to_string(),
            "MyTable".to_string(),
            "A".to_string(),
            &mut tx,
        );
        mdm.create_index(
            "indexB".to_string(),
            "MyTable".to_string(),
            "B".to_string(),
            &mut tx,
        );
        let idxmap = mdm.get_index_info("MyTable".to_string(), &mut tx);

        let ii = idxmap.get(&"A".to_string()).unwrap();
        let dis_val = 1 + 50 / 3;
        assert_eq!(ii.blocks_accessed(&mut tx), 0);
        assert_eq!(ii.records_output(), 50 / dis_val);
        assert_eq!(ii.distinct_values("A".to_string()), 1);
        assert_eq!(ii.distinct_values("B".to_string()), 1 + 50 / 3);

        let ii = idxmap.get(&"B".to_string()).unwrap();
        assert_eq!(ii.blocks_accessed(&mut tx), 0);
        assert_eq!(ii.records_output(), 50 / dis_val);
        assert_eq!(ii.distinct_values("A".to_string()), 1 + 50 / 3);
        assert_eq!(ii.distinct_values("B".to_string()), 1);

        tx.commit();
        teardown();
        Ok(())
    }
}
