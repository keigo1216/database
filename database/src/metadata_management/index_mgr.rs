use std::collections::HashMap;

use crate::indexing::hash_index::HashIndex;
use crate::metadata_management::state_mgr::StatMgr;
use crate::metadata_management::state_mgr::StateInfo;
use crate::metadata_management::table_mgr::TableMgr;
use crate::record_management::layout::Layout;
use crate::record_management::schema::{Schema, Type};
use crate::record_management::table_scan::TableScan;
use crate::transaction_manager::transaction::Transaction;

use super::table_mgr::MAX_NAME;

#[derive(Clone, Debug)]
pub struct IndexInfo {
    idxname: String,
    fldname: String,
    _tbl_schema: Schema,
    idx_layout: Layout,
    si: StateInfo,
}

pub struct IndexMgr {
    layout: Layout,
    tbl_mgr: TableMgr,
    stat_mgr: StatMgr,
}

impl IndexMgr {
    pub fn new(is_new: bool, tbl_mgr: TableMgr, stat_mgr: StatMgr, tx: &mut Transaction) -> Self {
        if is_new {
            let mut sch = Schema::new();
            sch.add_string_field("indexname".to_string(), MAX_NAME);
            sch.add_string_field("tablename".to_string(), MAX_NAME);
            sch.add_string_field("fieldname".to_string(), MAX_NAME);
            tbl_mgr.create_table("idxcat".to_string(), sch, tx);
        }
        Self {
            layout: tbl_mgr.get_layout("idxcat".to_string(), tx),
            tbl_mgr,
            stat_mgr,
        }
    }

    pub fn create_index(
        &self,
        idxname: String,
        tblname: String,
        fldname: String,
        tx: &mut Transaction,
    ) {
        let mut ts = TableScan::new(tx, "idxcat".to_string(), self.layout.clone());
        ts.insert(tx);
        ts.set_string(tx, &"indexname".to_string(), idxname);
        ts.set_string(tx, &"tablename".to_string(), tblname);
        ts.set_string(tx, &"fieldname".to_string(), fldname);
        ts.close(tx);
    }

    pub fn get_index_info(
        &mut self,
        tblname: String,
        tx: &mut Transaction,
    ) -> HashMap<String, IndexInfo> {
        let mut result: HashMap<String, IndexInfo> = HashMap::new();
        let mut ts = TableScan::new(tx, "idxcat".to_string(), self.layout.clone());
        while ts.next(tx) {
            if ts.get_string(tx, &"tablename".to_string()) == tblname {
                let idxname = ts.get_string(tx, &"indexname".to_string());
                let fldname = ts.get_string(tx, &"fieldname".to_string());
                let tbl_layout = self.tbl_mgr.get_layout(tblname.clone(), tx);
                let tblsi = self
                    .stat_mgr
                    .get_stat_info(tblname.clone(), tbl_layout.clone(), tx);
                let index_info =
                    IndexInfo::new(idxname.clone(), fldname.clone(), tbl_layout.schema(), tblsi);
                result.insert(fldname.clone(), index_info);
            }
        }
        ts.close(tx);
        return result;
    }
}

impl IndexInfo {
    pub fn new(idxname: String, fldname: String, tbl_schema: Schema, si: StateInfo) -> Self {
        Self {
            idxname,
            fldname: fldname.clone(),
            _tbl_schema: tbl_schema.clone(),
            idx_layout: Self::create_idx_layout(fldname, tbl_schema),
            si,
        }
    }

    pub fn open(&self) -> HashIndex {
        let _sch = Schema::new();
        return HashIndex::new(self.idxname.clone(), self.idx_layout.clone());
    }

    pub fn blocks_accessed(&self, tx: &mut Transaction) -> i32 {
        let rpb = tx.block_size() / self.idx_layout.slot_size(); // get records per block
        let num_blocks = self.si.records_output() / rpb;
        return HashIndex::search_cost(num_blocks, rpb);
    }

    pub fn records_output(&self) -> i32 {
        return self.si.records_output() / self.si.distinct_values(&self.fldname);
    }

    pub fn distinct_values(&self, fname: String) -> i32 {
        return if self.fldname == fname {
            1
        } else {
            self.si.distinct_values(&self.fldname)
        };
    }

    fn create_idx_layout(fldname: String, tbl_schema: Schema) -> Layout {
        let mut sch = Schema::new();
        sch.add_int_field("block".to_string());
        sch.add_int_field("id".to_string());
        if tbl_schema.get_type_(&fldname) == Type::INTEGER.into() {
            sch.add_int_field("dataval".to_string());
        } else {
            let fldlen = tbl_schema.get_length(&fldname);
            sch.add_string_field("dataval".to_string(), fldlen);
        }
        return Layout::new_from_schema(sch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimpleDB;
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
    fn test_index_mgr() -> Result<()> {
        setup();
        let db = SimpleDB::new("indexmgrtest".to_string(), 400, 8);
        let mut tx = db.new_tx();
        let tm = TableMgr::new(true, &mut tx);
        let mut sch = Schema::new();
        let statmgr = StatMgr::new(tm.clone(), &mut tx);

        sch.add_int_field("sid".to_string());
        sch.add_string_field("sname".to_string(), 10);
        sch.add_int_field("majorId".to_string());
        tm.create_table("student".to_string(), sch, &mut tx);

        let layout = tm.get_layout("student".to_string(), &mut tx);
        let mut ts = TableScan::new(&mut tx, "student".to_string(), layout.clone());
        ts.insert(&mut tx);
        ts.set_int(&mut tx, &"sid".to_string(), 1);
        ts.set_string(&mut tx, &"sname".to_string(), "Joe".to_string());
        ts.set_int(&mut tx, &"majorId".to_string(), 20);
        ts.insert(&mut tx);
        ts.set_int(&mut tx, &"sid".to_string(), 2);
        ts.set_string(&mut tx, &"sname".to_string(), "Aoi".to_string());
        ts.set_int(&mut tx, &"majorId".to_string(), 10);

        let mut idxmgr = IndexMgr::new(true, tm.clone(), statmgr.clone(), &mut tx);
        idxmgr.create_index(
            "sidIdx".to_string(),
            "student".to_string(),
            "sid".to_string(),
            &mut tx,
        );
        idxmgr.create_index(
            "snameIdx".to_string(),
            "student".to_string(),
            "sname".to_string(),
            &mut tx,
        );

        let indexes = idxmgr.get_index_info("student".to_string(), &mut tx);
        assert_eq!(indexes.len(), 2);

        let sid_info = indexes.get("sid").unwrap();
        assert_eq!(sid_info.si.records_output(), 2);
        assert_eq!(sid_info.si.distinct_values(&"sid".to_string()), 1);
        assert_eq!(sid_info.blocks_accessed(&mut tx), 0);

        let sname_info = indexes.get("sname").unwrap();
        assert_eq!(sid_info.si.records_output(), 2);
        assert_eq!(sid_info.si.distinct_values(&"sname".to_string()), 1);
        assert_eq!(sname_info.blocks_accessed(&mut tx), 0);

        tx.commit();
        teardown();
        Ok(())
    }
}
