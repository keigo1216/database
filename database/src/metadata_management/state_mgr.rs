use std::collections::HashMap;

use crate::metadata_management::table_mgr::TableMgr;
use crate::record_management::layout::Layout;
use crate::record_management::table_scan::TableScan;
use crate::transaction_manager::transaction::Transaction;

pub struct StatMgr {
    tbl_mgr: TableMgr,
    table_states: HashMap<String, StateInfo>,
    num_calls: i32,
}

impl StatMgr {
    pub fn new(tbl_mgr: TableMgr, tx: &mut Transaction) -> Self {
        let mut stat_mgr = Self {
            tbl_mgr,
            table_states: HashMap::new(),
            num_calls: 0,
        };
        stat_mgr.refresh_statistics(tx);
        return stat_mgr;
    }

    pub fn get_stat_info(
        &mut self,
        tblname: String,
        layout: Layout,
        tx: &mut Transaction,
    ) -> StateInfo {
        self.num_calls += 1;
        if self.num_calls % 100 == 0 {
            self.refresh_statistics(tx);
        }
        let si = self.table_states.get(&tblname);
        match si {
            Some(si) => si.clone(),
            None => self.calc_table_state(tblname, layout, tx),
        }
    }

    fn refresh_statistics(&mut self, tx: &mut Transaction) {
        self.table_states = HashMap::new();
        self.num_calls = 0;
        let tcat_layout = self.tbl_mgr.get_layout("tblcat".to_string(), tx);
        let mut tcat = TableScan::new(tx, "tblcat".to_string(), tcat_layout.clone());
        while tcat.next(tx) {
            // get table name and its layout
            let tblname = tcat.get_string(tx, &"tblname".to_string());
            let layout = self.tbl_mgr.get_layout(tblname.clone(), tx);
            // recalculate the statistics of the table
            let si = self.calc_table_state(tblname.clone(), layout, tx);
            // insert the statistics into table_states
            self.table_states.insert(tblname.clone(), si);
        }
        tcat.close(tx);
    }

    /// caclulate the statistics of a table (at table name is tblname and layout is layout)
    fn calc_table_state(
        &mut self,
        tblname: String,
        layout: Layout,
        tx: &mut Transaction,
    ) -> StateInfo {
        let mut num_recs = 0;
        let mut num_blocks = 0;
        let mut ts = TableScan::new(tx, tblname.clone(), layout.clone());
        while ts.next(tx) {
            num_recs += 1;
            num_blocks = ts.get_rid().block_number() + 1;
        }
        ts.close(tx);
        return StateInfo::new(num_blocks, num_recs);
    }
}

#[derive(Debug, Clone, Hash)]
pub struct StateInfo {
    num_blocks: i32,
    num_recs: i32,
}

impl StateInfo {
    pub fn new(num_blocks: i32, num_recs: i32) -> Self {
        Self {
            num_blocks,
            num_recs,
        }
    }

    pub fn blocks_accessed(&self) -> i32 {
        self.num_blocks
    }

    pub fn records_output(&self) -> i32 {
        self.num_recs
    }

    pub fn distinct_values(&self, _fldname: &String) -> i32 {
        return 1 + (self.num_recs / 3); // this is wildly inaccurate!
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::fs;

    use crate::record_management::schema::Schema;
    use crate::SimpleDB;

    fn setup() {
        let db_directory = "./db".to_string();
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    #[test]
    fn test_state_mgr() -> Result<()> {
        setup();
        let db = SimpleDB::new("statmgrtest".to_string(), 400, 8);
        let mut tx = db.new_tx();
        let tm = TableMgr::new(true, &mut tx);
        let mut statmgr = StatMgr::new(tm.clone(), &mut tx);
        // tx.commit();

        // // create table
        let mut sch = Schema::new();
        sch.add_int_field("SID".to_string());
        sch.add_string_field("SName".to_string(), 10);
        sch.add_int_field("MajorId".to_string());
        tm.create_table("STUDENT".to_string(), sch, &mut tx);

        // // insert records
        let layout = tm.get_layout("STUDENT".to_string(), &mut tx);
        let mut ts = TableScan::new(&mut tx, "STUDENT".to_string(), layout.clone());
        ts.insert(&mut tx);
        ts.set_int(&mut tx, &"SID".to_string(), 1);
        ts.set_string(&mut tx, &"SName".to_string(), "Joe".to_string());
        ts.set_int(&mut tx, &"MajorId".to_string(), 20);
        ts.insert(&mut tx);
        ts.set_int(&mut tx, &"SID".to_string(), 2);
        ts.set_string(&mut tx, &"SName".to_string(), "Aoi".to_string());
        ts.set_int(&mut tx, &"MajorId".to_string(), 10);

        // // check statistics
        let si = statmgr.get_stat_info("STUDENT".to_string(), layout, &mut tx);
        assert_eq!(si.blocks_accessed(), 1);
        assert_eq!(si.records_output(), 2);
        assert_eq!(si.distinct_values(&"SID".to_string()), 1);

        ts.close(&mut tx);
        tx.commit();
        Ok(())
    }
}
