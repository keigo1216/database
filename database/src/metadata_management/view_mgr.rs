use crate::metadata_management::table_mgr::{TableMgr, MAX_NAME};
use crate::record_management::schema::Schema;
use crate::record_management::table_scan::TableScan;
use crate::transaction_manager::transaction::Transaction;

pub const MAX_VIEWDEF: i32 = 100; // max view def chars

pub struct ViewMgr {
    tbl_mgr: TableMgr,
}

impl ViewMgr {
    pub fn new(is_new: bool, tbl_mgr: TableMgr, tx: &mut Transaction) -> Self {
        if is_new {
            let mut sch = Schema::new();
            sch.add_string_field("viewname".to_string(), MAX_NAME);
            sch.add_string_field("viewdef".to_string(), MAX_VIEWDEF);
            tbl_mgr.create_table("viewcat".to_string(), sch, tx);
        }
        Self { tbl_mgr }
    }

    pub fn create_view(&self, vname: String, vdef: String, tx: &mut Transaction) {
        let layout = self.tbl_mgr.get_layout("viewcat".to_string(), tx);
        let mut ts = TableScan::new(tx, "viewcat".to_string(), layout);
        ts.insert(tx);
        ts.set_string(tx, &"viewname".to_string(), vname);
        ts.set_string(tx, &"viewdef".to_string(), vdef);
        ts.close(tx);
    }

    pub fn get_view_def(&self, vname: String, tx: &mut Transaction) -> Option<String> {
        let layout = self.tbl_mgr.get_layout("viewcat".to_string(), tx);
        let mut ts = TableScan::new(tx, "viewcat".to_string(), layout);
        let mut result = None;
        while ts.next(tx) {
            if ts.get_string(tx, &"viewname".to_string()) == vname {
                let viewdef = ts.get_string(tx, &"viewdef".to_string());
                result = Some(viewdef);
            }
        }
        ts.close(tx);
        return result;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimpleDB;
    use anyhow::Result;

    #[test]
    fn test_view_mgr() -> Result<()> {
        let db = SimpleDB::new("viewmgrtest".to_string(), 400, 8);
        let mut tx = db.new_tx();
        let tm = TableMgr::new(true, &mut tx);

        let vm = ViewMgr::new(true, tm, &mut tx);
        // check layout
        let layout = vm.tbl_mgr.get_layout("viewcat".to_string(), &mut tx);
        assert_eq!(
            layout.schema().get_length(&"viewname".to_string()),
            MAX_NAME
        );
        assert_eq!(
            layout.schema().get_length(&"viewdef".to_string()),
            MAX_VIEWDEF
        );

        // Test create_view
        vm.create_view(
            "view1".to_string(),
            "select A from MyTable".to_string(),
            &mut tx,
        );
        vm.create_view(
            "view2".to_string(),
            "select B from MyTable".to_string(),
            &mut tx,
        );
        assert_eq!(
            vm.get_view_def("view1".to_string(), &mut tx),
            Some("select A from MyTable".to_string())
        );
        assert_eq!(
            vm.get_view_def("view2".to_string(), &mut tx),
            Some("select B from MyTable".to_string())
        );

        Ok(())
    }
}
