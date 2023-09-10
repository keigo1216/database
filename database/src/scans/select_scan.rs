use crate::scans::common::ScanType;
use crate::scans::predicate::Predicate;

use crate::common::Constant;
use crate::record_management::rid::RID;
use crate::scans::common::Scan;
use crate::scans::common::UpdateScan;
use crate::transaction_manager::transaction::Transaction;

pub struct SelectScan {
    s: Box<ScanType>,
    pred: Predicate,
}

impl SelectScan {
    pub fn new(s: Box<ScanType>, pred: Predicate) -> Self {
        return Self { s, pred };
    }

    pub fn get_scan_type(&self) -> &ScanType {
        return &self.s;
    }
}

impl Scan for SelectScan {
    fn before_first(&mut self, tx: &mut Transaction) {
        self.s.before_first(tx);
    }

    fn next(&mut self, tx: &mut Transaction) -> bool {
        while self.s.next(tx) {
            let pred = self.pred.clone();
            if pred.is_satisfied(&mut self.s, tx) {
                return true;
            }
        }
        return false;
    }

    fn get_int(&mut self, fldname: String, tx: &mut Transaction) -> i32 {
        self.s.get_int(fldname, tx)
    }

    fn get_string(&mut self, fldname: String, tx: &mut Transaction) -> String {
        self.s.get_string(fldname, tx)
    }

    fn get_val(&mut self, fldname: String, tx: &mut Transaction) -> crate::common::Constant {
        self.s.get_val(fldname, tx)
    }

    fn has_field(&self, fldname: String) -> bool {
        self.s.has_field(fldname)
    }

    fn close(&mut self, tx: &mut Transaction) {
        self.s.close(tx);
    }
}

impl UpdateScan for SelectScan {
    fn set_int(&mut self, fldname: String, newval: i32, tx: &mut Transaction) {
        self.s.set_int(fldname, newval, tx);
    }

    fn set_string(&mut self, fldname: String, newval: String, tx: &mut Transaction) {
        self.s.set_string(fldname, newval, tx);
    }

    fn set_val(&mut self, fldname: String, newval: Constant, tx: &mut Transaction) {
        self.s.set_val(fldname, newval, tx);
    }

    fn delete(&mut self, tx: &mut Transaction) {
        self.s.delete(tx);
    }

    fn insert(&mut self, tx: &mut Transaction) {
        self.s.insert(tx)
    }

    fn get_rid(&self) -> RID {
        self.s.get_rid()
    }

    fn move_to_rid(&mut self, rid: RID, tx: &mut Transaction) {
        self.s.move_to_rid(rid, tx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record_management::schema::Schema;
    use crate::record_management::table_scan::TableScan;
    use crate::scans::{expression::Expression, term::Term};
    use crate::SimpleDB;
    use anyhow::{Ok, Result};
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
    fn test_select_scan() -> Result<()> {
        setup();
        let db = SimpleDB::new("selectscantest".to_string(), 400, 10);
        let mut tx = db.new_tx();
        let mut mdm = db.new_metadata_mgr(&mut tx);

        // create table
        let mut sch = Schema::new();
        sch.add_int_field("sid".to_string());
        sch.add_string_field("sname".to_string(), 10);
        sch.add_int_field("majorId".to_string());
        mdm.create_table("student".to_string(), sch, &mut tx);

        // insert data
        let layout = mdm.get_layout("student".to_string(), &mut tx);
        let mut ts = TableScan::new(&mut tx, "student".to_string(), layout.clone());
        ts.insert(&mut tx);
        ts.set_int(&mut tx, &"sid".to_string(), 1);
        ts.set_string(&mut tx, &"sname".to_string(), "joe".to_string());
        ts.set_int(&mut tx, &"majorId".to_string(), 10);
        ts.insert(&mut tx);
        ts.set_int(&mut tx, &"sid".to_string(), 2);
        ts.set_string(&mut tx, &"sname".to_string(), "Aoi".to_string());
        ts.set_int(&mut tx, &"majorId".to_string(), 20);
        ts.insert(&mut tx);
        ts.set_int(&mut tx, &"sid".to_string(), 3);
        ts.set_string(&mut tx, &"sname".to_string(), "Bob".to_string());
        ts.set_int(&mut tx, &"majorId".to_string(), 10);
        ts.close(&mut tx);

        // Q = select (STUDENT, MajorId = 10)

        // STUDENT node
        let layout = mdm.get_layout("student".to_string(), &mut tx);
        let ts = TableScan::new(&mut tx, "student".to_string(), layout.clone());

        // SELECT node
        let mut pred = Predicate::new();
        let lhs = Expression::new_from_fldname("majorId".to_string());
        let c = Constant::Int(10);
        let rhs = Expression::new_from_val(c);
        let t = Term::new(lhs, rhs);
        pred.add_term(t);
        let mut ss = SelectScan::new(Box::new(ScanType::TableScan(ts)), pred);

        ss.next(&mut tx);
        assert_eq!(ss.get_int("sid".to_string(), &mut tx), 1);
        ss.next(&mut tx);
        assert_eq!(ss.get_int("sid".to_string(), &mut tx), 3);

        ss.close(&mut tx);
        tx.commit();

        teardown();
        Ok(())
    }
}
