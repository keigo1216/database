use std::collections::HashSet;

use crate::common::Constant;
use crate::scans::common::Scan;
use crate::scans::common::ScanType;
use crate::transaction_manager::transaction::Transaction;

pub struct ProjectScan {
    s: Box<ScanType>,
    field_list: HashSet<String>,
}

impl ProjectScan {
    pub fn new(s: Box<ScanType>, field_list: HashSet<String>) -> Self {
        return Self { s, field_list };
    }
}

impl Scan for ProjectScan {
    fn before_first(&mut self, tx: &mut Transaction) {
        self.s.before_first(tx);
    }

    fn next(&mut self, tx: &mut Transaction) -> bool {
        self.s.next(tx)
    }

    fn get_int(&mut self, fldname: String, tx: &mut Transaction) -> i32 {
        if self.has_field(fldname.clone()) {
            self.s.get_int(fldname, tx)
        } else {
            panic!("field {} not found.", fldname);
        }
    }

    fn get_string(&mut self, fldname: String, tx: &mut Transaction) -> String {
        if self.has_field(fldname.clone()) {
            self.s.get_string(fldname, tx)
        } else {
            panic!("field {} not found.", fldname);
        }
    }

    fn get_val(&mut self, fldname: String, tx: &mut Transaction) -> Constant {
        if self.has_field(fldname.clone()) {
            self.s.get_val(fldname, tx)
        } else {
            panic!("field {} not found.", fldname);
        }
    }

    fn has_field(&self, fldname: String) -> bool {
        self.field_list.contains(&fldname)
    }

    fn close(&mut self, tx: &mut Transaction) {
        self.s.close(tx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record_management::schema::Schema;
    use crate::record_management::table_scan::TableScan;
    use crate::scans::predicate::Predicate;
    use crate::scans::select_scan::SelectScan;
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
    fn test_project_scan() -> Result<()> {
        setup();
        let db = SimpleDB::new("projectscantest".to_string(), 400, 10);
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

        // Q = project (select (STUDENT, MajorId = 10), {sname})

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
        let ss = SelectScan::new(Box::new(ScanType::TableScan(ts)), pred);

        // PROJECT node
        let mut field_list = HashSet::new();
        field_list.insert("sname".to_string());
        let mut ps = ProjectScan::new(Box::new(ScanType::SelectScan(ss)), field_list);

        ps.next(&mut tx);
        assert_eq!(
            ps.get_string("sname".to_string(), &mut tx),
            "joe".to_string()
        );
        ps.next(&mut tx);
        assert_eq!(
            ps.get_string("sname".to_string(), &mut tx),
            "Bob".to_string()
        );

        ps.close(&mut tx);
        tx.commit();

        teardown();
        Ok(())
    }
}
