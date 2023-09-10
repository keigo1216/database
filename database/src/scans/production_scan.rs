use crate::scans::common::Scan;
use crate::scans::common::ScanType;
use crate::transaction_manager::transaction::Transaction;

pub struct ProductionScan {
    s1: Box<ScanType>,
    s2: Box<ScanType>,
}

impl ProductionScan {
    pub fn new(mut s1: Box<ScanType>, s2: Box<ScanType>, tx: &mut Transaction) -> Self {
        s1.next(tx);
        return Self { s1, s2 };
    }
}

impl Scan for ProductionScan {
    fn before_first(&mut self, tx: &mut Transaction) {
        self.s1.before_first(tx);
        self.s1.next(tx);
        self.s2.before_first(tx);
    }

    fn next(&mut self, tx: &mut Transaction) -> bool {
        if self.s2.next(tx) {
            return true;
        } else {
            self.s2.before_first(tx);
            return self.s2.next(tx) && self.s1.next(tx);
        }
    }

    fn get_int(&mut self, fldname: String, tx: &mut Transaction) -> i32 {
        if self.s1.has_field(fldname.clone()) {
            println!("Hello");
            self.s1.get_int(fldname, tx)
        } else {
            self.s2.get_int(fldname, tx)
        }
    }

    fn get_string(&mut self, fldname: String, tx: &mut Transaction) -> String {
        if self.s1.has_field(fldname.clone()) {
            self.s1.get_string(fldname, tx)
        } else {
            self.s2.get_string(fldname, tx)
        }
    }

    fn get_val(&mut self, fldname: String, tx: &mut Transaction) -> crate::common::Constant {
        if self.s1.has_field(fldname.clone()) {
            self.s1.get_val(fldname, tx)
        } else {
            self.s2.get_val(fldname, tx)
        }
    }

    fn has_field(&self, fldname: String) -> bool {
        self.s1.has_field(fldname.clone()) || self.s2.has_field(fldname)
    }

    fn close(&mut self, tx: &mut Transaction) {
        self.s1.close(tx);
        self.s2.close(tx);
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
    fn test_production_scan() -> Result<()> {
        setup();
        let db = SimpleDB::new("productionscantest".to_string(), 400, 10);
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

        // create table
        let mut sch = Schema::new();
        sch.add_int_field("dId".to_string());
        sch.add_string_field("dname".to_string(), 10);
        mdm.create_table("dept".to_string(), sch, &mut tx);

        // insert data
        let layout = mdm.get_layout("dept".to_string(), &mut tx);
        let mut ts = TableScan::new(&mut tx, "dept".to_string(), layout.clone());
        ts.insert(&mut tx);
        ts.set_int(&mut tx, &"dId".to_string(), 10);
        ts.set_string(&mut tx, &"dname".to_string(), "CS".to_string());
        ts.insert(&mut tx);
        ts.set_int(&mut tx, &"dId".to_string(), 20);
        ts.set_string(&mut tx, &"dname".to_string(), "EE".to_string());

        // Q = select (product (STUDENT, DEPT), MajorId = dId)

        // STUDENT and DEPT node
        let st_layout = mdm.get_layout("student".to_string(), &mut tx);
        let st_ts = TableScan::new(&mut tx, "student".to_string(), st_layout.clone());
        let de_layout = mdm.get_layout("dept".to_string(), &mut tx);
        let de_ts = TableScan::new(&mut tx, "dept".to_string(), de_layout.clone());

        // product node
        let prod = ProductionScan::new(
            Box::new(ScanType::TableScan(st_ts)),
            Box::new(ScanType::TableScan(de_ts)),
            &mut tx,
        );

        // MajorId = dId
        let mut pred = Predicate::new();
        let lhs = Expression::new_from_fldname("majorId".to_string());
        let rhs = Expression::new_from_fldname("dId".to_string());
        let t = Term::new(lhs, rhs);
        pred.add_term(t);
        let mut ss = SelectScan::new(Box::new(ScanType::ProductionScan(prod)), pred);

        ss.next(&mut tx);
        assert_eq!(ss.get_int("sid".to_string(), &mut tx), 1);
        assert_eq!(
            ss.get_string("sname".to_string(), &mut tx),
            "joe".to_string()
        );
        assert_eq!(ss.get_int("majorId".to_string(), &mut tx), 10);
        assert_eq!(ss.get_int("dId".to_string(), &mut tx), 10);
        assert_eq!(
            ss.get_string("dname".to_string(), &mut tx),
            "CS".to_string()
        );

        ss.next(&mut tx);
        assert_eq!(ss.get_int("sid".to_string(), &mut tx), 2);
        assert_eq!(
            ss.get_string("sname".to_string(), &mut tx),
            "Aoi".to_string()
        );
        assert_eq!(ss.get_int("majorId".to_string(), &mut tx), 20);
        assert_eq!(ss.get_int("dId".to_string(), &mut tx), 20);
        assert_eq!(
            ss.get_string("dname".to_string(), &mut tx),
            "EE".to_string()
        );

        ss.close(&mut tx);
        tx.commit();

        teardown();
        Ok(())
    }
}
