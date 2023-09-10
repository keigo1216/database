use crate::common::Constant;
use crate::record_management::rid::RID;

use crate::record_management::table_scan::TableScan;
use crate::scans::project_scan::ProjectScan;
use crate::scans::select_scan::SelectScan;
use crate::transaction_manager::transaction::Transaction;

use super::production_scan::ProductionScan;

pub trait Scan {
    fn before_first(&mut self, tx: &mut Transaction);
    fn next(&mut self, tx: &mut Transaction) -> bool;
    fn get_int(&mut self, fldname: String, tx: &mut Transaction) -> i32;
    fn get_string(&mut self, fldname: String, tx: &mut Transaction) -> String;
    fn get_val(&mut self, fldname: String, tx: &mut Transaction) -> Constant;
    fn has_field(&self, fldname: String) -> bool;
    fn close(&mut self, tx: &mut Transaction);
}

pub trait UpdateScan: Scan {
    fn set_int(&mut self, fldname: String, newval: i32, tx: &mut Transaction);
    fn set_string(&mut self, fldname: String, newval: String, tx: &mut Transaction);
    fn set_val(&mut self, fldname: String, newval: Constant, tx: &mut Transaction);
    fn delete(&mut self, tx: &mut Transaction);
    fn insert(&mut self, tx: &mut Transaction);
    fn get_rid(&self) -> RID;
    fn move_to_rid(&mut self, rid: RID, tx: &mut Transaction);
}

pub enum ScanType {
    SelectScan(SelectScan),
    ProjectScan(ProjectScan),
    ProductionScan(ProductionScan),
    TableScan(TableScan),
}

impl Scan for ScanType {
    fn before_first(&mut self, tx: &mut Transaction) {
        match self {
            ScanType::SelectScan(s) => s.before_first(tx),
            ScanType::ProjectScan(s) => s.before_first(tx),
            ScanType::ProductionScan(s) => s.before_first(tx),
            ScanType::TableScan(s) => s.before_first(tx),
        }
    }

    fn next(&mut self, tx: &mut Transaction) -> bool {
        match self {
            ScanType::SelectScan(s) => s.next(tx),
            ScanType::ProjectScan(s) => s.next(tx),
            ScanType::ProductionScan(s) => s.next(tx),
            ScanType::TableScan(s) => s.next(tx),
        }
    }

    fn get_int(&mut self, fldname: String, tx: &mut Transaction) -> i32 {
        match self {
            ScanType::SelectScan(s) => s.get_int(fldname, tx),
            ScanType::ProjectScan(s) => s.get_int(fldname, tx),
            ScanType::ProductionScan(s) => s.get_int(fldname, tx),
            ScanType::TableScan(s) => s.get_int(tx, &fldname),
        }
    }

    fn get_string(&mut self, fldname: String, tx: &mut Transaction) -> String {
        match self {
            ScanType::SelectScan(s) => s.get_string(fldname, tx),
            ScanType::ProjectScan(s) => s.get_string(fldname, tx),
            ScanType::ProductionScan(s) => s.get_string(fldname, tx),
            ScanType::TableScan(s) => s.get_string(tx, &fldname),
        }
    }

    fn get_val(&mut self, fldname: String, tx: &mut Transaction) -> Constant {
        match self {
            ScanType::SelectScan(s) => s.get_val(fldname, tx),
            ScanType::ProjectScan(s) => s.get_val(fldname, tx),
            ScanType::ProductionScan(s) => s.get_val(fldname, tx),
            ScanType::TableScan(s) => s.get_value(tx, &fldname),
        }
    }

    fn has_field(&self, fldname: String) -> bool {
        match self {
            ScanType::SelectScan(s) => s.has_field(fldname),
            ScanType::ProjectScan(s) => s.has_field(fldname),
            ScanType::ProductionScan(s) => s.has_field(fldname),
            ScanType::TableScan(s) => s.has_field(&fldname),
        }
    }

    fn close(&mut self, tx: &mut Transaction) {
        match self {
            ScanType::SelectScan(s) => s.close(tx),
            ScanType::ProjectScan(s) => s.close(tx),
            ScanType::ProductionScan(s) => s.close(tx),
            ScanType::TableScan(s) => s.close(tx),
        }
    }
}

impl UpdateScan for ScanType {
    fn set_int(&mut self, fldname: String, newval: i32, tx: &mut Transaction) {
        match self {
            ScanType::SelectScan(s) => s.set_int(fldname, newval, tx),
            ScanType::TableScan(s) => s.set_int(tx, &fldname, newval),
            _ => panic!("not implemented UpdateScan::set_int"),
        }
    }

    fn set_string(&mut self, fldname: String, newval: String, tx: &mut Transaction) {
        match self {
            ScanType::SelectScan(s) => s.set_string(fldname, newval, tx),
            ScanType::TableScan(s) => s.set_string(tx, &fldname, newval),
            _ => panic!("not implemented UpdateScan::set_string"),
        }
    }

    fn set_val(&mut self, fldname: String, newval: Constant, tx: &mut Transaction) {
        match self {
            ScanType::SelectScan(s) => s.set_val(fldname, newval, tx),
            ScanType::TableScan(s) => s.set_value(tx, &fldname, newval),
            _ => panic!("not implemented UpdateScan::set_val"),
        }
    }

    fn delete(&mut self, tx: &mut Transaction) {
        match self {
            ScanType::SelectScan(s) => s.delete(tx),
            ScanType::TableScan(s) => s.delete(tx),
            _ => panic!("not implemented UpdateScan::delete"),
        }
    }

    fn insert(&mut self, tx: &mut Transaction) {
        match self {
            ScanType::SelectScan(s) => s.insert(tx),
            ScanType::TableScan(s) => s.insert(tx),
            _ => panic!("not implemented UpdateScan::insert"),
        }
    }

    fn get_rid(&self) -> RID {
        match self {
            ScanType::SelectScan(s) => s.get_rid(),
            ScanType::TableScan(s) => s.get_rid(),
            _ => panic!("not implemented UpdateScan::get_rid"),
        }
    }

    fn move_to_rid(&mut self, rid: RID, tx: &mut Transaction) {
        match self {
            ScanType::SelectScan(s) => s.move_to_rid(rid, tx),
            ScanType::TableScan(s) => s.move_to_rid(tx, rid),
            _ => panic!("not implemented UpdateScan::move_to_rid"),
        }
    }
}
