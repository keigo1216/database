use std::collections::HashMap;

use crate::record_management::layout::Layout;
use crate::record_management::schema::Schema;
use crate::record_management::table_scan::TableScan;
use crate::transaction_manager::transaction::Transaction;

pub const MAX_NAME: i32 = 16;

/// Table Catalog
/// This table is for manages table data
/// tcat is a table for storing metadata of all tables
/// | TblName | SlotSize |
/// fcat is a table for storing metadata of all fields
/// | TblName | FieldName | Type | Length | Offset |
#[derive(Debug, Clone)]
pub struct TableMgr {
    tcat_layout: Layout,
    fcat_layout: Layout,
}

impl TableMgr {
    pub fn new(is_new: bool, tx: &mut Transaction) -> Self {
        // create tcat layout
        let mut tcat_schema = Schema::new();
        tcat_schema.add_string_field("tblname".to_string(), MAX_NAME);
        tcat_schema.add_int_field("slotsize".to_string());
        let tcat_layout = Layout::new_from_schema(tcat_schema.clone());

        let mut fcat_schema = Schema::new();
        fcat_schema.add_string_field("tblname".to_string(), MAX_NAME);
        fcat_schema.add_string_field("fldname".to_string(), MAX_NAME);
        fcat_schema.add_int_field("type".to_string());
        fcat_schema.add_int_field("length".to_string());
        fcat_schema.add_int_field("offset".to_string());
        let fcat_layout = Layout::new_from_schema(fcat_schema.clone());

        let table_mgr = Self {
            tcat_layout,
            fcat_layout,
        };

        if is_new {
            table_mgr.create_table("tblcat".to_string(), tcat_schema.clone(), tx);
            table_mgr.create_table("fldcat".to_string(), fcat_schema.clone(), tx);
        }

        return table_mgr;
    }

    pub fn create_table(&self, tblname: String, sch: Schema, tx: &mut Transaction) {
        let layout = Layout::new_from_schema(sch.clone());
        // insert one record into tblcat
        // | TblName | SlotSize |
        let mut tcat = TableScan::new(tx, "tblcat".to_string(), self.tcat_layout.clone());
        tcat.insert(tx);
        tcat.set_string(tx, &"tblname".to_string(), tblname.clone());
        tcat.set_int(tx, &"slotsize".to_string(), layout.slot_size());
        tcat.close(tx);

        // insert a record into fldcat for each field
        // | TblName | FieldName | Type | Length | Offset |
        let mut fcat = TableScan::new(tx, "fldcat".to_string(), self.fcat_layout.clone());
        fcat.insert(tx);
        for field_name in sch.get_fields().clone().iter() {
            fcat.set_string(tx, &"tblname".to_string(), tblname.clone());
            fcat.set_string(tx, &"fldname".to_string(), field_name.clone());
            fcat.set_int(tx, &"type".to_string(), sch.get_type_(field_name).into());
            fcat.set_int(tx, &"length".to_string(), sch.get_length(field_name));
            fcat.set_int(tx, &"offset".to_string(), layout.offset(field_name));
            fcat.insert(tx);
        }
        fcat.close(tx);
    }

    pub fn get_layout(&self, tblname: String, tx: &mut Transaction) -> Layout {
        let mut size = -1;
        // Get the size of record from tblcat table
        let mut tcat = TableScan::new(tx, "tblcat".to_string(), self.tcat_layout.clone());
        while tcat.next(tx) {
            if tcat.get_string(tx, &"tblname".to_string()) == tblname {
                // find the table
                size = tcat.get_int(tx, &"slotsize".to_string());
                break;
            }
        }
        tcat.close(tx);

        // Get the schema from fldcat table
        let mut sch = Schema::new();
        let mut offsets = HashMap::new();
        let mut fcat = TableScan::new(tx, "fldcat".to_string(), self.fcat_layout.clone());
        while fcat.next(tx) {
            if fcat.get_string(tx, &"tblname".to_string()) == tblname {
                // find the table
                let fldname = fcat.get_string(tx, &"fldname".to_string());
                let fldtype = fcat.get_int(tx, &"type".to_string());
                let fldlen = fcat.get_int(tx, &"length".to_string());
                let offset = fcat.get_int(tx, &"offset".to_string());
                sch.add_field(fldname.clone(), fldtype.into(), fldlen);
                offsets.insert(fldname, offset);
            }
        }
        fcat.close(tx);

        return Layout::new(sch, offsets, size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{record_management::schema::Schema, SimpleDB};
    use anyhow::Result;
    use std::fs;

    fn setup() {
        let db_directory = "./db".to_string();
        // delete db_directory if exists
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    fn teardown() {
        let db_directory = "./db".to_string();
        // delete db_directory if exists
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    #[test]
    pub fn test_table_mgr() -> Result<()> {
        setup();
        let db = SimpleDB::new("tblmgrtest".to_string(), 400, 8);
        let mut tx = db.new_tx();
        let tm = TableMgr::new(true, &mut tx);

        let mut sch = Schema::new();
        sch.add_int_field("A".to_string());
        sch.add_string_field("B".to_string(), 9);
        tm.create_table("MyTable".to_string(), sch.clone(), &mut tx);

        let layout = tm.get_layout("MyTable".to_string(), &mut tx);
        let size = layout.slot_size();
        assert_eq!(size, 4 + 4 + 4 + 9);
        let sch2 = layout.schema();
        assert_eq!(sch2.get_fields().len(), 2);
        for field_name in sch2.get_fields().iter() {
            assert_eq!(sch2.get_type_(field_name), sch.get_type_(field_name));
            assert_eq!(sch2.get_length(field_name), sch.get_length(field_name));
        }

        tx.commit();
        teardown();

        Ok(())
    }
}
