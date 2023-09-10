use std::fmt::Display;

use crate::common::Constant;
use crate::record_management::schema::Schema;
use crate::scans::common::ScanType;
use crate::transaction_manager::transaction::Transaction;

use super::common::Scan;

#[derive(Clone)]
pub struct Expression {
    val: Option<Constant>,
    fldname: Option<String>,
}

impl Expression {
    pub fn new_from_val(val: Constant) -> Self {
        return Self {
            val: Some(val),
            fldname: None,
        };
    }

    pub fn new_from_fldname(fldname: String) -> Self {
        return Self {
            val: None,
            fldname: Some(fldname),
        };
    }

    pub fn is_field_name(&self) -> bool {
        return self.fldname.is_some();
    }

    pub fn as_constant(&self) -> Option<Constant> {
        return self.val.clone();
    }

    pub fn as_field_name(&self) -> Option<String> {
        return self.fldname.clone();
    }

    pub fn evaluate(&self, s: &mut ScanType, tx: &mut Transaction) -> Constant {
        match (&self.val.clone(), &self.fldname) {
            (Some(v), None) => {
                return v.clone();
            }
            (None, Some(f)) => {
                return s.get_val(f.clone(), tx);
            }
            _ => {
                panic!("Expression is malformed.");
            }
        }
    }

    pub fn applies_to(&self, sch: &Schema) -> bool {
        match (&self.val.clone(), &self.fldname) {
            (Some(_), None) => {
                return true;
            }
            (None, Some(f)) => {
                return sch.has_field(&f);
            }
            _ => {
                panic!("Expression is malformed.");
            }
        }
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.val.clone(), &self.fldname) {
            (Some(v), None) => {
                return write!(f, "{}", v);
            }
            (None, Some(fname)) => {
                return write!(f, "{}", fname);
            }
            _ => {
                panic!("Expression is malformed.");
            }
        }
    }
}
