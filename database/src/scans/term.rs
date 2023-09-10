use std::fmt::Display;

use crate::{
    common::Constant, planning::plan::Plan, record_management::schema::Schema,
    scans::expression::Expression, transaction_manager::transaction::Transaction,
};

use super::common::ScanType;

#[derive(Clone)]
pub struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.lhs, self.rhs)
    }
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        return Self { lhs, rhs };
    }

    pub fn is_satisfied(&self, s: &mut ScanType, tx: &mut Transaction) -> bool {
        let lhs_val = self.lhs.evaluate(s, tx);
        let rhs_val = self.rhs.evaluate(s, tx);
        return lhs_val == rhs_val;
    }

    pub fn applies_to(&self, sch: &Schema) -> bool {
        return self.lhs.applies_to(sch) && self.rhs.applies_to(sch);
    }

    pub fn reduction_factor(&self, _p: Plan) {
        todo!("Term.reduction_factor()")
    }

    pub fn equates_with_constant(&self, fldname: String) -> Option<Constant> {
        if self.lhs.is_field_name()
            && self.lhs.as_field_name().unwrap() == fldname
            && !self.rhs.is_field_name()
        {
            return self.rhs.as_constant();
        } else if self.rhs.is_field_name()
            && self.rhs.as_field_name().unwrap() == fldname
            && !self.lhs.is_field_name()
        {
            return self.lhs.as_constant();
        } else {
            None
        }
    }

    pub fn equates_with_field(&self, fldname: String) -> Option<String> {
        if self.lhs.is_field_name()
            && self.lhs.as_field_name().unwrap() == fldname
            && !self.rhs.is_field_name()
        {
            return self.rhs.as_field_name();
        } else if self.rhs.is_field_name()
            && self.rhs.as_field_name().unwrap() == fldname
            && !self.lhs.is_field_name()
        {
            return self.lhs.as_field_name();
        } else {
            None
        }
    }
}
