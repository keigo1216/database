use std::fmt::Display;

use crate::scans::common::ScanType;
use crate::scans::term::Term;
use crate::transaction_manager::transaction::Transaction;

#[derive(Clone)]
pub struct Predicate {
    terms: Vec<Term>,
}

impl Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        // append all terms with and
        for (i, t) in self.terms.iter().enumerate() {
            if i != 0 && i != self.terms.len() - 1 {
                s.push_str(" and ");
            }
            s.push_str(&format!("{}", t));
        }
        write!(f, "{}", s)
    }
}

impl Predicate {
    pub fn new() -> Self {
        return Self { terms: Vec::new() };
    }

    pub fn add_term(&mut self, t: Term) {
        self.terms.push(t);
    }

    pub fn conjoin_with(&mut self, p: Predicate) {
        self.terms.extend(p.terms);
    }

    pub fn is_satisfied(&self, s: &mut ScanType, tx: &mut Transaction) -> bool {
        for t in self.terms.iter() {
            if !t.is_satisfied(s, tx) {
                return false;
            }
        }
        return true;
    }
}
