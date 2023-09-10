use std::fmt::Display;

pub mod integer {
    pub const BYTES: i32 = std::mem::size_of::<i32>() as i32;
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum Constant {
    Int(i32),
    String(String),
}

impl Display for Constant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Constant::Int(i) => write!(f, "{}", i),
            Constant::String(s) => write!(f, "{}", s),
        }
    }
}
