pub mod integer {
    pub const BYTES: i32 = std::mem::size_of::<i32>() as i32;
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum Constant {
    Int(i32),
    String(String),
}
