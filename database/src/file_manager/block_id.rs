use std::collections::hash_map::DefaultHasher;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

#[derive(Clone, PartialEq, Debug)]
pub struct BlockId {
    filename: String,
    blknum: i32,
}

impl BlockId {
    pub fn new(filename: String, blknum: i32) -> Self {
        Self { filename, blknum }
    }

    pub fn filename(&self) -> String {
        self.filename.clone()
    }

    pub fn number(&self) -> i32 {
        self.blknum
    }

    // to do: change argument type to generics
    // to do: delete this method
    pub fn equals(&self, other: &Self) -> bool {
        self.filename == other.filename && self.blknum == other.blknum
    }

    // to do: delete this method
    pub fn to_string(&self) -> String {
        format!("[ file {} block {} ]", self.filename, self.blknum)
    }

    pub fn hash_code(&self) -> i32 {
        let mut hasher = DefaultHasher::new();
        self.to_string().hash(&mut hasher);
        hasher.finish() as i32
    }
}

impl Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[ file {} block {} ]", self.filename, self.blknum)
    }
}
