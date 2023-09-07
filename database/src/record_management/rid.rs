use std::fmt::Display;

#[derive(PartialEq)]
pub struct RID {
    blknum: i32,
    slot: i32,
}

impl RID {
    pub fn new(blknum: i32, slot: i32) -> Self {
        Self { blknum, slot }
    }

    pub fn block_number(&self) -> i32 {
        self.blknum
    }

    pub fn slot_number(&self) -> i32 {
        self.slot
    }
}

impl Display for RID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}, {}]", self.blknum, self.slot)
    }
}
