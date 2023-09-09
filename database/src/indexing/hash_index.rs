#[allow(unused_imports)]
use std::collections::hash_map::DefaultHasher;
#[allow(unused_imports)]
use std::hash::{Hash, Hasher};

use crate::common::Constant;
use crate::indexing::index::Index;
use crate::record_management::rid::RID;
use crate::record_management::{layout::Layout, table_scan::TableScan};
use crate::transaction_manager::transaction::Transaction;

const NUM_BUCKETS: i32 = 100;

#[allow(dead_code)]
pub struct HashIndex {
    idxname: String,
    layout: Layout,
    search_key: Option<Constant>,
    ts: Option<TableScan>,
}

impl HashIndex {
    pub fn new(idxname: String, layout: Layout) -> Self {
        Self {
            idxname,
            layout,
            search_key: None,
            ts: None,
        }
    }

    pub fn search_cost(num_blocks: i32, _rpb: i32) -> i32 {
        return num_blocks / NUM_BUCKETS;
    }
}

#[allow(unused_variables)]
impl Index for HashIndex {
    fn before_first(&mut self, search_key: Constant, tx: &mut Transaction) {
        todo!("HashIndex.before_first()");
        // self.close(tx);
        // self.search_key = Some(search_key.clone());

        // // convert hash value
        // let mut hasher = DefaultHasher::new();
        // search_key.hash(&mut hasher);
        // let bucket = hasher.finish() % NUM_BUCKETS as u64;

        // // open the appropriate bucket
        // let tblname = self.idxname.clone() + bucket.to_string().as_str();
        // self.ts = Some(TableScan::new(tx, tblname, self.layout.clone()));
    }

    fn next(&mut self, tx: &mut Transaction) -> bool {
        todo!("HashIndex.next()");
        // match &mut self.ts {
        //     Some(ts) => {
        //         while ts.next(tx) {
        //             if ts.get_value(tx, &"dataval".to_string()) == self.search_key.clone().expect("HashIndex.next() called before before_first()") {
        //                 return true;
        //             }
        //         }
        //         return false;
        //     }
        //     None => {
        //         panic!("HashIndex.next() called before before_first()");
        //     }
        // }
    }

    fn get_data_rid(&mut self, tx: &mut Transaction) -> RID {
        todo!("HashIndex.get_data_rid()");
        // match self.ts.as_mut() {
        //     Some(ts) => {
        //         let blknum = ts.get_int(tx, &"block".to_string());
        //         let id = ts.get_int(tx, &"id".to_string());
        //         return RID::new(blknum, id);
        //     }
        //     None => {
        //         panic!("HashIndex.get_data_rid() called before before_first()");
        //     }
        // }
    }

    fn insert(&mut self, val: Constant, data_rid: RID, tx: &mut Transaction) {
        todo!("HashIndex.insert()");
        // self.before_first(val.clone(), tx); // go to the bucket
        // match self.ts.as_mut() {
        //     Some(ts) => {
        //         ts.insert(tx);
        //         ts.set_int(tx, &"block".to_string(), data_rid.block_number());
        //         ts.set_int(tx, &"id".to_string(), data_rid.slot_number());
        //         ts.set_value(tx, &"dataval".to_string(), val.clone());
        //     }
        //     None => {
        //         panic!("HashIndex.insert() called before before_first()");
        //     }
        // }
    }

    fn delete(&mut self, val: Constant, data_rid: RID, tx: &mut Transaction) {
        todo!("HashIndex.delete()");
        // self.before_first(val.clone(), tx);
        // while self.next(tx) {
        //     if self.get_data_rid(tx) == data_rid {
        //         self.ts.as_mut().expect("HashIndex.delete() called before before_first()").delete(tx);
        //         return;
        //     }
        // }
    }

    fn close(&mut self, tx: &mut Transaction) {
        todo!("HashIndex.close()");
        // if let Some(ts) = &mut self.ts {
        //     ts.close(tx);
        // }
    }
}
