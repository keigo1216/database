use std::collections::HashMap;

use crate::common::integer;
use crate::file_manager::page::Page;
use crate::record_management::schema::Schema;

use super::schema::Type;

/// Create a instance for each database table
/// A layout is a collection of offsets for each field in a record
#[derive(Clone, Debug)]
pub struct Layout {
    schema: Schema,
    offsets: HashMap<String, i32>,
    slot_size: i32,
}

impl Layout {
    pub fn new(schema: Schema, offsets: HashMap<String, i32>, slot_size: i32) -> Self {
        Self {
            schema,
            offsets,
            slot_size,
        }
    }

    /// create a new layout from schema
    /// For example -
    /// Record | empty/inuse flag | field 1 (int) | field 2 (int) | field 3 (int) | ...
    /// Bytes  | 1 byte           | 4 bytes       | 4 bytes       | 4 bytes       | ...
    /// Offset | 0                | 4             | 8             | 12            | ...
    pub fn new_from_schema(schema: Schema) -> Self {
        let mut offsets = HashMap::new();
        let mut pos = integer::BYTES; // space for the empty / inuse flag
        for field_name in schema.get_fields().iter() {
            // set offset for each field
            offsets.insert(field_name.to_string(), pos);
            pos += Self::length_in_bytes(&schema, field_name);
        }
        Self {
            schema,
            offsets,
            slot_size: pos,
        }
    }

    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    pub fn offset(&self, field_name: &String) -> i32 {
        match self.offsets.get(field_name) {
            Some(offset) => *offset,
            None => panic!("invalid field name"),
        }
    }

    pub fn slot_size(&self) -> i32 {
        self.slot_size
    }

    fn length_in_bytes(schema: &Schema, field_name: &String) -> i32 {
        let field_type: Type = schema.get_type_(field_name).into();
        match field_type {
            Type::INTEGER => integer::BYTES,
            Type::VARCHAR => Page::max_length(schema.get_length(field_name)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record_management::schema::Schema;
    use anyhow::Result;
    use std::fs;

    fn setup(db_directory: String) {
        // delete db_deirectory if exists
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    fn teardown(db_directory: String) {
        // delete db_deirectory if exists
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    #[test]
    fn test_layout() -> Result<()> {
        let db_directory = "./db/layouttest".to_string();
        setup(db_directory.clone());
        let mut sch = Schema::new();
        sch.add_int_field("A".to_string());
        sch.add_string_field("B".to_string(), 9);
        let layout = Layout::new_from_schema(sch.clone());

        // check offset and slot size
        assert_eq!(layout.offset(&"A".to_string()), integer::BYTES);
        assert_eq!(
            layout.offset(&"B".to_string()),
            integer::BYTES + integer::BYTES
        );
        assert_eq!(
            layout.slot_size(),
            integer::BYTES + integer::BYTES + Page::max_length(9)
        );

        teardown(db_directory.clone());
        Ok(())
    }
}
