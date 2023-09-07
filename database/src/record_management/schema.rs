use std::collections::HashMap;

#[derive(Debug, Hash, Clone)]
struct FieldInfo {
    type_: i32,
    length: i32,
}

impl FieldInfo {
    fn new(type_: i32, length: i32) -> Self {
        Self { type_, length }
    }
}

pub enum Type {
    INTEGER,
    VARCHAR,
}

impl Into<i32> for Type {
    fn into(self) -> i32 {
        match self {
            Type::INTEGER => 4,
            Type::VARCHAR => 12,
        }
    }
}

impl Into<Type> for i32 {
    fn into(self) -> Type {
        match self {
            4 => Type::INTEGER,
            12 => Type::VARCHAR,
            _ => panic!("invalid type"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FieldInfo>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            info: HashMap::new(),
        }
    }

    pub fn add_field(&mut self, field_name: String, type_: i32, length: i32) {
        self.fields.push(field_name.clone());
        self.info.insert(field_name, FieldInfo::new(type_, length));
    }

    pub fn add_int_field(&mut self, field_name: String) {
        self.add_field(field_name, Type::INTEGER.into(), 0);
    }

    pub fn add_string_field(&mut self, field_name: String, length: i32) {
        self.add_field(field_name, Type::VARCHAR.into(), length);
    }

    pub fn add(&mut self, field_name: String, sch: Self) {
        let type_ = sch.get_type_(&field_name);
        let length = sch.get_length(&field_name);
        self.add_field(field_name, type_, length);
    }

    pub fn add_all(&mut self, sch: Self) {
        for field_name in sch.fields.iter() {
            self.add(field_name.clone(), sch.clone());
        }
    }

    pub fn get_fields(&self) -> Vec<String> {
        self.fields.clone()
    }

    pub fn has_field(&self, field_name: &String) -> bool {
        self.fields.contains(field_name)
    }

    pub fn get_type_(&self, field_name: &String) -> i32 {
        match self.info.get(field_name) {
            Some(info) => info.type_,
            None => panic!("field {} not found", field_name),
        }
    }

    pub fn get_length(&self, field_name: &String) -> i32 {
        match self.info.get(field_name) {
            Some(info) => info.length,
            None => panic!("field {} not found", field_name),
        }
    }
}
