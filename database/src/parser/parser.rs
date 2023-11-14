use crate::parser::tokenize::Lexer;
use crate::parser::tokenize::{Reserved, TokenKind};
// use crate::scans::expression::Expression;
// use crate::scans::predicate::Predicate;
use crate::record_management::schema::{Schema, Type};
use crate::record_management::table_scan::TableScan;
use crate::scans::common::{Scan, ScanType};
use crate::scans::production_scan::ProductionScan;
use crate::SimpleDB;
// use crate::scans::select_scan::SelectScan;

#[derive(Debug)]
pub enum Object {
    CreateTable(CreateTableData),
    Insert(InsertData),
    Query(QueryData),
}

#[derive(Debug)]
pub struct CreateTableData {
    pub tblname: String,
    pub schema: Schema,
}

#[derive(Debug)]
pub struct InsertData {
    pub tblname: String,
    pub flds: Vec<String>,
    pub vals: Vec<String>,
}

#[derive(Debug)]
pub struct QueryData {
    pub fields: QueryFields,
    pub tables: Vec<String>,
}

#[derive(Debug)]
pub enum QueryFields {
    AllFields,
    Fields(Vec<String>),
}

pub trait Execute {
    fn execute(&mut self, db: &SimpleDB);
}

impl Execute for Object {
    fn execute(&mut self, db: &SimpleDB) {
        match self {
            Object::CreateTable(d) => {
                d.execute(db);
            }
            Object::Insert(d) => {
                d.execute(db);
            }
            Object::Query(d) => {
                d.execute(db);
            }
        }
    }
}

impl Execute for CreateTableData {
    fn execute(&mut self, db: &SimpleDB) {
        let mut tx = db.new_tx(); // new transaction
        let mut mdm = db.new_metadata_mgr(&mut tx);
        mdm.create_table(self.tblname.clone(), self.schema.clone(), &mut tx);
        tx.commit();
    }
}

impl Execute for InsertData {
    fn execute(&mut self, db: &SimpleDB) {
        let mut tx = db.new_tx(); // new transaction
        let mut mdm = db.new_metadata_mgr(&mut tx);
        let layout = mdm.get_layout(self.tblname.clone(), &mut tx);
        let mut ts = TableScan::new(&mut tx, self.tblname.clone(), layout.clone());
        ts.insert(&mut tx);
        for (i, fldname) in self.flds.iter().enumerate() {
            if !ts.has_field(fldname) {
                panic!("field {} not found.", fldname);
            }
            match layout.schema().get_type_(fldname).into() {
                Type::INTEGER => {
                    let val = self.vals[i].parse::<i32>();
                    match val {
                        Ok(v) => {
                            ts.set_int(&mut tx, fldname, v);
                        }
                        Err(_) => {
                            panic!("value {} is not integer.", self.vals[i]);
                        }
                    }
                }
                Type::VARCHAR => {
                    ts.set_string(&mut tx, fldname, self.vals[i].clone());
                }
            }
        }
        ts.close(&mut tx);
        tx.commit();
    }
}

impl Execute for QueryData {
    fn execute(&mut self, db: &SimpleDB) {
        let mut tx = db.new_tx(); // new transaction
        let mut mdm = db.new_metadata_mgr(&mut tx);
        let mut schema = Schema::new();

        let mut scan = {
            let layout = mdm.get_layout(self.tables[0].clone(), &mut tx);
            schema.add_all(layout.schema().clone());
            let ts = Box::new(ScanType::TableScan(TableScan::new(
                &mut tx,
                self.tables[0].clone(),
                layout.clone(),
            )));
            ts
        };

        for tblname in self.tables.iter().skip(1) {
            let layout = mdm.get_layout(tblname.clone(), &mut tx);
            schema.add_all(layout.schema().clone());
            let ts = Box::new(ScanType::TableScan(TableScan::new(
                &mut tx,
                tblname.clone(),
                layout.clone(),
            )));
            scan = Box::new(ScanType::ProductionScan(ProductionScan::new(
                scan, ts, &mut tx,
            )));
        }

        let mut select_scan = match self.fields {
            QueryFields::AllFields => scan,
            QueryFields::Fields(_) => {
                todo!("select fields not implemented.");
            }
        };

        // print row
        while select_scan.next(&mut tx) {
            for fldname in schema.get_fields() {
                let val = select_scan.get_val(fldname.clone(), &mut tx);
                print!("{} ", val);
            }
            println!();
        }
    }
}

pub struct Parser {
    lex: Lexer,
}

impl Parser {
    pub fn new(s: String) -> Self {
        Self { lex: Lexer::new(s) }
    }

    pub fn field(&mut self) -> String {
        self.lex.eat_id()
    }

    /// < Constant > ::= StrTok
    fn constant(&mut self) -> String {
        return self.lex.eat_id();
    }

    // <Sql> ::= <Updatecmd> | <Query>
    pub fn sql(mut self) -> Object {
        if self
            .lex
            .match_keyword(TokenKind::RESERVED(Reserved::SELECT))
        {
            return Object::Query(self.query());
        } else {
            return self.update_cmd();
        }
    }

    /// < UpdateCmd > ::= <Insert> | <Create> | ..
    fn update_cmd(&mut self) -> Object {
        if self
            .lex
            .match_keyword(TokenKind::RESERVED(Reserved::INSERT))
        {
            return Object::Insert(self.insert());
        } else if self
            .lex
            .match_keyword(TokenKind::RESERVED(Reserved::CREATE))
        {
            return self.create();
        } else {
            todo!("update_cmd not implemented.");
        }
    }

    /// < Create > ::= <CreateTable> | ..
    fn create(&mut self) -> Object {
        self.lex.eat_keyword(TokenKind::RESERVED(Reserved::CREATE));
        if self.lex.match_keyword(TokenKind::RESERVED(Reserved::TABLE)) {
            let d = self.create_table();
            return Object::CreateTable(d);
        } else {
            todo!("create not implemented.");
        }
    }

    fn insert(&mut self) -> InsertData {
        self.lex.eat_keyword(TokenKind::RESERVED(Reserved::INSERT));
        self.lex.eat_keyword(TokenKind::RESERVED(Reserved::INTO));
        let tblname = self.lex.eat_id();
        self.lex.eat_keyword(TokenKind::LPAR);
        let flds = self.field_list();
        self.lex.eat_keyword(TokenKind::RPAR);
        self.lex.eat_keyword(TokenKind::RESERVED(Reserved::VALUES));
        self.lex.eat_keyword(TokenKind::LPAR);
        let vals = self.const_list();
        self.lex.eat_keyword(TokenKind::RPAR);
        return InsertData {
            tblname,
            flds,
            vals,
        };
    }

    /// < FieldList > ::= < Field > [, < FieldList >]
    fn field_list(&mut self) -> Vec<String> {
        let mut l: Vec<String> = Vec::new();
        l.push(self.field());
        if self.lex.match_keyword(TokenKind::COMMA) {
            self.lex.eat_keyword(TokenKind::COMMA);
            l.extend(self.field_list());
        }
        return l;
    }

    /// < ConstList > ::= < Constant > [, < ConstList > ]
    fn const_list(&mut self) -> Vec<String> {
        let mut l: Vec<String> = Vec::new();
        l.push(self.constant());
        if self.lex.match_keyword(TokenKind::COMMA) {
            self.lex.eat_keyword(TokenKind::COMMA);
            l.extend(self.const_list());
        }
        return l;
    }

    /// <CreateTable> ::= CREATE TABLE IdTok ( <FieldDefs> )
    fn create_table(&mut self) -> CreateTableData {
        self.lex.eat_keyword(TokenKind::RESERVED(Reserved::TABLE));
        let tblname = self.lex.eat_id();
        self.lex.eat_keyword(TokenKind::LPAR);
        let schema = self.field_defs();
        self.lex.eat_keyword(TokenKind::RPAR);
        return CreateTableData { tblname, schema };
    }

    /// < FieldDefs > ::= < FieldDef > [, < FieldDefs >]
    pub fn field_defs(&mut self) -> Schema {
        let mut schema = self.field_def();
        if self.lex.match_keyword(TokenKind::COMMA) {
            self.lex.eat_keyword(TokenKind::COMMA);
            let schema2 = self.field_defs();
            schema.add_all(schema2);
        }
        schema
    }

    /// < FieldDef > ::= IdTok < FieldType >
    pub fn field_def(&mut self) -> Schema {
        let fldname = self.lex.eat_id();
        let schema = self.field_type(fldname);
        return schema;
    }

    /// < FieldType > ::= INT | VARCHAR ( IntTok )
    pub fn field_type(&mut self, fldname: String) -> Schema {
        let mut schema = Schema::new();
        if self.lex.match_keyword(TokenKind::RESERVED(Reserved::INT)) {
            self.lex.eat_keyword(TokenKind::RESERVED(Reserved::INT));
            schema.add_int_field(fldname);
        } else if self
            .lex
            .match_keyword(TokenKind::RESERVED(Reserved::VARCHAR))
        {
            self.lex.eat_keyword(TokenKind::RESERVED(Reserved::VARCHAR));
            self.lex.eat_keyword(TokenKind::LPAR);
            let str_len = self.lex.eat_int_constant();
            self.lex.eat_keyword(TokenKind::RPAR);
            schema.add_string_field(fldname, str_len);
        } else {
            panic!("field type not found.");
        }
        return schema;
    }

    pub fn query(&mut self) -> QueryData {
        self.lex.eat_keyword(TokenKind::RESERVED(Reserved::SELECT));
        let fields = self.select_pattern();
        self.lex.eat_keyword(TokenKind::RESERVED(Reserved::FROM));
        let tables = self.table_list();
        return QueryData { fields, tables };
        // let pred = Predicate::new();
        // if self.lex.match_keyword(TokenKind::RESERVED(Reserved::WHERE)) {
        //     self.lex.eat_keyword(TokenKind::RESERVED(Reserved::WHERE));
        //     pred = self.predicate();
        // }
    }

    fn select_pattern(&mut self) -> QueryFields {
        if self.lex.match_keyword(TokenKind::RESERVED(Reserved::ASTER)) {
            self.lex.eat_keyword(TokenKind::RESERVED(Reserved::ASTER));
            return QueryFields::AllFields;
        } else {
            let fields = self.select_list();
            return QueryFields::Fields(fields);
        }
    }

    fn select_list(&mut self) -> Vec<String> {
        let mut l: Vec<String> = Vec::new();
        l.push(self.field());
        if self.lex.match_keyword(TokenKind::COMMA) {
            self.lex.eat_keyword(TokenKind::COMMA);
            l.extend(self.select_list());
        }
        return l;
    }

    fn table_list(&mut self) -> Vec<String> {
        let mut l: Vec<String> = Vec::new();
        l.push(self.lex.eat_id());
        if self.lex.match_keyword(TokenKind::COMMA) {
            self.lex.eat_keyword(TokenKind::COMMA);
            l.extend(self.table_list());
        }
        return l;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::fs;

    fn setup() {
        let db_directory = "./db".to_string();
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    fn teardown() {
        let db_directory = "./db".to_string();
        if fs::metadata(db_directory.clone()).is_ok() {
            fs::remove_dir_all(db_directory.clone()).unwrap();
        }
    }

    #[test]
    fn test_parse() -> Result<()> {
        setup();
        let db = SimpleDB::new("parsetest".to_string(), 400, 8);

        let s = String::from("CREATE TABLE STUDENT (sid INT, name VARCHAR(20), age INT)");
        let parser = Parser::new(s);
        parser.sql().execute(&db);

        let s = String::from("INSERT INTO STUDENT (sid, name, age) VALUES (1, 'Alice', 18)");
        let parser = Parser::new(s);
        parser.sql().execute(&db);

        let s = String::from("SELECT * FROM STUDENT");
        let parser = Parser::new(s);
        // println!("{:?}", parser.sql());
        parser.sql().execute(&db);

        teardown();
        Ok(())
    }
}
