use database::parser::parser::{Execute, Parser};
use database::SimpleDB;
use std::io::{self, BufRead};

fn main() {
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let db = SimpleDB::new("logfile".to_string(), 400, 8);

    loop {
        let mut input = String::new();
        if let Err(_) = reader.read_line(&mut input) {
            break;
        }

        if input.trim().is_empty() {
            break;
        }

        // remove \n
        input = input.trim().to_string();

        let parser = Parser::new(input);
        parser.sql().execute(&db);
    }
}
