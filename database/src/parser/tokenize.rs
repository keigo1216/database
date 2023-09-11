use std::collections::VecDeque;

#[derive(Debug, PartialEq, Clone)]
pub enum TokenKind {
    RESERVED(Reserved),
    SINGLEQUOTE,
    DOUBLEQUOTE,
    EQUAL,
    LESS,
    GREATER,
    LESSEQUAL,
    GREATEREQUAL,
    NOTEQUAL,
    LPAR,
    RPAR,
    COMMA,
    TOK(String),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Reserved {
    SELECT,
    FROM,
    WHERE,
    AND,
    INSERT,
    INTO,
    VALUES,
    DELETE,
    UPDATE,
    SET,
    CREATE,
    TABLE,
    VARCHAR,
    INT,
    VIEW,
    AS,
    INDEX,
    ON,
    ASTER,
}

impl Reserved {
    pub fn len(&self) -> usize {
        match self {
            Reserved::SELECT => 6,
            Reserved::FROM => 4,
            Reserved::WHERE => 5,
            Reserved::AND => 3,
            Reserved::INSERT => 6,
            Reserved::INTO => 4,
            Reserved::VALUES => 6,
            Reserved::DELETE => 6,
            Reserved::UPDATE => 6,
            Reserved::SET => 3,
            Reserved::CREATE => 6,
            Reserved::TABLE => 5,
            Reserved::VARCHAR => 7,
            Reserved::INT => 3,
            Reserved::VIEW => 4,
            Reserved::AS => 2,
            Reserved::INDEX => 5,
            Reserved::ON => 2,
            Reserved::ASTER => 1,
        }
    }
    pub fn to_str(&self) -> &str {
        match self {
            Reserved::SELECT => "select",
            Reserved::FROM => "from",
            Reserved::WHERE => "where",
            Reserved::AND => "and",
            Reserved::INSERT => "insert",
            Reserved::INTO => "into",
            Reserved::VALUES => "values",
            Reserved::DELETE => "delete",
            Reserved::UPDATE => "update",
            Reserved::SET => "set",
            Reserved::CREATE => "create",
            Reserved::TABLE => "table",
            Reserved::VARCHAR => "varchar",
            Reserved::INT => "int",
            Reserved::VIEW => "view",
            Reserved::AS => "as",
            Reserved::INDEX => "index",
            Reserved::ON => "on",
            Reserved::ASTER => "*",
        }
    }
}
pub struct Lexer {
    input: String,
    tokenized_position: usize,
    lex_position: usize,
    tokenized: VecDeque<TokenKind>,
}

impl Lexer {
    pub fn new(input: String) -> Self {
        let mut lex = Self {
            input,
            tokenized_position: 0,
            lex_position: 0,
            tokenized: VecDeque::new(),
        };
        lex.tokenize(); // tokenize input
        return lex;
    }

    fn tokenize(&mut self) {
        let mut s = self.input.clone();
        // tokenize s
        while s.len() > 0 {
            self.tokenized_position += 1;
            // skip whitespace
            Self::skip_whitespace(&mut s);

            // match reserved words
            if Self::is_reserved_word(&mut s, Reserved::SELECT) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::SELECT));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::FROM) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::FROM));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::WHERE) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::WHERE));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::AND) {
                self.tokenized.push_back(TokenKind::RESERVED(Reserved::AND));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::INSERT) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::INSERT));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::INTO) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::INTO));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::VALUES) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::VALUES));

                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::DELETE) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::DELETE));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::UPDATE) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::UPDATE));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::SET) {
                self.tokenized.push_back(TokenKind::RESERVED(Reserved::SET));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::CREATE) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::CREATE));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::TABLE) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::TABLE));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::VARCHAR) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::VARCHAR));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::INT) {
                self.tokenized.push_back(TokenKind::RESERVED(Reserved::INT));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::VIEW) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::VIEW));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::AS) {
                self.tokenized.push_back(TokenKind::RESERVED(Reserved::AS));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::INDEX) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::INDEX));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::ON) {
                self.tokenized.push_back(TokenKind::RESERVED(Reserved::ON));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::ASTER) {
                self.tokenized
                    .push_back(TokenKind::RESERVED(Reserved::ASTER));
                continue;
            }

            // match left parenthesis
            if s.chars().next().unwrap() == '(' {
                s.remove(0);
                self.tokenized.push_back(TokenKind::LPAR);
                continue;
            }

            // match right parenthesis
            if s.chars().next().unwrap() == ')' {
                s.remove(0);
                self.tokenized.push_back(TokenKind::RPAR);
                continue;
            }

            // match comma
            if s.chars().next().unwrap() == ',' {
                s.remove(0);
                self.tokenized.push_back(TokenKind::COMMA);
                continue;
            }

            // match single quote
            if s.chars().next().unwrap() == '\'' {
                s.remove(0);
                self.tokenized.push_back(TokenKind::SINGLEQUOTE);
                continue;
            }

            // match double quote
            if s.chars().next().unwrap() == '"' {
                s.remove(0);
                self.tokenized.push_back(TokenKind::DOUBLEQUOTE);
                continue;
            }

            // match equal
            if s.chars().next().unwrap() == '=' {
                s.remove(0);
                self.tokenized.push_back(TokenKind::EQUAL);
                continue;
            }

            // match not equal
            if s.len() > 1 && s.chars().next().unwrap() == '!' && s.chars().nth(1).unwrap() == '=' {
                s.remove(0);
                s.remove(0);
                self.tokenized.push_back(TokenKind::NOTEQUAL);
                continue;
            }

            // match greater equal
            if s.len() > 1 && s.chars().next().unwrap() == '>' && s.chars().nth(1).unwrap() == '=' {
                s.remove(0);
                s.remove(0);
                self.tokenized.push_back(TokenKind::GREATEREQUAL);
                continue;
            }

            // match less equal
            if s.len() > 1 && s.chars().next().unwrap() == '<' && s.chars().nth(1).unwrap() == '=' {
                s.remove(0);
                s.remove(0);
                self.tokenized.push_back(TokenKind::LESSEQUAL);
                continue;
            }

            // match greater
            if s.chars().next().unwrap() == '>' {
                s.remove(0);
                self.tokenized.push_back(TokenKind::GREATER);
                continue;
            }

            // match less
            if s.chars().next().unwrap() == '<' {
                s.remove(0);
                self.tokenized.push_back(TokenKind::LESS);
                continue;
            }

            // match string
            let next = s.chars().next().unwrap();
            if next.is_alphanumeric() || next == '_' {
                let mut str = String::new();
                str.push(s.remove(0));
                while s.len() > 0
                    && (s.chars().next().unwrap().is_alphanumeric()
                        || s.chars().next().unwrap() == '_')
                {
                    str.push(s.remove(0));
                }
                self.tokenized.push_back(TokenKind::TOK(str));
                continue;
            } else {
                panic!("invalid token: {}", s);
            }
        }
    }

    pub fn eat_int_constant(&mut self) -> i32 {
        let pos = self.lex_position;
        let front_token = self.tokenized[pos].clone();
        if let TokenKind::TOK(t) = front_token {
            self.lex_position += 1;
            // return t.parse::<i32>().unwrap();
            match t.parse::<i32>() {
                Ok(i) => return i,
                Err(_) => panic!("expected int"),
            }
        } else {
            panic!("expected int, but got {:?}", front_token);
        }
    }

    // pub fn eat_id(&mut self) -> String {
    //     let mut pos = self.lex_position;
    //     let front_token = self.tokenized[pos].clone();
    //     if let TokenKind::TOK(t) = front_token {
    //         self.lex_position += 1;
    //         return t.clone();
    //     } else if let TokenKind::SINGLEQUOTE = front_token {
    //         pos += 1;
    //         let front_token = self.tokenized[pos].clone();
    //         if let TokenKind::TOK(t) = front_token {
    //             pos += 1;

    //         }
    //     } else if let TokenKind::DOUBLEQUOTE = front_token {

    //     } else {
    //         panic!("expected id, but got {:?}", front_token);
    //     }
    // }

    pub fn eat_id(&mut self) -> String {
        let pos = self.lex_position;
        let front_token = self.tokenized[pos].clone();
        if front_token == TokenKind::SINGLEQUOTE {
            self.lex_position += 1;
            let s = self.eat_string();
            let pos = self.lex_position;
            let front_token = self.tokenized[pos].clone();
            if front_token == TokenKind::SINGLEQUOTE {
                self.lex_position += 1;
                return s;
            } else {
                panic!("expected single quote, but got {:?}", front_token);
            }
        } else if front_token == TokenKind::DOUBLEQUOTE {
            self.lex_position += 1;
            let s = self.eat_string();
            let pos = self.lex_position;
            let front_token = self.tokenized[pos].clone();
            if front_token == TokenKind::DOUBLEQUOTE {
                self.lex_position += 1;
                return s;
            } else {
                panic!("expected single quote, but got {:?}", front_token);
            }
        } else {
            return self.eat_string();
        }
    }

    fn eat_string(&mut self) -> String {
        let pos = self.lex_position;
        let front_token = self.tokenized[pos].clone();
        if let TokenKind::TOK(t) = front_token {
            self.lex_position += 1;
            return t.clone();
        } else {
            panic!("expected string, but got {:?}", front_token);
        }
    }

    pub fn eat_keyword(&mut self, keyword: TokenKind) {
        let pos = self.lex_position;
        let front_token = self.tokenized[pos].clone();
        if keyword == front_token {
            self.lex_position += 1;
        } else {
            panic!("expected {:?}, but got {:?}", keyword, front_token);
        }
    }

    pub fn match_keyword(&mut self, keyword: TokenKind) -> bool {
        let pos = self.lex_position;
        if pos >= self.tokenized.len() {
            return false;
        }
        let front_token = self.tokenized[pos].clone();
        if keyword == front_token {
            return true;
        } else {
            return false;
        }
    }

    fn is_reserved_word(s: &mut String, reserved_word: Reserved) -> bool {
        if s.len() < reserved_word.len() {
            return false;
        }
        if !s.to_lowercase().starts_with(reserved_word.to_str()) {
            return false;
        }

        // check if the next character is alphanumeric or underscore
        let next = s.chars().nth(reserved_word.len());
        match next {
            Some(c) => {
                if c.is_alphanumeric() || c == '_' {
                    // not reserved word
                    return false;
                }
            }
            None => {}
        }

        s.replace_range(..reserved_word.len(), "");
        return true;
    }

    fn skip_whitespace(s: &mut String) {
        while s.len() > 0 && s.chars().next().unwrap().is_whitespace() {
            s.remove(0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_tokenize() -> Result<()> {
        let s = "SeLect * from student where sname = \"Alice\" and sage < 20 and sname = 'Bob' and sage > 10 and sname != 'Cindy' and sage <= 30 and sname >= 'David'".to_string();
        let lex = Lexer::new(s);
        let v = lex.tokenized;
        assert_eq!(v.len(), 40);
        assert_eq!(v[0], TokenKind::RESERVED(Reserved::SELECT));
        assert_eq!(v[1], TokenKind::TOK("*".to_string()));
        assert_eq!(v[2], TokenKind::RESERVED(Reserved::FROM));
        assert_eq!(v[3], TokenKind::TOK("student".to_string()));
        assert_eq!(v[4], TokenKind::RESERVED(Reserved::WHERE));
        assert_eq!(v[5], TokenKind::TOK("sname".to_string()));
        assert_eq!(v[6], TokenKind::EQUAL);
        assert_eq!(v[7], TokenKind::DOUBLEQUOTE);
        assert_eq!(v[8], TokenKind::TOK("Alice".to_string()));
        assert_eq!(v[9], TokenKind::DOUBLEQUOTE);
        assert_eq!(v[10], TokenKind::RESERVED(Reserved::AND));
        assert_eq!(v[11], TokenKind::TOK("sage".to_string()));
        assert_eq!(v[12], TokenKind::LESS);
        assert_eq!(v[13], TokenKind::TOK("20".to_string()));
        assert_eq!(v[14], TokenKind::RESERVED(Reserved::AND));
        assert_eq!(v[15], TokenKind::TOK("sname".to_string()));
        assert_eq!(v[16], TokenKind::EQUAL);
        assert_eq!(v[17], TokenKind::SINGLEQUOTE);
        assert_eq!(v[18], TokenKind::TOK("Bob".to_string()));
        assert_eq!(v[19], TokenKind::SINGLEQUOTE);
        assert_eq!(v[20], TokenKind::RESERVED(Reserved::AND));
        assert_eq!(v[21], TokenKind::TOK("sage".to_string()));
        assert_eq!(v[22], TokenKind::GREATER);
        assert_eq!(v[23], TokenKind::TOK("10".to_string()));
        assert_eq!(v[24], TokenKind::RESERVED(Reserved::AND));
        assert_eq!(v[25], TokenKind::TOK("sname".to_string()));
        assert_eq!(v[26], TokenKind::NOTEQUAL);
        assert_eq!(v[27], TokenKind::SINGLEQUOTE);
        assert_eq!(v[28], TokenKind::TOK("Cindy".to_string()));
        assert_eq!(v[29], TokenKind::SINGLEQUOTE);
        assert_eq!(v[30], TokenKind::RESERVED(Reserved::AND));
        assert_eq!(v[31], TokenKind::TOK("sage".to_string()));
        assert_eq!(v[32], TokenKind::LESSEQUAL);
        assert_eq!(v[33], TokenKind::TOK("30".to_string()));
        assert_eq!(v[34], TokenKind::RESERVED(Reserved::AND));
        assert_eq!(v[35], TokenKind::TOK("sname".to_string()));
        assert_eq!(v[36], TokenKind::GREATEREQUAL);
        assert_eq!(v[37], TokenKind::SINGLEQUOTE);
        assert_eq!(v[38], TokenKind::TOK("David".to_string()));
        assert_eq!(v[39], TokenKind::SINGLEQUOTE);

        Ok(())
    }
}
