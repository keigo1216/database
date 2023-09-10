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
        }
    }
}
pub struct Tokenize {}

impl Tokenize {
    pub fn tokenize(mut s: String) -> VecDeque<TokenKind> {
        let mut v: VecDeque<TokenKind> = VecDeque::new();

        // tokenize s
        while s.len() > 0 {
            println!("s: {}", s);
            // skip whitespace
            Self::skip_whitespace(&mut s);

            // match reserved words
            if Self::is_reserved_word(&mut s, Reserved::SELECT) {
                v.push_back(TokenKind::RESERVED(Reserved::SELECT));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::FROM) {
                v.push_back(TokenKind::RESERVED(Reserved::FROM));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::WHERE) {
                v.push_back(TokenKind::RESERVED(Reserved::WHERE));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::AND) {
                v.push_back(TokenKind::RESERVED(Reserved::AND));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::INSERT) {
                v.push_back(TokenKind::RESERVED(Reserved::INSERT));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::INTO) {
                v.push_back(TokenKind::RESERVED(Reserved::INTO));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::VALUES) {
                v.push_back(TokenKind::RESERVED(Reserved::VALUES));

                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::DELETE) {
                v.push_back(TokenKind::RESERVED(Reserved::DELETE));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::UPDATE) {
                v.push_back(TokenKind::RESERVED(Reserved::UPDATE));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::SET) {
                v.push_back(TokenKind::RESERVED(Reserved::SET));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::CREATE) {
                v.push_back(TokenKind::RESERVED(Reserved::CREATE));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::TABLE) {
                v.push_back(TokenKind::RESERVED(Reserved::TABLE));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::VARCHAR) {
                v.push_back(TokenKind::RESERVED(Reserved::VARCHAR));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::INT) {
                v.push_back(TokenKind::RESERVED(Reserved::INT));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::VIEW) {
                v.push_back(TokenKind::RESERVED(Reserved::VIEW));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::AS) {
                v.push_back(TokenKind::RESERVED(Reserved::AS));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::INDEX) {
                v.push_back(TokenKind::RESERVED(Reserved::INDEX));
                continue;
            }
            if Self::is_reserved_word(&mut s, Reserved::ON) {
                v.push_back(TokenKind::RESERVED(Reserved::ON));
                continue;
            }

            // match single quote
            if s.chars().next().unwrap() == '\'' {
                s.remove(0);
                v.push_back(TokenKind::SINGLEQUOTE);
                continue;
            }

            // match double quote
            if s.chars().next().unwrap() == '"' {
                s.remove(0);
                v.push_back(TokenKind::DOUBLEQUOTE);
                continue;
            }

            // match equal
            if s.chars().next().unwrap() == '=' {
                s.remove(0);
                v.push_back(TokenKind::EQUAL);
                continue;
            }

            // match not equal
            if s.len() > 1 && s.chars().next().unwrap() == '!' && s.chars().nth(1).unwrap() == '=' {
                s.remove(0);
                s.remove(0);
                v.push_back(TokenKind::NOTEQUAL);
                continue;
            }

            // match greater equal
            if s.len() > 1 && s.chars().next().unwrap() == '>' && s.chars().nth(1).unwrap() == '=' {
                s.remove(0);
                s.remove(0);
                v.push_back(TokenKind::GREATEREQUAL);
                continue;
            }

            // match less equal
            if s.len() > 1 && s.chars().next().unwrap() == '<' && s.chars().nth(1).unwrap() == '=' {
                s.remove(0);
                s.remove(0);
                v.push_back(TokenKind::LESSEQUAL);
                continue;
            }

            // match greater
            if s.chars().next().unwrap() == '>' {
                s.remove(0);
                v.push_back(TokenKind::GREATER);
                continue;
            }

            // match less
            if s.chars().next().unwrap() == '<' {
                s.remove(0);
                v.push_back(TokenKind::LESS);
                continue;
            }

            // match string
            let next = s.chars().next().unwrap();
            if next.is_alphanumeric() || next == '_' || next == '*' {
                let mut str = String::new();
                str.push(s.remove(0));
                while s.len() > 0 && s.chars().next().unwrap().is_alphanumeric() {
                    str.push(s.remove(0));
                }
                v.push_back(TokenKind::TOK(str));
                continue;
            } else {
                panic!("invalid token: {}", s);
            }
        }

        return v;
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
        let v = Tokenize::tokenize(s);
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
