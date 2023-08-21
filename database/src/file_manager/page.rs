use bytebuffer::ByteBuffer;
use std::mem;

#[derive(Clone)]
pub struct Page {
    pub(crate) bb: ByteBuffer,
    // charset?
}

// block sizeって4バイト単位？
// index out of rangeが怖い

impl Page {
    // A constructor for creating data buffers
    pub fn new(block_size: i32) -> Self {
        let mut bb = ByteBuffer::new();
        bb.resize(block_size as usize);
        Self { bb }
    }

    // A constructor for creating log buffers
    pub fn new_log(b: Vec<u8>) -> Self {
        Self {
            bb: ByteBuffer::from_vec(b),
        }
    }

    pub fn set_byte_buffer(&mut self, bb: ByteBuffer) -> () {
        self.bb = bb;
    }

    pub fn get_int(&mut self, offset: i32) -> Result<i32, std::io::Error> {
        self.bb.set_rpos(offset as usize);
        self.bb.read_i32() // move rpos from offset to offset + 4
    }

    pub fn set_int(&mut self, offset: i32, n: i32) -> () {
        self.bb.set_wpos(offset as usize);
        self.bb.write_i32(n) // move wpos from offset to offset + 4
    }

    /// Read a byte sequence of the length specified by the first 4 bytes (from offset to offset + 4).
    /// algorithm:
    /// 1. set rpos to offset
    /// 2. read i32 (named l) from rpos
    /// 3. read bytes from rpos + 4 to rpos + 4 + l
    pub fn get_bytes(&mut self, offset: i32) -> Result<Vec<u8>, std::io::Error> {
        self.bb.set_rpos(offset as usize);
        let length = self.bb.read_i32(); // to do: error handling
        match length {
            Ok(l) => self.bb.read_bytes(l as usize), // to do: error handling
            Err(e) => Err(e),
        }
    }

    /// Write a byte sequence of the length specified by the first 4 bytes (from offset to offset + 4).
    /// algorithm:
    /// 1. set wpos to offset
    /// 2. write i32 (named l) to wpos
    /// 3. write bytes from wpos + 4 to wpos + 4 + l
    pub fn set_bytes(&mut self, offset: i32, b: Vec<u8>) -> () {
        self.bb.set_wpos(offset as usize);
        self.bb.write_i32(b.len() as i32);
        self.bb.write_bytes(&b);
    }

    pub fn get_string(&mut self, offset: i32) -> Result<String, std::io::Error> {
        match self.get_bytes(offset) {
            Ok(b) => {
                match String::from_utf8(b) {
                    Ok(s) => Ok(s),
                    Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)), // to do: error handling nessessary?, unwrap?
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn set_string(&mut self, offset: i32, s: String) -> () {
        let b = s.into_bytes();
        self.set_bytes(offset, b);
    }

    pub fn max_length(strlen: i32) -> i32 {
        // represents every UTF-8 character by 1 byte
        mem::size_of::<i32>() as i32 + strlen
    }

    pub(crate) fn contents(&mut self) -> ByteBuffer {
        self.bb.set_rpos(0);
        self.bb.set_wpos(0);
        self.bb.clone()
    }
}

// テスト
#[cfg(test)]
mod tests {

    use super::*;
    use anyhow::{Ok, Result};

    #[test]
    fn test_page() -> Result<()> {
        // test fn new
        {
            let page = Page::new(400);
            assert_eq!(page.bb.len(), 400);
        }

        // test fn new_log
        {
            let mut page = Page::new_log(vec![1, 2, 3]);
            assert_eq!(page.bb.len(), 3);
            assert_eq!(page.bb.read_bytes(3).unwrap(), vec![1, 2, 3]);
        }

        // test fn byte_buffer
        {
            let mut page = Page::new(400);
            let byte_buffer = ByteBuffer::from_vec(vec![1, 2, 3]);
            page.set_byte_buffer(byte_buffer);
            assert_eq!(page.bb.into_vec(), vec![1, 2, 3]);
        }

        // test fn get_int
        {
            let mut page = Page::new_log(vec![1, 2, 3, 4, 5, 6]);
            assert_eq!(page.get_int(1).unwrap(), 0x02030405);
        }

        // test fn set_int
        {
            let mut page = Page::new_log(vec![1, 2, 3]);
            page.set_int(1, 5);
            assert_eq!(page.bb.read_bytes(5).unwrap(), vec![1, 0, 0, 0, 5]);
        }

        // test fn get_bytes
        {
            // BigEndian
            // 2 -> 0x00000002
            // LittleEndian
            // 2 -> 0x02000000
            let mut page = Page::new_log(vec![0, 0, 0, 2, 5, 6]); // ByteBuffer store data by BigEndian
            assert_eq!(page.get_bytes(0).unwrap(), vec![5, 6]);
        }

        // test fn set_bytes
        {
            let mut page = Page::new_log(vec![0, 0, 0, 2, 5, 6]);
            page.set_bytes(0, vec![1, 2, 3]);
            // page.bb.set_rpos(4);
            assert_eq!(page.bb.read_bytes(7).unwrap(), vec![0, 0, 0, 3, 1, 2, 3]);
        }

        // test fn get_string
        {
            let mut page = Page::new_log(vec![0, 0, 0, 2, 5, 6]);
            assert_eq!(
                page.get_string(0).unwrap(),
                String::from_utf8(vec![5, 6]).unwrap()
            );
        }

        // test fn set_string
        {
            let mut page = Page::new_log(vec![0, 0, 0, 2, 5, 6]);
            page.set_string(0, String::from_utf8(vec![1, 2, 3]).unwrap());
            assert_eq!(page.bb.read_bytes(7).unwrap(), vec![0, 0, 0, 3, 1, 2, 3]);
        }

        // // test fn max_length
        // {
        //     assert_eq!(Page::max_length(3), 7);
        // }

        // test fn contents
        {
            let mut page = Page::new_log(vec![0, 0, 0, 2, 5, 6]);
            assert_eq!(page.contents().get_rpos(), 0);
            assert_eq!(page.contents().get_wpos(), 0);
        }

        Ok(())
    }
}
