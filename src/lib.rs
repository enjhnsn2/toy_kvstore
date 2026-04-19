use std::fs::File;
use std::io::{Read, Write, Seek};

// Perhaps we should not pass arrays on the stack?
// Could potentially replace File with an in-memory representation?
// Potential property: all used page ides are less than max_pageid, and all page ids in reserve are less than max_pageid

pub const PAGE_SIZE: usize = 4096; 

pub type PageId = usize;

pub type IOError = std::io::Error;

pub struct Page {
    id: PageId,
    data: [u8; PAGE_SIZE],
}

pub struct DataAccessLayer {
    file: File,
    reserve: Vec<PageId>,
    max_pageid: PageId ,
}

impl DataAccessLayer {
    // File will be closed automatically when it goes out of scope
    pub fn new(file_path: &str) -> Self {
        // create file if it doesn't exist
        if !std::path::Path::new(file_path).exists() {
            File::create(file_path).expect("Failed to create file");
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)
            .expect("Failed to open file");
        DataAccessLayer { file, reserve: Vec::new(), max_pageid: 0 }
    }

    pub fn fresh_page_id(&mut self) -> PageId {
        if let Some(page_id) = self.reserve.pop() {
            page_id
        } else {
            let page_id = self.max_pageid;
            self.max_pageid += 1;
            page_id
        }
    }

    pub fn release_page_id(&mut self, page_id: PageId) {
        self.reserve.push(page_id);
    }

    fn seek_to_page(&mut self, page_id: PageId) -> Result<(), IOError> {
        let offset = page_id * PAGE_SIZE;
        self.file.seek(std::io::SeekFrom::Start(offset as u64))?;
        Ok(())
    }

    #[flux_rs::trusted] // read_ecaxt has a precondition?
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page, IOError> {
        self.seek_to_page(page_id)?;
        let data = {
            let mut buf = [0u8; PAGE_SIZE];
            self.file.read_exact(&mut buf)?;
            buf
        };
        Ok(Page { id: page_id, data })
    }

    pub fn write_page(&mut self, page: &Page) -> Result<(), IOError> {
        self.seek_to_page(page.id)?;
        self.file.write_all(&page.data)?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let mut dal = DataAccessLayer::new("./testfile.dat");
        let page_id = dal.fresh_page_id();
        let page = Page { id: page_id, data: [33u8; PAGE_SIZE] };
        dal.write_page(&page).expect("Failed to write page");

    }
}
