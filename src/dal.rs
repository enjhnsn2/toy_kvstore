use std::fs::File;
use std::io::{Read, Seek, Write};
extern crate flux_core;

// Perhaps we should not pass arrays on the stack?
// Could potentially replace File with an in-memory representation?
// Potential property: all used page ids are less than max_pageid, and all page ids in reserve are less than max_pageid

pub const PAGE_SIZE: usize = 4096;
pub const MAX_PAGES: usize = 4096;

pub type PageId = usize;

pub type IOError = std::io::Error;

pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
}

#[flux_rs::refined_by(max_pageid: int, reserve_len: int)]
#[flux_rs::invariant(reserve_len < max_pageid)]
#[flux_rs::invariant(max_pageid < MAX_PAGES)]
pub struct DataAccessLayer {
    file: File,
    #[flux_rs::field([PageId{id: id < max_pageid}; _])]
    reserve: [PageId; MAX_PAGES],
    #[flux_rs::field(usize[reserve_len])]
    reserve_len: usize,
    #[flux_rs::field(PageId[max_pageid])]
    max_pageid: PageId,
}

impl DataAccessLayer {
    // File will be closed automatically when it goes out of scope
    pub fn new(file_path: &str) -> Result<Self, IOError> {
        if !std::path::Path::new(file_path).exists() {
            File::create(file_path)?;
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)?;
        Ok(DataAccessLayer {
            file,
            reserve: [0; MAX_PAGES],
            reserve_len: 0,
            max_pageid: 1,
        })
    }

    pub fn fresh_page_id(&mut self) -> PageId {
        if self.reserve_len > 0 {
            self.reserve_len -= 1;
            self.reserve[self.reserve_len]
        } else {
            let page_id = self.max_pageid;
            self.max_pageid += 1;
            page_id
        }
    }

    #[flux_rs::spec(fn(&mut Self[@self], PageId{id: id < self.max_pageid}))]
    pub fn release_page_id(&mut self, page_id: PageId) {
        self.reserve[self.reserve_len] = page_id;
        self.reserve_len += 1;
    }

    fn seek_to_page(&mut self, page_id: PageId) -> Result<(), IOError> {
        let offset = page_id * PAGE_SIZE;
        self.file.seek(std::io::SeekFrom::Start(offset as u64))?;
        Ok(())
    }

    #[flux_rs::trusted] // read_exact has a precondition?
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page, IOError> {
        self.seek_to_page(page_id)?;
        let data = {
            let mut buf = [0u8; PAGE_SIZE];
            flux_rs::assert(buf.len() > 0);
            let n = self.file.read(&mut buf)?;
            if n < PAGE_SIZE {
                return Err(IOError::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            buf
        };
        Ok(Page { id: page_id, data })
    }

    pub fn write_page(&mut self, page: &Page) -> Result<(), IOError> {
        self.seek_to_page(page.id)?;
        let n = self.file.write(&page.data)?;
        if n < PAGE_SIZE {
            return Err(IOError::new(
                std::io::ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        Ok(())
    }
}
