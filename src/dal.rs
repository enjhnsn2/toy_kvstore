use std::fs::File;
use std::io::{Read, Seek, Write};
extern crate flux_core;

// Perhaps we should not pass arrays on the stack?
// Could potentially replace File with an in-memory representation?
// Potential property: all used page ids are less than next_pageid, and all page ids in reserve are less than next_pageid

pub const PAGE_SIZE: usize = 4096;
pub const MAX_PAGES: usize = 256;

pub type PageId = usize;

pub type IOError = std::io::Error;

pub struct Page {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
}

#[flux_rs::refined_by(next_pageid: int, reserve_len: int)]
#[flux_rs::invariant(reserve_len < next_pageid)]
#[flux_rs::invariant(next_pageid <= MAX_PAGES)]
pub struct DataAccessLayer {
    file: File,
    #[flux_rs::field(usize[reserve_len])]
    reserve_len: usize,
    #[flux_rs::field(PageId[next_pageid])]
    next_pageid: PageId,
    #[flux_rs::field([PageId{id: id < next_pageid}; _])]
    reserve: [PageId; MAX_PAGES],
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
            next_pageid: 1, // 0 is reserved for metadata
        })
    }

    #[flux_rs::trusted] // need to check its well-formed
    pub fn from_metadata(file_path: &str, page: &Page) -> Result<Self, IOError> {
        let buf = &page.data;
        let reserve_len = usize::from_le_bytes(buf[0..8].try_into().unwrap());
        let next_pageid = usize::from_le_bytes(buf[8..16].try_into().unwrap());
        let mut reserve = [0usize; MAX_PAGES];
        for i in 0..MAX_PAGES {
            let start = 16 + i * 8;
            reserve[i] = usize::from_le_bytes(buf[start..start + 8].try_into().unwrap());
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)?;
        Ok(DataAccessLayer {
            file,
            reserve_len,
            next_pageid,
            reserve,
        })
    }

    // Prepares fields reserve, reserve_len, and next_pageid for serialization.
    pub fn metadata_page(&self) -> Page {
        let data = {
            let mut buf = [0u8; PAGE_SIZE];
            // Serialize reserve_len and next_pageid into the first 16 bytes of the page data
            buf[0..8].copy_from_slice(&self.reserve_len.to_le_bytes());
            buf[8..16].copy_from_slice(&self.next_pageid.to_le_bytes());
            for (i, &id) in self.reserve.iter().enumerate() {
                let start = 16 + i * 8;
                buf[start..start + 8].copy_from_slice(&id.to_le_bytes());
            }
            buf
        };
        Page { id: 0, data }
    }

    pub fn fresh_page_id(&mut self) -> Result<PageId, IOError> {
        if self.reserve_len > 0 {
            self.reserve_len -= 1;
            Ok(self.reserve[self.reserve_len])
        } else if self.next_pageid < MAX_PAGES {
            let page_id = self.next_pageid;
            self.next_pageid += 1;
            Ok(page_id)
        } else {
            Err(IOError::new(std::io::ErrorKind::Other, "max pages reached"))
        }
    }

    #[flux_rs::trusted] // assignment might be unsafe?
    #[flux_rs::spec(fn(&mut Self[@self], PageId{id: id < self.next_pageid}) requires self.next_pageid < MAX_PAGES)]
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

    // pub fn serialize
}
