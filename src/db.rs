use crate::bst::{Bst, BstError};
use crate::dal::{DataAccessLayer, IOError, PAGE_SIZE, Page};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum DbError {
    /// An I/O error from the storage layer.
    Io(IOError),
    /// An error from the BST index (full, not found, invalid key).
    Index(BstError),
    /// The value supplied to `put` exceeds PAGE_SIZE bytes.
    ValueTooLarge,
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Io(e) => write!(f, "I/O error: {}", e),
            DbError::Index(e) => write!(f, "index error: {}", e),
            DbError::ValueTooLarge => {
                write!(f, "value exceeds maximum size of {} bytes", PAGE_SIZE)
            }
        }
    }
}

impl std::error::Error for DbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DbError::Io(e) => Some(e),
            DbError::Index(e) => Some(e),
            DbError::ValueTooLarge => None,
        }
    }
}

impl From<IOError> for DbError {
    fn from(e: IOError) -> Self {
        DbError::Io(e)
    }
}

impl From<BstError> for DbError {
    fn from(e: BstError) -> Self {
        DbError::Index(e)
    }
}

// ---------------------------------------------------------------------------
// Db
// ---------------------------------------------------------------------------

/// Coordination layer that ties together the BST index and the storage layer.
///
/// The BST maps string keys to PageIds; the DAL owns the file and the page
/// free-list.  `Db` keeps them consistent: every key in the index has exactly
/// one allocated page, and every page allocated by the DAL has exactly one key
/// pointing to it.
///
/// # Persistence
/// The BST index is held in memory only.  The DAL's metadata page (page 0) is
/// flushed on `flush()` but the tree structure is not yet serialized.
/// Reopening the file therefore requires rebuilding the index from the pages.
pub struct Db {
    dal: DataAccessLayer,
    index: Bst,
}

impl Db {
    /// Create a new (or overwrite an existing) database at `file_path`.
    pub fn new(file_path: &str) -> Result<Self, DbError> {
        let dal = DataAccessLayer::new(file_path)?;
        Ok(Db {
            dal,
            index: Bst::new(),
        })
    }

    /// Persist the DAL metadata (free-list, next_pageid) to page 0.
    pub fn flush(&mut self) -> Result<(), DbError> {
        let meta = self.dal.metadata_page();
        self.dal.write_page(&meta)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Core operations
    // -----------------------------------------------------------------------

    /// Store `value` under `key`.  `value` must be at most `PAGE_SIZE` bytes.
    ///
    /// If `key` already exists its page is overwritten in place (no new page is
    /// allocated).  If `key` is new a fresh page is allocated from the DAL.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DbError> {
        if value.len() > PAGE_SIZE {
            return Err(DbError::ValueTooLarge);
        }

        // Reuse the existing page if the key is already indexed, otherwise
        // allocate a fresh one and add it to the index.
        let page_id = match self.index.get(key) {
            Ok(id) => id,
            Err(BstError::NotFound) => {
                let id = self.dal.fresh_page_id()?;
                self.index.insert(key, id)?;
                id
            }
            Err(e) => return Err(DbError::Index(e)),
        };

        let mut data = [0u8; PAGE_SIZE];
        data[..value.len()].copy_from_slice(value);
        self.dal.write_page(&Page { id: page_id, data })?;
        Ok(())
    }

    /// Return the raw page data stored under `key`.
    ///
    /// Returns `DbError::Index(BstError::NotFound)` if the key does not exist.
    pub fn get(&mut self, key: &[u8]) -> Result<Page, DbError> {
        let page_id = self.index.get(key)?;
        let page = self.dal.read_page(page_id)?;
        Ok(page)
    }

    /// Remove `key` and release its backing page to the DAL free-list.
    ///
    /// Returns `DbError::Index(BstError::NotFound)` if the key does not exist.
    pub fn delete(&mut self, key: &[u8]) -> Result<(), DbError> {
        let page_id = self.index.get(key)?;
        self.index.delete(key)?;
        self.dal.release_page_id(page_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dal::PAGE_SIZE;

    fn temp_path(name: &str) -> String {
        format!("/tmp/db_test_{}.dat", name)
    }

    fn cleanup(path: &str) {
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn put_and_get_roundtrip() {
        let path = temp_path("put_get");
        cleanup(&path);
        let mut db = Db::new(&path).unwrap();

        db.put(b"hello", b"world").unwrap();
        let page = db.get(b"hello").unwrap();
        assert_eq!(&page.data[..5], b"world");

        cleanup(&path);
    }

    #[test]
    fn put_overwrites_in_place() {
        let path = temp_path("overwrite");
        cleanup(&path);
        let mut db = Db::new(&path).unwrap();

        db.put(b"key", b"first").unwrap();
        let page_id_first = db.index.get(b"key").unwrap();

        db.put(b"key", b"second").unwrap();
        let page_id_second = db.index.get(b"key").unwrap();

        // Same page should be reused.
        assert_eq!(page_id_first, page_id_second);

        let page = db.get(b"key").unwrap();
        assert_eq!(&page.data[..6], b"second");

        cleanup(&path);
    }

    #[test]
    fn delete_removes_key_and_frees_page() {
        let path = temp_path("delete");
        cleanup(&path);
        let mut db = Db::new(&path).unwrap();

        db.put(b"foo", b"bar").unwrap();
        db.delete(b"foo").unwrap();

        assert!(matches!(
            db.get(b"foo"),
            Err(DbError::Index(BstError::NotFound))
        ));

        // The freed page_id should be reused on the next put.
        let id_before = {
            db.put(b"baz", b"qux").unwrap();
            db.index.get(b"baz").unwrap()
        };
        assert_eq!(id_before, 1); // page 1 was reused

        cleanup(&path);
    }

    #[test]
    fn get_missing_key_returns_not_found() {
        let path = temp_path("get_missing");
        cleanup(&path);
        let mut db = Db::new(&path).unwrap();

        assert!(matches!(
            db.get(b"ghost"),
            Err(DbError::Index(BstError::NotFound))
        ));

        cleanup(&path);
    }

    #[test]
    fn value_too_large_is_rejected() {
        let path = temp_path("too_large");
        cleanup(&path);
        let mut db = Db::new(&path).unwrap();

        let big = vec![0u8; PAGE_SIZE + 1];
        assert!(matches!(db.put(b"k", &big), Err(DbError::ValueTooLarge)));

        cleanup(&path);
    }

    #[test]
    fn flush_writes_metadata() {
        let path = temp_path("flush");
        cleanup(&path);
        let mut db = Db::new(&path).unwrap();

        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap();

        // Verify the file exists and is non-empty.
        let meta = std::fs::metadata(&path).unwrap();
        assert!(meta.len() > 0);

        cleanup(&path);
    }

    #[test]
    fn multiple_keys_independent() {
        let path = temp_path("multi");
        cleanup(&path);
        let mut db = Db::new(&path).unwrap();

        db.put(b"alpha", b"AAA").unwrap();
        db.put(b"beta", b"BBB").unwrap();
        db.put(b"gamma", b"CCC").unwrap();

        assert_eq!(&db.get(b"alpha").unwrap().data[..3], b"AAA");
        assert_eq!(&db.get(b"beta").unwrap().data[..3], b"BBB");
        assert_eq!(&db.get(b"gamma").unwrap().data[..3], b"CCC");

        cleanup(&path);
    }
}
