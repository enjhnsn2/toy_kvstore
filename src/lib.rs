pub mod bst;
pub mod dal;

#[cfg(test)]
mod tests {
    use super::dal::*;

    #[test]
    fn basic() {
        let mut dal = DataAccessLayer::new("./testfile.dat").expect("Failed to open DAL");
        let page_id = dal.fresh_page_id().expect("Failed to get page id");
        let page = Page {
            id: page_id,
            data: [33u8; PAGE_SIZE],
        };
        dal.write_page(&page).expect("Failed to write page");
    }

    #[test]
    fn test_persistence() {
        let db_path = "./test_persistence.dat";

        // Initialize db
        let mut dal = DataAccessLayer::new(db_path).expect("failed to open DAL");

        // Create a new page
        let page_id = dal.fresh_page_id().expect("failed to get page id");
        let mut p = Page {
            id: page_id,
            data: [0u8; PAGE_SIZE],
        };
        p.data[..4].copy_from_slice(b"data");

        // Commit it
        dal.write_page(&p).expect("failed to write page");
        let meta = dal.metadata_page();
        dal.write_page(&meta).expect("failed to write freelist");

        // Close the db (file closes automatically on drop)
        drop(dal);

        // We expect the freelist state was saved, so we write to
        // page number 3 and not overwrite the one at number 2
        let meta_page = {
            let mut tmp = DataAccessLayer::new(db_path).expect("failed to open DAL");
            tmp.read_page(0).expect("failed to read metadata page")
        };
        let mut dal = DataAccessLayer::from_metadata(db_path, &meta_page)
            .expect("failed to restore DAL from metadata");

        let page_id = dal.fresh_page_id().expect("failed to get page id");
        let mut p = Page {
            id: page_id,
            data: [0u8; PAGE_SIZE],
        };
        p.data[..5].copy_from_slice(b"data2");
        dal.write_page(&p).expect("failed to write page");

        // Create a page and free it so the released pages will be updated
        let page_num = dal.fresh_page_id().expect("failed to get page id");
        dal.release_page_id(page_num);

        // Commit it
        let meta = dal.metadata_page();
        dal.write_page(&meta).expect("failed to write freelist");
    }
}
