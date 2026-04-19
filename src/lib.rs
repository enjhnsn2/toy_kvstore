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
}
