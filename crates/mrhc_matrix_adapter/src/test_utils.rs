#![allow(clippy::unwrap_used)]

use std::fs;
use std::path::Path;

/// Asserts the expected items are present in the specified directory and no others.
pub fn assert_directory(dir: impl AsRef<Path>, expected: Vec<&str>) {
    let dir = dir.as_ref();

    // Make sure all expected paths exist
    for expected in &expected {
        let path = dir.join(expected);

        assert!(
            fs::exists(&path).unwrap(),
            "Expected path '{}' to exist",
            path.to_string_lossy(),
        );
    }

    // Make sure there is no unexpected directory entry
    for path in fs::read_dir(dir).unwrap().filter_map(Result::ok) {
        let mut found = false;

        for expected in &expected {
            let path = path.path();
            let file_name = path.file_name().unwrap_or_default();

            if file_name.to_string_lossy().as_ref() == *expected {
                found = true;
                break;
            }
        }

        assert!(
            found,
            "Unexpected directory entry {}",
            path.path().to_string_lossy()
        );
    }
}

/// Asserts that the file contains the expected content.
pub fn assert_file_content(file: impl AsRef<Path>, expected: &[u8]) {
    let file = file.as_ref();
    let content = fs::read(file).unwrap();
    assert_eq!(
        content, expected,
        "Content of file {file:?} does not match the expected content"
    );
}
