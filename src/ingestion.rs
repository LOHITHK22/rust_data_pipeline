use polars::prelude::*;
use anyhow::Result;

pub fn read_csv(path: &str) -> Result<DataFrame> {
    let df = CsvReader::from_path(path)?
        .has_header(true)
        .finish()?;
    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_read_csv_existing_file() {
        // Create a temporary CSV file for testing
        let path = "test.csv";
        let data = "id,name\n1,John\n2,Jane\n";
        let mut file = File::create(path).expect("Failed to create test file");
        file.write_all(data.as_bytes()).expect("Failed to write test data");

        // Test reading the CSV file
        let result = read_csv(path);
        assert!(result.is_ok(), "Failed to read CSV file: {:?}", result);

        // Cleanup: Remove the temporary test file
        std::fs::remove_file(path).expect("Failed to delete test file");
    }

    #[test]
    fn test_read_csv_nonexistent_file() {
        let path = "nonexistent_file.csv";

        // Test reading a nonexistent CSV file
        let result = read_csv(path);
        assert!(result.is_err(), "Expected error for nonexistent file: {:?}", result);
    }
}