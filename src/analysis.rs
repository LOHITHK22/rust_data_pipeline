use polars::prelude::*;
use anyhow::Result;

pub fn basic_statistics(df: &DataFrame) -> Result<()> {
    let columns_to_skip = vec!["gender", "preferred_category"];

    for col_name in df.get_column_names() {
        if !columns_to_skip.contains(&col_name) {
            let series = df.column(col_name)?;

            println!("Column: {}", col_name);
            println!("Mean: {:?}", series.mean());

            // Calculate min and max
            if let Some(min_value) = series.min::<f64>() {
                println!("Min: {:?}", min_value);
            } else {
                println!("Min: None");
            }
            if let Some(max_value) = series.max::<f64>() {
                println!("Max: {:?}", max_value);
            } else {
                println!("Max: None");
            }

            // Calculate standard deviation
            let std = series.std_as_series(1).f64()?.get(0);
            println!("Standard Deviation: {:?}", std);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::frame::DataFrame;

    #[test]
    fn test_basic_statistics() {
        // Create a sample DataFrame for testing
        let df = DataFrame::new(vec![
            Series::new("age", &[30, 25, 35]),
            Series::new("income", &[50000, 60000, 70000]),
            Series::new("membership_years", &[5, 3, 7]),
        ])
        .unwrap();

        // Call the basic_statistics function
        let result = basic_statistics(&df);
        assert!(result.is_ok(), "Failed to calculate basic statistics: {:?}", result);
    }
}
