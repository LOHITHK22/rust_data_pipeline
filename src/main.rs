mod ingestion;   // Module for data ingestion operations
mod storage;     // Module for data storage operations
mod analysis;    // Module for data analysis operations

use anyhow::Result;   // Import anyhow for handling Result<T, E>

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Data Ingestion
    let df = ingestion::read_csv(r"C:\Rustclass\customer_segmentation_data.csv")?;  // Read CSV data into a DataFrame
    
    // Print ingested data for verification
    println!("Ingested Data:");
    println!("{}", df);  // Print the DataFrame to verify data ingestion

    // 2. Analysis (Example: Basic Statistics)
    analysis::basic_statistics(&df)?;  // Perform basic statistical analysis on the DataFrame
    
    // 3. Data Storage (Example: PostgreSQL)
    // Store DataFrame in PostgreSQL database asynchronously
    storage::store_in_postgres(&df, "postgres://postgres:postgres@localhost/customerdb").await?;
    
    // Print success message upon completion
    println!("Data pipeline completed successfully!");
    
    Ok(())  // Return Ok to indicate successful execution
}
