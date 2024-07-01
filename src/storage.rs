use sqlx::postgres::PgPool;   // Import SQLx PostgreSQL pool for database connection
use anyhow::Result;           // Import anyhow for handling Result<T, E>
use polars::prelude::*;       // Import Polars for DataFrame operations

/// Asynchronous function to store a Polars DataFrame in a PostgreSQL database.
///
/// # Arguments
///
/// * `df` - A reference to the Polars DataFrame containing data to be stored.
/// * `connection_string` - A string slice representing the PostgreSQL connection string.
///
/// # Errors
///
/// Returns an error if the PostgreSQL connection fails, if there are issues with SQL queries,
/// or if there are errors during data extraction and insertion.
///
/// # Example
///
/// ```
/// use anyhow::Result;
/// use polars::prelude::*;
/// use tokio::main;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let path = "test.csv";
///     let df = polars::read_csv(path)?;
///
///     // Store DataFrame in PostgreSQL
///     store_in_postgres(&df, "postgres://user:password@localhost/dbname").await?;
///
///     Ok(())
/// }
/// ```

// Asynchronous function to store DataFrame in PostgreSQL database
pub async fn store_in_postgres(df: &DataFrame, connection_string: &str) -> Result<()> {
    // Establish a PostgreSQL connection pool
    let pool = PgPool::connect(connection_string).await?;  // Connect to PostgreSQL using connection string

    // Define the SQL statements for creating and inserting data into the table
    let create_table_query = r#"
        CREATE TABLE IF NOT EXISTS customer_data (
            id SERIAL PRIMARY KEY,
            age INT,
            gender VARCHAR(10),
            income INT,
            membership_years INT,
            purchase_frequency INT,
            preferred_category VARCHAR(50),
            last_purchase_amount FLOAT
        )
    "#;

    let insert_query = r#"
        INSERT INTO customer_data (id, age, gender, income, membership_years, purchase_frequency, preferred_category, last_purchase_amount)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    "#;

    // Execute the create table query to ensure the table exists
    sqlx::query(create_table_query)
        .execute(&pool)
        .await?;  // Await the execution and propagate any errors

    // Extract data from DataFrame and insert into PostgreSQL table
    for row in 0..df.height() {  // Iterate over each row in the DataFrame
        // Extract column values from the DataFrame for the current row
        let id = df.column("id")?.i64().unwrap().get(row);  // Assuming "id" is of type i64
        let age = df.column("age")?.i64().unwrap().get(row);  // Assuming "age" is of type i64
        let gender = df.column("gender")?.utf8().unwrap().get(row).to_owned();  // Assuming "gender" is of type String
        let income = df.column("income")?.i64().unwrap().get(row);  // Assuming "income" is of type i64
        let membership_years = df.column("membership_years")?.i64().unwrap().get(row);  // Assuming "membership_years" is of type i64
        let purchase_frequency = df.column("purchase_frequency")?.i64().unwrap().get(row);  // Assuming "purchase_frequency" is of type i64
        let preferred_category = df.column("preferred_category")?.utf8().unwrap().get(row).to_owned();  // Assuming "preferred_category" is of type String
        let last_purchase_amount = df.column("last_purchase_amount")?.f64().unwrap().get(row);  // Assuming "last_purchase_amount" is of type f64

        // Execute the insert query with parameters for the current row
        sqlx::query(insert_query)
            .bind(id)
            .bind(age)
            .bind(gender)
            .bind(income)
            .bind(membership_years)
            .bind(purchase_frequency)
            .bind(preferred_category)
            .bind(last_purchase_amount)
            .execute(&pool)
            .await?;  // Await the execution and propagate any errors
    }

    Ok(())  // Return Ok if the entire process completes successfully
}
