use arrow::array::{Int64Array, StringArray, Float64Array};
use arrow::record_batch::RecordBatch;
use arrow_csv::WriterBuilder;
use arrow_schema::{DataType, Field, Schema};
use clap::Parser;
use parquet::arrow::ArrowWriter;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use anyhow::Result;


#[derive(Parser, Debug)]
#[command(name = "relational-db-gen")]
#[command(author = "Basaltic-Red Team")]
#[command(version = "0.1.0")]
#[command(about = "Relational Test Database Generator (5 to 100 linked tables)", long_about = None)]
struct Args {
    /// Number of tables to generate (default 5, min 5, max 100)
    #[arg(short, long, default_value_t = 5)]
    tables: usize,

    /// Number of rows per table
    #[arg(short, long, default_value_t = 100)]
    rows: usize,

    /// Output directory to store generated relational tables
    #[arg(short, long, default_value = "data/relational")]
    output_dir: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let num_tables = args.tables.clamp(5, 100);
    fs::create_dir_all(&args.output_dir)?;

    println!("🚀 Generating Relational Test Database with {} linked tables into '{}'...", num_tables, args.output_dir.display());

    // 1. Users Table (Core Entity)
    generate_users_table(&args.output_dir, args.rows)?;
    println!("  - Created users.parquet");

    // 2. Products Table (Core Entity)
    generate_products_table(&args.output_dir, args.rows)?;
    println!("  - Created products.csv");

    // 3. Orders Table (FK -> users.id)
    generate_orders_table(&args.output_dir, args.rows)?;
    println!("  - Created orders.parquet");

    // 4. Order Items Table (FK -> orders.id, FK -> products.id)
    generate_order_items_table(&args.output_dir, args.rows)?;
    println!("  - Created order_items.csv");

    // 5. Payments Table (FK -> orders.id)
    generate_payments_table(&args.output_dir, args.rows)?;
    println!("  - Created payments.parquet");

    // Dynamic Extra Tables (if tables > 5, up to 100)
    for i in 6..=num_tables {
        generate_extra_relational_table(&args.output_dir, i, args.rows)?;
        println!("  - Created table_{:03}.csv", i);
    }

    println!("\n✅ Relational Database generation completed successfully!");
    println!("💡 Run 'bazan graph {} --output er_graph.md' to visualize the ER diagram!", args.output_dir.display());

    Ok(())
}

fn generate_users_table(dir: &Path, rows: usize) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("username", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
    ]));

    let ids: Vec<i64> = (1..=rows as i64).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("user_{}", i)).collect();
    let emails: Vec<String> = ids.iter().map(|i| format!("user_{}@example.com", i)).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
            Arc::new(StringArray::from(emails.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
        ],
    )?;

    let path = dir.join("users.parquet");
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn generate_products_table(dir: &Path, rows: usize) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("product_name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));

    let ids: Vec<i64> = (1..=rows as i64).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("Product_{}", i)).collect();
    let prices: Vec<f64> = ids.iter().map(|i| (*i as f64) * 19.99).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(prices)),
        ],
    )?;

    let path = dir.join("products.csv");
    let file = File::create(path)?;
    let mut writer = WriterBuilder::new().with_header(true).build(file);
    writer.write(&batch)?;
    Ok(())
}

fn generate_orders_table(dir: &Path, rows: usize) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("total_amount", DataType::Float64, false),
    ]));

    let ids: Vec<i64> = (1..=rows as i64).collect();
    let user_ids: Vec<i64> = ids.iter().map(|i| (i % rows as i64) + 1).collect();
    let amounts: Vec<f64> = ids.iter().map(|i| (*i as f64) * 49.50).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(user_ids)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )?;

    let path = dir.join("orders.parquet");
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn generate_order_items_table(dir: &Path, rows: usize) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
        Field::new("product_id", DataType::Int64, false),
        Field::new("quantity", DataType::Int64, false),
    ]));

    let ids: Vec<i64> = (1..=rows as i64).collect();
    let order_ids: Vec<i64> = ids.iter().map(|i| (i % rows as i64) + 1).collect();
    let product_ids: Vec<i64> = ids.iter().map(|i| ((i * 3) % rows as i64) + 1).collect();
    let quantities: Vec<i64> = ids.iter().map(|i| (i % 5) + 1).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(order_ids)),
            Arc::new(Int64Array::from(product_ids)),
            Arc::new(Int64Array::from(quantities)),
        ],
    )?;

    let path = dir.join("order_items.csv");
    let file = File::create(path)?;
    let mut writer = WriterBuilder::new().with_header(true).build(file);
    writer.write(&batch)?;
    Ok(())
}

fn generate_payments_table(dir: &Path, rows: usize) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));

    let ids: Vec<i64> = (1..=rows as i64).collect();
    let order_ids: Vec<i64> = ids.iter().map(|i| (i % rows as i64) + 1).collect();
    let amounts: Vec<f64> = ids.iter().map(|i| (*i as f64) * 49.50).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(order_ids)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )?;

    let path = dir.join("payments.parquet");
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn generate_extra_relational_table(dir: &Path, table_num: usize, rows: usize) -> Result<()> {
    let table_name = format!("table_{:03}", table_num);
    let parent_fk = if table_num % 2 == 0 { "users_id" } else { "orders_id" };

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(parent_fk, DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let ids: Vec<i64> = (1..=rows as i64).collect();
    let fk_ids: Vec<i64> = ids.iter().map(|i| (i % rows as i64) + 1).collect();
    let values: Vec<f64> = ids.iter().map(|i| *i as f64 * 10.0).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(fk_ids)),
            Arc::new(Float64Array::from(values)),
        ],
    )?;

    let path = dir.join(format!("{}.csv", table_name));
    let file = File::create(path)?;
    let mut writer = WriterBuilder::new().with_header(true).build(file);
    writer.write(&batch)?;
    Ok(())
}
