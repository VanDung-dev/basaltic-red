use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "bazan")]
#[command(author = "Basaltic-Red Team")]
#[command(version = "0.1.0")]
#[command(about = "Bazan CLI - Headless SIMD Matrix Data Engine & BI Tool", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Slice row range from a matrix file (Zero-copy)
    SliceRows {
        /// Path to the data file (supports csv, tsv, psv, txt, json, jsonl, ndjson, parquet, feather, avro, xlsx, orc, msgpack)
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Offset (starting row index, 0-indexed)
        #[arg(short, long, default_value_t = 0)]
        offset: usize,

        /// Limit (number of rows to read)
        #[arg(short, long, default_value_t = 50)]
        limit: usize,

        /// Optional output path to save sliced data (e.g. output.parquet or output.csv)
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Slice selected columns & row range from a matrix file (Column Projection)
    SliceCols {
        /// Path to the data file
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Comma-separated column names (e.g. id,email,salary)
        #[arg(short, long, value_delimiter = ',')]
        cols: Vec<String>,

        /// Offset (starting row index)
        #[arg(short, long, default_value_t = 0)]
        offset: usize,

        /// Limit (number of rows to read)
        #[arg(short, long, default_value_t = 50)]
        limit: usize,

        /// Optional output path to save sliced data
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Split a large matrix file into smaller part files
    Split {
        /// Path to the data file
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Maximum number of rows per part file
        #[arg(short, long, default_value_t = 100000)]
        max_rows: usize,

        /// Output directory to store part files
        #[arg(short, long, default_value = "./parts")]
        output_dir: PathBuf,

        /// Format for output part files (parquet, csv, jsonl)
        #[arg(short, long, default_value = "parquet")]
        format: String,
    },

    /// Preview first N rows of a matrix file in terminal table format
    Preview {
        /// Path to the data file
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Number of rows to preview
        #[arg(short, long, default_value_t = 20)]
        limit: usize,
    },

    /// Extract Data Dictionary (Schema, Data Types & Nullability) in Markdown format
    Dict {
        /// Path to the data file
        #[arg(value_name = "FILE")]
        file: PathBuf,

        /// Optional output path for markdown file (e.g. schema.md)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Auto-detect relationships & generate Mermaid ER Diagram
    Graph {
        /// Path to data file or directory containing matrix files
        #[arg(value_name = "PATH")]
        path: PathBuf,

        /// Optional output path for Mermaid markdown file (e.g. er_graph.md)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Filter matrix file(s) or directory based on dynamic column rules into Clean & Trash matrices (Multi-threaded Parallel)
    Filter {
        /// Path to single file, directory, or glob pattern (e.g. data/*.parquet)
        #[arg(value_name = "PATH")]
        file: String,

        /// Filter rule expression (e.g. --rule "age >= 18" --rule "salary > 1000")
        #[arg(short, long)]
        rule: Vec<String>,

        /// Output path for Clean Matrix (e.g. clean.parquet or clean.csv)
        #[arg(long, default_value = "clean.parquet")]
        clean_output: PathBuf,

        /// Output path for Trash Matrix (e.g. trash.parquet or trash.csv)
        #[arg(long, default_value = "trash.parquet")]
        trash_output: PathBuf,

        /// Number of worker threads for parallel filtering (defaults to CPU logical core count)
        #[arg(short, long)]
        threads: Option<usize>,

        /// Explicit Hive partition subfolder filter pattern (e.g. -p "year=2026/month=08")
        #[arg(short, long)]
        partition_filter: Option<String>,
    },

    /// Pack a directory hierarchy and Hive partitions into a single container file (.bazan)
    Pack {
        /// Path to input directory containing data files / Hive partitions
        #[arg(value_name = "DIR")]
        input_dir: PathBuf,

        /// Output .bazan container file path
        #[arg(short, long, default_value = "lakehouse.bazan")]
        output: PathBuf,
    },

    /// Inspect tables, Hive partition entries, row counts & catalog manifest inside a .bazan container file
    Inspect {
        /// Path to .bazan container file
        #[arg(value_name = "FILE")]
        file: PathBuf,
    },

    /// Execute SQL query directly on .bazan container files, Parquet/CSV files, or directory trees
    Sql {
        /// SQL query string (e.g. "SELECT id, salary FROM 'lakehouse.bazan' WHERE age >= 18")
        #[arg(value_name = "QUERY")]
        query: String,

        /// Optional output file path to save SQL query results (e.g. output.parquet or output.csv)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}
