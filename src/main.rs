use clap::Parser;
use basaltic_red::cli::{Cli, Commands};
use basaltic_red::engine::MatrixEngine;
use anyhow::Result;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);

    match cli.command {
        Commands::SliceRows { file, offset, limit, output } => {
            let file_str = file.to_str().unwrap();
            let batch = engine.slice_rows_native(file_str, offset, limit)?;
            println!("✅ Sliced {} rows from offset {} (Total columns: {})", batch.num_rows(), offset, batch.num_columns());
            
            if let Some(out_path) = output {
                let out_str = out_path.to_str().unwrap();
                println!("💾 Saving output to {}...", out_str);
            }
        }
        Commands::SliceCols { file, cols, offset, limit, output } => {
            let file_str = file.to_str().unwrap();
            let batch = engine.slice_cols_native(file_str, &cols, offset, limit)?;
            println!("✅ Sliced {} rows and {} columns: {:?}", batch.num_rows(), batch.num_columns(), cols);

            if let Some(out_path) = output {
                let out_str = out_path.to_str().unwrap();
                println!("💾 Saving output to {}...", out_str);
            }
        }
        Commands::Split { file, max_rows, output_dir, format } => {
            let file_str = file.to_str().unwrap();
            let out_dir_str = output_dir.to_str().unwrap();
            let parts = engine.split_file_native(file_str, max_rows, out_dir_str, &format)?;
            println!("✅ Split matrix into {} part files in directory '{}'", parts, out_dir_str);
        }
        Commands::Preview { file, limit } => {
            let file_str = file.to_str().unwrap();
            let batch = engine.slice_rows_native(file_str, 0, limit)?;
            println!("🔍 Matrix Preview (First {} rows):\n", limit);
            println!("{:#?}", batch);
        }
        Commands::Dict { file, output } => {
            let file_str = file.to_str().unwrap();
            let batch = engine.slice_rows_native(file_str, 0, 1)?;
            let schema = batch.schema();

            let mut dict = String::from("# Data Dictionary\n\n| Column Name | Data Type | Nullable |\n| :--- | :--- | :--- |\n");
            for field in schema.fields() {
                dict.push_str(&format!("| `{}` | `{}` | `{}` |\n", field.name(), field.data_type(), field.is_nullable()));
            }

            if let Some(out_path) = output {
                std::fs::write(&out_path, &dict)?;
                println!("📄 Data Dictionary exported to {}", out_path.display());
            } else {
                println!("{}", dict);
            }
        }
        Commands::Graph { path, output } => {
            let path_str = path.to_str().unwrap();
            let out_str = output.as_ref().map(|p| p.to_str().unwrap());
            let mermaid = engine.generate_er_graph(path_str, out_str)?;
            if output.is_none() {
                println!("{}", mermaid);
            } else {
                println!("📊 Mermaid ER Diagram saved to {}", output.unwrap().display());
            }
        }
        Commands::Filter { file, rule, clean_output, trash_output, threads } => {
            use basaltic_red::engine::dynamic_filter::FilterRule;
            use basaltic_red::engine::parallel_filter::save_batch_to_file;
            use std::time::Instant;

            let parsed_rules: Vec<FilterRule> = rule.iter()
                .map(|r| FilterRule::parse(r))
                .collect::<anyhow::Result<Vec<_>>>()?;

            let start = Instant::now();
            let summary = engine.filter_files_parallel(&file, &parsed_rules, threads)?;
            let elapsed = start.elapsed();

            if let Some(ref clean_b) = summary.clean_batch {
                save_batch_to_file(clean_b, &clean_output)?;
            }
            if let Some(ref trash_b) = summary.trash_batch {
                save_batch_to_file(trash_b, &trash_output)?;
            }

            println!("⚡ Parallel Multi-Threaded Filter Summary for '{}':", file);
            println!("   Total Files Processed: {}", summary.total_files);
            println!("   Total Rows Evaluated : {}", summary.total_rows);
            println!("   Clean Rows           : {} -> Saved to {}", summary.clean_rows, clean_output.display());
            println!("   Trash Rows           : {} -> Saved to {}", summary.trash_rows, trash_output.display());
            println!("   Execution Time       : {:.2?}", elapsed);
        }
    }

    Ok(())
}

