use anyhow::Result;
use basaltic_red::cli::{Cli, Commands};
use basaltic_red::engine::MatrixEngine;
use clap::Parser;
use std::path::Path;

/// Convert a user-supplied path to a UTF-8 string, failing with a clean error
/// instead of panicking on non-UTF8 filenames (possible on Unix).
fn path_str<'a>(p: &'a Path, what: &str) -> anyhow::Result<&'a str> {
    p.to_str()
        .ok_or_else(|| anyhow::anyhow!("{} path is not valid UTF-8: {:?}", what, p))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);

    match cli.command {
        Commands::SliceRows {
            file,
            offset,
            limit,
            output,
        } => {
            let file_str = path_str(&file, "input")?;
            let batch = engine.slice_rows_native(file_str, offset, limit)?;
            println!(
                "✅ Sliced {} rows from offset {} (Total columns: {})",
                batch.num_rows(),
                offset,
                batch.num_columns()
            );

            if let Some(out_path) = output {
                let out_str = path_str(&out_path, "output")?;
                println!("💾 Saving output to {}...", out_str);
            }
        }
        Commands::SliceCols {
            file,
            cols,
            offset,
            limit,
            output,
        } => {
            let file_str = path_str(&file, "input")?;
            let batch = engine.slice_cols_native(file_str, &cols, offset, limit)?;
            println!(
                "✅ Sliced {} rows and {} columns: {:?}",
                batch.num_rows(),
                batch.num_columns(),
                cols
            );

            if let Some(out_path) = output {
                let out_str = path_str(&out_path, "output")?;
                println!("💾 Saving output to {}...", out_str);
            }
        }
        Commands::Split {
            file,
            max_rows,
            output_dir,
            format,
        } => {
            let file_str = path_str(&file, "input")?;
            let out_dir_str = path_str(&output_dir, "output directory")?;
            let parts = engine.split_file_native(file_str, max_rows, out_dir_str, &format)?;
            println!(
                "✅ Split matrix into {} part files in directory '{}'",
                parts, out_dir_str
            );
        }
        Commands::Preview { file, limit } => {
            let file_str = path_str(&file, "input")?;
            let batch = engine.slice_rows_native(file_str, 0, limit)?;
            println!("🔍 Matrix Preview (First {} rows):\n", limit);
            println!("{:#?}", batch);
        }
        Commands::Dict { file, output } => {
            let file_str = path_str(&file, "input")?;
            let batch = engine.slice_rows_native(file_str, 0, 1)?;
            let schema = batch.schema();

            let mut dict = String::from("# Data Dictionary\n\n| Column Name | Data Type | Nullable |\n| :--- | :--- | :--- |\n");
            for field in schema.fields() {
                dict.push_str(&format!(
                    "| `{}` | `{}` | `{}` |\n",
                    field.name(),
                    field.data_type(),
                    field.is_nullable()
                ));
            }

            if let Some(out_path) = output {
                std::fs::write(&out_path, &dict)?;
                println!("📄 Data Dictionary exported to {}", out_path.display());
            } else {
                println!("{}", dict);
            }
        }
        Commands::Graph { path, output } => {
            let input_str = path_str(&path, "input")?;
            let out_str = output.as_ref().map(|p| path_str(p, "output")).transpose()?;
            let mermaid = engine.generate_er_graph(input_str, out_str)?;
            match output {
                None => println!("{}", mermaid),
                Some(p) => println!("📊 Mermaid ER Diagram saved to {}", p.display()),
            }
        }
        Commands::Filter {
            file,
            rule,
            clean_output,
            trash_output,
            threads,
            partition_filter,
        } => {
            use basaltic_red::engine::dynamic_filter::FilterRule;
            use basaltic_red::engine::parallel_filter::save_batch_to_file;
            use std::time::Instant;

            let parsed_rules: Vec<FilterRule> = rule
                .iter()
                .map(|r| FilterRule::parse(r))
                .collect::<Result<Vec<_>, basaltic_red::error::BazanError>>()?;

            let start = Instant::now();
            let summary = engine.filter_files_parallel_native(
                &file,
                &parsed_rules,
                partition_filter.as_deref(),
                threads,
            )?;
            let elapsed = start.elapsed();

            if let Some(ref clean_b) = summary.clean_batch {
                save_batch_to_file(clean_b, &clean_output)?;
            }
            if let Some(ref trash_b) = summary.trash_batch {
                save_batch_to_file(trash_b, &trash_output)?;
            }

            println!(
                "⚡ Parallel & Partition-Pruned Filter Summary for '{}':",
                file
            );
            println!("   Total Files Processed: {}", summary.total_files);
            println!(
                "   Pruned Subdirectories: {} (Skipped I/O completely)",
                summary.pruned_dirs
            );
            println!("   Total Rows Evaluated : {}", summary.total_rows);
            println!(
                "   Clean Rows           : {} -> Saved to {}",
                summary.clean_rows,
                clean_output.display()
            );
            println!(
                "   Trash Rows           : {} -> Saved to {}",
                summary.trash_rows,
                trash_output.display()
            );
            println!("   Execution Time       : {:.2?}", elapsed);
        }
        Commands::Sql { query, output } => {
            use basaltic_red::engine::parallel_filter::save_batch_to_file;
            use comfy_table::presets::UTF8_FULL;
            use comfy_table::{Attribute, Cell, Color, ContentArrangement, Table};
            use std::time::Instant;

            let start = Instant::now();
            let batch = engine.execute_sql(&query).await?;
            let elapsed = start.elapsed();

            println!("🏛️ Apache DataFusion SQL Engine Execution Summary:");
            println!("   Executed Query   : \"{}\"", query);
            println!(
                "   Result Shape     : {} rows × {} columns",
                batch.num_rows(),
                batch.num_columns()
            );
            println!("   Execution Time   : {:.2?}\n", elapsed);

            let schema = batch.schema();
            let mut table = Table::new();
            table
                .load_preset(UTF8_FULL)
                .set_content_arrangement(ContentArrangement::Dynamic);

            let mut header_cells = Vec::new();
            for field in schema.fields() {
                header_cells.push(
                    Cell::new(field.name())
                        .add_attribute(Attribute::Bold)
                        .fg(Color::Cyan),
                );
            }
            table.set_header(header_cells);

            // Render up to first 50 rows for clean CLI preview
            let preview_rows = batch.num_rows().min(50);
            for row_idx in 0..preview_rows {
                let mut row_cells = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let val_str = arrow::util::display::array_value_to_string(col, row_idx)
                        .unwrap_or_else(|_| "NULL".to_string());
                    row_cells.push(Cell::new(val_str));
                }
                table.add_row(row_cells);
            }

            println!("{table}");
            if batch.num_rows() > 50 {
                println!("\n💡 Displaying first 50 rows of {}. Export to file using --output for full results.", batch.num_rows());
            }

            if let Some(out_path) = output {
                save_batch_to_file(&batch, &out_path)?;
                println!(
                    "\n💾 Saved SQL Query Results ({}) to {}",
                    batch.num_rows(),
                    out_path.display()
                );
            }
        }
    }

    Ok(())
}
