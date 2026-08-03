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
        Commands::Filter { file, rule, clean_output, trash_output, threads, partition_filter } => {
            use basaltic_red::engine::dynamic_filter::FilterRule;
            use basaltic_red::engine::parallel_filter::save_batch_to_file;
            use std::time::Instant;

            let parsed_rules: Vec<FilterRule> = rule.iter()
                .map(|r| FilterRule::parse(r))
                .collect::<anyhow::Result<Vec<_>>>()?;

            let start = Instant::now();
            let summary = engine.filter_files_parallel(&file, &parsed_rules, partition_filter.as_deref(), threads)?;
            let elapsed = start.elapsed();

            if let Some(ref clean_b) = summary.clean_batch {
                save_batch_to_file(clean_b, &clean_output)?;
            }
            if let Some(ref trash_b) = summary.trash_batch {
                save_batch_to_file(trash_b, &trash_output)?;
            }

            println!("⚡ Parallel & Partition-Pruned Filter Summary for '{}':", file);
            println!("   Total Files Processed: {}", summary.total_files);
            println!("   Pruned Subdirectories: {} (Skipped I/O completely)", summary.pruned_dirs);
            println!("   Total Rows Evaluated : {}", summary.total_rows);
            println!("   Clean Rows           : {} -> Saved to {}", summary.clean_rows, clean_output.display());
            println!("   Trash Rows           : {} -> Saved to {}", summary.trash_rows, trash_output.display());
            println!("   Execution Time       : {:.2?}", elapsed);
        }
        Commands::Pack { input_dir, output } => {
            use std::time::Instant;
            let start = Instant::now();
            let (total_entries, total_bytes) = engine.pack_directory_to_bazan(&input_dir, &output)?;
            let elapsed = start.elapsed();

            println!("📦 Container Pack Completed Successfully!");
            println!("   Input Directory  : {}", input_dir.display());
            println!("   Output Container : {} ({:.2} MB)", output.display(), total_bytes as f64 / 1_048_576.0);
            println!("   Packed Tables/Files: {}", total_entries);
            println!("   Pack Duration    : {:.2?}", elapsed);
        }
        Commands::Inspect { file } => {
            use basaltic_red::engine::container::read_bazan_manifest;
            use std::time::Instant;
            use comfy_table::{Table, Cell, Color, Attribute, ContentArrangement};
            use comfy_table::presets::UTF8_FULL;

            let start = Instant::now();
            let manifest = read_bazan_manifest(&file)?;
            let elapsed = start.elapsed();

            println!("🔍 Inspecting .bazan Container File: '{}'", file.display());
            println!("   Container Spec Version : v{}", manifest.version);
            println!("   Total Packed Entries   : {}", manifest.entries.len());
            println!("   Manifest Read Speed    : {:.2?}\n", elapsed);

            let mut table = Table::new();
            table
                .load_preset(UTF8_FULL)
                .set_content_arrangement(ContentArrangement::Dynamic)
                .set_header(vec![
                    Cell::new("STT").add_attribute(Attribute::Bold).fg(Color::Cyan),
                    Cell::new("Entry Internal Path").add_attribute(Attribute::Bold).fg(Color::Green),
                    Cell::new("Format").add_attribute(Attribute::Bold).fg(Color::Yellow),
                    Cell::new("Rows Count").add_attribute(Attribute::Bold).fg(Color::Magenta),
                    Cell::new("Byte Size").add_attribute(Attribute::Bold).fg(Color::Blue),
                    Cell::new("Byte Offset").add_attribute(Attribute::Bold),
                ]);

            for (idx, entry) in manifest.entries.iter().enumerate() {
                table.add_row(vec![
                    Cell::new(idx + 1),
                    Cell::new(&entry.path),
                    Cell::new(&entry.format),
                    Cell::new(entry.num_rows),
                    Cell::new(format!("{:.2} KB", entry.length as f64 / 1024.0)),
                    Cell::new(entry.offset),
                ]);
            }

            println!("{table}");
        }
    }

    Ok(())
}

