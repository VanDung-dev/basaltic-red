mod gen;
mod progress;
mod writers;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use progress::{Dashboard, ProgressItem};

const ALL_FORMATS: &[&str] = &[
    "csv", "tsv", "psv", "txt", "json", "jsonl", "ndjson", "parquet", "feather", "avro", "xlsx",
    "orc", "msgpack",
];

#[derive(Parser)]
#[command(
    name = "bigdata-gen",
    about = "Generate big datasets in multiple formats",
    disable_help_flag = true
)]
struct Cli {
    #[arg(
        default_value = "all",
        help = "Format(s): csv,tsv,psv,txt,json,jsonl,ndjson,parquet,feather,avro,xlsx,orc,msgpack or all"
    )]
    format: Vec<String>,

    #[arg(
        long = "rows",
        default_value = "100000",
        help = "Number of rows per format"
    )]
    rows: u64,

    #[arg(
        long = "cols",
        default_value = "30",
        help = "Number of columns (default 30, max 100"
    )]
    cols: usize,

    #[arg(long = "output", help = "Output file path (single format only)")]
    output: Option<String>,

    #[arg(
        long = "output-dir",
        default_value = "bigdata_output",
        help = "Output directory for multiple formats"
    )]
    output_dir: String,

    #[arg(long = "seed", default_value = "42", help = "Random seed")]
    seed: u64,

    #[arg(
        short = 'j',
        long = "jobs",
        default_value = "4",
        help = "Max concurrent format generators"
    )]
    jobs: usize,

    #[arg(long = "help", help = "Print help information")]
    help: bool,
}

fn resolve_formats(input: &[String]) -> Vec<String> {
    let expanded: Vec<String> = input
        .iter()
        .flat_map(|s| {
            s.split(',')
                .map(|p| p.trim().to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    let mut out: Vec<String> = Vec::new();
    for f in &expanded {
        match f.as_str() {
            "all" => {
                for item in ALL_FORMATS {
                    if !out.contains(&item.to_string()) {
                        out.push(item.to_string());
                    }
                }
            }
            "arrow" | "ipc" => {
                if !out.contains(&"feather".to_string()) {
                    out.push("feather".to_string());
                }
            }
            other => {
                if !out.contains(&other.to_string()) {
                    out.push(other.to_string());
                }
            }
        }
    }
    out
}

fn print_help() {
    println!(
        r#"
BigData Generator - High-performance synthetic dataset generator

USAGE:
    bigdata-gen [FORMATS...] [OPTIONS]

FORMATS:
    all                          Generate all 13 supported formats
    csv, tsv, psv, txt           Text delimited formats
    json, jsonl, ndjson          JSON formats (Formatted Array, Single-line, LineDelimited)
    parquet, feather, avro, orc  Binary & Columnar formats
    xlsx                         Excel Workbook (max 1M rows)
    msgpack                      MessagePack binary JSON

OPTIONS:
    --rows <NUM>                 Number of rows per format [default: 100000]
    --cols <NUM>                 Number of columns (1..100, default: 30)
                                 Dynamically expands by repeating base schema
    --output <PATH>              Output file path (single format only)
    --output-dir <DIR>           Output directory [default: bigdata_output]
    --seed <NUM>                 Random seed [default: 42]
    -j, --jobs <NUM>             Max concurrent generators [default: 4]
    --help                       Print help information

EXAMPLES:
    bigdata-gen all --rows 10000000 --cols 60 --output-dir ./data
    bigdata-gen csv --rows 1000000 --cols 30 --output data.csv
"#
    );
}

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    if args.help {
        print_help();
        return Ok(());
    }

    let cols = args.cols.clamp(1, 100);

    let fmts = resolve_formats(&args.format);

    let seeds: Vec<u64> = fmts
        .iter()
        .enumerate()
        .map(|(i, _)| args.seed.wrapping_add(i as u64 * 7919))
        .collect();

    // ponytail: cap xlsx to max 1M rows (Excel single-sheet limit)
    let get_rows_for_fmt = |fmt: &str| -> u64 {
        if fmt == "xlsx" {
            args.rows.min(1_000_000)
        } else {
            args.rows
        }
    };

    if fmts.len() == 1 {
        let fmt = &fmts[0];
        let fmt_rows = get_rows_for_fmt(fmt);
        let path = args.output.clone().unwrap_or(format!("bigdata.{}", fmt));
        let item = Arc::new(ProgressItem::new(fmt, fmt_rows));
        let items = vec![item.clone()];

        let dashboard = Dashboard::new(items.clone(), cols);
        dashboard.initial_render();

        let display_item = item.clone();
        let display_handle = std::thread::spawn(move || loop {
            let finished = display_item.finished.load(Ordering::Relaxed);
            let db = Dashboard::new(vec![display_item.clone()], cols);
            db.render();
            if finished {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        });

        if let Err(e) = writers::run_format(fmt, &path, seeds[0], fmt_rows, cols, &item) {
            eprintln!("\n[{}] ERROR: {:#}", fmt, e);
            item.error.store(true, Ordering::Relaxed);
        }
        item.finished.store(true, Ordering::Relaxed);
        display_handle.join().ok();

        print!("\n");
        return Ok(());
    }

    std::fs::create_dir_all(&args.output_dir)?;

    let items: Vec<Arc<ProgressItem>> = fmts
        .iter()
        .map(|f| Arc::new(ProgressItem::new(f, get_rows_for_fmt(f))))
        .collect();

    let dashboard = Dashboard::new(items.clone(), cols);
    dashboard.initial_render();

    let display_items = items.clone();
    let display_handle = std::thread::spawn(move || loop {
        let all_finished = display_items
            .iter()
            .all(|p| p.finished.load(Ordering::Relaxed));
        let db = Dashboard::new(display_items.clone(), cols);
        db.render();
        if all_finished {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    });

    // ponytail: simple bounded worker queue using std mpsc (max concurrency = args.jobs)
    let concurrency = args.jobs.max(1);
    let (tx, rx) =
        std::sync::mpsc::channel::<(String, String, u64, u64, usize, Arc<ProgressItem>)>();
    let rx = Arc::new(std::sync::Mutex::new(rx));

    for (i, fmt) in fmts.iter().enumerate() {
        let path = format!("{}/bigdata.{}", args.output_dir, fmt);
        let fmt_rows = get_rows_for_fmt(fmt);
        tx.send((
            fmt.clone(),
            path,
            seeds[i],
            fmt_rows,
            cols,
            items[i].clone(),
        ))
        .unwrap();
    }

    drop(tx);

    let mut workers = Vec::new();
    for _ in 0..concurrency {
        let rx = rx.clone();
        workers.push(std::thread::spawn(move || {
            while let Ok((fmt, path, seed, rows, cols, progress)) = {
                let lock = rx.lock().unwrap();
                lock.recv()
            } {
                if let Err(e) = writers::run_format(&fmt, &path, seed, rows, cols, &progress) {
                    eprintln!("\n[{}] ERROR: {:#}", fmt, e);
                    progress.error.store(true, Ordering::Relaxed);
                }
                progress.finished.store(true, Ordering::Relaxed);
            }
        }));
    }

    for w in workers {
        let _ = w.join();
    }
    display_handle.join().ok();

    print!("\n");
    Ok(())
}
