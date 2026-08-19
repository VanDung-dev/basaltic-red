use std::sync::Arc;
use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use basaltic_red::engine::dynamic_filter::FilterRule;
use basaltic_red::engine::formats::{open_delimited_csv, open_parquet};
use basaltic_red::engine::partition::{matches_partition_rules, parse_path_partitions};
use basaltic_red::engine::MatrixEngine;

fn generate_synthetic_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("passenger_count", DataType::Int64, true),
        Field::new("fare_amount", DataType::Float64, true),
        Field::new("trip_distance", DataType::Float64, true),
        Field::new("vendor_id", DataType::Utf8, true),
    ]));

    let mut passengers = Vec::with_capacity(num_rows);
    let mut fares = Vec::with_capacity(num_rows);
    let mut distances = Vec::with_capacity(num_rows);
    let mut vendors = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        passengers.push((i % 12) as i64); // 0..11 (some invalid if min=1, max=9)
        fares.push(if i % 10 == 0 { -5.0 } else { 12.5 + (i % 50) as f64 });
        distances.push(1.5 + (i % 20) as f64);
        vendors.push(if i % 2 == 0 { "VTS" } else { "CMT" });
    }

    let p_arr = Arc::new(Int64Array::from(passengers));
    let f_arr = Arc::new(Float64Array::from(fares));
    let d_arr = Arc::new(Float64Array::from(distances));
    let v_arr = Arc::new(StringArray::from(vendors));

    RecordBatch::try_new(schema, vec![p_arr, f_arr, d_arr, v_arr]).unwrap()
}

fn bench_filter_batch_native(c: &mut Criterion) {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let mut group = c.benchmark_group("SIMD Static Filter");

    for &size in &[10_000, 100_000, 500_000] {
        let batch = generate_synthetic_batch(size);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &batch, |b, batch| {
            b.iter(|| {
                let (clean, trash) = engine.filter_batch_native(black_box(batch), size);
                black_box((clean.num_rows(), trash.num_rows()));
            });
        });
    }
    group.finish();
}

fn bench_filter_batch_dynamic(c: &mut Criterion) {
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    let mut group = c.benchmark_group("SIMD Dynamic Filter");

    let size = 100_000;
    let batch = generate_synthetic_batch(size);

    // Rule set 1: 5 simple rules (< 64 rules)
    let rules_small = vec![
        FilterRule::parse("passenger_count >= 1").unwrap(),
        FilterRule::parse("passenger_count <= 9").unwrap(),
        FilterRule::parse("fare_amount >= 0.01").unwrap(),
        FilterRule::parse("trip_distance > 0.0").unwrap(),
        FilterRule::parse("vendor_id == 'VTS'").unwrap(),
    ];

    group.throughput(Throughput::Elements(size as u64));
    group.bench_function("5 rules (< 64)", |b| {
        b.iter(|| {
            let res = engine.filter_batch_dynamic(black_box(&batch), black_box(&rules_small));
            black_box(res.unwrap());
        });
    });

    // Rule set 2: 70 rules (> 64 rules multi-chunk bitmasking)
    let mut rules_large = Vec::new();
    for i in 0..70 {
        if i % 2 == 0 {
            rules_large.push(FilterRule::parse(&format!("passenger_count >= {}", i % 5)).unwrap());
        } else {
            rules_large.push(FilterRule::parse(&format!("fare_amount >= {:.2}", (i % 20) as f64)).unwrap());
        }
    }

    group.bench_function("70 rules (> 64 multi-chunk)", |b| {
        b.iter(|| {
            let res = engine.filter_batch_dynamic(black_box(&batch), black_box(&rules_large));
            black_box(res.unwrap());
        });
    });

    group.finish();
}

fn bench_partition_evaluation(c: &mut Criterion) {
    let mut group = c.benchmark_group("Partition Pruning");

    let path = std::path::Path::new("data/lake/year=2026/month=08/day=04/vendor=VTS/file_001.parquet");
    let rules = vec![
        FilterRule::parse("year == '2026'").unwrap(),
        FilterRule::parse("month >= '05'").unwrap(),
        FilterRule::parse("day <= '20'").unwrap(),
    ];

    group.bench_function("parse_and_match_partitions", |b| {
        b.iter(|| {
            let partitions = parse_path_partitions(black_box(path));
            let matches = matches_partition_rules(black_box(&partitions), black_box(&rules));
            black_box(matches);
        });
    });

    group.finish();
}

fn bench_streaming_io(c: &mut Criterion) {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let mut group = c.benchmark_group("Streaming IO");
    let size = 50_000;
    let batch = generate_synthetic_batch(size);

    let temp_dir = tempfile::tempdir().unwrap();
    let parquet_path = temp_dir.path().join("bench_sample.parquet");
    let csv_path = temp_dir.path().join("bench_sample.csv");

    // Write Parquet file
    {
        let file = std::fs::File::create(&parquet_path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    // Write CSV file
    {
        let file = std::fs::File::create(&csv_path).unwrap();
        let mut writer = arrow_csv::WriterBuilder::new().with_header(true).build(file);
        writer.write(&batch).unwrap();
    }

    group.throughput(Throughput::Elements(size as u64));

    let parquet_str = parquet_path.to_string_lossy().to_string();
    group.bench_function("parquet_streaming_open_and_consume", |b| {
        b.iter(|| {
            let src = open_parquet(black_box(&parquet_str), 10_000).unwrap();
            let mut total = 0usize;
            for batch in src.batches {
                total += batch.unwrap().num_rows();
            }
            black_box(total);
        });
    });

    let csv_str = csv_path.to_string_lossy().to_string();
    group.bench_function("csv_streaming_open_and_consume", |b| {
        b.iter(|| {
            let src = open_delimited_csv(black_box(&csv_str), 10_000, b',').unwrap();
            let mut total = 0usize;
            for batch in src.batches {
                total += batch.unwrap().num_rows();
            }
            black_box(total);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_filter_batch_native,
    bench_filter_batch_dynamic,
    bench_partition_evaluation,
    bench_streaming_io
);
criterion_main!(benches);
