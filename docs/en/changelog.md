---
title: Changelog
description: Version history and release notes for basaltic-red
icon: material/history
---

# Changelog

## Unreleased

*Only changes reflected in the current `src/` tree.*

??? note "Improvements (30)"

    * 2026-08-21

        * **Lake Map**: Data-lake indexing with consistency checks, `create_map` / `doctor` Python functions, and zero-copy memory-mapped catalog loading.

    * 2026-08-18

        * **PyAPI**: Introduced `PyBatchIterator` with enhanced SQL engine integration.

    * 2026-08-17

        * **Formats**: Magic-byte file format sniffing capability.

    * 2026-08-16

        * **Formats**: Base templates for delimited reading and row chunking; plugin modules organizing adapters and templates into tiers.

    * 2026-08-15

        * **Engine**: Multi-chunk SIMD bitmasking in `filter_batch_dynamic`; additional hints for non-Parquet data detection.
        * **Formats & PyAPI**: Dynamic custom format handler registration exposed through Python registration functions.

    * 2026-08-12

        * **Engine**: Schema-based row byte estimation and batch sizing derived from the first file's schema.
        * **SQL & PyAPI**: Auto-normalize cache for non-native SQL files; lazy streaming execution for `PyBatchIterator`.

    * 2026-08-11

        * **Engine**: Enhanced memory budgeting with a global Rayon pool; DataFusion native listing table support for file querying; one-time hint when reading non-Parquet files.
        * **Ingest**: Parallelized file ingestion with target conflict handling.

    * 2026-08-10

        * **JSON**: Top-level JSON arrays streamed as bare object streams.
        * **Ingest**: Native directory ingestion with optional normalization to Parquet, exposed through the Python API with a batch-size recommendation module.
        * **Runtime**: Global tokio runtime and unified row batch budgeting.

    * 2026-08-08

        * **Readers**: Column projection pushed into CSV and Parquet readers.
        * **PyAPI**: New pyapi modules with migrated function modules.
        * **Engine**: Matrix data processing performance optimizations.

    * 2026-08-06

        * **SQL**: Streaming execution with iterators.
        * **PyAPI**: Mutex-guarded `PyBatchIterator` source with a PyArrow conversion bridge.
        * **Utils**: Sorted file listing in the discovery utility.

    * 2026-08-05

        * **Security**: CSV injection guard sanitizing dangerous cells.
        * **Formats**: Unified handling behind `FormatHandler::open`; native `orc-rust` reader replacing the Parquet-based path (`orc-rust`, `rust_xlsxwriter` dependencies).
        * **Engine**: `clamp_batch_size` integrated into all handlers; improved input validation for file paths.
        * **Build**: `[lib]` section enabling Python extension and Rust library builds.

    * 2026-08-04

        * **Filtering**: Multi-threaded `filter_files_parallel` with Hive-style partition pruning.

    * 2026-08-03

        * **Engine**: Slicing, filtering, splitting, and ER diagram generation driven by dynamic column rules.

    * 2026-08-02

        * **Formats**: XLSX, Avro, Feather, ORC, and MsgPack processing support with dependencies for these formats.

    * 2026-08-01

        * **Formats**: TXT and PSV file processing support.

    * 2026-07-30

        * **Engine**: `MatrixEngine` core with processing, filtering, and Parquet streaming; `filter_batch_native` batch filtering with error auditing; CSV/TSV/JSON/Parquet and NDJSON/JSONL streaming; sample preview and data dictionary export; audit error-code bitmask constants.
        * **Utils**: Data-file discovery utilities with filtering options.
        * **PyAPI**: PyO3 Python bindings for `MatrixEngine`.

??? warning "Fix (8)"

    * 2026-08-20

        * **Engine**: Hardened file handling and regex initialization.
        * **Engine (IO)**: Reused file handles with reader state reset after schema inference instead of reopening files.
        * **SQL Cache**: Invalidated cached Parquet files when the source modification time is newer than the cache.
        * **PyAPI**: Released the GIL around async blocking calls (`py.detach`) in lazy stream sources.

    * 2026-08-18

        * **Build**: Corrected `PyBatchIterator` module export placement.

    * 2026-08-17

        * **Engine**: Improved error handling and file path resolution.

    * 2026-08-12

        * **Ingest**: Restricted `ingest_normalize` visibility to the crate (`pub(crate)`).

    * 2026-08-11

        * **Engine**: Consistent non-Parquet hints across slice and extension-resolution paths with clearer error messages.
