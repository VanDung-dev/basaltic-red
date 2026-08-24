---
title: Clean & Trash Partitioning
description: Partitioning records with bitwise audit error codes
icon: material/call-split
---

# Clean & Trash Partitioning Workflow

When data quality filtering executes, records are separated into two distinct destination tables:

---

## 1. Clean Table
Contains all records that satisfy **100%** of the validation rules. Retains the exact original schema.

## 2. Trash Table
Contains all anomalous records that violated at least one rule. Automatically augmented with audit columns:
- `audit_error_code` (`UInt64`): Bitmask where the $i$-th bit is $1$ if rule $i$ was violated (first 64 rules).
- `audit_violated_rules` (`List<UInt32>`, only when rules > 64): full list of violated rule indices.

```python
clean_batch, trash_batch = br.filter.filter_matrix("data/sample.parquet", rules=rules)
```

Encoding details: [Audit Error Codes](../reference/audit-codes.md).

## Writing Clean/Trash Trees on Disk

[`br.lake.process_and_write_lake`](../architecture/lakehouse-pipeline.md) splits every Parquet file under a directory into two mirrored Parquet trees using the **static** engine thresholds (not custom rules):

```python
stats = br.lake.process_and_write_lake(
    "input_dir", "output/clean/", "output/trash/",
    partition_filter=None, batch_size=65536,
)
# → (total_files, total_rows, clean_rows, trash_rows)
```

Trash files carry the static `audit_error_code`; output is ZSTD-compressed and preserves relative layout.
