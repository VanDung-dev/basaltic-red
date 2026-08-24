---
title: Audit Error Codes & Schema
description: Bitmask encoding and audit columns attached to trash output
icon: material/alert-circle
---

# Audit Error Codes

When dynamic rules run (`filter_matrix`), every Trash row records **which** rules it violated.

## `audit_error_code` — `UInt64`, nullable

Bit *i* is set when rule *i* (0-indexed) was violated:

$$\text{audit\_error\_code} = \sum_{i \in \text{violated},\, i < 64} 2^i$$

Example: code `0b101` (=5) means rules 0 and 2 failed; rule 1 passed.

!!! warning "Only the first 64 rules fit in the UInt64"

    The column stores chunk 0 of the bitmask. With more than 64 rules, violations of rule ≥ 64 are **not** reflected in this column.

## `audit_violated_rules` — `List<UInt32>`, nullable

Added to the Trash schema only when **rules > 64**. Each row holds the full sorted list of violated rule indices across all chunks, e.g. `[0, 2, 71]`.

## Static-Threshold Audit

The static fast path (`process_batch`, `process_file`, `process_and_write_lake`, `preview_sample`) uses three fixed bit flags instead:

| Flag | Bit | Condition |
| :--- | :--- | :--- |
| `ERR_INVALID_PASSENGER` | 1<<0 | `passenger_count` null or outside `[min_passenger, max_passenger]` |
| `ERR_INVALID_FARE` | 1<<1 | `fare_amount` null or below `min_fare` |
| `ERR_INVALID_SPEED` | 1<<2 | fare violation **and** `trip_distance > 0` (distance/fare anomaly) |

The code is computed as a vectorized weighted sum (`p*1 + f*2 + s*4`) and stored as a non-null `UInt64` in static-mode Trash output. Missing columns contribute no violations.

## Clean Table Schema

Unchanged — the Clean table always keeps the exact input schema with no extra columns.
