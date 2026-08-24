---
title: Rule Syntax Reference
description: Grammar, operators, and evaluation semantics of dynamic filter rules
icon: material/filter-variant
---

# Rule Syntax Reference

Dynamic rules are plain strings parsed by `FilterRule::parse` (`src/engine/dynamic_filter.rs`):

```text
<column> <operator> <value>
```

## Operators

| Operator | Description | Supported column types |
| :--- | :--- | :--- |
| `>` | Greater than | Int8/16/32/64, UInt8/16/32/64, Float32/64 |
| `>=` | Greater than or equal | same as `>` |
| `<` | Less than | same as `>` |
| `<=` | Less than or equal | same as `>` |
| `==` | Equal | numeric types + `Utf8`, `LargeUtf8` |
| `!=` | Not equal | numeric types + `Utf8`, `LargeUtf8` |

Parsing picks the **first** operator found, longest-first (`>=` before `>`), so whitespace placement is flexible: `"age>=18"` and `"age >= 18"` are equivalent.

## Values

- Numeric rules parse the right-hand side to the column's element type (e.g. `"fare_amount >= 2.5"`, `"passenger_count > 0"`).
- String comparisons may be quoted — both `'N'` and `"N"` have quotes trimmed: `"store_and_fwd_flag == 'N'"`.

## Evaluation Semantics

| Situation | Result for the row |
| :--- | :--- |
| Rule satisfied | bit stays 0 → row remains Clean |
| Rule violated **or value is NULL** | bit set → row goes to Trash with `audit_error_code` |
| Column missing from schema | rule silently skipped (no rows flagged by it) |
| Column type unsupported (dates, decimals, lists…) | rule silently skipped for that batch |
| Value fails to parse into the column's type | rule silently skipped |

Rules never mutate data; they only decide Clean vs Trash membership. See [SIMD Bitmask Kernel](../architecture/simd-kernel.md) for the encoding of violations.
