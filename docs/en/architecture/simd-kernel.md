---
title: SIMD Bitmask Kernel
description: Zero-allocation multi-chunk bitmask validation kernel and audit column encoding
icon: material/cpu-64-bit
---

# Multi-Chunk Bitmask Kernel

The dynamic quality filter lives in `src/engine/dynamic_filter.rs`. It evaluates N arbitrary rules over a full `RecordBatch` **without intermediate boolean arrays**, writing results into raw `u64` bitmasks in place. Loops are plain Rust over Arrow arrays; LLVM auto-vectorizes the hot paths (no hand-written SIMD intrinsics).

---

## Rule Parsing

`FilterRule::parse()` splits each rule string on the first operator found (longest-first so `>=` wins over `>`):

```text
"<column> <op> <value>"   op ∈ { >=, <=, ==, !=, >, < }
```

- Values may be quoted (`'` or `"`) — quotes are trimmed before parsing.
- Numeric comparisons parse the right-hand side to the column's element type.
- String columns (`Utf8`, `LargeUtf8`) compare lexicographically.

## Evaluation Loop

Each rule owns one bit position: rule *i* → chunk `i / 64`, bit `i % 64`. Rows violating any rule get that bit set and are marked not-clean:

```rust
for (rule_idx, rule) in rules.iter().enumerate() {
    let chunk_idx = rule_idx / 64;
    let bit = 1u64 << (rule_idx % 64);
    // per-column typed loop writes directly into error_chunks_raw[chunk_idx]
}
```

Properties:

- **Zero allocation in the inner loop** — masks are preallocated once per batch (`Vec<Vec<u64>>`).
- **Multi-chunk scaling** — `num_chunks = ceil(rules / 64)`; 1 or 500+ rules work identically.
- **NULL handling** — null cells always fail the rule (row goes to Trash).
- **Unknown column or unsupported dtype** — the rule is silently skipped for that batch (no rows flagged by it).

Supported column types: `Int8/16/32/64`, `UInt8/16/32/64`, `Float32/64`, `Utf8`, `LargeUtf8`.

---

## Audit Columns on the Trash Table

After evaluation, rows are split with `filter_record_batch` and the Trash batch is augmented:

| Column | Type | Present when |
| :--- | :--- | :--- |
| `audit_error_code` | `UInt64` | always — bitmask of violated rules **0–63** (first chunk) |
| `audit_violated_rules` | `List<UInt32>` | only when **rules > 64** — list of all violated rule indices |

Decoding `audit_error_code`: bit *i* set ⇔ rule *i* was violated, e.g. code `0b101` means rules 0 and 2 failed. For rule sets beyond 64 rules, read `audit_violated_rules` instead of relying on the single `UInt64`.

Full encoding details: [Audit Error Codes reference](../reference/audit-codes.md).
