---
title: Lake Doctor & Self-Healing
description: Step-by-step workflow for data lake integrity verification and drift remediation
icon: material/doctor
---

# Lake Doctor & Self-Healing Workflow

## Operational Life Cycle

The Lake Doctor is the mandatory entrypoint and exit point for robust data lake pipelines:

```mermaid
sequenceDiagram
    autonumber
    participant P as Data Pipeline
    participant D as Lake Doctor

    P->>D: entry check — doctor("data", auto_heal=True)
    D-->>P: HEALTHY baseline · catalog ready
    P->>P: ingest · slicing · SIMD filters · SQL
    P->>D: exit check — doctor("data", auto_heal=True)
    D-->>P: 100% integrity · zero orphaned state
```

## Example Code

```python
import basaltic_red as br

# Diagnostic run
status = br.lake.doctor("data", auto_heal=False)
if status["status"] != "HEALTHY":
    print("Drift detected! Healing lake...")
    br.lake.doctor("data", auto_heal=True)
```
