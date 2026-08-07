#!/usr/bin/env bash
#
# Auto-test for the `bazan` CLI — covers every subcommand listed in README.md:
# slice-rows, slice-cols, split, preview, dict, graph, filter, sql.
#
# Usage:
#   ./tests/cli/test_cli.sh                # uses target/release/bazan
#   BAZAN_BIN=/path/to/bazan ./tests/cli/test_cli.sh
#
# Exit code 0 = all passed, 1 = at least one failed.

set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BAZAN_BIN="${BAZAN_BIN:-$REPO_ROOT/target/release/bazan}"

if [[ ! -x "$BAZAN_BIN" ]]; then
    echo "✗ bazan binary not found at $BAZAN_BIN (run: cargo build --release --bin bazan)" >&2
    exit 1
fi

WORK="$(mktemp -d /tmp/bazan-cli-test.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

PASS=0
FAIL=0

# assert_cmd <name> <expected-substring> -- <cmd...>
assert_cmd() {
    local name="$1" expected="$2"
    shift 3
    local out
    out="$("$@" 2>&1)"
    local rc=$?
    if (( rc != 0 )); then
        echo "✗ $name: command failed (rc=$rc)"
        echo "    $*"
        FAIL=$((FAIL + 1))
    elif [[ "$out" != *"$expected"* ]]; then
        echo "✗ $name: output missing '$expected'"
        echo "    got: $(printf '%s' "$out" | head -1)"
        FAIL=$((FAIL + 1))
    else
        echo "✓ $name"
        PASS=$((PASS + 1))
    fi
}

# assert_file <name> <path>
assert_file() {
    if [[ -f "$2" ]]; then
        echo "✓ $1 ($2)"
        PASS=$((PASS + 1))
    else
        echo "✗ $1: file not created: $2"
        FAIL=$((FAIL + 1))
    fi
}

# --- fixtures ---------------------------------------------------------------
printf 'passenger_count,fare_amount,trip_distance\n1,15.5,2.5\n2,-5.0,0.0\n0,20.0,3.1\n5,100.0,10.0\n12,50.0,1.2\n1,0.0,5.0\n' > taxi.csv
printf 'id,age,salary\n1,25,1000\n2,15,500\n3,30,1200\n4,17,400\n' > people.csv
mkdir -p db && cp people.csv db/data.csv

# --- 1. slice-rows -----------------------------------------------------------
assert_cmd "slice-rows" "Sliced 2 rows from offset 1" -- "$BAZAN_BIN" slice-rows taxi.csv --offset 1 --limit 2

# --- 2. slice-cols -----------------------------------------------------------
assert_cmd "slice-cols" 'Sliced 2 rows and 2 columns' -- "$BAZAN_BIN" slice-cols people.csv --cols id,salary --limit 2

# --- 3. split ----------------------------------------------------------------
assert_cmd "split" "Split matrix into 2 part files" -- "$BAZAN_BIN" split people.csv --max-rows 2 --output-dir parts --format parquet
assert_file "split part 1" parts/people_part_001.parquet
assert_file "split part 2" parts/people_part_002.parquet

# --- 4. preview --------------------------------------------------------------
assert_cmd "preview" "Matrix Preview (First 2 rows)" -- "$BAZAN_BIN" preview taxi.csv --limit 2
assert_cmd "preview columns" "passenger_count" -- "$BAZAN_BIN" preview taxi.csv --limit 2

# --- 5. dict -----------------------------------------------------------------
assert_cmd "dict" "Data Dictionary exported to" -- "$BAZAN_BIN" dict people.csv --output schema.md
assert_file "dict markdown" schema.md

# --- 6. graph ----------------------------------------------------------------
assert_cmd "graph" "Mermaid ER Diagram saved to" -- "$BAZAN_BIN" graph people.csv --output er.md
assert_file "graph mermaid" er.md

# --- 7. filter (parallel, with clean/trash output) ---------------------------
assert_cmd "filter summary" "Clean Rows" -- "$BAZAN_BIN" filter people.csv --rule "age >= 18" --clean-output clean.parquet --trash-output trash.parquet
assert_file "filter clean" clean.parquet
assert_file "filter trash" trash.parquet

# --- 8. filter with hive partition pruning ----------------------------------
mkdir -p lake/year=2026/month=08 lake/year=2025/month=08
cp people.csv lake/year=2026/month=08/a.csv
printf 'id,age,salary\n5,20,999\n' > lake/year=2025/month=08/b.csv
assert_cmd "filter partition-pruned" "Total Files Processed: 1" -- "$BAZAN_BIN" filter lake --rule "age >= 18" -p "year=2026/month=08" --clean-output pp_clean.parquet --trash-output pp_trash.parquet

# --- 9. sql on a file --------------------------------------------------------
assert_cmd "sql" "2 rows × 2 columns" -- "$BAZAN_BIN" sql "SELECT id, salary FROM 'people.csv' WHERE age >= 18 ORDER BY salary DESC"

# --- 10. bad subcommand must exit non-zero ----------------------------------
if "$BAZAN_BIN" no-such-command >/dev/null 2>&1; then
    echo "✗ unknown subcommand: should have exited non-zero"
    FAIL=$((FAIL + 1))
else
    echo "✓ unknown subcommand rejected"
    PASS=$((PASS + 1))
fi

# --- 11. security: injected code must never execute --------------------------
# Canary: "SECBREACH" only appears if a payload actually runs — shell/python/rust
# execution would concatenate `echo -n SEC && echo BREACH` into "SECBREACH".
# As inert text the two halves stay apart, so the marker never shows up in any
# output. Any appearance of SECBREACH = code execution = security breach.
# The check below proves the canary is sound (execution DOES produce the marker).
if [[ "$(echo -n SEC && echo BREACH)" == "SECBREACH" ]]; then
    echo "✓ canary sound: execution would produce SECBREACH"
    PASS=$((PASS + 1))
else
    echo "✗ canary broken: SECBREACH not produced by execution"
    FAIL=$((FAIL + 1))
fi

cat > malicious.csv <<'MAL'
id,payload,note
0,$(echo -n SEC && echo BREACH),cmdsubst
1,`echo -n SEC && echo BREACH`,backtick
2,__import__('os').system("echo -n SEC && echo BREACH"),python
3,=cmd|' /C echo -n SEC && echo BREACH'!A0,formula
4,; echo -n SEC && echo BREACH,semicolon
5,| echo -n SEC && echo BREACH,pipe
6,"println!(""{}{}"", ""SEC"".to_owned() + ""BREACH"")",rust
MAL

# assert_no_marker <name> -- <cmd...>  : rc==0 AND output must not contain the canary
assert_no_marker() {
    local name="$1"
    shift 2
    local out
    out="$("$@" 2>&1)"
    local rc=$?
    if (( rc != 0 )); then
        echo "✗ $name: command failed (rc=$rc)"
        FAIL=$((FAIL + 1))
    elif [[ "$out" == *"SECBREACH"* ]]; then
        echo "✗ $name: CANARY EXECUTED — SECBREACH found in output"
        FAIL=$((FAIL + 1))
    else
        echo "✓ $name: no code execution"
        PASS=$((PASS + 1))
    fi
}

# assert_no_marker_in <name> <path>  : file must exist and contain no canary
assert_no_marker_in() {
    if [[ ! -f "$2" ]]; then
        echo "✗ $1: missing file $2"
        FAIL=$((FAIL + 1))
    elif grep -q "SECBREACH" "$2"; then
        echo "✗ $1: CANARY found in $2"
        FAIL=$((FAIL + 1))
    else
        echo "✓ $1: no canary in $2"
        PASS=$((PASS + 1))
    fi
}

assert_no_marker "preview malicious" -- "$BAZAN_BIN" preview malicious.csv --limit 10
assert_no_marker "slice-rows malicious" -- "$BAZAN_BIN" slice-rows malicious.csv --offset 0 --limit 10
assert_no_marker "slice-cols malicious" -- "$BAZAN_BIN" slice-cols malicious.csv --cols id,payload,note --limit 10
assert_no_marker "dict malicious" -- "$BAZAN_BIN" dict malicious.csv --output msec.md
assert_no_marker_in "dict markdown clean" msec.md
assert_no_marker "graph malicious" -- "$BAZAN_BIN" graph malicious.csv --output gsec.md
assert_no_marker_in "graph mermaid clean" gsec.md
assert_no_marker "filter malicious" -- "$BAZAN_BIN" filter malicious.csv --rule "id > 0" --clean-output sec_clean.csv --trash-output sec_trash.csv
assert_no_marker_in "filter clean csv" sec_clean.csv
assert_no_marker_in "filter trash csv" sec_trash.csv

# --- 12. CSV injection hardening: dangerous cells get a ' prefix on write -----
# Spreadsheet formulas (= + @ and non-numeric -) must be neutralized in CSV output.
printf 'id,payload\n0,=1+1\n1,+2+2\n2,@SUM(A1)\n3,-1+1\n4,-5.0\n5,plain\n' > inj.csv
assert_cmd "split inj" "Split matrix into 3 part files" -- "$BAZAN_BIN" split inj.csv --max-rows 2 --output-dir injparts --format csv
if grep -q "'=1+1" injparts/inj_part_001.csv && grep -q "'+2+2" injparts/inj_part_001.csv \
   && grep -q "'@SUM(A1)" injparts/inj_part_002.csv && grep -q "'-1+1" injparts/inj_part_002.csv \
   && grep -q -- ",-5.0" injparts/inj_part_003.csv && ! grep -q -- "'-5.0" injparts/inj_part_003.csv \
   && grep -q -- ",plain" injparts/inj_part_003.csv; then
    echo "✓ csv injection escaped in split output"
    PASS=$((PASS + 1))
else
    echo "✗ csv injection: dangerous cells not escaped in split output"
    FAIL=$((FAIL + 1))
fi
assert_cmd "sql to csv" "Saved SQL Query Results" -- "$BAZAN_BIN" sql "SELECT payload FROM 'malicious.csv' WHERE id = 3" --output inj_out.csv
if grep -q "'=cmd|" inj_out.csv; then
    echo "✓ csv injection escaped in sql output"
    PASS=$((PASS + 1))
else
    echo "✗ csv injection: formula not escaped in sql output"
    FAIL=$((FAIL + 1))
fi

# --- 13. symlinked dirs must not escape the input scope ----------------------
mkdir -p outside
printf 'id,x\n1,secret\n' > outside/secret.csv
ln -s ../outside lake/link
assert_cmd "filter skips symlink" "Total Files Processed" -- "$BAZAN_BIN" filter lake --rule "age >= 18" --clean-output sym_clean.csv --trash-output sym_trash.csv
if grep -q "secret" sym_clean.csv sym_trash.csv 2>/dev/null; then
    echo "✗ symlink: filter followed a link outside the input scope"
    FAIL=$((FAIL + 1))
else
    echo "✓ symlink: outside files not processed"
    PASS=$((PASS + 1))
fi
rm lake/link

echo
echo "=== bazan CLI: $PASS passed, $FAIL failed ==="
(( FAIL == 0 ))
