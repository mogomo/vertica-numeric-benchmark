#!/usr/bin/env bash
# =============================================================================
# DISCLAIMER — READ BEFORE RUNNING
# =============================================================================
# Provided AS IS, without warranty of any kind. The author and contributors
# accept no responsibility or liability for any damage, data loss, downtime,
# or other consequence arising from use of this script.
#
# Run only in a QA / development / dedicated benchmark environment, never
# against production. The script will:
#   * Create and DROP tables inside the target schema (default: num_bench)
#     without prompting. Any pre-existing table whose name collides with one
#     the script uses WILL be dropped. Use a dedicated schema.
#   * Generate sustained heavy CPU, memory, disk, and network load on every
#     node for the entire run (~3 hours at default scale). Concurrent
#     workloads on the same cluster will be slowed and may fail.
#   * Modify cluster-wide data collector retention via
#     set_data_collector_policy() and set_data_collector_time_policy().
#     These changes persist after the script ends.
#
# Before running: confirm the database/schema with --status, confirm
# sufficient free disk per node, confirm no critical workloads are active,
# and read the source. You assume all risk.
# =============================================================================

# =============================================================================
# bench_numeric.sh
#
# Reproducible Vertica NUMERIC benchmark + sanity-check suite.
#
# Measures storage and performance of NUMERIC(18), NUMERIC(19), NUMERIC(28),
# NUMERIC(38), and FLOAT under two data shapes (bounded and high-entropy)
# and two projection sort orders (favorable and typical-production), then
# runs eight sanity checks that verify the methodology and reproduce key
# correctness findings about SUM and AVG.
#
# Companion to the field guide "When NUMERIC(18) Isn't Enough."
# Repo: https://github.com/mogomo/vertica-numeric-benchmark
#
# =============================================================================
# QUICK START
# =============================================================================
#
#   ./bench_numeric.sh --rows 2000000000 --iter 5 --phase all
#
#     --rows N         total fact rows per typed table (default 2,000,000,000)
#     --iter K         iterations per (op, type) for performance, drops i1
#                      (default 5; use 3 for faster runs)
#     --phase X        which phases to run (default all)
#                        all                run everything start to finish
#                        N                  run phase N (e.g. 1, 2, 3)
#                        Na                 run sub-phase (e.g. 3a, 3b)
#                        N-M                run phase range (e.g. 3-5)
#                        N,M,P              run specific phases (e.g. 5,7,8)
#     --status         show what's already built and exit
#     --force          rebuild tables even if they already exist at target rows
#     --keep           don't drop temporary tables at end
#     --schema NAME    schema name (default num_bench)
#     --db NAME        database name (default $VSQL_DATABASE if set)
#     --user NAME      vsql username (default dbadmin)
#
# =============================================================================
# PHASES
# =============================================================================
#
#   0   Setup (schema, data collector). Free, always runs when in scope.
#   1   Build seq_n (id sequence at target row count).
#   2   Build bounded_src (5M distinct, repeated) and hientropy_src.
#   3   Build typed fact tables (8 tables, one sub-phase each):
#         3a b_n18    3b b_n19    3c b_n28    3d b_n38    3e b_float
#         3f h_n18    3g h_n19    3h h_n38    3i h_float
#       NUMERIC(28) is included so the in-band cost check (phase 8c) can run.
#   4   Build matched-type dimension and insert-source tables.
#   5   Storage measurement (column_storage, both shapes).
#   6   Performance benchmark (scan/groupby/join + INSERT, all 4 types).
#   7   Extract per-iteration timings and compute medians.
#   8   Sanity checks:
#         8a Encoding + per-node row balance + config parameters
#         8b Sort-order sensitivity (build b_n19_unsorted, compare bytes/row)
#         8c In-band cost (interleaved n19/n28/n38 at 5 iter)
#         8d Mixed sort ORDER BY (id, k)
#         8e Scale validation - smaller-scale repeat of 8b
#         8f SUM silent-overflow reproduction (100M rows, NUMERIC(18))
#         8g AVG digit cliff at 14/15/16/17/18 digits
#         8h Final consolidated report
#
# =============================================================================
# DISK BUDGET (5-node cluster)
# =============================================================================
#
# At 2B rows: ~120 GB cluster-wide num_bench at peak (24 GB per node).
# At 1B rows: ~60 GB cluster-wide.
# At 500M rows: ~30 GB cluster-wide.
# Plus working space for chunked INSERT mergeout (~30 GB peak per node at 2B).
# Sanity check tables add ~25 GB cluster-wide at 2B scale.
#
# Plan for 200 GB free per node at 2B; less at smaller scales.
#
# =============================================================================
# RUNTIME (5-node cluster, 16 GB RAM/node, version 26.1)
# =============================================================================
#
# At 2B rows with --iter 5: approximately 2.5 - 3 hours total.
#   Phase 1   ~3 min          Phase 6   ~30 min
#   Phase 2   ~25 min         Phase 7   instant
#   Phase 3   ~70 min         Phase 8   ~45 min
#   Phase 4   ~10 min
#   Phase 5   instant
#
# At 1B rows: roughly half. At 500M: roughly a quarter.
#
# =============================================================================
# RESTART AFTER A FAILURE
# =============================================================================
#
# Each table-build phase checks if the output already exists at the target
# row count; if so, it skips the build. So if a phase fails partway through,
# fix the cause (most commonly disk pressure or transient resource contention)
# then re-run the same command. The script will skip what's already built
# and pick up where it left off.
#
# To rebuild a specific table from scratch, use --force on its sub-phase:
#   ./bench_numeric.sh --force --phase 3c
#
# =============================================================================
# OUTPUT
# =============================================================================
#
# All log output goes to stdout and to bench_numeric.log (cumulative).
# The final phase-8h report summarizes all findings in a single block
# you can paste into a GitHub issue or your team chat.
#
# Key intermediate outputs:
#   - Phase 5 prints bytes/row per (table, type)
#   - Phase 7 prints median ms per (op, type) and ratios vs NUMERIC(18)
#   - Each phase-8 sub-phase prints its own findings
#
# =============================================================================
set -euo pipefail

# --- Configuration ---------------------------------------------------------
VSQL_USER=${VSQL_USER:-dbadmin}
VSQL_PASSWORD=${VSQL_PASSWORD:-}
VSQL_DATABASE=${VSQL_DATABASE:-}
SCHEMA=${SCHEMA:-num_bench}
LOGFILE=${LOGFILE:-bench_numeric.log}

BENCH_ROWS_DEFAULT=2000000000
BENCH_ROWS=${BENCH_ROWS:-$BENCH_ROWS_DEFAULT}
ITERATIONS=${ITERATIONS:-5}

# Insert chunk size for memory safety on the high-entropy + wide-numeric
# combination (n38, FLOAT). Single-shot 2B-row INSERTs can OOM small clusters.
# Default 500M-row chunks fit comfortably in 16 GB nodes.
CHUNK_SIZE=${CHUNK_SIZE:-500000000}

# Derived parameters (scaled from BENCH_ROWS)
DISTINCT_COUNT=$(( BENCH_ROWS / 100 ))     # bounded shape: ~100 rows per distinct value
DIM_ROWS=$(( BENCH_ROWS / 100 ))           # dim has ~1% of fact rows
INSERT_ROWS=$(( BENCH_ROWS / 5 ))          # insert source is 20% of fact rows

# Argument parsing
PHASE_ARG=""
FORCE=false
STATUS_ONLY=false
KEEP=false

while [ $# -gt 0 ]; do
    case "$1" in
        --phase)   PHASE_ARG="$2"; shift 2;;
        --rows)    BENCH_ROWS="$2"; shift 2;;
        --iter)    ITERATIONS="$2"; shift 2;;
        --schema)  SCHEMA="$2"; shift 2;;
        --db)      VSQL_DATABASE="$2"; shift 2;;
        --user)    VSQL_USER="$2"; shift 2;;
        --status)  STATUS_ONLY=true; shift;;
        --force)   FORCE=true; shift;;
        --keep)    KEEP=true; shift;;
        -h|--help) sed -n '3,60p' "$0"; exit 0;;
        *) echo "Unknown argument: $1. Run with -h for help." >&2; exit 1;;
    esac
done

# Re-derive parameters if --rows was passed
DISTINCT_COUNT=$(( BENCH_ROWS / 100 ))
DIM_ROWS=$(( BENCH_ROWS / 100 ))
INSERT_ROWS=$(( BENCH_ROWS / 5 ))

# Phase resolution: convert --phase argument into list of sub-phase IDs to run
ALL_SUBPHASES="0 1 2 3a 3b 3c 3d 3e 3f 3g 3h 3i 4 5 6 7 8a 8b 8c 8d 8e 8f 8g 8h"

resolve_phases() {
    local arg="$1"
    if [ -z "$arg" ] || [ "$arg" = "all" ]; then
        echo "$ALL_SUBPHASES"; return
    fi
    if [[ "$arg" == *","* ]]; then
        local out=""
        IFS=',' read -ra parts <<< "$arg"
        for p in "${parts[@]}"; do
            out="$out $(resolve_phases "$p")"
        done
        echo "$out" | tr -s ' '
        return
    fi
    if [[ "$arg" == *"-"* ]]; then
        local start="${arg%-*}"
        local end="${arg#*-}"
        local out=""
        local in_range=false
        local end_main_seen=false
        for sp in $ALL_SUBPHASES; do
            local main_phase="${sp%[a-z]}"
            if [ "$main_phase" = "$start" ] || [ "$sp" = "$start" ]; then
                in_range=true
            fi
            if [[ "$end" =~ ^[0-9]+$ ]] && $end_main_seen && [ "$main_phase" != "$end" ]; then
                in_range=false
            fi
            if $in_range; then
                out="$out $sp"
            fi
            if [[ "$end" =~ ^[0-9]+$ ]] && [ "$main_phase" = "$end" ]; then
                end_main_seen=true
            fi
            if [[ "$end" =~ [a-z]$ ]] && [ "$sp" = "$end" ]; then
                in_range=false
            fi
        done
        echo "$out" | tr -s ' '
        return
    fi
    if [[ "$arg" =~ ^[0-9]+$ ]]; then
        local out=""
        for sp in $ALL_SUBPHASES; do
            local main_phase="${sp%[a-z]}"
            if [ "$main_phase" = "$arg" ]; then
                out="$out $sp"
            fi
        done
        echo "$out" | tr -s ' '
        return
    fi
    echo "$arg"
}

PHASES_TO_RUN="$(resolve_phases "$PHASE_ARG")"

# Generate a unique run ID so PROFILE labels don't collide across runs
TEST_ID="$(date +'%y%m%d%H%M%S')"
LBL="bn_${TEST_ID}"

# --- vsql wrappers ---------------------------------------------------------
# vrun:   silent execution (errors surface, NOTICEs filtered out)
# vshow:  formatted output (for tables the user wants to see)
# vquery: scalar result (one cell, returned via -At)

VSQL_OPTS="-U $VSQL_USER -w $VSQL_PASSWORD"
if [ -n "$VSQL_DATABASE" ]; then VSQL_OPTS="$VSQL_OPTS -d $VSQL_DATABASE"; fi

vrun() {
    vsql $VSQL_OPTS -X -q -v ON_ERROR_STOP=1 -f - \
         > /dev/null \
         2> >(grep -v -E 'NOTICE|^HINT:|INFO [0-9]+' >&2 || true)
}

vshow() {
    vsql $VSQL_OPTS -X -v ON_ERROR_STOP=1 -f -
}

vquery() {
    vsql $VSQL_OPTS -X -At -c "$1"
}

log() { echo "[$(date +'%H:%M:%S')] $*" | tee -a "$LOGFILE"; }
phase() {
    echo "" | tee -a "$LOGFILE"
    echo "=========================================================" | tee -a "$LOGFILE"
    log "$*"
    echo "=========================================================" | tee -a "$LOGFILE"
}

# Check whether a table exists with at least min_rows rows
table_ready() {
    local tname="$1"
    local min_rows="$2"
    local exists=$(vquery "SELECT COUNT(*) FROM v_catalog.tables WHERE table_schema='${SCHEMA}' AND table_name='${tname}';" 2>/dev/null || echo 0)
    if [ "$exists" != "1" ]; then return 1; fi
    local rows=$(vquery "SELECT COUNT(*) FROM ${SCHEMA}.${tname};" 2>/dev/null || echo 0)
    if [ "$rows" -lt "$min_rows" ]; then return 1; fi
    return 0
}

# Decide whether a sub-phase should run.
# Args: <sub-phase ID> [<output-table> <expected-min-rows>]
should_run() {
    local sp="$1"
    local tname="${2:-}"
    local min_rows="${3:-1}"

    if ! echo " $PHASES_TO_RUN " | grep -q " $sp "; then
        return 1
    fi
    if [ "$FORCE" = "true" ]; then
        return 0
    fi
    if [ -n "$tname" ] && table_ready "$tname" "$min_rows"; then
        log "  Skipping ${sp}: ${tname} already exists with at least ${min_rows} rows. Use --force to rebuild."
        return 1
    fi
    return 0
}

# Run a single PROFILE'd query with cold cache, label tagged for later extraction
profile_one() {
    local lbl_suffix="$1"
    local sql="$2"
    local full_label="${LBL}_${lbl_suffix}"
    vrun <<EOF
SELECT CLEAR_CACHES();
PROFILE ${sql/__LABEL__/$full_label} ;
EOF
}

# Compute a/b safely; return "N/A" if either operand is empty/zero
safe_ratio() {
    local num="$1" den="$2" prec="${3:-3}"
    if [ -n "$num" ] && [ -n "$den" ] && [ "$den" != "0" ]; then
        awk "BEGIN { printf \"%.${prec}f\", ${num} / ${den} }"
    else
        echo "N/A"
    fi
}

# Build a fact table from a source, using chunked INSERTs for memory safety.
# Args: target_table source_table coltype select_expression order_by_clause segmented_by_col [target_rows]
build_chunked() {
    local target="$1"
    local src="$2"
    local coltype="$3"
    local select_expr="$4"
    local order_by="$5"
    local seg_col="$6"
    local target_rows="${7:-$BENCH_ROWS}"
    local n_chunks=$(( (target_rows + CHUNK_SIZE - 1) / CHUNK_SIZE ))

    log "  Building ${SCHEMA}.${target} (${coltype}, ${target_rows} rows, ${n_chunks} chunks of up to ${CHUNK_SIZE})..."
    vrun <<EOF
SET SEARCH_PATH = ${SCHEMA}, public;
DROP TABLE IF EXISTS ${target} CASCADE;
CREATE TABLE ${target} (
    id INT NOT NULL,
    k  ${coltype} NOT NULL,
    v  INT NOT NULL
)
ORDER BY ${order_by} SEGMENTED BY HASH(${seg_col}) ALL NODES;
EOF

    for chunk in $(seq 1 "$n_chunks"); do
        local low=$(( (chunk - 1) * CHUNK_SIZE + 1 ))
        local high=$(( chunk * CHUNK_SIZE ))
        if [ "$high" -gt "$target_rows" ]; then high="$target_rows"; fi
        log "    Chunk ${chunk}/${n_chunks}: id in [${low}, ${high}]..."
        vrun <<EOF
SET SEARCH_PATH = ${SCHEMA}, public;
INSERT /*+DIRECT*/ INTO ${target}
SELECT id, ${select_expr}, v FROM ${src} WHERE id BETWEEN ${low} AND ${high};
COMMIT;
EOF
    done

    log "    Final mergeout + statistics..."
    vrun <<EOF
SELECT ANALYZE_STATISTICS('${SCHEMA}.${target}');
SELECT DO_TM_TASK('mergeout', '${SCHEMA}.${target}');
EOF
    log "  ${target}: $(vquery "SELECT COUNT(*) FROM ${SCHEMA}.${target};") rows"
}

# Status mode prints what's currently built and exits
if [ "$STATUS_ONLY" = "true" ]; then
    echo "=== ${SCHEMA} schema status ==="
    vshow <<EOF
SELECT
    anchor_table_name AS table_name,
    SUM(row_count) AS rows,
    ROUND(SUM(used_bytes)/1024.0/1024/1024, 2) AS gb_cluster_wide
FROM v_monitor.column_storage
WHERE anchor_table_schema = '${SCHEMA}'
GROUP BY anchor_table_name
ORDER BY anchor_table_name;
EOF
    exit 0
fi

# --- Pre-flight ------------------------------------------------------------
date > "$LOGFILE"
log "bench_numeric.sh starting"
log "Test ID: ${TEST_ID}"
log "Database: ${VSQL_DATABASE:-<default>}, schema: ${SCHEMA}, user: ${VSQL_USER}"
log "Scale: ${BENCH_ROWS} fact rows, ${DISTINCT_COUNT} distinct values, ${ITERATIONS} iterations per (op, type)"
log "Chunk size for memory-safe builds: ${CHUNK_SIZE}"
log "Phases requested: ${PHASE_ARG:-all} (resolved: ${PHASES_TO_RUN})"
log "Force rebuild: ${FORCE}, keep temp tables: ${KEEP}"

VERSION=$(vquery "SELECT VERSION();" 2>/dev/null || echo "")
if [ -z "$VERSION" ]; then
    echo "ERROR: cannot reach Vertica via vsql." >&2
    echo "Check VSQL_USER / VSQL_PASSWORD / VSQL_DATABASE environment variables, or pass --user --db." >&2
    exit 1
fi
log "Vertica: ${VERSION}"

# v_catalog.columns is used as a row generator for seq_n. Confirm it's big enough.
# The cross-join is 3-way, so each factor needs to be at least cbrt(BENCH_ROWS).
COL_COUNT=$(vquery "SELECT COUNT(*) FROM v_catalog.columns;")
SEED_NEEDED=$(awk "BEGIN { printf \"%d\", (${BENCH_ROWS}) ^ (1/3) + 100 }")
if [ "$COL_COUNT" -lt "$SEED_NEEDED" ]; then
    log "WARN: v_catalog.columns has only ${COL_COUNT} rows; need at least ${SEED_NEEDED} per cross-join factor for the seq_n build."
else
    log "v_catalog.columns row count: ${COL_COUNT} (sufficient: need ${SEED_NEEDED} per factor for 3-way cross-join)"
fi

# Result tracking for the final report
declare -A FINDINGS

# ===========================================================================
# PHASE 0: Setup
# ===========================================================================
if echo " $PHASES_TO_RUN " | grep -q " 0 "; then
    phase "PHASE 0: Schema and data-collector setup"
    vrun <<EOF
CREATE SCHEMA IF NOT EXISTS ${SCHEMA};
SET SEARCH_PATH = ${SCHEMA}, public;
SELECT set_config_parameter('MaxDataCollectorFileSize','1000000000');
SELECT set_data_collector_policy('ExecutionEngineProfiles','2000','100000000');
SELECT set_data_collector_time_policy('RequestsIssued',  '7 days'::interval);
SELECT set_data_collector_time_policy('QueryExecutions', '7 days'::interval);
SELECT /*+LABEL(${LBL}_run_start)*/ '${LBL}_run_start' AS marker FROM dual;
EOF
    log "Setup complete."
fi

# ===========================================================================
# PHASE 1: id sequence (seq_n)
# ===========================================================================
if should_run "1" "seq_n" "$BENCH_ROWS"; then
    phase "PHASE 1: Build seq_n (${BENCH_ROWS}-row id sequence)"

    # Determine cross-join factors from BENCH_ROWS. Use cube-root-ish split.
    SEED_A=$(awk "BEGIN { printf \"%d\", int(${BENCH_ROWS} ^ (1/3)) + 1 }")
    SEED_B=$SEED_A
    SEED_C=$(( BENCH_ROWS / SEED_A / SEED_B + 10 ))
    log "  Cross-join factors: ${SEED_A} x ${SEED_B} x ${SEED_C} = $((SEED_A * SEED_B * SEED_C)) rows, capped at ${BENCH_ROWS}"

    if [ "$SEED_A" -gt "$COL_COUNT" ] || [ "$SEED_B" -gt "$COL_COUNT" ] || [ "$SEED_C" -gt "$COL_COUNT" ]; then
        echo "ERROR: row generator factor exceeds v_catalog.columns size (${COL_COUNT})." >&2
        echo "       Lower --rows or use a larger generator." >&2
        exit 1
    fi

    vrun <<EOF
SET SEARCH_PATH = ${SCHEMA}, public;
DROP TABLE IF EXISTS seq_n CASCADE;
CREATE TABLE seq_n (i INT NOT NULL)
    ORDER BY i SEGMENTED BY HASH(i) ALL NODES;

INSERT /*+DIRECT*/ INTO seq_n
SELECT ROW_NUMBER() OVER ()
FROM      (SELECT 1 AS x FROM v_catalog.columns LIMIT ${SEED_A}) a
CROSS JOIN (SELECT 1 AS x FROM v_catalog.columns LIMIT ${SEED_B}) b
CROSS JOIN (SELECT 1 AS x FROM v_catalog.columns LIMIT ${SEED_C}) c
LIMIT ${BENCH_ROWS};
COMMIT;
SELECT ANALYZE_STATISTICS('${SCHEMA}.seq_n');
EOF
    log "  seq_n: $(vquery "SELECT COUNT(*) FROM ${SCHEMA}.seq_n;") rows"
fi

# ===========================================================================
# PHASE 2: source data tables (bounded + high-entropy)
# ===========================================================================
if should_run "2" "bounded_src" "$BENCH_ROWS"; then
    phase "PHASE 2a: Build bounded_src (${DISTINCT_COUNT} distinct values, repeated)"
    vrun <<EOF
SET SEARCH_PATH = ${SCHEMA}, public;
DROP TABLE IF EXISTS bounded_src CASCADE;
CREATE TABLE bounded_src (
    id INT          NOT NULL,
    k  NUMERIC(18,0) NOT NULL,
    v  INT           NOT NULL
)
ORDER BY k SEGMENTED BY HASH(k) ALL NODES;

INSERT /*+DIRECT*/ INTO bounded_src
SELECT i, MOD(i - 1, ${DISTINCT_COUNT}) + 1, MOD(i, 1000)
FROM seq_n;
COMMIT;
SELECT ANALYZE_STATISTICS('${SCHEMA}.bounded_src');
EOF
    log "  bounded_src: $(vquery "SELECT COUNT(*) FROM ${SCHEMA}.bounded_src;") rows, $(vquery "SELECT COUNT(DISTINCT k) FROM ${SCHEMA}.bounded_src;") distinct"
fi

if should_run "2" "hientropy_src" "$BENCH_ROWS"; then
    phase "PHASE 2b: Build hientropy_src (high-entropy 18-digit values)"
    vrun <<EOF
SET SEARCH_PATH = ${SCHEMA}, public;
DROP TABLE IF EXISTS hientropy_src CASCADE;
CREATE TABLE hientropy_src (
    id INT          NOT NULL,
    k  NUMERIC(18,0) NOT NULL,
    v  INT           NOT NULL
)
ORDER BY k SEGMENTED BY HASH(k) ALL NODES;

INSERT /*+DIRECT*/ INTO hientropy_src
SELECT i,
       MOD(ABS(HASH(i)), 1000000000000000000)::NUMERIC(18,0),
       MOD(i, 1000)
FROM seq_n;
COMMIT;
SELECT ANALYZE_STATISTICS('${SCHEMA}.hientropy_src');
EOF
    log "  hientropy_src: $(vquery "SELECT COUNT(*) FROM ${SCHEMA}.hientropy_src;") rows"
fi

# ===========================================================================
# PHASE 3: typed fact tables
# ===========================================================================
if should_run "3a" "b_n18"   "$BENCH_ROWS"; then phase "PHASE 3a: Build b_n18 (NUMERIC(18,0) bounded)";       build_chunked "b_n18"   "bounded_src"   "NUMERIC(18,0)" "k::NUMERIC(18,0)" "k" "k"; fi
if should_run "3b" "b_n19"   "$BENCH_ROWS"; then phase "PHASE 3b: Build b_n19 (NUMERIC(19,0) bounded)";       build_chunked "b_n19"   "bounded_src"   "NUMERIC(19,0)" "k::NUMERIC(19,0)" "k" "k"; fi
if should_run "3c" "b_n28"   "$BENCH_ROWS"; then phase "PHASE 3c: Build b_n28 (NUMERIC(28,0) bounded)";       build_chunked "b_n28"   "bounded_src"   "NUMERIC(28,0)" "k::NUMERIC(28,0)" "k" "k"; fi
if should_run "3d" "b_n38"   "$BENCH_ROWS"; then phase "PHASE 3d: Build b_n38 (NUMERIC(38,0) bounded)";       build_chunked "b_n38"   "bounded_src"   "NUMERIC(38,0)" "k::NUMERIC(38,0)" "k" "k"; fi
if should_run "3e" "b_float" "$BENCH_ROWS"; then phase "PHASE 3e: Build b_float (FLOAT bounded)";              build_chunked "b_float" "bounded_src"   "FLOAT"         "k::FLOAT"         "k" "k"; fi
if should_run "3f" "h_n18"   "$BENCH_ROWS"; then phase "PHASE 3f: Build h_n18 (NUMERIC(18,0) high-entropy)";  build_chunked "h_n18"   "hientropy_src" "NUMERIC(18,0)" "k::NUMERIC(18,0)" "k" "k"; fi
if should_run "3g" "h_n19"   "$BENCH_ROWS"; then phase "PHASE 3g: Build h_n19 (NUMERIC(19,0) high-entropy)";  build_chunked "h_n19"   "hientropy_src" "NUMERIC(19,0)" "k::NUMERIC(19,0)" "k" "k"; fi
if should_run "3h" "h_n38"   "$BENCH_ROWS"; then phase "PHASE 3h: Build h_n38 (NUMERIC(38,0) high-entropy)";  build_chunked "h_n38"   "hientropy_src" "NUMERIC(38,0)" "k::NUMERIC(38,0)" "k" "k"; fi
if should_run "3i" "h_float" "$BENCH_ROWS"; then phase "PHASE 3i: Build h_float (FLOAT high-entropy)";         build_chunked "h_float" "hientropy_src" "FLOAT"         "k::FLOAT"         "k" "k"; fi

# ===========================================================================
# PHASE 4: matched-type dim and insert-source tables
# ===========================================================================
build_dim() {
    local tname="$1"
    local coltype="$2"
    local tbl="dim_${tname}"
    if table_ready "$tbl" "$DIM_ROWS" && [ "$FORCE" != "true" ]; then
        log "  Skipping ${tbl}: already exists."
        return
    fi
    log "  Building ${SCHEMA}.${tbl} (${coltype}, ${DIM_ROWS} rows)..."
    vrun <<EOF
SET SEARCH_PATH = ${SCHEMA}, public;
DROP TABLE IF EXISTS ${tbl} CASCADE;
CREATE TABLE ${tbl} (
    k       ${coltype} NOT NULL,
    payload INT NOT NULL
)
ORDER BY k SEGMENTED BY HASH(k) ALL NODES;
INSERT /*+DIRECT*/ INTO ${tbl}
SELECT i::${coltype}, MOD(i, 100) FROM seq_n WHERE i <= ${DIM_ROWS};
COMMIT;
SELECT ANALYZE_STATISTICS('${SCHEMA}.${tbl}');
SELECT DO_TM_TASK('mergeout', '${SCHEMA}.${tbl}');
EOF
}

build_ins_src() {
    local tname="$1"
    local coltype="$2"
    local tbl="ins_src_${tname}"
    if table_ready "$tbl" "$INSERT_ROWS" && [ "$FORCE" != "true" ]; then
        log "  Skipping ${tbl}: already exists."
        return
    fi
    log "  Building ${SCHEMA}.${tbl} (${coltype}, ${INSERT_ROWS} rows)..."
    vrun <<EOF
SET SEARCH_PATH = ${SCHEMA}, public;
DROP TABLE IF EXISTS ${tbl} CASCADE;
CREATE TABLE ${tbl} (
    id INT NOT NULL,
    k  ${coltype} NOT NULL,
    v  INT NOT NULL
)
ORDER BY k SEGMENTED BY HASH(k) ALL NODES;
INSERT /*+DIRECT*/ INTO ${tbl}
SELECT id, k::${coltype}, v FROM bounded_src WHERE id <= ${INSERT_ROWS};
COMMIT;
SELECT ANALYZE_STATISTICS('${SCHEMA}.${tbl}');
SELECT DO_TM_TASK('mergeout', '${SCHEMA}.${tbl}');
EOF
}

if echo " $PHASES_TO_RUN " | grep -q " 4 "; then
    phase "PHASE 4: Build matched-type dim and insert-source tables"
    build_dim     n18   "NUMERIC(18,0)"
    build_dim     n19   "NUMERIC(19,0)"
    build_dim     n28   "NUMERIC(28,0)"
    build_dim     n38   "NUMERIC(38,0)"
    build_dim     float "FLOAT"
    build_ins_src n18   "NUMERIC(18,0)"
    build_ins_src n19   "NUMERIC(19,0)"
    build_ins_src n28   "NUMERIC(28,0)"
    build_ins_src n38   "NUMERIC(38,0)"
    build_ins_src float "FLOAT"
    log "  Matched-type tables built."
fi

# ===========================================================================
# PHASE 5: storage measurement
# ===========================================================================
if echo " $PHASES_TO_RUN " | grep -q " 5 "; then
    phase "PHASE 5: Storage measurement (bytes/row of column k)"
    log "Per-table bytes/row from v_monitor.column_storage:"
    vshow <<EOF
SELECT
    CASE WHEN anchor_table_name LIKE 'b_%' THEN 'bounded'
         WHEN anchor_table_name LIKE 'h_%' THEN 'high_entropy'
    END                                                                  AS shape,
    anchor_table_name                                                    AS table_name,
    SUM(row_count)                                                       AS rows,
    SUM(used_bytes)                                                      AS used_bytes,
    ROUND(SUM(used_bytes)::FLOAT / NULLIF(SUM(row_count), 0), 4)         AS bytes_per_row,
    ROUND(SUM(used_bytes)/1024.0/1024/1024, 3)                           AS gb_cluster_wide
FROM v_monitor.column_storage
WHERE anchor_table_schema = '${SCHEMA}'
  AND anchor_table_name IN ('b_n18','b_n19','b_n28','b_n38','b_float',
                            'h_n18','h_n19','h_n38','h_float')
  AND column_name = 'k'
GROUP BY shape, anchor_table_name
ORDER BY shape, anchor_table_name;
EOF

    log "Storage ratios vs NUMERIC(18) per shape:"
    vshow <<EOF
WITH s AS (
    SELECT
        CASE WHEN anchor_table_name LIKE 'b_%' THEN 'bounded'
             WHEN anchor_table_name LIKE 'h_%' THEN 'high_entropy'
        END AS shape,
        SUBSTR(anchor_table_name, 3) AS type,
        SUM(used_bytes)::FLOAT / NULLIF(SUM(row_count),0) AS bpr
    FROM v_monitor.column_storage
    WHERE anchor_table_schema = '${SCHEMA}'
      AND anchor_table_name IN ('b_n18','b_n19','b_n28','b_n38','b_float',
                                'h_n18','h_n19','h_n38','h_float')
      AND column_name = 'k'
    GROUP BY shape, anchor_table_name
),
n18b AS (SELECT shape, bpr AS n18_bpr FROM s WHERE type = 'n18')
SELECT s.shape, s.type, ROUND(s.bpr, 4) AS bytes_per_row,
       ROUND(s.bpr / NULLIF(n18b.n18_bpr, 0), 4) AS ratio_vs_n18
FROM s LEFT JOIN n18b USING (shape)
ORDER BY s.shape,
         CASE s.type WHEN 'n18' THEN 1 WHEN 'n19' THEN 2 WHEN 'n28' THEN 3
                     WHEN 'n38' THEN 4 WHEN 'float' THEN 5 END;
EOF
fi

# ===========================================================================
# PHASE 6: performance benchmark
# ===========================================================================
if echo " $PHASES_TO_RUN " | grep -q " 6 "; then
    phase "PHASE 6: Performance benchmark - scan / groupby / join / insert"
    log "Configuration: ${ITERATIONS} iterations per (op, type), cold cache between every query."
    log "Warm-up iteration is dropped during median computation."

    for tbl in b_n18 b_n19 b_n38 b_float dim_n18 dim_n19 dim_n38 dim_float ins_src_n18 ins_src_n19 ins_src_n38 ins_src_float; do
        if ! table_ready "$tbl" 1; then
            echo "ERROR: required table ${SCHEMA}.${tbl} missing. Run earlier phases first." >&2
            exit 1
        fi
    done

    TYPES=("n18" "n19" "n38" "float")
    for tname in "${TYPES[@]}"; do
        table="b_${tname}"
        dim="dim_${tname}"
        src="ins_src_${tname}"
        log "--- type: ${tname} (fact=${table}, dim=${dim}, src=${src}) ---"

        for iter in $(seq 1 "$ITERATIONS"); do
            profile_one "scan_${tname}_i${iter}" \
"SELECT /*+LABEL(__LABEL__)*/ COUNT(*), MIN(k), MAX(k), AVG(k::FLOAT) FROM ${SCHEMA}.${table}"

            profile_one "groupby_${tname}_i${iter}" \
"SELECT /*+LABEL(__LABEL__)*/ k, COUNT(*) AS cnt FROM ${SCHEMA}.${table} GROUP BY k ORDER BY cnt DESC LIMIT 1"

            profile_one "join_${tname}_i${iter}" \
"SELECT /*+LABEL(__LABEL__)*/ COUNT(*) FROM ${SCHEMA}.${table} t JOIN ${SCHEMA}.${dim} d ON t.k = d.k"

            log "  iter ${iter}/${ITERATIONS} done"
        done

        # INSERT - one shot, fresh empty target each time
        local_coltype=""
        case "$tname" in
            n18)   local_coltype="NUMERIC(18,0)";;
            n19)   local_coltype="NUMERIC(19,0)";;
            n38)   local_coltype="NUMERIC(38,0)";;
            float) local_coltype="FLOAT";;
        esac
        target="ins_tgt_${tname}"
        vrun <<EOF
SET SEARCH_PATH = ${SCHEMA}, public;
DROP TABLE IF EXISTS ${target} CASCADE;
CREATE TABLE ${target} (
    id INT NOT NULL,
    k  ${local_coltype} NOT NULL,
    v  INT NOT NULL
)
ORDER BY k SEGMENTED BY HASH(k) ALL NODES;
EOF
        profile_one "insert_${tname}_i1" \
"INSERT /*+DIRECT,LABEL(__LABEL__)*/ INTO ${SCHEMA}.${target} SELECT id, k, v FROM ${SCHEMA}.${src}"
        log "  insert into ${target} done"
    done

    vrun <<EOF
SELECT /*+LABEL(${LBL}_run_end)*/ '${LBL}_run_end' AS marker FROM dual;
EOF
fi

# ===========================================================================
# PHASE 7: extract timings
# ===========================================================================
if echo " $PHASES_TO_RUN " | grep -q " 7 "; then
    phase "PHASE 7: Extract per-iteration timings and compute medians"

    log "Median timings (ms) per (op, type), drops i1 as warm-up, with ratio vs n18:"
    vshow <<EOF
WITH t AS (
    SELECT
        SPLIT_PART(request_label, '_', 3) AS op,
        SPLIT_PART(request_label, '_', 4) AS type,
        SPLIT_PART(request_label, '_', 5) AS iter,
        request_duration_ms
    FROM query_requests
    WHERE request_label LIKE '${LBL}_%'
      AND request_label NOT LIKE '%_run_start'
      AND request_label NOT LIKE '%_run_end'
      AND request_duration_ms IS NOT NULL
),
iter_counts AS (
    SELECT op, type, COUNT(DISTINCT iter) AS iter_cnt FROM t GROUP BY op, type
),
trimmed AS (
    SELECT t.op, t.type, t.request_duration_ms
    FROM t JOIN iter_counts c USING (op, type)
    WHERE c.iter_cnt = 1 OR t.iter <> 'i1'
),
agg AS (
    SELECT op, type,
           COUNT(*)                                          AS n,
           ROUND(MIN(request_duration_ms), 1)                AS min_ms,
           ROUND(MAX(request_duration_ms), 1)                AS max_ms,
           ROUND(APPROXIMATE_MEDIAN(request_duration_ms), 1) AS median_ms
    FROM trimmed GROUP BY op, type
),
baseline AS (SELECT op, median_ms AS n18_median FROM agg WHERE type = 'n18')
SELECT a.op, a.type, a.n, a.min_ms, a.median_ms, a.max_ms,
       ROUND(a.median_ms / NULLIF(b.n18_median, 0), 3) AS ratio_vs_n18
FROM agg a LEFT JOIN baseline b USING (op)
ORDER BY a.op,
         CASE a.type WHEN 'n18' THEN 1 WHEN 'n19' THEN 2
                     WHEN 'n38' THEN 3 WHEN 'float' THEN 4 END;
EOF
fi

# ===========================================================================
# PHASE 8a: encoding + segmentation balance + config parameters
# ===========================================================================
if echo " $PHASES_TO_RUN " | grep -q " 8a "; then
    phase "PHASE 8a: Methodology checks (encoding, segmentation, config)"

    log "Encoding type per table.column (should all be AUTO):"
    vshow <<EOF
SELECT SPLIT_PART(projection_name, '_b', 1) AS table_base,
       encoding_type,
       COUNT(*) AS n_projections
FROM v_catalog.projection_columns
WHERE table_schema = '${SCHEMA}'
  AND table_name IN ('b_n18','b_n19','b_n28','b_n38','b_float',
                     'h_n18','h_n19','h_n38','h_float')
  AND table_column_name = 'k'
GROUP BY table_base, encoding_type
ORDER BY table_base, encoding_type;
EOF

    NON_AUTO=$(vquery "
SELECT COUNT(*) FROM v_catalog.projection_columns
WHERE table_schema='${SCHEMA}'
  AND table_name IN ('b_n18','b_n19','b_n28','b_n38','b_float','h_n18','h_n19','h_n38','h_float')
  AND table_column_name='k' AND UPPER(encoding_type) <> 'AUTO';")

    log ""
    log "Per-node row balance (worst-case skew across the 8 typed tables):"
    vshow <<EOF
WITH per_node AS (
    SELECT anchor_table_name, node_name, SUM(row_count) AS rows
    FROM v_monitor.projection_storage
    WHERE projection_schema = '${SCHEMA}'
      AND anchor_table_name IN ('b_n18','b_n19','b_n38','b_float',
                                'h_n18','h_n19','h_n38','h_float')
    GROUP BY anchor_table_name, node_name
)
SELECT anchor_table_name AS table_name,
       MIN(rows) AS min_rows, MAX(rows) AS max_rows,
       ROUND(MAX(rows)::FLOAT / NULLIF(MIN(rows), 0), 5) AS skew_ratio,
       ROUND((MAX(rows) - MIN(rows))::FLOAT * 100 / NULLIF(MIN(rows), 0), 3) AS skew_pct
FROM per_node GROUP BY anchor_table_name
ORDER BY skew_ratio DESC;
EOF

    WORST_SKEW=$(vquery "
WITH pn AS (
    SELECT anchor_table_name AS t, node_name AS n, SUM(row_count) AS r
    FROM v_monitor.projection_storage
    WHERE projection_schema='${SCHEMA}'
      AND anchor_table_name IN ('b_n18','b_n19','b_n38','b_float','h_n18','h_n19','h_n38','h_float')
    GROUP BY anchor_table_name, node_name
)
SELECT ROUND(MAX(MAX(r)::FLOAT/NULLIF(MIN(r),0)) OVER(), 5)
FROM pn GROUP BY t LIMIT 1;" 2>/dev/null || echo "")

    log ""
    log "Numeric configuration parameters:"
    vshow <<EOF
SELECT parameter_name, current_value, default_value,
       CASE WHEN current_value = default_value THEN 'default' ELSE 'CHANGED' END AS status
FROM v_monitor.configuration_parameters
WHERE parameter_name IN ('AllowNumericOverflow', 'NumericSumExtraPrecisionDigits')
ORDER BY parameter_name;
EOF

    ANO=$(vquery "SELECT current_value FROM v_monitor.configuration_parameters WHERE parameter_name='AllowNumericOverflow';")
    NSE=$(vquery "SELECT current_value FROM v_monitor.configuration_parameters WHERE parameter_name='NumericSumExtraPrecisionDigits';")

    FINDINGS["8a_encoding"]="non-AUTO encodings: ${NON_AUTO}"
    FINDINGS["8a_skew"]="worst skew ratio: ${WORST_SKEW}"
    FINDINGS["8a_config"]="AllowNumericOverflow=${ANO}, NumericSumExtraPrecisionDigits=${NSE}"
    log "  ${FINDINGS[8a_encoding]} | ${FINDINGS[8a_skew]} | ${FINDINGS[8a_config]}"
fi

# ===========================================================================
# PHASE 8b: sort-order sensitivity
# ===========================================================================
if echo " $PHASES_TO_RUN " | grep -q " 8b "; then
    phase "PHASE 8b: Sort-order sensitivity (build b_n19_unsorted, compare bytes/row)"

    if ! table_ready "b_n19_unsorted" "$BENCH_ROWS"; then
        build_chunked "b_n19_unsorted" "bounded_src" "NUMERIC(19,0)" "k::NUMERIC(19,0)" "id" "id"
    fi

    log "Bytes/row comparison: NUMERIC(19) sorted on k vs sorted on id:"
    vshow <<EOF
SELECT anchor_table_name AS table_name,
       SUM(row_count) AS rows,
       ROUND(SUM(used_bytes)::FLOAT / NULLIF(SUM(row_count), 0), 4) AS bytes_per_row
FROM v_monitor.column_storage
WHERE anchor_table_schema = '${SCHEMA}'
  AND anchor_table_name IN ('b_n18','b_n19','b_n19_unsorted')
  AND column_name = 'k'
GROUP BY anchor_table_name
ORDER BY bytes_per_row;
EOF

    BPR_S=$(vquery "SELECT ROUND(SUM(used_bytes)::FLOAT/NULLIF(SUM(row_count),0),4) FROM v_monitor.column_storage WHERE anchor_table_schema='${SCHEMA}' AND anchor_table_name='b_n19' AND column_name='k';")
    BPR_U=$(vquery "SELECT ROUND(SUM(used_bytes)::FLOAT/NULLIF(SUM(row_count),0),4) FROM v_monitor.column_storage WHERE anchor_table_schema='${SCHEMA}' AND anchor_table_name='b_n19_unsorted' AND column_name='k';")
    PENALTY=$(safe_ratio "$BPR_U" "$BPR_S" 2)

    FINDINGS["8b_penalty"]="sort-order penalty (n19 unsorted/sorted): ${PENALTY}x"
    log "  ${FINDINGS[8b_penalty]}"
fi

# ===========================================================================
# PHASE 8c: in-band cost (interleaved n19/n28/n38)
# ===========================================================================
if echo " $PHASES_TO_RUN " | grep -q " 8c "; then
    phase "PHASE 8c: In-band cost (interleaved benchmark of n19, n28, n38)"

    if ! table_ready "b_n28" "$BENCH_ROWS"; then
        log "ERROR: b_n28 missing. Run --phase 3c first." >&2
        exit 1
    fi
    # Make sure all three are equally settled
    vrun <<EOF
SELECT DO_TM_TASK('mergeout', '${SCHEMA}.b_n19');
SELECT DO_TM_TASK('mergeout', '${SCHEMA}.b_n28');
SELECT DO_TM_TASK('mergeout', '${SCHEMA}.b_n38');
EOF

    log "Running 5 interleaved iterations (each iter runs scan_n19, scan_n28, scan_n38, then groupby and join the same way)..."
    for iter in 1 2 3 4 5; do
        for tname in n19 n28 n38; do
            profile_one "ibscan_${tname}_i${iter}" \
"SELECT /*+LABEL(__LABEL__)*/ COUNT(*), MIN(k), MAX(k), AVG(k::FLOAT) FROM ${SCHEMA}.b_${tname}"
        done
        for tname in n19 n28 n38; do
            profile_one "ibgroupby_${tname}_i${iter}" \
"SELECT /*+LABEL(__LABEL__)*/ k, COUNT(*) AS cnt FROM ${SCHEMA}.b_${tname} GROUP BY k ORDER BY cnt DESC LIMIT 1"
        done
        for tname in n19 n28 n38; do
            profile_one "ibjoin_${tname}_i${iter}" \
"SELECT /*+LABEL(__LABEL__)*/ COUNT(*) FROM ${SCHEMA}.b_${tname} t JOIN ${SCHEMA}.dim_${tname} d ON t.k = d.k"
        done
        log "  iter ${iter}/5 done"
    done

    log "Median timings and ratios vs n19:"
    vshow <<EOF
WITH t AS (
    SELECT SUBSTR(SPLIT_PART(request_label,'_',3), 3) AS op,
           SPLIT_PART(request_label,'_',4) AS type,
           SPLIT_PART(request_label,'_',5) AS iter,
           request_duration_ms
    FROM query_requests
    WHERE request_label LIKE '${LBL}_ib%' AND SPLIT_PART(request_label,'_',5) <> 'i1'
),
agg AS (SELECT op, type, ROUND(APPROXIMATE_MEDIAN(request_duration_ms), 0) AS median_ms FROM t GROUP BY op, type),
baseline AS (SELECT op, median_ms AS n19_median FROM agg WHERE type='n19')
SELECT a.op, a.type, a.median_ms,
       ROUND(a.median_ms / NULLIF(b.n19_median, 0), 3) AS ratio_vs_n19
FROM agg a LEFT JOIN baseline b USING (op)
ORDER BY CASE a.op WHEN 'scan' THEN 1 WHEN 'groupby' THEN 2 WHEN 'join' THEN 3 END,
         CASE a.type WHEN 'n19' THEN 1 WHEN 'n28' THEN 2 WHEN 'n38' THEN 3 END;
EOF

    N28S=$(vquery "WITH t AS (SELECT request_duration_ms FROM query_requests WHERE request_label LIKE '${LBL}_ibscan_n28_%' AND SPLIT_PART(request_label,'_',5) <> 'i1') SELECT ROUND(APPROXIMATE_MEDIAN(request_duration_ms),0) FROM t;" 2>/dev/null || echo "")
    N19S=$(vquery "WITH t AS (SELECT request_duration_ms FROM query_requests WHERE request_label LIKE '${LBL}_ibscan_n19_%' AND SPLIT_PART(request_label,'_',5) <> 'i1') SELECT ROUND(APPROXIMATE_MEDIAN(request_duration_ms),0) FROM t;" 2>/dev/null || echo "")
    N28_RATIO=$(safe_ratio "$N28S" "$N19S" 3)

    FINDINGS["8c_n28_vs_n19"]="n28/n19 scan ratio: ${N28_RATIO} (expect within 1.0 ± 0.05)"
    log "  ${FINDINGS[8c_n28_vs_n19]}"
fi

# ===========================================================================
# PHASE 8d: mixed sort
# ===========================================================================
if echo " $PHASES_TO_RUN " | grep -q " 8d "; then
    phase "PHASE 8d: Mixed sort ORDER BY (id, k)"

    if ! table_ready "b_n19_id_k" "$BENCH_ROWS"; then
        build_chunked "b_n19_id_k" "bounded_src" "NUMERIC(19,0)" "k::NUMERIC(19,0)" "id, k" "id"
    fi

    log "Bytes/row: ORDER BY k vs ORDER BY id, k vs ORDER BY id:"
    vshow <<EOF
SELECT anchor_table_name AS table_name,
       ROUND(SUM(used_bytes)::FLOAT / NULLIF(SUM(row_count), 0), 4) AS bytes_per_row
FROM v_monitor.column_storage
WHERE anchor_table_schema = '${SCHEMA}'
  AND anchor_table_name IN ('b_n19','b_n19_id_k','b_n19_unsorted')
  AND column_name = 'k'
GROUP BY anchor_table_name
ORDER BY bytes_per_row;
EOF

    BPR_K=$(vquery "SELECT ROUND(SUM(used_bytes)::FLOAT/NULLIF(SUM(row_count),0),4) FROM v_monitor.column_storage WHERE anchor_table_schema='${SCHEMA}' AND anchor_table_name='b_n19' AND column_name='k';")
    BPR_IDK=$(vquery "SELECT ROUND(SUM(used_bytes)::FLOAT/NULLIF(SUM(row_count),0),4) FROM v_monitor.column_storage WHERE anchor_table_schema='${SCHEMA}' AND anchor_table_name='b_n19_id_k' AND column_name='k';")

    FINDINGS["8d_mixed"]="ORDER BY k=${BPR_K}, ORDER BY id,k=${BPR_IDK} bpr (mixed sort gives no compression benefit)"
    log "  ${FINDINGS[8d_mixed]}"
fi

# ===========================================================================
# PHASE 8e: scale validation
# ===========================================================================
if echo " $PHASES_TO_RUN " | grep -q " 8e "; then
    phase "PHASE 8e: Scale validation (compare sort-order penalty at smaller scale)"

    SMALL_SCALE=$(( BENCH_ROWS / 4 ))
    if [ "$SMALL_SCALE" -lt 100000000 ]; then SMALL_SCALE=100000000; fi

    if ! table_ready "b_n19_small" "$SMALL_SCALE"; then
        log "  Building b_n19_small (${SMALL_SCALE} rows, ORDER BY k)..."
        vrun <<EOF
SET SEARCH_PATH = ${SCHEMA}, public;
DROP TABLE IF EXISTS b_n19_small CASCADE;
CREATE TABLE b_n19_small (id INT NOT NULL, k NUMERIC(19,0) NOT NULL, v INT NOT NULL)
    ORDER BY k SEGMENTED BY HASH(k) ALL NODES;
INSERT /*+DIRECT*/ INTO b_n19_small SELECT id, k::NUMERIC(19,0), v FROM bounded_src WHERE id <= ${SMALL_SCALE};
COMMIT;
SELECT ANALYZE_STATISTICS('${SCHEMA}.b_n19_small');
SELECT DO_TM_TASK('mergeout', '${SCHEMA}.b_n19_small');
EOF
    fi

    if ! table_ready "b_n19_small_unsorted" "$SMALL_SCALE"; then
        log "  Building b_n19_small_unsorted (${SMALL_SCALE} rows, ORDER BY id)..."
        vrun <<EOF
SET SEARCH_PATH = ${SCHEMA}, public;
DROP TABLE IF EXISTS b_n19_small_unsorted CASCADE;
CREATE TABLE b_n19_small_unsorted (id INT NOT NULL, k NUMERIC(19,0) NOT NULL, v INT NOT NULL)
    ORDER BY id SEGMENTED BY HASH(id) ALL NODES;
INSERT /*+DIRECT*/ INTO b_n19_small_unsorted SELECT id, k::NUMERIC(19,0), v FROM bounded_src WHERE id <= ${SMALL_SCALE};
COMMIT;
SELECT ANALYZE_STATISTICS('${SCHEMA}.b_n19_small_unsorted');
SELECT DO_TM_TASK('mergeout', '${SCHEMA}.b_n19_small_unsorted');
EOF
    fi

    log "Sort-order penalty by scale:"
    vshow <<EOF
SELECT anchor_table_name AS table_name,
       SUM(row_count) AS rows,
       ROUND(SUM(used_bytes)::FLOAT / NULLIF(SUM(row_count), 0), 4) AS bytes_per_row
FROM v_monitor.column_storage
WHERE anchor_table_schema = '${SCHEMA}'
  AND anchor_table_name IN ('b_n19','b_n19_unsorted','b_n19_small','b_n19_small_unsorted')
  AND column_name = 'k'
GROUP BY anchor_table_name ORDER BY anchor_table_name;
EOF

    SMALL_S=$(vquery "SELECT ROUND(SUM(used_bytes)::FLOAT/NULLIF(SUM(row_count),0),4) FROM v_monitor.column_storage WHERE anchor_table_schema='${SCHEMA}' AND anchor_table_name='b_n19_small' AND column_name='k';")
    SMALL_U=$(vquery "SELECT ROUND(SUM(used_bytes)::FLOAT/NULLIF(SUM(row_count),0),4) FROM v_monitor.column_storage WHERE anchor_table_schema='${SCHEMA}' AND anchor_table_name='b_n19_small_unsorted' AND column_name='k';")
    BIG_S=$(vquery "SELECT ROUND(SUM(used_bytes)::FLOAT/NULLIF(SUM(row_count),0),4) FROM v_monitor.column_storage WHERE anchor_table_schema='${SCHEMA}' AND anchor_table_name='b_n19' AND column_name='k';")
    BIG_U=$(vquery "SELECT ROUND(SUM(used_bytes)::FLOAT/NULLIF(SUM(row_count),0),4) FROM v_monitor.column_storage WHERE anchor_table_schema='${SCHEMA}' AND anchor_table_name='b_n19_unsorted' AND column_name='k';" 2>/dev/null || echo "")
    PEN_SMALL=$(safe_ratio "$SMALL_U" "$SMALL_S" 2)
    PEN_BIG=$(safe_ratio "$BIG_U" "$BIG_S" 2)

    FINDINGS["8e_scale"]="sort-order penalty: ${SMALL_SCALE} rows = ${PEN_SMALL}x, ${BENCH_ROWS} rows = ${PEN_BIG}x"
    log "  ${FINDINGS[8e_scale]}"
fi

# ===========================================================================
# PHASE 8f: SUM silent overflow
# ===========================================================================
if echo " $PHASES_TO_RUN " | grep -q " 8f "; then
    phase "PHASE 8f: SUM silent-overflow reproduction"

    log "Building 100M-row table of 999999999999999999::NUMERIC(18,0)..."
    vrun <<EOF
SET SEARCH_PATH = ${SCHEMA}, public;
DROP TABLE IF EXISTS sum_overflow_test CASCADE;
CREATE TABLE sum_overflow_test (v NUMERIC(18,0) NOT NULL)
    ORDER BY v SEGMENTED BY HASH(v) ALL NODES;
INSERT /*+DIRECT*/ INTO sum_overflow_test
SELECT 999999999999999999::NUMERIC(18,0) FROM seq_n WHERE i <= 100000000;
COMMIT;
EOF

    SUM_BUILTIN=$(vquery "SELECT SUM(v) FROM ${SCHEMA}.sum_overflow_test;")
    SUM_CAST=$(vquery "SELECT SUM(v::NUMERIC(38,0)) FROM ${SCHEMA}.sum_overflow_test;")
    EXPECTED="99999999999999999900000000"

    log "  Built-in SUM(v):                  ${SUM_BUILTIN}"
    log "  Pre-cast SUM(v::NUMERIC(38,0)):   ${SUM_CAST}"
    log "  Mathematical answer:              ${EXPECTED}"

    if [ "$SUM_CAST" = "$EXPECTED" ]; then
        if [ "$SUM_BUILTIN" != "$EXPECTED" ]; then
            FINDINGS["8f_sum"]="silent overflow REPRODUCED (built-in returned ${SUM_BUILTIN}, pre-cast returned exact ${EXPECTED})"
        else
            FINDINGS["8f_sum"]="no silent overflow on this cluster (built-in returned exact value; check NumericSumExtraPrecisionDigits)"
        fi
    else
        FINDINGS["8f_sum"]="UNEXPECTED: pre-cast SUM did not return expected value"
    fi
    log "  ${FINDINGS[8f_sum]}"

    if [ "$KEEP" = "false" ]; then
        vrun <<EOF
DROP TABLE IF EXISTS ${SCHEMA}.sum_overflow_test CASCADE;
EOF
    fi
fi

# ===========================================================================
# PHASE 8g: AVG digit cliff
# ===========================================================================
if echo " $PHASES_TO_RUN " | grep -q " 8g "; then
    phase "PHASE 8g: AVG FLOAT-precision cliff"

    log "Testing AVG at 14, 15, 16, 17, 18 digits (4 identical rows per magnitude):"
    vshow <<EOF
WITH probes AS (
    SELECT '14-digit'::VARCHAR AS magnitude,        12345678901234::NUMERIC(38,0) AS v
    UNION ALL SELECT '14-digit',                    12345678901234::NUMERIC(38,0)
    UNION ALL SELECT '14-digit',                    12345678901234::NUMERIC(38,0)
    UNION ALL SELECT '14-digit',                    12345678901234::NUMERIC(38,0)
    UNION ALL SELECT '15-digit',                   123456789012345::NUMERIC(38,0)
    UNION ALL SELECT '15-digit',                   123456789012345::NUMERIC(38,0)
    UNION ALL SELECT '15-digit',                   123456789012345::NUMERIC(38,0)
    UNION ALL SELECT '15-digit',                   123456789012345::NUMERIC(38,0)
    UNION ALL SELECT '16-digit',                  1234567890123456::NUMERIC(38,0)
    UNION ALL SELECT '16-digit',                  1234567890123456::NUMERIC(38,0)
    UNION ALL SELECT '16-digit',                  1234567890123456::NUMERIC(38,0)
    UNION ALL SELECT '16-digit',                  1234567890123456::NUMERIC(38,0)
    UNION ALL SELECT '17-digit',                 12345678901234567::NUMERIC(38,0)
    UNION ALL SELECT '17-digit',                 12345678901234567::NUMERIC(38,0)
    UNION ALL SELECT '17-digit',                 12345678901234567::NUMERIC(38,0)
    UNION ALL SELECT '17-digit',                 12345678901234567::NUMERIC(38,0)
    UNION ALL SELECT '18-digit',                999999999999999999::NUMERIC(38,0)
    UNION ALL SELECT '18-digit',                999999999999999999::NUMERIC(38,0)
    UNION ALL SELECT '18-digit',                999999999999999999::NUMERIC(38,0)
    UNION ALL SELECT '18-digit',                999999999999999999::NUMERIC(38,0)
)
SELECT magnitude,
       MAX(v)                                                              AS input_value,
       AVG(v)::NUMERIC(38,0)                                               AS avg_cast_to_numeric,
       SUM(v::NUMERIC(38,0))/COUNT(*)                                      AS exact_sum_div_count,
       AVG(v)::NUMERIC(38,0) - MAX(v)                                      AS diff,
       CASE WHEN AVG(v)::NUMERIC(38,0) = MAX(v) THEN 'EXACT' ELSE 'LOST' END AS avg_status
FROM probes GROUP BY magnitude ORDER BY magnitude;
EOF

    FINDINGS["8g_avg"]="AVG digit cliff: see table above (expected: EXACT through 16, LOST from 17)"
    log "  ${FINDINGS[8g_avg]}"
fi

# ===========================================================================
# PHASE 8h: final consolidated report
# ===========================================================================
if echo " $PHASES_TO_RUN " | grep -q " 8h "; then
    phase "PHASE 8h: Final consolidated report"

    echo "" | tee -a "$LOGFILE"
    echo "==========================================================" | tee -a "$LOGFILE"
    echo "                FINAL REPORT" | tee -a "$LOGFILE"
    echo "==========================================================" | tee -a "$LOGFILE"
    echo "Cluster: ${VSQL_DATABASE:-<default>}" | tee -a "$LOGFILE"
    echo "Vertica: ${VERSION}" | tee -a "$LOGFILE"
    echo "Scale:   ${BENCH_ROWS} rows per typed table" | tee -a "$LOGFILE"
    echo "Test ID: ${LBL}" | tee -a "$LOGFILE"
    echo "" | tee -a "$LOGFILE"
    echo "Methodology checks:" | tee -a "$LOGFILE"
    echo "  ${FINDINGS[8a_encoding]:-not run}" | tee -a "$LOGFILE"
    echo "  ${FINDINGS[8a_skew]:-not run}" | tee -a "$LOGFILE"
    echo "  ${FINDINGS[8a_config]:-not run}" | tee -a "$LOGFILE"
    echo "" | tee -a "$LOGFILE"
    echo "Sort-order findings:" | tee -a "$LOGFILE"
    echo "  ${FINDINGS[8b_penalty]:-not run}" | tee -a "$LOGFILE"
    echo "  ${FINDINGS[8d_mixed]:-not run}" | tee -a "$LOGFILE"
    echo "  ${FINDINGS[8e_scale]:-not run}" | tee -a "$LOGFILE"
    echo "" | tee -a "$LOGFILE"
    echo "In-band cost:" | tee -a "$LOGFILE"
    echo "  ${FINDINGS[8c_n28_vs_n19]:-not run}" | tee -a "$LOGFILE"
    echo "" | tee -a "$LOGFILE"
    echo "Correctness reproductions:" | tee -a "$LOGFILE"
    echo "  ${FINDINGS[8f_sum]:-not run}" | tee -a "$LOGFILE"
    echo "  ${FINDINGS[8g_avg]:-not run}" | tee -a "$LOGFILE"
    echo "" | tee -a "$LOGFILE"
    echo "Storage and performance results: see Phase 5 and Phase 7 above." | tee -a "$LOGFILE"
    echo "Per-iteration timings query:" | tee -a "$LOGFILE"
    echo "  SELECT request_label, request_duration_ms FROM query_requests" | tee -a "$LOGFILE"
    echo "    WHERE request_label LIKE '${LBL}_%' ORDER BY request_label;" | tee -a "$LOGFILE"
    echo "==========================================================" | tee -a "$LOGFILE"
fi

# ===========================================================================
# Optional cleanup of temporary tables
# ===========================================================================
if [ "$KEEP" = "false" ] && echo " $PHASES_TO_RUN " | grep -q " 8h "; then
    log ""
    log "Dropping temporary check tables (use --keep to retain)..."
    vrun <<EOF
SET SEARCH_PATH = ${SCHEMA}, public;
DROP TABLE IF EXISTS b_n19_unsorted        CASCADE;
DROP TABLE IF EXISTS b_n19_id_k            CASCADE;
DROP TABLE IF EXISTS b_n19_small           CASCADE;
DROP TABLE IF EXISTS b_n19_small_unsorted  CASCADE;
EOF
fi

log ""
log "DONE. Full log saved to: ${LOGFILE}"
log "Test ID for cross-referencing query_requests: ${LBL}"
