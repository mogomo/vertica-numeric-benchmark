# vertica-numeric-benchmark

A reproducible benchmark suite for measuring how Vertica's NUMERIC and FLOAT data types behave at production scale, with emphasis on the **NUMERIC(18) → NUMERIC(19+) precision boundary** and the **projection sort-order** decisions that turn out to dominate storage and performance.

This is the companion repository for the field guide ["When NUMERIC(18) Isn't Enough"](#companion-blog-post). Run it on your own cluster to verify every claim in the article against your hardware, your version of Vertica, and your data shapes.

---

## ⚠️ Disclaimer — read before running

Provided **AS IS**, without warranty of any kind. The author and contributors accept no responsibility or liability for any damage, data loss, downtime, or other consequence arising from use of this script.

**Run only in a QA / development / dedicated benchmark environment, never against production.** The script will:

- Create and **DROP** tables inside the target schema (default: `num_bench`) without prompting. Any pre-existing table whose name collides with one the script uses **will be dropped**. Always use a dedicated schema.
- Generate sustained heavy CPU, memory, disk, and network load on every node for the entire run (~3 hours at default scale). Concurrent workloads on the same cluster will be slowed and may fail.
- Modify cluster-wide data collector retention via `set_data_collector_policy()` and `set_data_collector_time_policy()`. These changes persist after the script ends.

Before running: confirm the database/schema with `--status`, confirm sufficient free disk per node, confirm no critical workloads are active, and read the source. **You assume all risk.**

---

## Table of contents

- [What this benchmark measures](#what-this-benchmark-measures)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [What the script does, phase by phase](#what-the-script-does-phase-by-phase)
- [Sanity checks (Phase 8) explained](#sanity-checks-phase-8-explained)
- [Disk and runtime budgets](#disk-and-runtime-budgets)
- [Output and how to interpret it](#output-and-how-to-interpret-it)
- [Restarting after a failure](#restarting-after-a-failure)
- [Running specific phases](#running-specific-phases)
- [Cleaning up](#cleaning-up)
- [What I see on my cluster](#what-i-see-on-my-cluster)
- [FAQ](#faq)
- [Companion blog post](#companion-blog-post)
- [Companion UDx repositories](#companion-udx-repositories)
- [License](#license)

---

## What this benchmark measures

The script runs **one consolidated benchmark + sanity-check pipeline** against a Vertica cluster you control. It produces:

1. **Storage** (bytes per row) for NUMERIC(18), NUMERIC(19), NUMERIC(28), NUMERIC(38), and FLOAT, on two data shapes:
   - **Bounded** — small distinct-value count, repeated (the friendly case for AUTO encoding)
   - **High-entropy** — random 18-digit values (the hostile case)

2. **Performance** (median latency in milliseconds, multiple iterations, cold-cache between every query) for four common operations on each type:
   - **Scan** — full-table aggregate (`SELECT COUNT(*), MIN(k), MAX(k), AVG(k::FLOAT)`)
   - **GROUP BY** — find the most-common value
   - **JOIN** — equi-join against a 1%-sized matched-type dimension
   - **INSERT** — bulk INSERT … SELECT of a 20%-sized matched-type source

3. **Eight sanity checks** that verify the methodology is fair and reproduce the article's most important correctness findings, including SUM silent-overflow at NUMERIC(18) and the AVG digit cliff.

Every storage and timing number printed by the script is reproducible — same row counts, same encodings, same configuration, same query labels — and every claim in the [companion blog post](#companion-blog-post) traces back to one of these phases.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| **Vertica** 11.0 or later | Tested on 24.2 and 26.1 |
| **vsql** in `PATH` | Comes with any Vertica installation |
| **Bash 4+** | Standard on any Linux distribution |
| **A user with table-creation privileges** | The script creates a schema (default `num_bench`) and tables inside it |
| **Cluster disk headroom** | See [Disk and runtime budgets](#disk-and-runtime-budgets) below |

The script reads connection parameters from environment variables in vsql's normal way — `VSQL_USER`, `VSQL_PASSWORD`, `VSQL_DATABASE` — or from command-line flags. No Python, Perl, or Java dependencies; pure Bash + vsql.

---

## Quick start

```bash
# Clone the repository
git clone https://github.com/mogomo/vertica-numeric-benchmark.git
cd vertica-numeric-benchmark

# Make it executable
chmod +x bench_numeric.sh

# Set your connection
export VSQL_USER=dbadmin
export VSQL_PASSWORD=...
export VSQL_DATABASE=mydb

# Run a quick test at 100M rows (fits easily on a small test cluster, ~30 minutes)
./bench_numeric.sh --rows 100000000 --iter 3

# Or run the full 2B-row benchmark in the background (~3 hours)
nohup ./bench_numeric.sh > bench_$(date +%Y%m%d_%H%M%S).out 2>&1 &
tail -f $(ls -t bench_*.out | head -1)
```

The script writes a cumulative log to `bench_numeric.log` in addition to stdout, so you can `tail -f` either stream.

---

## Configuration

All flags have sensible defaults; nothing is required.

| Flag | Default | What it does |
|---|---|---|
| `--rows N` | `2000000000` (2 billion) | Total fact rows per typed table. Smaller values run faster; ratios remain meaningful down to about 100M rows |
| `--iter K` | `5` | Iterations per (operation, type) for performance phase. The first iteration is dropped as warm-up |
| `--phase X` | `all` | Which phase(s) to run. See [Running specific phases](#running-specific-phases) |
| `--schema NAME` | `num_bench` | Schema to create tables in |
| `--db NAME` | `$VSQL_DATABASE` | Database name (defaults to vsql's environment) |
| `--user NAME` | `dbadmin` | vsql username |
| `--status` | (off) | Print what's already built and exit |
| `--force` | (off) | Rebuild tables even if they exist at the right size |
| `--keep` | (off) | Don't drop the temporary check tables at the end |

The script also respects environment variables: `VSQL_USER`, `VSQL_PASSWORD`, `VSQL_DATABASE`, `SCHEMA`, `LOGFILE`, `BENCH_ROWS`, `ITERATIONS`, `CHUNK_SIZE`. Command-line flags take precedence.

### Choosing a row count

The default of 2 billion mirrors the row count used in the companion blog post. For your own runs, the right scale depends on what you want to learn:

- **100M rows** — fast smoke test (~30 minutes). Storage ratios converge nicely at this scale; performance ratios are roughly correct but a little noisy.
- **500M rows** — fast realistic run (~75 minutes). All ratios stable and meaningful; sort-order penalty visible but smaller than at 2B.
- **2B rows** — full benchmark (~3 hours). Storage ratios fully developed; sort-order penalty at its dramatic ~27× value; performance ratios at maximum signal-to-noise.
- **5B+ rows** — only attempt with significant disk and patience. Storage ratios continue to widen but cluster runtime grows faster than data volume.

The script linearly scales the dimension and INSERT-source row counts to match (1% and 20% of fact-row count, respectively), so you don't need to think about those.

---

## What the script does, phase by phase

The pipeline is split into nine phases (0 through 8), with several phases broken into sub-phases (3a, 3b, … and 8a, 8b, …) that can be run individually. Every phase that builds a table checks first whether the table already exists at the target row count; if so, it skips the build. So re-running the script after a partial failure naturally picks up where it left off.

| Phase | What it does | Skippable? |
|---|---|---|
| **0** | Set up schema, configure data collector retention | Free, always runs when in scope |
| **1** | Build `seq_n` — an N-row sequence used as a row generator for everything else | Skipped if `seq_n` exists at target rows |
| **2** | Build `bounded_src` (low-cardinality) and `hientropy_src` (random 18-digit) source data | Skipped per-table |
| **3** | Build the nine typed fact tables (3a–3i): b_n18, b_n19, b_n28, b_n38, b_float, h_n18, h_n19, h_n38, h_float | Each sub-phase skippable independently |
| **4** | Build matched-type dimension and insert-source tables for each NUMERIC and FLOAT precision | Skipped per-table |
| **5** | Storage measurement — print bytes/row for every typed column k | Idempotent (just SELECTs against `column_storage`) |
| **6** | Performance benchmark — scan / GROUP BY / JOIN / INSERT, multiple iterations per (op, type), cold cache before every query | Re-runs append new timings; older timings remain in `query_requests` |
| **7** | Extract per-iteration timings, compute medians, print ratios vs NUMERIC(18) | Idempotent |
| **8** | Eight sanity checks (8a–8h) — see next section | Each sub-phase skippable independently |

The sub-phase notation lets you do precise re-runs: `--phase 3c` rebuilds just `b_n28`, `--phase 8c` re-runs just the in-band cost check. See [Running specific phases](#running-specific-phases).

---

## Sanity checks (Phase 8) explained

Eight focused checks. Each verifies one specific claim from the blog post.

### 8a — Methodology fairness

Verifies three things that, if any of them are wrong, would invalidate the rest of the results:

1. **All projections use AUTO encoding.** Reads `v_catalog.projection_columns` and confirms `encoding_type = 'AUTO'` for every k column. If a non-AUTO encoding shows up, you have a tuning override on the cluster and the comparison isn't apples-to-apples.

2. **Per-node row balance is even.** Reads `v_monitor.projection_storage` per node and reports the worst-case skew ratio across the eight typed tables. On a healthy default-segmented setup this should be at most 1.001 (0.1% skew).

3. **Numeric configuration parameters are at default.** Specifically `AllowNumericOverflow` (default 1, permits silent SUM overflow) and `NumericSumExtraPrecisionDigits` (default 6, adds a few digits of headroom). The blog post's findings about SUM behavior depend on these defaults.

### 8b — Sort-order sensitivity

Builds `b_n19_unsorted` (same data as `b_n19` but with `ORDER BY id` instead of `ORDER BY k`) and reports the storage ratio. On a 2B-row bounded table this lands around **26-27×** — the headline finding of the blog post. AUTO encoding compresses the typed column dramatically when it's the primary sort key and pays roughly the slot width when it isn't.

### 8c — In-band cost (interleaved n19/n28/n38)

Runs scan / GROUP BY / JOIN against `b_n19`, `b_n28`, and `b_n38` in **interleaved order** (one iteration alternates types) with cold cache between every query. Five iterations, dropping the first as warm-up.

The interleaving is important because it controls for cluster-state drift — if the cluster slows down during the run, all three types feel it equally. Without interleaving, an isolated benchmark of `b_n28` after busy work on the other tables can look 4× slower than it really is. The script's earlier in-house version found and corrected exactly that pitfall.

The expected result: **n28/n19 within ±1% across all three operations**, n38/n19 in the 1.08–1.22× range. This validates the blog post's claim that all NUMERIC(19–37) cost the same at runtime.

### 8d — Mixed sort doesn't help

Builds `b_n19_id_k` with `ORDER BY (id, k)` — typed column as a secondary sort key. Compares its bytes/row to both `b_n19` (`ORDER BY k`) and `b_n19_unsorted` (`ORDER BY id`).

The expected and surprising result: **bytes/row for `b_n19_id_k` is identical to `b_n19_unsorted`**. AUTO encoding gives no partial credit for putting the typed column near the front of a compound sort. Either it's the primary sort key, or it isn't.

### 8e — Scale validation

Builds a quarter-scale (`BENCH_ROWS / 4`, with a floor of 100M) version of `b_n19` and `b_n19_unsorted`, then compares the sort-order penalty at small scale to the penalty at full scale. The expected pattern is **super-linear growth** — at 500M rows the penalty is around 11×, growing to around 27× at 2B. Production-scale tables will see proportionally larger penalties, which is worth knowing before you size storage.

### 8f — SUM silent overflow

Builds a 100M-row table holding only the value `999999999999999999` (18 nines) as `NUMERIC(18,0)`. The mathematical SUM is a 26-digit number; running plain `SUM(v)` on a default-configured cluster returns a negative number — silent integer wraparound, no error raised.

The script then runs `SUM(v::NUMERIC(38,0))` on the same data, which returns the exact 26-digit answer. **The pre-cast pattern works without changing the column type.**

If your cluster has `AllowNumericOverflow=0` or a tuned `NumericSumExtraPrecisionDigits`, you'll see different output here — that's informative too. The script reports clearly which case applies.

### 8g — AVG digit cliff

Tests `AVG()` at five magnitudes (14, 15, 16, 17, and 18 digits) using four identical rows per magnitude — so the average should equal the input. Compares `AVG(v)::NUMERIC(38,0)` against the input, with a `diff` column making any precision loss explicit.

The expected and reproducible result: **EXACT through 16 digits, LOST from 17 onward.** At 17 digits, `AVG()` returns 12345678901234568 when the input is 12345678901234567 — a one-off error visible in the `diff` column. The workaround `SUM(v::NUMERIC(38,0)) / COUNT(*)` is included in the same query to show it returns the exact value at all five magnitudes.

### 8h — Final consolidated report

Pulls together the key one-line findings from 8a–8g into a single report block at the end of the log. Suitable for pasting into a GitHub issue, a Slack thread, or your team's design-review document. Looks like:

```
==========================================================
                FINAL REPORT
==========================================================
Cluster: mydb
Vertica: Vertica Analytic Database v26.1.0-0
Scale:   2000000000 rows per typed table
Test ID: bn_260502143012

Methodology checks:
  non-AUTO encodings: 0
  worst skew ratio: 1.00012
  AllowNumericOverflow=1, NumericSumExtraPrecisionDigits=6

Sort-order findings:
  sort-order penalty (n19 unsorted/sorted): 26.79x
  ORDER BY k=0.1504, ORDER BY id,k=4.0290 bpr (mixed sort gives no compression benefit)
  sort-order penalty: 500000000 rows = 10.85x, 2000000000 rows = 26.79x

In-band cost:
  n28/n19 scan ratio: 1.000 (expect within 1.0 ± 0.05)

Correctness reproductions:
  silent overflow REPRODUCED (built-in returned -2537764290215403776,
                              pre-cast returned exact 99999999999999999900000000)
  AVG digit cliff: see table above (expected: EXACT through 16, LOST from 17)
==========================================================
```

---

## Disk and runtime budgets

Five-node cluster, 16 GB RAM per node, default Vertica 26.1 settings. Budget linearly with node count.

### Disk (cluster-wide GB at peak)

| Scale | Phase 3 tables | Sanity check tables (8b/d/e) | Total peak |
|---|---|---|---|
| 100M | ~7 GB | ~3 GB | ~10 GB |
| 500M | ~30 GB | ~7 GB | ~40 GB |
| 1B | ~60 GB | ~12 GB | ~75 GB |
| 2B | ~120 GB | ~25 GB | ~150 GB |

Plus mergeout working space (typically 30–50% on top of the above). On a 5-node cluster, the 2B scale wants about 200 GB free per node.

### Runtime (5 nodes, 16 GB/node, Vertica 26.1, `--iter 5`)

| Scale | Phase 1–4 (build) | Phase 6 (perf) | Phase 8 (sanity) | Total |
|---|---|---|---|---|
| 100M | ~10 min | ~5 min | ~10 min | **~25 min** |
| 500M | ~30 min | ~15 min | ~25 min | **~70 min** |
| 1B | ~55 min | ~25 min | ~35 min | **~115 min** |
| 2B | ~110 min | ~35 min | ~45 min | **~190 min** |

These are wall-clock times on a moderately-loaded cluster. A dedicated benchmark cluster will be faster; a busy one will be slower. The script is designed to interleave benchmarks fairly so cluster-state drift doesn't bias type comparisons (see the methodology note in 8c).

---

## Output and how to interpret it

The script produces three streams of output, all useful in different ways.

**Stdout** — human-readable progress, with timestamps on every line. Phase headers separate the major sections. Storage and performance tables print as part of phases 5 and 7. Sanity check findings print as part of each 8x sub-phase, and a consolidated report prints at the end.

**`bench_numeric.log`** — same content as stdout but persisted to disk. Cumulative across runs, so a re-run after a failure produces a complete log.

**Vertica's `query_requests` table** — every benchmarked query is labeled with a unique test ID (printed at the start and end of the run, format `bn_YYMMDDHHMMSS`). To pull all the per-iteration timings for a specific run:

```sql
SELECT request_label, request_duration_ms, start_timestamp
FROM   query_requests
WHERE  request_label LIKE 'bn_260502143012_%'
ORDER  BY request_label;
```

To compare timings across two runs (say, before and after a Vertica version upgrade):

```sql
SELECT
    SPLIT_PART(request_label, '_', 3) AS op,
    SPLIT_PART(request_label, '_', 4) AS type,
    AVG(CASE WHEN request_label LIKE 'bn_260502143012_%' THEN request_duration_ms END) AS run_a_ms,
    AVG(CASE WHEN request_label LIKE 'bn_260503090000_%' THEN request_duration_ms END) AS run_b_ms
FROM query_requests
WHERE request_label LIKE 'bn_2605%'
GROUP BY op, type
ORDER BY op, type;
```

### Reading the storage table

Phase 5 prints the bytes/row for every typed column k, plus a ratio against NUMERIC(18) within each shape. Read the **bounded** rows for the favorable case (projection sorted on k, low-cardinality data); read the **high_entropy** rows for the production-realistic case.

### Reading the performance table

Phase 7 prints median latency in milliseconds for each (op, type) combination, plus a ratio against NUMERIC(18). Lower is faster. The "n" column shows how many iterations were aggregated (will be `iter - 1` for queries with multiple iterations, since iteration 1 is dropped as warm-up; `1` for INSERT which runs once).

### Reading the sanity-check tables

Each 8x phase prints both raw numbers and a one-line summary in the log. The 8h final report block consolidates the summaries.

---

## Restarting after a failure

The script is designed to fail gracefully and resume cleanly. If a phase fails — most commonly because of disk pressure, transient resource contention, or a networking hiccup — the partial work is preserved. Tables that finished building stay built; only the phase that failed didn't complete.

To resume:

```bash
# See what's already built
./bench_numeric.sh --status

# Re-run; phases that already completed will be skipped
./bench_numeric.sh
```

The `should_run` helper inside the script checks `table_ready()` before every build phase — if the table exists at the target row count, the phase reports "Skipping" and moves on.

To force a rebuild of a specific table (for example, if you suspect it was built incorrectly):

```bash
./bench_numeric.sh --force --phase 3c   # rebuild b_n28 only
```

---

## Running specific phases

The `--phase` argument accepts:

| Form | Example | What it does |
|---|---|---|
| `all` | `--phase all` | Default. Runs every phase in order |
| Single number | `--phase 3` | Runs all sub-phases of phase 3 (3a through 3i) |
| Single sub-phase | `--phase 8c` | Runs only the in-band cost check |
| Range | `--phase 5-7` | Runs phases 5, 6, 7 |
| Sub-phase range | `--phase 8a-8c` | Runs 8a, 8b, 8c |
| Comma list | `--phase 5,7,8h` | Runs the listed phases |

**Common workflows:**

```bash
# Just print the storage and performance tables, assuming everything's built
./bench_numeric.sh --phase 5,7

# Re-run the sanity checks against an existing build
./bench_numeric.sh --phase 8

# Quick correctness reproductions (no big builds needed)
./bench_numeric.sh --phase 8f,8g

# Rebuild and re-benchmark NUMERIC(28) only
./bench_numeric.sh --force --phase 3c
./bench_numeric.sh --phase 8c
```

---

## Cleaning up

To drop just the temporary sanity-check tables (default behavior at end of a full run):

```bash
./bench_numeric.sh --phase 8h    # already drops at end if --keep wasn't set
```

To drop the entire benchmark schema:

```bash
vsql -c "DROP SCHEMA num_bench CASCADE;"
```

The script never modifies anything outside its own schema. The only system-level change it makes is calling `set_data_collector_policy` and `set_data_collector_time_policy` to ensure query timings are retained for at least 7 days — those changes persist across runs but don't affect anything outside the benchmark.

---

## What I see on my cluster

For reference, here's what the 2B-row run produced on a 5-node Vertica 26.1 cluster (16 GB RAM/node) used to write the companion blog post.

**Storage (bytes/row of column k):**

| Type | Bounded, ORDER BY k | Bounded, ORDER BY id | High-entropy, ORDER BY k |
|---|---|---|---|
| NUMERIC(18) | 1.0012 | 1.0012 | 4.9496 |
| NUMERIC(19) | 0.1504 | 4.0290 | 8.4775 |
| NUMERIC(28) | 0.1504 | (not built) | (not built) |
| NUMERIC(38) | 0.1854 | 4.0338 | 8.4818 |
| FLOAT | 0.1004 | 3.4094 | 6.6090 |

**Performance (ratios vs NUMERIC(18), bounded data, sorted on k):**

| Operation | n19 | n28 | n38 | float |
|---|---|---|---|---|
| Scan | 0.77× | 0.77× | 0.90× | 0.25× |
| GROUP BY | 1.19× | 1.19× | 1.46× | 0.73× |
| JOIN | 1.09× | 1.08× | 1.18× | 0.88× |
| INSERT | 1.30× | 1.30× | 1.49× | 1.08× |

**Sanity check headlines:**

- Worst per-node skew across 8 typed tables: 1.00012 (0.012%)
- Sort-order penalty (n19 unsorted/sorted): 26.79× at 2B; 10.85× at 500M
- ORDER BY (id, k) bytes/row identical to ORDER BY id alone
- n28/n19 scan ratio: 1.00× (in-band cost is essentially zero)
- SUM silent overflow reproduced: built-in returned −2,537,764,290,215,403,776; pre-cast returned exact 99,999,999,999,999,999,900,000,000
- AVG digit cliff at exactly 17 digits

Your numbers will differ in absolute terms (different hardware, version, configuration) but the **ratios** and **directional findings** should match closely. If they don't, that's interesting and worth investigating — open an issue and let's compare notes.

---

## FAQ

**Why isn't there a Python or Docker version?** Pure Bash + vsql means no version-compatibility surprises and no extra installation. The script runs identically on a developer laptop and on a production-adjacent benchmark cluster.

**Can I run this on a single-node cluster?** Yes. Per-node skew checks become trivially passing (one node, no skew possible) but everything else works. The runtime improves significantly because there's no inter-node coordination.

**Can I add my own data type?** Yes. The script's typed-table builds are parameterized — to add a new type, copy any of the `should_run "3X" ...; build_chunked ...` lines, change the type and select expression, and add the corresponding `build_dim` and `build_ins_src` calls in phase 4. The benchmark loop iterates a fixed type list (`TYPES` array in phase 6) — extend it to include your new type.

**The benchmark is taking longer than the README says.** Check whether other workloads are running on the cluster — `SELECT * FROM v_monitor.system_resource_usage` is your friend. Also check for background mergeouts piling up: `SELECT * FROM v_monitor.tuple_mover_operations`. The benchmark is sensitive to cluster state, which is why the in-band check (8c) uses interleaving — but the per-phase timings assume a relatively idle cluster.

**Does the script change any non-default Vertica configuration?** No. Phase 0 calls `set_data_collector_policy` and `set_data_collector_time_policy` to ensure query timings are retained long enough for phase 7 to extract them, but doesn't touch numeric, query, or memory parameters.

**Can I share the output with Anthropic / OpenTelemetry / a vendor?** The output contains your cluster's version string, your schema name, and timing data. It does not contain any of your data — the benchmark uses synthetic data only. Read through `bench_numeric.log` before sharing, but it's typically safe to share without redaction.

---

## Companion blog post

[**When NUMERIC(18) Isn't Enough — A Vertica Field Guide for Eight Real Use Cases**](https://github.com/mogomo/vertica-numeric-benchmark/blob/main/When_NUMERIC_18_Isnt_Enough.pdf)

The blog post walks through eight common production scenarios involving Vertica's NUMERIC and FLOAT types, with empirical data sourced from the benchmark in this repository. If you're trying to decide which type to use, or whether to migrate an existing column, start there.

---

## Companion UDx repositories

For the rare workload that needs more precision than NUMERIC(38) can provide combined with very large row counts, two open-source UDxes extend Vertica's exact arithmetic:

- **[vertica-exact-sum-udx](https://github.com/mogomo/vertica-exact-sum-udx)** — `exact_sum()` dynamically extends working precision based on input precision and row count. Raises a clear error rather than returning silently rounded results when even NUMERIC(1024) is insufficient.

- **[vertica-exact-avg-udx](https://github.com/mogomo/vertica-exact-avg-udx)** — `exact_avg()` does the same for averages, keeping the entire calculation in NUMERIC instead of FLOAT.

Both are documented as production-use-with-validation and are best evaluated in your own environment before adoption.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).

Contributions, issues, and observations from your own clusters are welcome. If you run this on a Vertica version or hardware configuration not listed in [What I see on my cluster](#what-i-see-on-my-cluster), open an issue with the final-report block and we'll add it to a community results section.

---

*Maintainer: [@mogomo](https://github.com/mogomo)*
