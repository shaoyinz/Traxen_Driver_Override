# Driver Override Classification 


## `iQC1.iQCMode` State

- 0 = Not Available     — system off / not engaged
- 1 = Available         — PRIMARY cruise-active state (truck under iQC control)
- 2 = Controlled Entry  — brief transition INTO active sub-control
- 3 = Active Control    — deeper engagement sub-state
- 4 = Hold              — holding state (speed/torque hold)
- 5 = Controlled Exit   — planned / safe ramp-down
- 6 = Throttle Override — DRIVER pushed the accelerator (explicit accel override)
- 7 = Fault             — system fault

`iQC1.iQCMode==2,3,4,5` indicate iQCruise is active, `iQC1.iQCMode==6` indicates a throttle override. For other cases, they are manual driving.

## Override Classes

- Throttle Override
    - Signal (THROTTLE_OVERRIDE): iQCMode transitions INTO state 6 (2/3/4→6). Followed by recovery sequence (6→2→3→4→3→0)
    - Without signal(ACCEL_PEDAL): Exit from active state → 0/5/7 AND AccelPedal > threshold in pre-window AND no brake signal AND mode 6 was NOT seen
- Brake Override(BRAKE_PEDAL): iQCMode exits an active state (any of 1,2,3,4) → 0/5/7 AND BrakeSwitch == 1 in the pre-window.
- Turn off ACC (ACC_OFF_BUTTON): iQCMode exits an active state (any of 1,2,3,4) → 0/5/7 AND no observation of BrakeSwitch == 1 in the pre-window.
- Unknown
---

## Code Structure

The extraction pipeline is split across four Python modules:

### `config.py` — Shared Constants and Column Mappings

Centralizes every tunable parameter and column name so that pipeline logic never contains magic numbers or hard-coded signal names.

**`CFG` dictionary — pipeline parameters:**

| Key | Default | Purpose |
|-----|---------|---------|
| `IQCMODE_ACTIVE` | {2, 3, 4, 5} | iQCMode values considered "system active" |
| `IQCMODE_THROTTLE_OVERRIDE` | 6 | iQCMode value for throttle override |
| `IQCMODE_INACTIVE` | {0, 1, 7} | iQCMode values considered "manual driving" |
| `MIN_INACTIVE_BRIDGE_S` | 2.0 s | Inactive blips shorter than this between two active runs are bridged (debounce) |
| `MAX_INTRA_FILE_GAP_S` | 30.0 s | Gaps larger than this break a session |
| `MIN_ACTIVE_SESSION_S` | 5.0 s | Session must have been active at least this long before an override is considered valid |
| `ACCEL_PEDAL_THRESHOLD_PCT` | 5.0 % | Accelerator pedal position above which an accel-pedal override is detected |
| `BRAKE_SWITCH_ON` | 1.0 | Value of BrakeSwitch that means "brake pressed" |
| `MIN_SPEED_KPH` | 15.0 kph | Events below this speed are flagged as noisy (possible stop/parking) |
| `THROTTLE_OVERRIDE_DEDUP_WINDOW_S` | 30.0 s | Active-exit events within this window after a throttle entry are flagged as system recovery, not new overrides |
| `PRE_OVERRIDE_S` / `POST_OVERRIDE_S` | 10.0 s each | Context window size before/after each override point |

**`COLS` dictionary — column aliases:**

Maps short keys (e.g. `"iqc_mode"`, `"accel_pedal"`, `"brake_sw_eng"`) to the actual CAN signal names in the parquet files (e.g. `"iQC1.iQCMode"`, `"EEC2_Engine.AccelPedalPos1"`, `"CCVS1_Engine.BrakeSwitch"`). Covers iQC system state, pedal/brake signals, cruise control buttons, speed, road geometry, radar CIPV, engine/torque, GPS, GVW, and misc signals.

**`CONTEXT_COLS`** — list of columns written into per-event 20-second context window CSVs (stage 9).

**`EXPORT_COLS`** — list of columns written into the final summary events CSV (stage 10).

---

### `data_loader.py` — Data Ingestion

Provides a two-step workflow to go from raw `.7z` archives to a pipeline-ready pandas DataFrame.

**`read_nov_data(data_dir, truck_ids)`**
- Scans all `.7z` archives in `data_dir`. Each archive file stem is treated as a truck ID.
- Extracts parquet files to a `_parquet_cache/` subdirectory on first run; reuses the cache on subsequent runs.
- Concatenates multiple parquet files per truck using `polars.concat(..., how="diagonal_relaxed")` so that columns missing in some files are filled with nulls rather than raising errors.
- Returns a `dict[str, pl.LazyFrame]` mapping truck IDs to lazy-loaded Polars frames.

**`prepare_truck_dataframe(lf, truck_id)`**
- Collects the LazyFrame into a pandas DataFrame.
- Detects **time period boundaries**: a new period starts whenever the raw `Timestamp` value decreases (system restart) or the `Date` column changes (new parquet file / day). Assigns a `time_period` integer label.
- Computes `elapsed_s` (seconds from the start of each period).
- Converts the raw numeric `Timestamp` into `datetime64` by adding it as a timedelta to the calendar `Date` (or a synthetic epoch if `Date` is absent).

---

### `pipeline.py` — Processing Stages

Contains all processing stages and the `run_pipeline()` orchestrator. Each stage takes a DataFrame and returns an updated DataFrame.

**Stage 2 — `dedup_and_check_gaps(df)`**
- Resolves duplicate timestamps using GPS quality: if only one row in a duplicate group has reasonable lat/lon, keep that row; if none do, keep the last; if multiple differ in content, print a warning.
- Adds `_dt_s` (inter-row time delta in seconds) for gap detection downstream.
- ❓ [As of 02/16] Addressed data continuity challenges caused by system resets, where timestamps represent time elapsed rather than absolute time. As mentioned before, I introduced a `time_period` column to partition these reset cycles; however, duplicate timestamps with valid GPS coordinates remain an issue. Notably, the majority of these duplicates coincide with iQCruise being in an 'off' state, suggesting this state can be used as a heuristic for further data cleaning and deduplication.

**Stage 2b — `clean_zero_gps(df)`**
- Replaces lat=0, lon=0 rows with NaN (GPS dropout placeholders). Other CAN signals on those rows remain valid.

**Stage 3 — `debounce_iqcmode(df)`**
- Uses run-length encoding to find short runs of INACTIVE modes (0/1/7) sandwiched between ACTIVE runs. If a run is shorter than `MIN_INACTIVE_BRIDGE_S`, it is bridged with the preceding active value (treats it as a logger glitch).
- Mode 6 (Throttle Override) is never bridged — it is always a real event.
- Writes the result to a new `iqcmode_clean` column; the raw `iQC1.iQCMode` is preserved.

**Stage 4 — `build_sessions(df)`**
- Segments contiguous blocks of `iqcmode_clean ∈ {2, 3, 4, 5, 6}` into numbered sessions. A session breaks when the mode leaves this set or when a time gap exceeds `MAX_INTRA_FILE_GAP_S`.
- Adds `session_id`, `session_start`, `session_end`, `session_dur_s` columns.

**Stage 5 — `detect_overrides(df)` → `events_df`**
- **Rule A (Throttle Entry):** Row where `iqcmode_clean` transitions into 6 from any other value.
- **Rule B (Active Exit):** Row where `iqcmode_clean` transitions from ACTIVE {2,3,4,5} to INACTIVE {0,1,7}.
- For each detected event, records the timestamp, previous/current modes, session duration at the point of override, and speed.
- Returns a new `events_df` DataFrame (one row per event).

**Stage 6 — `dedup_throttle_exits(events_df)`**
- After a throttle override (mode→6), the system goes through a recovery sequence (6→2→3→4→3→0). The final exit to 0 triggers Rule B but is not a new driver action.
- Flags any ACTIVE_EXIT within `THROTTLE_OVERRIDE_DEDUP_WINDOW_S` (30 s) after a THROTTLE_ENTRY as `is_throttle_exit_dup = True`.

**Stage 7 — `filter_events(events_df)`**
- Marks events as `is_noisy = True` (flagged, not deleted) based on:
  1. Post-throttle-override system exits (from stage 6).
  2. Session too short before override (< `MIN_ACTIVE_SESSION_S`).
  3. Very low speed (< `MIN_SPEED_KPH`).

**Stage 8 — `classify_overrides(df, events_df)`**
- Looks at the 10-second pre-window and classifies each event by priority:
  1. **THROTTLE_OVERRIDE** — raw detection type is `THROTTLE_ENTRY` (mode 6 was seen).
  2. **BRAKE_PEDAL** — BrakeSwitch == 1 in the pre-window.
  3. **ACCEL_PEDAL** — AccelPedal > 5% threshold in pre-window, no brake signal (throttle override without mode 6).
  4. **ACC_OFF_BUTTON** — Cruise control switch changed in pre-window, no pedal activity.
  5. **UNKNOWN** — none of the above criteria met.
- Enriches each event with context features: average speed pre/post, CIPV distance, road grade, road curvature, altitude, retarder usage, GVW, EH localization status, and GPS coordinates.

**Stage 9 — `save_context_windows(df, events_df, output_dir)`**
- For each clean (non-noisy) event, extracts a ±10-second window from the full DataFrame.
- Writes one CSV per event containing the columns defined in `CONTEXT_COLS`, with an `is_override_point` flag marking the exact override row.

**Stage 10 — `export_events(events_df, output_path)`**
- Writes the full events table (all events including noisy ones) to a summary CSV with the columns defined in `EXPORT_COLS`.

**Stage 11 (within `run_pipeline`)** — Saves the processed full DataFrame as a parquet file so that `override_idx` values can be used for later lookups.

**`run_pipeline(df, output_path, context_dir)`** — Orchestrates stages 2–10 in order, prints a summary of total/clean/noisy event counts and override type distribution.

---

### `main.py` — Entry Point

Ties everything together with a simple top-level script:

1. **Configure parameters** — Sets `data_dir`, `truck_ids` (allow-list), `output_path`, `context_dir`, and optionally overrides `CFG` thresholds.
2. **Load data** — Calls `read_nov_data()` to get a dict of truck LazyFrames.
3. **Process each truck** — Iterates over trucks, calls `prepare_truck_dataframe()` to materialize the data, then `run_pipeline()` to extract and classify override events.
4. **Output routing** — Generates per-truck output filenames (e.g. `override_events_5FT0217.csv`) and per-truck context directories.

---
# Example: Truck `5FT0217`

## Statistics

- ~20% of the raw logs have iQCMode = 2, 3, 4, 5
- Based on the pipeline above, we detected the following 1924 events:

| override_type       | Count | %      |
|---------------------|-------|--------|
| THROTTLE_OVERRIDE   | 1,683 | 87.47% |
| ACC_OFF_BUTTON      |    87 |  4.52% |
| UNKNOWN             |    86 |  4.47% |
| ACCEL_PEDAL         |    64 |  3.33% |
| BRAKE_PEDAL         |     4 |  0.21% |

