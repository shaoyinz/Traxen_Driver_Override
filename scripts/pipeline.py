"""
pipeline.py
-----------
Processing stages for the iQCruise driver-override extraction pipeline.

Stages
------
  2.  dedup_and_check_gaps      – resolve duplicate timestamps, flag large gaps
  2b. clean_zero_gps            – null out GPS dropout placeholders
  3.  debounce_iqcmode          – bridge short inactive blips (logger glitches)
  4.  build_sessions            – segment contiguous iQC-active periods
  5.  detect_overrides          – find every throttle-entry and active-state exit
  6.  dedup_throttle_exits      – flag system exits that follow a throttle override
  7.  filter_events             – mark noisy / low-quality events
  8.  classify_overrides        – label each event with an override type
  9.  save_context_windows      – write per-event 20-second CSVs
  10. export_events             – write the summary events CSV

Orchestrator
------------
  run_pipeline(df, output_path, context_dir)
      Accepts an already-loaded pandas DataFrame (produced by
      data_loader.prepare_truck_dataframe) and runs all stages in order.

Dependencies: pandas, numpy
"""

import os
import warnings

import numpy as np
import pandas as pd

from config import CFG, COLS, CONTEXT_COLS, EXPORT_COLS

warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------------------------------------------------------------------
# GPS validity helpers
# ---------------------------------------------------------------------------

_LAT_BOUNDS      = (-90.0,  90.0)
_LON_BOUNDS      = (-180.0, 180.0)
_LAT_ZERO_THRESH = 1.0   # |lat| < this treated as null/0 fill
_LON_ZERO_THRESH = 1.0


def _is_reasonable_latlon(lat, lon) -> bool:
    if pd.isna(lat) or pd.isna(lon):
        return False
    if abs(lat) < _LAT_ZERO_THRESH or abs(lon) < _LON_ZERO_THRESH:
        return False
    if not (_LAT_BOUNDS[0] <= lat <= _LAT_BOUNDS[1]):
        return False
    if not (_LON_BOUNDS[0] <= lon <= _LON_BOUNDS[1]):
        return False
    return True


# ---------------------------------------------------------------------------
# Stage 2 – Dedup & gap detection
# ---------------------------------------------------------------------------

def dedup_and_check_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """Resolve duplicate timestamps using lat/lon quality, then flag large gaps.

    Resolution rules per duplicate group:
      1. No row has reasonable lat/lon  → keep the last row.
      2. Exactly one row has reasonable lat/lon  → keep that row.
      3. Multiple rows have reasonable lat/lon but differ in content
         → keep all and print a warning for manual review.

    A ``_dt_s`` column (inter-row delta in seconds) is added for gap detection
    downstream.
    """
    print("[2] Dedup & gap detection ...")

    lat_col = COLS["lat"]
    lon_col = COLS["lon"]

    dedup_cols = [COLS["ts"]]
    if "time_period" in df.columns:
        dedup_cols.insert(0, "time_period")
    dup_mask   = df.duplicated(subset=dedup_cols, keep=False)
    dup_groups = df[dup_mask].groupby(dedup_cols)

    rows_to_drop  = []
    both_good_any = False

    for ts, group in dup_groups:
        if len(group) < 2:
            continue

        good = group.apply(
            lambda r: _is_reasonable_latlon(
                r.get(lat_col, np.nan),
                r.get(lon_col, np.nan),
            ),
            axis=1,
        )
        n_good = good.sum()

        if n_good == 0:
            rows_to_drop.extend(group.index[:-1].tolist())
            print(f"    [{ts}] All duplicates have nonsense lat/lon – kept last of {len(group)} rows.")

        elif n_good == 1:
            good_idx  = good[good].index[0]
            drop_idxs = group.index[group.index != good_idx].tolist()
            rows_to_drop.extend(drop_idxs)
            print(f"    [{ts}] Kept 1 row with valid lat/lon, dropped {len(drop_idxs)}.")

        else:
            good_rows = group[good]
            ignore_cols = [COLS["ts"], lat_col, lon_col]
            if "time_period" in good_rows.columns:
                ignore_cols.append("time_period")
            check = good_rows.drop(columns=[c for c in ignore_cols if c in good_rows.columns])
            if check.nunique().max() == 1:
                rows_to_drop.extend(good_rows.index[:-1].tolist())
                print(f"    [{ts}] {len(good_rows)} identical duplicates with valid lat/lon – kept last.")
            else:
                both_good_any = True

    before = len(df)
    df = df.drop(index=rows_to_drop).reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        print(f"    Removed {dropped:,} duplicate row(s) total.")

    df["_dt_s"] = df[COLS["ts"]].diff().dt.total_seconds().fillna(0)
    n_gaps = (df["_dt_s"] > CFG["MAX_INTRA_FILE_GAP_S"]).sum()
    if n_gaps:
        print(f"    Found {n_gaps} time gap(s) > {CFG['MAX_INTRA_FILE_GAP_S']} s.")
    return df


# ---------------------------------------------------------------------------
# Stage 2b – Clean zero GPS
# ---------------------------------------------------------------------------

def clean_zero_gps(df: pd.DataFrame) -> pd.DataFrame:
    """Replace lat=0 & lon=0 with NaN (GPS dropout placeholder).

    Other CAN signals on the same rows (speed, torque, pedals, iQCMode) are
    independently sourced and remain valid – only the GPS columns are nulled.
    """
    lat_col = COLS["lat"]
    lon_col = COLS["lon"]
    mask = (df[lat_col] == 0) & (df[lon_col] == 0)
    if mask.any():
        df.loc[mask, lat_col] = np.nan
        df.loc[mask, lon_col] = np.nan
        print(f"[2b] Nulled GPS coords on {mask.sum()} zero lat/lon row(s).")
    return df


# ---------------------------------------------------------------------------
# Stage 3 – Debounce iQCMode
# ---------------------------------------------------------------------------

def debounce_iqcmode(df: pd.DataFrame) -> pd.DataFrame:
    """Eliminate logger glitches without masking real override events.

    Rule: if a run of INACTIVE rows (0/1/7) lasts less than
    ``MIN_INACTIVE_BRIDGE_S`` seconds AND is sandwiched between two
    ACTIVE-state runs, bridge it with the preceding active value.

    Mode 6 (Throttle Override) is always preserved – it is a real event.
    Result is written to a new ``iqcmode_clean`` column; the raw
    ``iQC1.iQCMode`` column is left untouched.
    """
    print("[3] Debouncing iQCMode ...")
    raw     = df[COLS["iqc_mode"]].copy()
    cleaned = raw.values.astype(float).copy()

    ACTIVE   = CFG["IQCMODE_ACTIVE"]
    INACTIVE = CFG["IQCMODE_INACTIVE"]
    vals, starts, lengths = _rle(cleaned)

    for i, (val, start, length) in enumerate(zip(vals, starts, lengths)):
        dur_s = length * 0.1  # 100 ms per row
        if (
            val in INACTIVE
            and dur_s < CFG["MIN_INACTIVE_BRIDGE_S"]
            and 0 < i < len(vals) - 1
            and vals[i - 1] in ACTIVE
            and vals[i + 1] in ACTIVE
        ):
            cleaned[start: start + length] = vals[i - 1]

    df["iqcmode_clean"] = cleaned
    n_changed = (df["iqcmode_clean"] != raw.fillna(-1)).sum()
    print(f"    Rows relabelled by debounce: {n_changed:,}")
    return df


def _rle(arr):
    """Run-length encoding → (values, start_indices, lengths)."""
    n = len(arr)
    if n == 0:
        return [], [], []
    starts = [0]
    for i in range(1, n):
        if arr[i] != arr[i - 1]:
            starts.append(i)
    starts.append(n)
    vals    = [arr[starts[j]]              for j in range(len(starts) - 1)]
    lengths = [starts[j + 1] - starts[j]  for j in range(len(starts) - 1)]
    return vals, starts[:-1], lengths


# ---------------------------------------------------------------------------
# Stage 4 – Build active sessions
# ---------------------------------------------------------------------------

def build_sessions(df: pd.DataFrame) -> pd.DataFrame:
    """Segment contiguous iQC-active periods into labelled sessions.

    A session is a contiguous block of ``iqcmode_clean`` in the set
    {2, 3, 4, 5, 6} with no inter-row gap exceeding
    ``MAX_INTRA_FILE_GAP_S``.  Mode 6 is included because the driver was
    under iQC control just before pressing the accelerator.

    Adds columns: ``session_id``, ``session_start``, ``session_end``,
    ``session_dur_s``.
    """
    print("[4] Building active sessions ...")

    SESSION_STATES = CFG["IQCMODE_ACTIVE"] | {CFG["IQCMODE_THROTTLE_OVERRIDE"]}
    mode = df["iqcmode_clean"].values
    dt   = df["_dt_s"].values

    session_id = np.full(len(df), np.nan)
    sid        = 0
    in_session = False
    seg_start  = 0

    for i in range(len(df)):
        in_s = mode[i] in SESSION_STATES
        if in_s:
            if not in_session:
                in_session = True
                seg_start  = i
            elif dt[i] > CFG["MAX_INTRA_FILE_GAP_S"]:
                session_id[seg_start:i] = sid
                sid       += 1
                seg_start  = i
        else:
            if in_session:
                session_id[seg_start:i] = sid
                sid += 1
            in_session = False

    if in_session:
        session_id[seg_start:] = sid

    df["session_id"] = session_id

    active = df.dropna(subset=["session_id"])
    if active.empty:
        print("    WARNING: no active sessions found.")
        df["session_start"] = pd.NaT
        df["session_end"]   = pd.NaT
        df["session_dur_s"] = np.nan
        return df

    sessions = (
        active.groupby("session_id")[COLS["ts"]]
        .agg(session_start="min", session_end="max")
    )
    sessions["session_dur_s"] = (
        sessions["session_end"] - sessions["session_start"]
    ).dt.total_seconds()
    df = df.merge(sessions, on="session_id", how="left")

    n_sessions = sessions.shape[0]
    total_h    = sessions["session_dur_s"].sum() / 3600
    print(f"    Active sessions found: {n_sessions}  (total active time: {total_h:.2f} h)")
    return df


# ---------------------------------------------------------------------------
# Stage 5 – Detect override events
# ---------------------------------------------------------------------------

def detect_overrides(df: pd.DataFrame) -> pd.DataFrame:
    """Detect every override event using two complementary rules.

    Rule A – Throttle Override:
        Any row where ``iqcmode_clean == 6`` and the previous row was not
        also 6.  Mode 6 means the system itself recorded the accelerator
        press and fires regardless of the preceding state.

    Rule B – Active-state exit:
        Row where ``iqcmode_clean`` is INACTIVE {0,1,7} and the previous row
        was ACTIVE {2,3,4,5}.  Catches brake-pedal, button-press, and unknown
        exits.  The recovery tail after a Throttle Override also triggers this
        rule; duplicates are removed in stage 6.
    """
    print("[5] Detecting override events ...")

    ACTIVE   = CFG["IQCMODE_ACTIVE"]
    INACTIVE = CFG["IQCMODE_INACTIVE"]
    T_OVR    = CFG["IQCMODE_THROTTLE_OVERRIDE"]

    mode      = df["iqcmode_clean"]
    mode_prev = mode.shift(1)

    mask_throttle = (mode_prev != T_OVR) & (mode == T_OVR)
    mask_exit     = (mode_prev.isin(ACTIVE)) & (mode.isin(INACTIVE))

    events = []
    for mask, raw_type in [(mask_throttle, "THROTTLE_ENTRY"),
                           (mask_exit,     "ACTIVE_EXIT")]:
        for idx in df[mask].index:
            prev_idx      = idx - 1 if idx > 0 else 0
            session_dur_s = _session_dur_at(df, prev_idx)
            events.append({
                "override_idx":          idx,
                "override_ts":           df.at[idx, COLS["ts"]],
                "raw_detection_type":    raw_type,
                "prev_mode":             mode_prev.at[idx],
                "cur_mode":              mode.at[idx],
                "preceding_session_id":  df.at[prev_idx, "session_id"],
                "session_dur_s_at_ovrd": session_dur_s,
                "speed_kph_at_ovrd":     df.at[idx, COLS["spd_wheel"]],
            })

    events_df = (
        pd.DataFrame(events)
        .sort_values("override_ts")
        .reset_index(drop=True)
    )
    print(f"    Raw events – throttle entries: {mask_throttle.sum()}, "
          f"active exits: {mask_exit.sum()}")
    return events_df


def _session_dur_at(df: pd.DataFrame, prev_idx: int) -> float:
    """Seconds the current session had been running at *prev_idx*."""
    ACTIVE = CFG["IQCMODE_ACTIVE"] | {CFG["IQCMODE_THROTTLE_OVERRIDE"]}
    j = prev_idx
    while j > 0 and df.at[j, "iqcmode_clean"] in ACTIVE:
        j -= 1
    return max(
        (df.at[prev_idx, COLS["ts"]] - df.at[j + 1, COLS["ts"]]).total_seconds(),
        0.0,
    )


# ---------------------------------------------------------------------------
# Stage 6 – De-duplicate post-throttle-override exits
# ---------------------------------------------------------------------------

def dedup_throttle_exits(events_df: pd.DataFrame) -> pd.DataFrame:
    """Flag ACTIVE_EXIT events that are part of the system's own recovery.

    After a Throttle Override (mode → 6) the system goes through a recovery
    sequence (6 → 2 → 3 → 4 → 3 → 0) before re-engaging.  The final
    ``→ 0`` is the system's controlled shutdown, not a new driver action.

    Any ACTIVE_EXIT within ``THROTTLE_OVERRIDE_DEDUP_WINDOW_S`` seconds
    after a THROTTLE_ENTRY is marked ``is_throttle_exit_dup = True``.
    """
    print("[6] De-duplicating post-throttle-override exits ...")
    events_df = events_df.copy()
    events_df["is_throttle_exit_dup"] = False

    throttle_times = events_df.loc[
        events_df["raw_detection_type"] == "THROTTLE_ENTRY", "override_ts"
    ].tolist()

    window = pd.Timedelta(seconds=CFG["THROTTLE_OVERRIDE_DEDUP_WINDOW_S"])
    for t_entry in throttle_times:
        mask = (
            (events_df["raw_detection_type"] == "ACTIVE_EXIT")
            & (events_df["override_ts"] > t_entry)
            & (events_df["override_ts"] <= t_entry + window)
        )
        events_df.loc[mask, "is_throttle_exit_dup"] = True

    n_duped = events_df["is_throttle_exit_dup"].sum()
    if n_duped:
        print(f"    Flagged {n_duped} exit(s) as system exit after throttle override.")
    return events_df


# ---------------------------------------------------------------------------
# Stage 7 – Filter noisy events
# ---------------------------------------------------------------------------

def filter_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """Mark low-quality events as noisy (flagged, not deleted).

    Criteria:
      1. Post-throttle-override system exits (stage 6).
      2. Session too short before override (< MIN_ACTIVE_SESSION_S).
      3. Very low speed (< MIN_SPEED_KPH) – possible stop or parking.
    """
    print("[7] Filtering noise ...")
    events_df = events_df.copy()
    events_df["is_noisy"] = events_df["is_throttle_exit_dup"]

    short = events_df["session_dur_s_at_ovrd"] < CFG["MIN_ACTIVE_SESSION_S"]
    events_df.loc[short, "is_noisy"] = True
    print(f"    Flagged (session < {CFG['MIN_ACTIVE_SESSION_S']} s): {short.sum()}")

    low_spd = events_df["speed_kph_at_ovrd"] < CFG["MIN_SPEED_KPH"]
    events_df.loc[low_spd, "is_noisy"] = True
    print(f"    Flagged (speed < {CFG['MIN_SPEED_KPH']} kph): {low_spd.sum()}")

    clean = (~events_df["is_noisy"]).sum()
    print(f"    Clean events: {clean} / {len(events_df)}")
    return events_df


# ---------------------------------------------------------------------------
# Stage 8 – Classify override type
# ---------------------------------------------------------------------------

def classify_overrides(df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """Label each event with an override type and enrich with context features.

    Classification hierarchy (priority order):
      1. THROTTLE_OVERRIDE  – raw_detection_type == 'THROTTLE_ENTRY'
      2. BRAKE_PEDAL        – BrakeSwitch == 1 in the 10-second pre-window
      3. ACCEL_PEDAL        – AccelPedal > threshold in pre-window, no brake
      4. ACC_OFF_BUTTON     – CC switch change in pre-window, no pedal activity
      5. UNKNOWN            – none of the above
    """
    print("[8] Classifying override types ...")

    pre_rows  = int(CFG["PRE_OVERRIDE_S"]  / 0.1)
    post_rows = int(CFG["POST_OVERRIDE_S"] / 0.1)

    rows = []
    for _, ev in events_df.iterrows():
        idx  = int(ev["override_idx"])
        pre  = df.iloc[max(0, idx - pre_rows): idx]
        post = df.iloc[idx: min(len(df), idx + post_rows)]

        max_accel    = _safe(pre, COLS["accel_pedal"],  "max")
        brake_active = _safe(pre, COLS["brake_sw_eng"], "max") == CFG["BRAKE_SWITCH_ON"]

        cc_btn_change = False
        for btn_col in [COLS["cc_enable_cab"], COLS["cc_enable_mc"],
                        COLS["cc_set_cab"],    COLS["cc_resume_cab"]]:
            if btn_col in df.columns and not pre.empty:
                s = pre[btn_col].dropna()
                if len(s) >= 2 and s.nunique() > 1:
                    cc_btn_change = True
                    break

        # Context features
        avg_spd_pre  = _safe(pre,  COLS["spd_wheel"], "mean")
        avg_spd_post = _safe(post, COLS["spd_wheel"], "mean")
        cipv_dist    = _at(df, idx, COLS["cipv_dist"])
        cipv_det     = _at(df, idx, COLS["cipv_detected"])
        road_grade   = _at(df, idx, COLS["road_grade"])
        road_curve   = _at(df, idx, COLS["road_curvature"])
        altitude     = _at(df, idx, COLS["altitude"])
        retarder_pre = _safe(pre, COLS["retarder_torq"], "mean")
        gvw          = _at(df, idx, COLS["gvw_est"])
        if pd.isna(gvw):
            gvw = _at(df, idx, COLS["gvw_raw"])
        eh_not_local = (
            bool(_safe(pre, COLS["eh_not_local"], "max") == 1.0)
            if COLS["eh_not_local"] in df.columns
            else False
        )
        lat = _at(df, idx, COLS["lat"])
        lon = _at(df, idx, COLS["lon"])

        # Classification
        if ev["raw_detection_type"] == "THROTTLE_ENTRY":
            override_type = "THROTTLE_OVERRIDE"
        elif brake_active:
            override_type = "BRAKE_PEDAL"
        elif max_accel > CFG["ACCEL_PEDAL_THRESHOLD_PCT"]:
            override_type = "ACCEL_PEDAL"
        elif cc_btn_change:
            override_type = "ACC_OFF_BUTTON"
        else:
            override_type = "UNKNOWN"

        rows.append({
            **ev.to_dict(),
            "override_type":      override_type,
            "max_accel_pre_pct":  max_accel,
            "brake_active_pre":   brake_active,
            "cc_btn_change_pre":  cc_btn_change,
            "avg_speed_pre_kph":  avg_spd_pre,
            "avg_speed_post_kph": avg_spd_post,
            "cipv_dist_m":        cipv_dist,
            "cipv_detected":      cipv_det,
            "road_grade_pct":     road_grade,
            "road_curvature":     road_curve,
            "altitude_m":         altitude,
            "retarder_pct_pre":   retarder_pre,
            "gvw_kg":             gvw,
            "eh_not_localized":   eh_not_local,
            "lat":                lat,
            "lon":                lon,
        })

    result = pd.DataFrame(rows)
    clean  = result[~result["is_noisy"]]
    print("    Override types (clean events only):")
    for t, c in clean["override_type"].value_counts().items():
        pct = 100 * c / len(clean) if len(clean) else 0
        print(f"      {t:<22}: {c}  ({pct:.0f} %)")
    return result


def _safe(subset: pd.DataFrame, col: str, agg: str):
    if col not in subset.columns or subset.empty:
        return np.nan
    s = subset[col].dropna()
    if s.empty:
        return np.nan
    return getattr(s, agg)()


def _at(df: pd.DataFrame, idx: int, col: str):
    if col not in df.columns:
        return np.nan
    return df.at[idx, col]


# ---------------------------------------------------------------------------
# Stage 9 – Save per-event context window CSVs
# ---------------------------------------------------------------------------

def save_context_windows(
    df: pd.DataFrame,
    events_df: pd.DataFrame,
    output_dir: str,
) -> None:
    """Write one CSV per clean override event covering ±10 seconds."""
    os.makedirs(output_dir, exist_ok=True)
    pre_rows  = int(CFG["PRE_OVERRIDE_S"]  / 0.1)
    post_rows = int(CFG["POST_OVERRIDE_S"] / 0.1)
    available = [c for c in CONTEXT_COLS if c in df.columns]

    clean = events_df[~events_df["is_noisy"]]
    print(f"[9] Saving {len(clean)} context windows -> {output_dir}/")

    for i, (_, ev) in enumerate(clean.iterrows()):
        idx    = int(ev["override_idx"])
        ts_str = str(ev["override_ts"]).replace(":", "-").replace(" ", "_")
        otype  = ev["override_type"]
        window = df.iloc[max(0, idx - pre_rows): min(len(df), idx + post_rows)][available].copy()
        window["is_override_point"] = False
        if idx in window.index:
            window.at[idx, "is_override_point"] = True
        fname = os.path.join(output_dir, f"event_{i:03d}_{otype}_{ts_str}.csv")
        window.to_csv(fname, index=False)

    print("    Done.")


# ---------------------------------------------------------------------------
# Stage 10 – Export events table
# ---------------------------------------------------------------------------

def export_events(events_df: pd.DataFrame, output_path: str) -> None:
    """Write the full events table (all events, noisy ones included) to CSV."""
    cols = [c for c in EXPORT_COLS if c in events_df.columns]
    out  = events_df[cols].sort_values("override_ts").reset_index(drop=True)
    out.to_csv(output_path, index=False)
    print(f"[10] Events table saved -> {output_path}  ({len(out)} rows)")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pipeline(
    df: pd.DataFrame,
    output_path: str,
    context_dir: str = "",
) -> pd.DataFrame:
    """Run all pipeline stages on an already-loaded DataFrame.

    Args:
        df:           Pandas DataFrame from ``data_loader.prepare_truck_dataframe``.
        output_path:  Path for the output events CSV.
        context_dir:  Directory for per-event context CSVs.
                      Pass an empty string to skip saving them.

    Returns:
        events_df with all events (clean and noisy) and classification columns.
    """
    print("\n" + "=" * 65)
    print("  Traxen iQCruise -- Override Event Extraction Pipeline v2")
    print("=" * 65 + "\n")

    df = dedup_and_check_gaps(df)
    df = clean_zero_gps(df)
    df = debounce_iqcmode(df)
    df = build_sessions(df)

    events_df = detect_overrides(df)
    events_df = dedup_throttle_exits(events_df)
    events_df = filter_events(events_df)
    events_df = classify_overrides(df, events_df)

    if context_dir:
        save_context_windows(df, events_df, context_dir)

    export_events(events_df, output_path)

    clean = events_df[~events_df["is_noisy"]]
    print("\n" + "-" * 65)
    print(f"  TOTAL raw events   : {len(events_df)}")
    print(f"  Clean events       : {len(clean)}")
    print(f"  Noisy / filtered   : {events_df['is_noisy'].sum()}")
    print(f"    of which throttle dup: {events_df['is_throttle_exit_dup'].sum()}")
    print(f"\n  All events (including noisy) by type:")
    for t, c in events_df["override_type"].value_counts().items():
        print(f"    {t:<22}: {c}")
    print("-" * 65 + "\n")

    return events_df
