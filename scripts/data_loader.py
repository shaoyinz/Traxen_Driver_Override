"""
data_loader.py
--------------
Loading utilities for Traxen iQCruise CAN telemetry.

Two-step workflow
-----------------
1. read_nov_data()          – scan all .7z archives in a directory and return
                              a dict of  truck_id -> polars LazyFrame.
                              Extraction is cached so repeated runs are fast.

2. prepare_truck_dataframe() – collect one LazyFrame, convert to a sorted
                               pandas DataFrame, and validate the Timestamp
                               column.  This is the DataFrame that the
                               pipeline stages in pipeline.py consume.

Dependencies: polars, py7zr, pandas, pyarrow
"""

from pathlib import Path
import re

import numpy as np
import pandas as pd
import polars as pl
import py7zr

from config import COLS

# Reject obviously malformed parquet schemas before concatenation.
# Normal truck files are ~245-277 cols in this dataset; malformed ones can have
# thousands of numeric-like "column names" (e.g., "0.00", "7.00.1").
_MAX_REASONABLE_COLS = 600
_NUMERIC_LIKE_COL_RE = re.compile(r"\s*\d+(\.\d+)*")
_MAX_NUMERIC_LIKE_COL_RATIO = 0.20


def _schema_suspicious_reason(schema: pl.Schema) -> str | None:
    """Return a reason string if schema looks malformed, otherwise None."""
    names = list(schema.names())
    n_cols = len(names)
    if n_cols == 0:
        return "empty schema"
    if n_cols > _MAX_REASONABLE_COLS:
        return f"too many columns ({n_cols} > {_MAX_REASONABLE_COLS})"

    n_numeric_like = sum(1 for n in names if _NUMERIC_LIKE_COL_RE.fullmatch(n))
    ratio = n_numeric_like / n_cols
    if ratio > _MAX_NUMERIC_LIKE_COL_RATIO:
        return (
            f"too many numeric-like column names "
            f"({n_numeric_like}/{n_cols} = {ratio:.1%})"
        )
    return None


# ---------------------------------------------------------------------------
# Step 1 – archive extraction + lazy scan
# ---------------------------------------------------------------------------

def read_nov_data(
    data_dir: str = "data/nov_data",
    truck_ids: list[str] | None = None,
) -> dict[str, pl.LazyFrame]:
    """Scan .7z archives under *data_dir* and return truck_id -> LazyFrame.

    Parquet files are extracted to a ``_parquet_cache`` sub-directory on the
    first call; subsequent calls reuse the cache.

    Args:
        data_dir:  Directory containing the .7z archives.
        truck_ids: Optional allow-list of truck IDs (e.g. ``["5FT0192"]``).
                   All trucks are loaded when ``None``.

    Returns:
        Dict mapping each truck_id to a polars LazyFrame.  Multiple parquet
        files for the same truck are concatenated with ``diagonal_relaxed``
        so missing columns across files are filled with nulls rather than
        raising an error.
    """
    data_dir  = Path(data_dir)
    cache_dir = data_dir / "_parquet_cache"

    # Create a cache directory for extracted parquet files. Create parent directories if needed.
    cache_dir.mkdir(parents=True, exist_ok=True)

    truck_lfs: dict[str, pl.LazyFrame] = {}

    for archive_path in sorted(data_dir.glob("*.7z")):
        truck_id = archive_path.stem
        if truck_ids is not None and truck_id not in truck_ids:
            continue

        truck_cache = cache_dir / truck_id
        if not truck_cache.exists():
            truck_cache.mkdir()
            with py7zr.SevenZipFile(archive_path, "r") as arc:
                arc.extractall(path=truck_cache)
            print(f"[loader] {truck_id}: extracted to cache")

        parquet_files = sorted(truck_cache.glob("*.parquet"))
        if not parquet_files:
            print(f"[loader] {truck_id}: no parquet files found in cache – skipping")
            continue

        valid_files: list[Path] = []
        bad_files: list[tuple[Path, str]] = []
        for f in parquet_files:
            try:
                # Force schema read early so corrupt files are skipped up-front.
                schema = pl.scan_parquet(f).collect_schema()
                suspicious = _schema_suspicious_reason(schema)
                if suspicious:
                    bad_files.append((f, f"suspicious schema: {suspicious}"))
                    continue
                valid_files.append(f)
            except Exception as exc:
                bad_files.append((f, str(exc)))

        if bad_files:
            print(f"[loader] {truck_id}: skipped {len(bad_files)} corrupt parquet file(s):")
            for bad_file, err in bad_files:
                print(f"    - {bad_file.name}: {err}")

        if not valid_files:
            print(f"[loader] {truck_id}: no valid parquet files after validation – skipping")
            continue

        lf = pl.concat(
            [
                pl.scan_parquet(f).with_columns(pl.lit(f.stem).alias("Date"))
                for f in valid_files
            ],
            how="diagonal_relaxed",
        )
        truck_lfs[truck_id] = lf
        print(
            f"[loader] {truck_id}: "
            f"{len(valid_files)} valid file(s), "
            f"{lf.collect_schema().len()} cols"
        )

    print(f"\n[loader] Total trucks loaded as LazyFrames: {len(truck_lfs)}")
    return truck_lfs


def read_data_dirs(
    data_dirs: list[str | Path],
    truck_ids: list[str] | None = None,
) -> dict[str, pl.LazyFrame]:
    """Load and merge trucks from multiple archive directories.

    Each directory is scanned with ``read_nov_data``. If a truck appears in
    more than one directory (e.g., ``nov_data`` + ``Summer_data``), all
    LazyFrames are concatenated with ``diagonal_relaxed``.
    """
    merged_lfs: dict[str, pl.LazyFrame] = {}
    usable_dirs: list[Path] = []

    for d in data_dirs:
        data_dir = Path(d)
        if not data_dir.exists() or not data_dir.is_dir():
            print(f"[loader] WARNING: data source missing, skipping -> {data_dir}")
            continue

        usable_dirs.append(data_dir)
        source_lfs = read_nov_data(data_dir=str(data_dir), truck_ids=truck_ids)
        for truck_id, lf in source_lfs.items():
            if truck_id in merged_lfs:
                merged_lfs[truck_id] = pl.concat(
                    [merged_lfs[truck_id], lf],
                    how="diagonal_relaxed",
                )
            else:
                merged_lfs[truck_id] = lf

    if not usable_dirs:
        joined = ", ".join(str(Path(d)) for d in data_dirs)
        raise FileNotFoundError(
            f"[loader] None of the configured data directories exist: {joined}"
        )

    print(f"\n[loader] Total trucks loaded across all sources: {len(merged_lfs)}")
    return merged_lfs


# ---------------------------------------------------------------------------
# Step 2 – Collect + Convert to Pandas + Assign Time Periods
# ---------------------------------------------------------------------------

def prepare_truck_dataframe(
    lf: pl.LazyFrame,
    truck_id: str = "",
) -> pd.DataFrame:
    """Collect a polars LazyFrame and return a pipeline-ready pandas DataFrame.

    Actions performed:
    - Collect the lazy query into memory.
    - Cast the Timestamp column to ``datetime64`` (pandas).
    - Sort by Timestamp and reset the integer index.

    Args:
        lf:       Polars LazyFrame for a single truck (from read_nov_data).
        truck_id: Optional label used only for log messages.

    Returns:
        Sorted pandas DataFrame ready to be passed into ``run_pipeline()``.

    Raises:
        ValueError: If the expected Timestamp column is absent.
    """
    ts_col = COLS["ts"]
    label  = truck_id or "truck"

    print(f"[loader] {label}: collecting LazyFrame ...")
    df: pd.DataFrame = lf.collect().to_pandas()

    if ts_col not in df.columns:
        raise ValueError(
            f"[loader] {label}: expected column '{ts_col}' not found. "
            f"Available columns: {list(df.columns[:10])} ..."
        )

    # -- Assign time periods ----------------------------------------------
    # A new time period starts when:
    # - the raw timestamp decreases (indicating a system reset or rollover), or
    # - the date changes (indicating a new parquet file / day).
    # - the raw timestamp increases but the date remains the same (indicating a new period within the same day).

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", format="%Y-%m-%d").dt.normalize()
    else:
        base = pd.Timestamp("2000-01-01")
        df["Date"] = base + pd.to_timedelta(df[ts_col], unit="s")

    # Sort by Date only (stable sort preserves within-day session order from
    # pl.concat, so overlapping sessions on the same day are NOT interleaved).
    df = df.sort_values("Date", kind="mergesort").reset_index(drop=True)

    df["ts_raw"] = pd.to_numeric(df[ts_col], errors="coerce")

    dts = df["ts_raw"].diff()

    # Normal cadence for positive diffs only (ignoring reboots and gaps between sessions)
    CONTINUITY_GAP = 30.0  # seconds; gaps larger than this are treated as session breaks, not cadence outliers

    # Build period break mask
    period_break = pd.Series(False, index=df.index)
    period_break.iloc[0] = True

    # Date change: new period when Date changes (indicating a new parquet file / day)
    period_break |= df["Date"].ne(df["Date"].shift()).fillna(False)

    # Hard boundary: timestamp decreases
    period_break |= dts.lt(0).fillna(False)

    # Adaptive boundary: timestamp increases but gap is larger than expected cadence (indicating a new session or period)
    period_break |= dts.gt(CONTINUITY_GAP).fillna(False)

    df['time_period'] = period_break.cumsum()

    # -- Convert Timestamp back to datetime so pipeline dt operations work ------
    # Use Date column as the calendar base when available; otherwise use a
    # synthetic epoch so that datetime arithmetic (diff, subtraction) is
    # still valid within each period.
    if "Date" in df.columns:
        df[ts_col] = df["Date"] + pd.to_timedelta(df["ts_raw"], unit="s")
    else:
        base = pd.Timestamp("2000-01-01")
        df[ts_col] = base + pd.to_timedelta(df["ts_raw"], unit="s")

    n_periods = df["time_period"].nunique()
    print(
        f"[loader] {label}: {len(df):,} rows, {df.shape[1]} cols | "
        f"{n_periods} time period(s)"
    )
    return df
