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

import numpy as np
import pandas as pd
import polars as pl
import py7zr

from config import COLS


# ---------------------------------------------------------------------------
# Step 1 – Archive Extraction + Lazy Scan
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

        lf = pl.concat(
            [
                pl.scan_parquet(f).with_columns(pl.lit(f.stem).alias("Date"))
                for f in parquet_files
            ],
            how="diagonal_relaxed",
        )
        truck_lfs[truck_id] = lf
        print(
            f"[loader] {truck_id}: "
            f"{len(parquet_files)} file(s), "
            f"{lf.collect_schema().len()} cols"
        )

    print(f"\n[loader] Total trucks loaded as LazyFrames: {len(truck_lfs)}")
    return truck_lfs


# ---------------------------------------------------------------------------
# Step 2 – Collect + Convert for Pipeline
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

    # -- Assign time periods (Timestamp resets on system restart) -----------
    # A new period starts whenever the raw Timestamp decreases (reset) or
    # the Date column changes (new parquet file / day).
    raw_ts = df[ts_col].values
    period_break = np.zeros(len(df), dtype=bool)
    period_break[0] = True
    period_break[1:] = raw_ts[1:] < raw_ts[:-1]

    if "Date" in df.columns:
        date_change = df["Date"].values[1:] != df["Date"].values[:-1]
        period_break[1:] |= date_change

    df["time_period"] = np.cumsum(period_break)

    # -- Elapsed time (seconds from start of each period) ------------------
    period_starts = df.groupby("time_period")[ts_col].transform("first")
    df["elapsed_s"] = df[ts_col] - period_starts

    # -- Convert Timestamp to datetime so pipeline dt operations work ------
    # Use Date column as the calendar base when available; otherwise use a
    # synthetic epoch so that datetime arithmetic (diff, subtraction) is
    # still valid within each period.
    if "Date" in df.columns:
        base = pd.to_datetime(df["Date"], format="%Y-%m-%d")
    else:
        base = pd.Timestamp("2000-01-01")
    df[ts_col] = base + pd.to_timedelta(df[ts_col], unit="s")

    n_periods = df["time_period"].nunique()
    print(
        f"[loader] {label}: {len(df):,} rows, {df.shape[1]} cols | "
        f"{n_periods} time period(s)"
    )
    return df
