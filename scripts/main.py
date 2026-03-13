"""
main.py
-------
Entry point for the iQCruise driver-override extraction pipeline.

Edit the parameters directly in main() below – no command-line flags needed.

Workflow
--------
  1. read_nov_data()             – scan .7z archives → dict of LazyFrames
  2. prepare_truck_dataframe()   – collect + convert one truck to pandas
  3. run_pipeline()              – run all processing stages, write outputs
"""

import pandas as pd

from config import CFG
from data_loader import prepare_truck_dataframe, read_data_dirs
from pipeline import export_events, run_pipeline
from pathlib import Path


TARGET_OVERRIDE_TYPES = {"THROTTLE_OVERRIDE", "THROTTLE_BRAKE_PEDAL"}


def filter_target_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """Keep only throttle override and throttle+brake pedal events."""
    filtered = events_df[events_df["override_type"].isin(TARGET_OVERRIDE_TYPES)].copy()
    print(
        f"[main] Kept {len(filtered)} / {len(events_df)} target events "
        f"({', '.join(sorted(TARGET_OVERRIDE_TYPES))})."
    )
    return filtered


def main() -> None:
    # -----------------------------------------------------------------------
    # Parameters – Edit these directly
    # -----------------------------------------------------------------------

    # Establish root path for relative paths. Allows running main.py from any CWD.
    ROOT = Path(__file__).resolve().parents[1] # repo root since main.py is in scripts/

    # Directories containing the .7z archives
    data_dirs: list[Path] = [
        ROOT / "data" / "nov_data",
        ROOT / "data" / "Summer_data",
    ]

    # Which trucks to process.
    # Set to None to load all trucks found in data_dirs.
    truck_ids: list[str] | None = ["5FT0192"]

    # Where to write the summary events CSV.
    output_path: str = ROOT / "temp" / "override_events.csv"

    # Directory for context-window parquet output.
    # Set to "" to skip writing it.
    context_dir: str = ROOT / "temp" / "event_windows"

    # -- Pipeline thresholds (override CFG defaults here if needed) ---------
    CFG["MIN_ACTIVE_SESSION_S"] = 40.0   # seconds a session must run before override
    CFG["MIN_SPEED_KPH"]        = 30.0  # events below this speed are flagged noisy

    # -----------------------------------------------------------------------
    # Run
    # -----------------------------------------------------------------------

    truck_lfs = read_data_dirs(data_dirs=data_dirs, truck_ids=None)

    for truck_id, lf in truck_lfs.items():
        print(f"\n{'='*65}")
        print(f"  Processing truck: {truck_id}")
        print(f"{'='*65}")

        df = prepare_truck_dataframe(lf, truck_id=truck_id)

        # Optionally route outputs per truck
        truck_output = output_path.with_name(
            f"{output_path.stem}_{truck_id}.csv"
        )
        truck_ctx_dir = f"{context_dir}/{truck_id}" if context_dir else ""

        events_df = run_pipeline(
            df            = df,
            output_path   = truck_output,
            context_dir   = truck_ctx_dir,
            write_outputs = False,
            truck_id      = truck_id,
            write_context = bool(truck_ctx_dir),
            write_events  = False,
        )

        events_df = filter_target_events(events_df)

        export_events(events_df, truck_output)


if __name__ == "__main__":
    main()
