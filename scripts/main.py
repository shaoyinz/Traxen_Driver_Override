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

from config import CFG
from data_loader import prepare_truck_dataframe, read_nov_data
from pipeline import run_pipeline
from pathlib import Path


def main() -> None:
    # -----------------------------------------------------------------------
    # Parameters – Edit these directly
    # -----------------------------------------------------------------------

    # Establish root path for relative paths. Allows running main.py from any CWD.
    ROOT = Path(__file__).resolve().parents[1] # repo root since main.py is in scripts/

    # Directory containing the .7z archives
    data_dir: str = ROOT / "data" / "nov_data"

    # Which trucks to process.
    # Set to None to load all trucks found in data_dir.
    truck_ids: list[str] | None = ["5FT0217"]

    # Where to write the summary events CSV.
    output_path: str = ROOT / "temp" / "override_events.csv"

    # Directory for per-event 20-second context CSVs.
    # Set to "" to skip writing them.
    context_dir: str = ROOT / "temp" / "event_windows"

    # -- Pipeline thresholds (override CFG defaults here if needed) ---------
    CFG["MIN_ACTIVE_SESSION_S"] = 5.0   # seconds a session must run before override
    CFG["MIN_SPEED_KPH"]        = 15.0  # events below this speed are flagged noisy

    # -----------------------------------------------------------------------
    # Run
    # -----------------------------------------------------------------------

    truck_lfs = read_nov_data(data_dir=data_dir, truck_ids=truck_ids)

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

        run_pipeline(
            df           = df,
            output_path  = truck_output,
            context_dir  = truck_ctx_dir,
        )


if __name__ == "__main__":
    main()
