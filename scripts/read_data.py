from pathlib import Path

import polars as pl
import py7zr


def read_nov_data(
    data_dir: str = "data/nov_data",
    truck_ids: list[str] | None = None,
) -> dict[str, pl.LazyFrame]:
    """Read .7z files under data/nov_data and return a dict mapping truck_id -> LazyFrame.

    Extracts parquet files to a cache directory so that polars can lazily scan them.

    Args:
        data_dir: Path to the directory containing .7z archives.
        truck_ids: Optional list of truck IDs to load (e.g. ["5FT0192"]).
                   Loads all trucks if None.
    """
    data_dir = Path(data_dir)
    cache_dir = data_dir / "_parquet_cache"
    cache_dir.mkdir(exist_ok=True)
    truck_lfs = {}

    for archive_path in sorted(data_dir.glob("*.7z")):
        truck_id = archive_path.stem
        if truck_ids is not None and truck_id not in truck_ids:
            continue

        truck_cache = cache_dir / truck_id
        if not truck_cache.exists():
            truck_cache.mkdir()
            with py7zr.SevenZipFile(archive_path, "r") as archive:
                archive.extractall(path=truck_cache)
            print(f"{truck_id}: extracted to cache")

        parquet_files = sorted(truck_cache.glob("*.parquet"))
        lf = pl.concat(
            [
                pl.scan_parquet(f).with_columns(
                    pl.lit(f.stem).alias("Date"),
                )
                for f in parquet_files
            ],
            how="diagonal_relaxed",
        )
        truck_lfs[truck_id] = lf
        print(f"{truck_id}: {len(parquet_files)} files, {lf.collect_schema().len()} cols")

    print(f"\nTotal: {len(truck_lfs)} trucks loaded as LazyFrames")
    return truck_lfs


def main() -> None:
    truck_lfs = read_nov_data()

    for truck_id, lf in truck_lfs.items():
        row_count = lf.select(pl.len()).collect().item()
        print(f"\n--- {truck_id}: {row_count:,} rows ---")
        print(lf.head(5).collect())


if __name__ == "__main__":
    main()
