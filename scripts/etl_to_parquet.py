# scripts/etl_to_parquet.py
"""
ETL script: CSV -> Parquet

Steps:
1. Read each of the seven raw CSVs using our custom reader.
2. Do light schema normalization (types, dates).
3. Store them as Parquet in data/processed/.

Usage:
    python scripts/etl_to_parquet.py
"""

import pandas as pd

from utils import (
    PROC_DIR,
    read_understat_csv,
    write_parquet,
    ensure_dir,
)


TABLES = [
    "appearances",
    "games",
    "leagues",
    "players",
    "shots",
    "teams",
    "teamstats",
]


def normalize_games(df: pd.DataFrame) -> pd.DataFrame:
    """Parse games.date as datetime."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def normalize_teamstats(df: pd.DataFrame) -> pd.DataFrame:
    """Parse teamstats.date as datetime."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def normalize_appearances(df: pd.DataFrame) -> pd.DataFrame:
    """Basic casting for appearances."""
    df = df.copy()
    # obvious integer columns
    int_cols = ["gameID", "playerID", "goals", "ownGoals",
                "shots", "yellowCard", "redCard",
                "time", "substituteIn", "substituteOut", "leagueID"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def main():
    ensure_dir(PROC_DIR)
    for name in TABLES:
        csv_name = f"{name}.csv"
        df = read_understat_csv(csv_name)

        # light per-table normalization
        if name == "games":
            df = normalize_games(df)
        elif name == "teamstats":
            df = normalize_teamstats(df)
        elif name == "appearances":
            df = normalize_appearances(df)

        out_path = PROC_DIR / f"{name}.parquet"
        write_parquet(df, out_path)

    print("ETL completed. Parquet files are in data/processed/.")


if __name__ == "__main__":
    main()
