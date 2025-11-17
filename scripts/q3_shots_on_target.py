# scripts/q3_shots_moving_average.py
"""
Question 3:
Calculate number of shots on target per team per season,
plus a 5-game moving average, ordered by total wins.

Uses:
- teamstats (team-level statistics)
- teams (team names)

Output:
- football_season
- team_name
- shots_on_target
- moving_avg_5_games
- wins
"""

import pandas as pd

from utils import (
    OUT_DIR,
    ensure_dir,
    load_parquet,
    write_parquet,
)


def main():
    ensure_dir(OUT_DIR)

    teamstats = load_parquet("teamstats")
    teams = load_parquet("teams")

    df = teamstats.copy()

    # Convert date if needed
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Join team_name
    teams_small = teams[["teamID", "name"]].rename(columns={"name": "team_name"})
    df = df.merge(
        teams_small,
        on="teamID",
        how="left",
        validate="many_to_one"
    )

    # Ensure numeric
    df["shotsOnTarget"] = pd.to_numeric(df["shotsOnTarget"], errors="coerce").fillna(0)
    df["result"] = df["result"].astype(str)

    # Sort per team per season by date
    df = df.sort_values(["season", "teamID", "date"])

    # Rolling 5-game moving average per team
    df["moving_avg_5_games"] = (
        df.groupby(["season", "teamID"])["shotsOnTarget"]
        .rolling(window=5, min_periods=1)
        .mean()
        .reset_index(level=[0, 1], drop=True)
    )

    # Compute aggregates per (season, team)
    grouped = (
        df.groupby(["season", "teamID", "team_name"])
        .agg(
            shots_on_target=("shotsOnTarget", "sum"),
            wins=("result", lambda x: (x == "W").sum()),
            moving_avg_5_games=("moving_avg_5_games", "last"),  # Final rolling value
        )
        .reset_index()
    )

    # Final formatting
    grouped = grouped.rename(columns={"season": "football_season"})

    # Sort by season, then wins desc
    grouped = grouped.sort_values(
        ["football_season", "wins"],
        ascending=[True, False]
    ).reset_index(drop=True)

    # Save output
    out_path = OUT_DIR / "q3_results.parquet"
    write_parquet(grouped, out_path)

    print(f"Q3 completed successfully. Results written to {out_path}")


if __name__ == "__main__":
    main()
