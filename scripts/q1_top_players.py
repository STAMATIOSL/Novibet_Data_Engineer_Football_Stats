# scripts/q1_top_players.py
"""
Question 1 (Data-Driven Correct Version):
Top 5 players with the most goals per league for each season.
Tie-breaker: total shots.

This follows the output schema provided and aligns with the available dataset
(no player-to-team mapping exists in appearances or shots).

Final Output Columns:
- football_season
- league_name
- player_name
- total_goals
- total_shots

Sorting Order:
1. league_name ASC
2. football_season ASC
3. total_goals DESC
4. total_shots DESC
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

    # Load parquet tables
    appearances = load_parquet("appearances")
    games = load_parquet("games")
    leagues = load_parquet("leagues")
    players = load_parquet("players")

    ap = appearances.copy()

    # Join season + leagueID from games
    games_small = games[["gameID", "leagueID", "season"]].copy()
    ap = ap.merge(
        games_small,
        on=["gameID", "leagueID"],
        how="left",
        validate="many_to_one"
    )

    # Add league_name
    leagues_small = leagues[["leagueID", "name"]].rename(columns={"name": "league_name"})
    ap = ap.merge(
        leagues_small,
        on="leagueID",
        how="left",
        validate="many_to_one"
    )

    # Add player_name
    players_small = players[["playerID", "name"]].rename(columns={"name": "player_name"})
    ap = ap.merge(
        players_small,
        on="playerID",
        how="left",
        validate="many_to_one"
    )

    # Convert numeric columns
    ap["goals"] = pd.to_numeric(ap["goals"], errors="coerce").fillna(0).astype(int)
    ap["shots"] = pd.to_numeric(ap["shots"], errors="coerce").fillna(0).astype(int)

    # Group by season, league, player
    grouped = (
        ap.groupby(
            ["season", "league_name", "playerID", "player_name"],
            dropna=False
        )
        .agg(
            total_goals=("goals", "sum"),
            total_shots=("shots", "sum"),
        )
        .reset_index()
    )

    # Sort for ranking
    grouped = grouped.sort_values(
        ["season", "league_name", "total_goals", "total_shots"],
        ascending=[True, True, False, False]
    )

    # Rank within each league-season
    grouped["rank"] = grouped.groupby(
        ["season", "league_name"]
    )["total_goals"].rank(method="first", ascending=False)

    # Keep top 5 per group
    top5 = grouped[grouped["rank"] <= 5].copy()

    # Final formatting
    top5 = top5.rename(columns={"season": "football_season"})
    final = top5[
        [
            "football_season",
            "league_name",
            "player_name",
            "total_goals",
            "total_shots",
        ]
    ].reset_index(drop=True)

    # Save
    out_path = OUT_DIR / "q1_results.parquet"
    write_parquet(final, out_path)

    print(f"Q1 completed successfully. Results written to {out_path}")


if __name__ == "__main__":
    main()
