# scripts/q2_goals_from_corners.py
"""
Question 2:
For each league and season, find in which half (first or second)
we had the most goals from corners.

Definition used:
- Goal: shots where shotResult == 'Goal'
- From corner: shots where 'Corner' or 'FromCorner' appears in
  'situation' or 'lastAction'.
- First half: minute in [1, 45]
- Second half: minute in [46, 90]

Output:
- football_season
- league_name
- half   ('first' or 'second')

Stored as: output/q2_results.parquet
"""

import pandas as pd

from utils import (
    OUT_DIR,
    ensure_dir,
    load_parquet,
    write_parquet,
)


def determine_half(minute: float) -> str:
    """Map minute to 'first', 'second' or 'other'."""
    if 1 <= minute <= 45:
        return "first"
    if 46 <= minute <= 90:
        return "second"
    return "other"


def main():
    ensure_dir(OUT_DIR)

    shots = load_parquet("shots")
    games = load_parquet("games")
    leagues = load_parquet("leagues")

    s = shots.copy()

    # 1. Mark goals
    s["is_goal"] = s["shotResult"].astype(str).str.lower().eq("goal")

    # 2. Mark corner-related shots
    corner_mask = (
        s["situation"].astype(str).str.contains("corner", case=False, na=False)
        | s["lastAction"].astype(str).str.contains("corner", case=False, na=False)
    )
    s["is_corner"] = corner_mask

    # 3. Filter corner goals
    cg = s[s["is_goal"] & s["is_corner"]].copy()

    # 4. Determine half
    cg["minute"] = pd.to_numeric(cg["minute"], errors="coerce")
    cg["half"] = cg["minute"].apply(determine_half)
    cg = cg[cg["half"].isin(["first", "second"])].copy()

    # 5. Bring in season & league from games
    games_small = games[["gameID", "leagueID", "season"]].copy()
    cg = cg.merge(
        games_small,
        on="gameID",
        how="left",
        validate="many_to_one",
    )

    # 6. Add league_name
    leagues_small = leagues[["leagueID", "name"]].rename(columns={"name": "league_name"})
    cg = cg.merge(
        leagues_small,
        on="leagueID",
        how="left",
        validate="many_to_one",
    )

    # 7. Count goals per (season, league, half)
    grp = (
        cg.groupby(["season", "league_name", "half"])
        .size()
        .reset_index(name="corner_goals")
    )

    # 8. For each (season, league), take half with max corner_goals
    idx = grp.groupby(["season", "league_name"])["corner_goals"].idxmax()
    top_half = grp.loc[idx].copy()

    top_half = top_half.rename(columns={"season": "football_season"})

    final = top_half[["football_season", "league_name", "half"]].reset_index(drop=True)

    out_path = OUT_DIR / "q2_results.parquet"
    write_parquet(final, out_path)
    print(f"Q2 done. Results written to {out_path}")


if __name__ == "__main__":
    main()
