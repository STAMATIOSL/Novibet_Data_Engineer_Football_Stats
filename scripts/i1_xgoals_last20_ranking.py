import pandas as pd
from utils import OUT_DIR, ensure_dir, load_parquet, write_parquet

def main():
    ensure_dir(OUT_DIR)

    shots = load_parquet("shots")
    games = load_parquet("games")
    leagues = load_parquet("leagues")

    df = shots.copy()

    # Convert numeric
    df["xGoal"] = pd.to_numeric(df["xGoal"], errors="coerce").fillna(0)
    df["minute"] = pd.to_numeric(df["minute"], errors="coerce").fillna(0)

    # Keep last 20 minutes: >= 70'
    df = df[df["minute"] >= 70].copy()

    # Join games → season + leagueID
    df = df.merge(
        games[["gameID", "leagueID", "season"]],
        on="gameID",
        how="left"
    )

    # Join leagues → league_name
    df = df.merge(
        leagues[["leagueID", "name"]].rename(columns={"name": "league_name"}),
        on="leagueID",
        how="left"
    )

    # Group per league & season
    grouped = (
        df.groupby(["season", "league_name"])
        ["xGoal"]
        .sum()
        .reset_index(name="total_xGoals_last20")
    )

    # Rename season
    grouped = grouped.rename(columns={"season": "football_season"})

    # Ranking per season
    grouped["league_rank"] = grouped.groupby(
        "football_season"
    )["total_xGoals_last20"].rank(
        method="dense", ascending=False
    ).astype(int)

    # Sort
    grouped = grouped.sort_values(
        ["football_season", "league_rank"]
    ).reset_index(drop=True)

    out_path = OUT_DIR / "i1_xgoals_last20_ranking.parquet"
    write_parquet(grouped, out_path)

    print("Insight 3 complete:", out_path)


if __name__ == "__main__":
    main()
