import pandas as pd
from utils import OUT_DIR, ensure_dir, load_parquet, write_parquet

def main():
    ensure_dir(OUT_DIR)

    teamstats = load_parquet("teamstats")
    games = load_parquet("games")
    leagues = load_parquet("leagues")

    df = teamstats.copy()
    df["fouls"] = pd.to_numeric(df["fouls"], errors="coerce").fillna(0)

    # Sum fouls for both teams per game
    fouls_per_game = (
        df.groupby("gameID")["fouls"].sum().reset_index(name="total_fouls_game")
    )

    fouls_per_game = fouls_per_game.merge(
        games[["gameID", "leagueID", "season"]],
        on="gameID",
        how="left"
    )

    fouls_per_game = fouls_per_game.merge(
        leagues[["leagueID", "name"]].rename(columns={"name": "league_name"}),
        on="leagueID",
        how="left"
    )

    grouped = fouls_per_game.groupby(
        ["season", "league_name"]
    )["total_fouls_game"].mean().reset_index()

    grouped = grouped.rename(
        columns={
            "season": "football_season",
            "total_fouls_game": "avg_fouls_per_game"
        }
    )

    out_path = OUT_DIR / "i2_avg_fouls_per_game.parquet"
    write_parquet(grouped, out_path)

    print("Insight 2 complete:", out_path)

if __name__ == "__main__":
    main()
