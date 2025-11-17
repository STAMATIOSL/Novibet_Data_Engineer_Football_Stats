import pandas as pd
import matplotlib.pyplot as plt

def main():
    df = pd.read_parquet("output/i2_avg_fouls_per_game.parquet")

    # Only league with highest fouls per season
    top_leagues = (
        df.sort_values(["football_season", "avg_fouls_per_game"], ascending=[True, False])
          .groupby("football_season")
          .head(1)
          .reset_index(drop=True)
    )

    labels = top_leagues["football_season"].astype(str) + " - " + top_leagues["league_name"]

    plt.figure(figsize=(14, 6))
    plt.bar(labels, top_leagues["avg_fouls_per_game"], width=0.4)

    plt.title("Most Aggressive League per Season (Avg Fouls per Game)")
    plt.xlabel("Season - League")
    plt.ylabel("AVG Fouls per Game")
    plt.xticks(rotation=45, ha="right")

    plt.tight_layout()
    plt.savefig("output/insight2_fouls.png")
    plt.close()

    print("Insight 2 plot saved as output/insight2_fouls.png")

if __name__ == "__main__":
    main()
