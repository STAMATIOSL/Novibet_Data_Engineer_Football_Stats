import pandas as pd
import matplotlib.pyplot as plt

def main():
    df = pd.read_parquet("output/i1_xgoals_last20_ranking.parquet")

    # Only rank 1 per season
    top_leagues = (
        df.sort_values(["football_season", "total_xGoals_last20"], ascending=[True, False])
          .groupby("football_season")
          .head(1)
          .reset_index(drop=True)
    )

    labels = top_leagues["football_season"].astype(str) + " - " + top_leagues["league_name"]

    plt.figure(figsize=(14, 6))
    plt.bar(labels, top_leagues["total_xGoals_last20"], width=0.4)

    plt.title("Top League per Season (xGoals in Last 20 Minutes)")
    plt.xlabel("Season - League")
    plt.ylabel("Total xGoals (70'-90')")
    plt.xticks(rotation=45, ha="right")

    plt.tight_layout()
    plt.savefig("output/insight1_xgoals_last20.png")
    plt.close()

    print("Insight 1 plot saved as output/insight1_xgoals_last20.png")

if __name__ == "__main__":
    main()
