import pandas as pd
import matplotlib.pyplot as plt

# Load insight result
df = pd.read_parquet("output/i2_avg_fouls_per_game.parquet")

# Keep only the league with the highest fouls per season
top_leagues = (
    df.sort_values(["football_season", "avg_fouls_per_game"], ascending=[True, False])
      .groupby("football_season")
      .head(1)
      .reset_index(drop=True)
)

# Compose labels: "Season - League"
labels = top_leagues["football_season"].astype(str) + " - " + top_leagues["league_name"]

# Plot
plt.figure(figsize=(14, 6))

# Thin bars
plt.bar(labels,
        top_leagues["avg_fouls_per_game"],
        width=0.4)

# Titles & labels
plt.title("Most Aggressive League per Season (Avg Fouls per Game)", fontsize=14)
plt.xlabel("Season - League", fontsize=12)
plt.ylabel("Average Fouls per Game", fontsize=12)

# Rotate x-axis labels for readability
plt.xticks(rotation=45, ha="right")

plt.tight_layout()
plt.show()

print(top_leagues)
