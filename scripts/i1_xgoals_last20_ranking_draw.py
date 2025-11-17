import pandas as pd
import matplotlib.pyplot as plt

# Load insight result
df = pd.read_parquet("output/i1_xgoals_last20_ranking.parquet")

# Keep only rank 1 per season
top_leagues = df[df["league_rank"] == 1].copy()

# Sort by season
top_leagues = top_leagues.sort_values("football_season")

# Prepare labels: "Season - League"
labels = top_leagues["football_season"].astype(str) + " - " + top_leagues["league_name"]

# Plot
plt.figure(figsize=(14, 6))

# Thinner bars → set width=0.4
plt.bar(labels,
        top_leagues["total_xGoals_last20"],
        width=0.4)

# Titles
plt.title("Top League per Season (xGoals in Last 20 Minutes)", fontsize=14)
plt.xlabel("Season - League", fontsize=12)
plt.ylabel("Total xGoals (70'–90')", fontsize=12)

# Rotate labels for readability
plt.xticks(rotation=45, ha="right")

# Tight layout
plt.tight_layout()

plt.show()

print(top_leagues)
