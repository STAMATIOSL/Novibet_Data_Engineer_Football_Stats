from fastapi import FastAPI
import pandas as pd
from fastapi.responses import FileResponse

app = FastAPI(
    title="Football Insights API",
    description="API exposing aggregated analytics (Q1, Q2, Insights)",
    version="1.0"
)

# Utility loader
def load_parquet(path):
    return pd.read_parquet(path)


# -------------------------------
# Q1: Top players per league/season
# -------------------------------
@app.get("/q1/top-players")
def get_q1_results():
    df = load_parquet("output/q1_results.parquet")
    return df.to_dict(orient="records")


# -------------------------------
# Q2: Corner goals per league/season
# -------------------------------
@app.get("/q2/corner-goals")
def get_q2_results():
    df = load_parquet("output/q2_results.parquet")
    return df.to_dict(orient="records")


# -------------------------------
# Q3: Avg shots on target
# -------------------------------
@app.get("/q3/avg-shots")
def get_q3_results():
    df = load_parquet("output/q3_results.parquet")
    return df.to_dict(orient="records")


# -------------------------------
# Insight 1: xGoals last 20 minutes ranking
# -------------------------------
@app.get("/insight1/xgoals-last20")
def get_insight1():
    df = load_parquet("output/i1_xgoals_last20_ranking.parquet")
    return df.to_dict(orient="records")


@app.get("/insight1/xgoals-last20/image")
def get_insight1_image():
    return FileResponse(
        "output/insight1_xgoals_last20.png",
        media_type="image/png",
        filename="insight1_xgoals_last20.png"
    )


# -------------------------------
# Insight 2: Avg fouls per game
# -------------------------------
@app.get("/insight2/fouls")
def get_insight2():
    df = load_parquet("output/i2_avg_fouls_per_game.parquet")
    return df.to_dict(orient="records")


@app.get("/insight2/fouls/image")
def get_insight2_image():
    return FileResponse(
        "output/insight2_fouls.png",
        media_type="image/png",
        filename="insight2_fouls.png"
    )