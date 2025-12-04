# Data Engineering – Football Analytics Assessment

This repository contains my full solution to the **Data Engineer Technical Assessment**.  
It includes data processing scripts, analytical tasks, two custom insights, and an optional API.

## Summary of Steps Taken

1. Validated raw CSV structure and quoting format.
2. Built an ETL layer to convert all tables to Parquet.
3. Implemented Q1–Q3 strictly based on available columns.
4. Designed two additional insights (xGoals last 20', avg fouls per game).
5. Added an optional FastAPI layer exposing JSON + PNG endpoints.

---

## Project Structure
```
project/
│
├── data/
│   ├── raw                 # Raw CSV files
│   ├── processed           # Parquet-converted raw data
├── output/                 # All task results + insight plots
│
├── scripts/
│   ├── etl_to_parquet.py
│   ├── generate_insight1_plot.py
│   ├── generate_insight2_plot.py
│   ├── i1_xgoals_last20_ranking_draw.py
│   ├── i1_xgoals_last20_ranking.py
│   ├── i2_avg_fouls_per_game_draw.py
│   ├── i2_avg_fouls_per_game.py
│   ├── q1_top_players.py
│   ├── q2_corner_goals.py
│   ├── q3_shots_moving_average.py
│   └── utils.py
│
├── notebooks/
│   ├── 01_data_ingestion.ipynb
│   ├── 02_cleaning_and_parquet.ipynb
│   ├── 03_question_1.ipynb
│   ├── 04_question_2.ipynb
│   ├── 05_question_3.ipynb
│   ├── see_results.ipynb
│
├── api/
│   └── main.py             # FastAPI server
│
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Create virtual environment
```
python -m venv venv
venv\Scripts\activate      # Windows
```

### 2. Install dependencies
```
pip install -r requirements.txt
```

### 3. Convert raw CSV → Parquet
Add all 7 csv files in `data_raw/` and run `python scripts/etl_to_parquet.py`.

---

## Running the Tasks

### 3a – Top Players per League & Season
```
python scripts/q1_top_players.py
```

### 3b – Corner Goals per League & Season
```
python scripts/q2_goals_from_corners.py
```

### 3c – Shots on Target Moving Average (5-match window)
```
python scripts/q3_shots_on_target.py
```

---

## 3d - Insights

### Insight 1 – xGoals in the Last 20 Minutes (per league/season)
```
python scripts/i1_xgoals_last20_ranking.py
python scripts/i1_xgoals_last20_ranking_draw.py
python scripts/generate_insight1_plot.py
```
PNG output: `output/insight1_xgoals_last20.png`


### Insight 2 – Average Fouls per Game (per league/season)
```
python scripts/i2_fouls_per_game.py
python scripts/i2_fouls_per_game_draw.py
python scripts/generate_insight2_plot.py
```
PNG output: `output/insight2_fouls.png`

---

## Bonus: API

Start the FastAPI server:
```
uvicorn api.main:app --reload
```

Swagger UI:
```
http://127.0.0.1:8000/docs
```

The API exposes:
- JSON endpoints for tasks & insights  
- PNG download endpoints for the plots  

---

## Description of Technical Decisions & Approach

### Data Validation
Before implementing the tasks, I validated the raw dataset thoroughly.

## Notes on Data Choices

- The dataset does **not** contain any column connecting players to teams.  
  Therefore, Q1 was implemented **per league & season**, without team-level stats.
- All outputs follow the required parquet format.


### Task 3a – Top Players per League & Season
- Aggregates goals + shots
- Joins with leagues and players
- Returns top 5 per league/season

### Task 3b – Corner Goals per League/Season
- Extracts goals from corner situations
- Splits into 1st/2nd half
- Aggregates per league & season

### Task 3c – Moving Average Shots on Target
- Team-level metric
- Uses rolling window of 5
- Sorted by team performance per season

### Task 3d
## Insight 1 – xGoals in the Last 20 Minutes
- Shows late pressure per league & season
- Ranks leagues by xGoals (70'–90')
- Visualization included

## Insight 2 – Average Fouls per Game
- Identifies aggressive leagues
- Aggregates fouls per match
- Visualization included

### Bonus – REST API with FastAPI
- JSON endpoints for data
- PNG endpoints for charts
- Clean modular design

## Included Visualizations

`output/insight1_xgoals_last20.png`  
`output/insight2_fouls.png`

Both downloadable via API.

## Repository Link

A public version of this project is also available on GitHub:

https://github.com/STAMATIOSL/Data_Engineer_Football_Stats

## Author
Stamatis  
Novibet Data Engineer Assessment (2025)
