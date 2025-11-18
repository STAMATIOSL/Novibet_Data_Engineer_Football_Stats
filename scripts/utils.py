# scripts/utils.py
"""
utils.py
Common helper functions

Key points:
- read_understat_csv: fixes the weird quoting in the provided CSVs
- write_parquet: convenience wrapper with logging
"""

import io
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROC_DIR = BASE_DIR / "data" / "processed"
OUT_DIR = BASE_DIR / "output"


def ensure_dir(path: Path):
    """Ensure directory exists."""
    path.mkdir(parents=True, exist_ok=True)


def fix_double_quoted_file(path: Path) -> io.StringIO:
    """
    The raw CSVs are in a 'double-encoded' CSV format:
    - The entire line is wrapped in quotes: "gameID,""playerID"",..."
    - Inside the line we see doubled quotes: ""playerID""
    
    This function:
    - strips the outer quotes (if present)
    - replaces "" with "
    - returns a file-like object (StringIO) usable by pandas.read_csv
    """
    lines = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n\r")
            if '""' in line:
                # typical line in this dataset: whole line quoted + internal ""
                if line.startswith('"') and line.endswith('"'):
                    line = line[1:-1]
                line = line.replace('""', '"')
            lines.append(line)
    return io.StringIO("\n".join(lines))


def read_understat_csv(name: str, nrows: Optional[int] = None) -> pd.DataFrame:
    """
    Read one of the assignment CSVs from data/raw/, fixing the quoting.
    
    Parameters
    ----------
    name : str
        Base name without path, e.g. 'appearances.csv'
    nrows : int, optional
        Number of rows to read for sampling.
    
    Returns
    -------
    pd.DataFrame
    """
    path = RAW_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Raw CSV not found: {path}")
    
    buf = fix_double_quoted_file(path)
    df = pd.read_csv(buf, nrows=nrows)
    return df


def write_parquet(df: pd.DataFrame, out_path: Path):
    """Write DataFrame to Parquet, ensuring parent directory exists."""
    ensure_dir(out_path.parent)
    df.to_parquet(out_path, index=False)


def load_parquet(table: str) -> pd.DataFrame:
    """
    Convenience loader for processed parquet tables.
    Expects file at data/processed/<table>.parquet, e.g. shots.parquet
    """
    path = PROC_DIR / f"{table}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")
    df = pd.read_parquet(path)
    return df
