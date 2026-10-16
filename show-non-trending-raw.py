import polars as pl
from pathlib import Path
pl.Config.set_tbl_rows(100)

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_PATH = PROJECT_ROOT / "data/raw/youtube/trending/trending_raw.parquet"

df = pl.read_parquet(DATA_PATH)

print("\nFirst 100 trending videos:")
print(df.head(100))