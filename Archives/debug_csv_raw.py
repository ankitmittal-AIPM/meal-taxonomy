# debug_csv_raw.py
# this gives shape of the CSV and first few lines
from pathlib import Path

path = Path("data/indian_food.csv")

print("=== RAW FILE CONTENT (first 5 lines) ===")
with path.open("r", encoding="utf-8") as f:
    for i in range(5):
        line = f.readline()
        if not line:
            break
        print(repr(line))

print("\n=== Pandas view of columns ===")
import pandas as pd
df = pd.read_csv(path)
print("Columns:", list(df.columns))
print("Shape:", df.shape)
print("\nFirst 3 rows:")
print(df.head(3))
print("\nLast 3 rows:")
print(df.tail(3))   