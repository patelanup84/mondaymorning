#!/usr/bin/env python3

import pandas as pd
from pathlib import Path

# Read the CSV file
csv_path = Path("data/raw/quickpossession_2025-09-20.csv")
df = pd.read_csv(csv_path)

print("CSV Structure:")
print(f"Columns: {list(df.columns)}")
print(f"Rows: {len(df)}")
print("\nFirst row:")
print(df.iloc[0].to_dict())
print("\nData types:")
print(df.dtypes)