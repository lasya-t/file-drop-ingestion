"""Simulate a week of partner file drops, including resends, a bad file and a slow upload."""
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

from filedrop.ingest import Ingestor

rng = np.random.default_rng(0)
root = Path(tempfile.mkdtemp())
ing = Ingestor(root, stable_wait=0.1)
next_id = 1


def drop(name, n, corrupt=False):
    global next_id
    df = pd.DataFrame({
        "order_id": np.arange(next_id, next_id + n),
        "order_date": "2025-11-0" + name[-5],
        "customer_id": rng.integers(1, 500, n),
        "amount": rng.uniform(5, 900, n).round(2),
    })
    next_id += n
    if corrupt:
        df.loc[3, "amount"] = -40.0
    df.to_csv(ing.landing / name, index=False)
    return df


for day in range(1, 8):
    drop(f"orders_2025110{day}.csv", int(rng.integers(800, 1200)), corrupt=(day == 4))
    if day == 5:  # partner resends day 3 under a new name
        (ing.landing / "orders_20251103_RESEND.csv").write_bytes((ing.archive / next(
            p.name for p in ing.archive.iterdir() if "20251103" in p.name)).read_bytes())
    r = ing.run_once()
    print(f"day {day}: loaded {r.loaded or '-'} rows={r.rows} dup={r.duplicates or '-'} quarantined={r.quarantined or '-'}")

total = ing.con.execute("SELECT count(*), count(DISTINCT order_id), count(DISTINCT source_file) FROM orders").fetchone()
print(f"\norders table: {total[0]:,} rows, {total[1]:,} distinct order ids, {total[2]} files")
print("quarantine reason:", (ing.quarantine / "orders_20251104.csv.error.txt").read_text().strip())
