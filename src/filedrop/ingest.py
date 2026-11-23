"""Watch a landing folder and load each new CSV exactly once.

landing/      partners drop files here (possibly still being written)
archive/      files that loaded, renamed with their checksum
quarantine/   files that failed validation, with a .error.txt next to them

Rules:
    * a file is only picked up once its size has stopped changing (still-uploading files are skipped)
    * the same content is never loaded twice, even under a different name (SHA-256 manifest)
    * each file loads in one transaction: all rows or none
"""
from __future__ import annotations

import hashlib
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import pandas as pd

EXPECTED_COLUMNS = ["order_id", "order_date", "customer_id", "amount"]

DDL = """
CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT, order_date DATE, customer_id BIGINT, amount DECIMAL(12, 2), source_file VARCHAR
);
CREATE TABLE IF NOT EXISTS ingest_manifest (
    sha256 VARCHAR PRIMARY KEY, file_name VARCHAR, rows BIGINT, loaded_at TIMESTAMP
);
"""


@dataclass
class RunReport:
    loaded: list[str] = field(default_factory=list)
    duplicates: list[str] = field(default_factory=list)
    quarantined: list[str] = field(default_factory=list)
    still_writing: list[str] = field(default_factory=list)
    rows: int = 0


def sha256(path: Path, chunk=1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while block := f.read(chunk):
            h.update(block)
    return h.hexdigest()


def is_stable(path: Path, wait: float) -> bool:
    """True if the file size didn't change over `wait` seconds."""
    before = path.stat().st_size
    time.sleep(wait)
    return path.stat().st_size == before and before > 0


def validate(df: pd.DataFrame) -> str | None:
    if list(df.columns) != EXPECTED_COLUMNS:
        return f"columns {list(df.columns)} != expected {EXPECTED_COLUMNS}"
    if df["order_id"].isna().any():
        return "order_id has nulls"
    if df["order_id"].duplicated().any():
        return f"{int(df['order_id'].duplicated().sum())} duplicate order_id values"
    if (pd.to_numeric(df["amount"], errors="coerce") < 0).any():
        return "negative amounts"
    return None


class Ingestor:
    def __init__(self, root: str | Path, db: str | Path = ":memory:", stable_wait: float = 1.0):
        self.root = Path(root)
        self.landing, self.archive, self.quarantine = (self.root / d for d in ("landing", "archive", "quarantine"))
        for d in (self.landing, self.archive, self.quarantine):
            d.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(db))
        self.con.execute(DDL)
        self.stable_wait = stable_wait

    def run_once(self) -> RunReport:
        report = RunReport()
        for path in sorted(self.landing.glob("*.csv")):
            if not is_stable(path, self.stable_wait):
                report.still_writing.append(path.name)
                continue
            digest = sha256(path)
            if self.con.execute("SELECT 1 FROM ingest_manifest WHERE sha256 = ?", [digest]).fetchone():
                report.duplicates.append(path.name)
                shutil.move(path, self.archive / f"DUPLICATE_{digest[:12]}_{path.name}")
                continue
            try:
                df = pd.read_csv(path)
                problem = validate(df)
            except Exception as e:  # unreadable file
                problem = f"could not parse: {e}"
            if problem:
                report.quarantined.append(path.name)
                shutil.move(path, self.quarantine / path.name)
                (self.quarantine / f"{path.name}.error.txt").write_text(problem + "\n")
                continue
            df["source_file"] = path.name
            self.con.execute("BEGIN")
            try:
                self.con.register("incoming", df)
                self.con.execute("INSERT INTO orders SELECT * FROM incoming")
                self.con.execute("INSERT INTO ingest_manifest VALUES (?, ?, ?, now())", [digest, path.name, len(df)])
                self.con.execute("COMMIT")
            except Exception:
                self.con.execute("ROLLBACK")
                raise
            finally:
                self.con.unregister("incoming")
            shutil.move(path, self.archive / f"{digest[:12]}_{path.name}")
            report.loaded.append(path.name)
            report.rows += len(df)
        return report

    def watch(self, interval: float = 10.0, iterations: int | None = None):
        i = 0
        while iterations is None or i < iterations:
            r = self.run_once()
            if r.loaded or r.duplicates or r.quarantined:
                print(f"loaded {r.loaded} ({r.rows} rows) dup {r.duplicates} quarantined {r.quarantined}")
            time.sleep(interval)
            i += 1
