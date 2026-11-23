import threading
import time

import pandas as pd
import pytest

from filedrop.ingest import Ingestor


def write(path, rows, columns=("order_id", "order_date", "customer_id", "amount")):
    pd.DataFrame(rows, columns=list(columns)).to_csv(path, index=False)


@pytest.fixture
def ing(tmp_path):
    return Ingestor(tmp_path, stable_wait=0.05)


def count(ing):
    return ing.con.execute("SELECT count(*) FROM orders").fetchone()[0]


def test_loads_and_archives(ing):
    write(ing.landing / "a.csv", [(1, "2025-01-01", 7, 9.5), (2, "2025-01-02", 8, 10)])
    r = ing.run_once()
    assert r.loaded == ["a.csv"] and r.rows == 2 and count(ing) == 2
    assert not list(ing.landing.iterdir()) and len(list(ing.archive.iterdir())) == 1


def test_same_content_new_name_is_skipped(ing):
    write(ing.landing / "a.csv", [(1, "2025-01-01", 7, 9.5)])
    ing.run_once()
    write(ing.landing / "a_resent.csv", [(1, "2025-01-01", 7, 9.5)])
    r = ing.run_once()
    assert r.duplicates == ["a_resent.csv"] and count(ing) == 1


@pytest.mark.parametrize("rows,cols,msg", [
    ([(1, "2025-01-01", 7)], ("order_id", "order_date", "customer"), "columns"),
    ([(1, "2025-01-01", 7, 5), (1, "2025-01-01", 8, 6)], None, "duplicate order_id"),
    ([(1, "2025-01-01", 7, -5)], None, "negative"),
])
def test_bad_files_quarantined_with_reason(ing, rows, cols, msg):
    write(ing.landing / "bad.csv", rows, cols or ("order_id", "order_date", "customer_id", "amount"))
    r = ing.run_once()
    assert r.quarantined == ["bad.csv"] and count(ing) == 0
    assert msg in (ing.quarantine / "bad.csv.error.txt").read_text()


def test_file_still_being_written_is_skipped(tmp_path):
    ing = Ingestor(tmp_path, stable_wait=0.3)
    path = ing.landing / "slow.csv"
    path.write_text("order_id,order_date,customer_id,amount\n")

    def keep_writing():
        for i in range(6):
            with open(path, "a") as f:
                f.write(f"{i},2025-01-01,1,1.0\n")
            time.sleep(0.1)

    t = threading.Thread(target=keep_writing)
    t.start()
    r = ing.run_once()
    t.join()
    assert r.still_writing == ["slow.csv"] and count(ing) == 0
    r = ing.run_once()
    assert r.loaded == ["slow.csv"] and count(ing) == 6
