# File-Drop Ingestion

Loads CSV files that partners drop into a landing folder, **exactly once**, without picking up half-uploaded files or letting bad files into the warehouse.

```
landing/      partners drop files here
archive/      loaded files, renamed with the start of their checksum
quarantine/   rejected files, each with a .error.txt saying why
```

## Rules

| Situation | What happens |
|---|---|
| File still being uploaded | skipped this round: it's only picked up once its size stops changing |
| Same file sent again (any name) | skipped: its SHA-256 is already in the `ingest_manifest` table |
| Wrong columns, null or duplicate IDs, negative amounts | moved to `quarantine/` with the reason |
| Valid file | rows + manifest entry inserted in **one transaction**, then the file is archived |

Checksums matter because partners often resend the same file with a new name (`..._RESEND.csv`, `... (1).csv`). Matching on the file name would load it twice.

## Demo: one week of drops

```
day 1: loaded ['orders_20251101.csv'] rows=1140
day 2: loaded ['orders_20251102.csv'] rows=949
day 3: loaded ['orders_20251103.csv'] rows=1174
day 4: quarantined=['orders_20251104.csv']                     <- one row with a negative amount
day 5: loaded ['orders_20251105.csv'] rows=1190 dup=['orders_20251103_RESEND.csv']
day 6: loaded ['orders_20251106.csv'] rows=879
day 7: loaded ['orders_20251107.csv'] rows=932

orders table: 6,264 rows, 6,264 distinct order ids, 6 files
quarantine reason: negative amounts
```

The resent day-3 file was caught by its checksum, and no duplicate order IDs reached the table.

## Tests
- load and archive
- same content under a new name is skipped
- three kinds of bad file each go to quarantine with the right reason
- a file being written in a background thread is skipped, then loaded on the next scan once it's complete

## How to run

```bash
pip install -r requirements.txt
PYTHONPATH=src python demo.py
PYTHONPATH=src python -m filedrop /data/partner_x --db warehouse.duckdb --interval 30
pytest
```

## Next steps
- Accept a `.done` marker file from senders that can write one
- Pick up from S3 or SFTP instead of a local folder
