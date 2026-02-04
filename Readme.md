# ETL Workshop: Scrape BCCI Stats → PostgreSQL

Scraping cricket stats with Python, saving to CSV, loading into PostgreSQL, and running queries.

| | |
|--|--|
| **Part A** | Scrape → CSV |
| **Part B** | Load → PostgreSQL → Queries |

---

## Prerequisites

- Python 3.10+
- PostgreSQL (e.g. [Neon](https://neon.tech) free tier)
- Code editor or Jupyter/Colab

---

## 1. Setup

### 1.1 Install dependencies

Install:

- `requests`
- `beautifulsoup4`
- `pandas`
- `psycopg[binary]`

(Use `pip install` or your environment manager.)

### 1.2 Imports

Import: `os`, `sys`, `typing` (`List`, `Literal`, `Optional`), `pandas`, `requests`, `BeautifulSoup` from `bs4`, `psycopg`, `dict_row` from `psycopg.rows`. Add `IPython.display` if you use notebooks.

---

## 2. API endpoints and config

### 2.1 URLs — copy exact names

Define these constants — use these names so later steps work when you paste:

| Copy name |
|-----------|
| `MOST_RUNS_TEST_URL` |
| `MOST_RUNS_ODI_URL` |
| `TOP_WICKET_TAKERS_ODI_URL` |
| `TOP_WICKET_TAKERS_TEST_URL` |

Assign the BCCI `getStats` URLs to each (Test most runs, ODI most runs, ODI top wickets, Test top wickets).

### 2.2 Output directory

Create a constant for the folder where CSVs will be saved. Use this name:

`OUT_DIR`

Set it to a path under current working directory (e.g. `"out"`).

### 2.3 Discipline type

Define a type alias for `"batting"` or `"bowling"` (e.g. `Literal["batting", "bowling"]`). Use this name:

`Discipline`

### 2.4 Jobs list

Create a list of tuples: `(url, basename, discipline)`. Use this variable name:

`jobs`

One tuple per stat (e.g. Test most runs, ODI most runs, Test top wickets, ODI top wickets). Basenames: `bcci_test_most_runs`, `bcci_odi_most_runs`, `bcci_test_top_wickets`, `bcci_odi_top_wickets`.

### 2.5 Column definitions

Create a dict mapping `"batting"` and `"bowling"` to lists of column names. Use this name:

`columns`

Batting columns (order matters): `Rank`, `Name`, `Matches`, `Inns`, `Avg`, `SR`, `HS`, `Fours`, `Sixes`, `Fifties`, `Centuries`, `Runs`.  
Bowling: `Rank`, `Name`, `Matches`, `Inns`, `Avg`, `Econ`, `SR`, `BBI`, `Four_w`, `Five_w`, `Wkts`.

### 2.6 Paths list

Create a **list of strings** to hold paths of saved CSV files. Use this name (copyable):

`saved_paths`

Initialize as empty, e.g. `saved_paths: List[str] = []` or `saved_paths = []`.

---

## 3. Fetch raw data

### 3.1 Create output directory

Use `os.makedirs(OUT_DIR, exist_ok=True)`.

### 3.2 Store HTML per job

Create a dict to hold HTML string per basename. Use this name (copyable):

`html_store`

Type: `dict[str, str]` (or `Dict[str, str]`).

### 3.3 Loop over jobs

For each `(url, basename, kind)` in `jobs`:

1. `requests.get(url)` with a timeout; call `raise_for_status()`.
2. Parse response as JSON; get the `"html"` value.
3. If `html` is missing, skip (e.g. `continue`).
4. Store: `html_store[basename] = html`.
5. Optionally print that you got data for `basename`.

---

## 4. Helper functions

### 4.1 Label normalizer

Write a function that takes a string (stat label from HTML) and returns a normalized label. Use this name (copyable):

`normalize_label`

Inside it, use a **mapping** dict. You can copy the whole block below (variable name `mapping`):

```python
mapping = {
    "matches": "Matches",
    "inns": "Inns",
    "avg": "Avg",
    "sr": "SR",
    "hs": "HS",
    "runs": "Runs",
    "4's": "Fours",
    "4s": "Fours",
    "6's": "Sixes",
    "6s": "Sixes",
    "50's": "Fifties",
    "50s": "Fifties",
    "100's": "Centuries",
    "100s": "Centuries",
    "econ": "Econ",
    "economy": "Econ",
    "wkts": "Wkts",
    "wickets": "Wkts",
    "bbi": "BBI",
    "best bowling": "BBI",
    "best": "BBI",
    "4w": "Four_w",
    "5w": "Five_w",
}
```

Logic: clean the input (e.g. strip, lower, replace smart quotes); return `mapping.get(cleaned, default)`.

### 4.2 Value coercion

Write a function that converts a string to `int`, `float`, or leaves it as string. Use:

`coerce_value`

Try `int`, then `float` if needed; on failure return stripped string.

---

## 5. Extract data from HTML

### 5.1 Main extractor

Write a function that takes `html` (str) and `kind` (`Discipline`). Use this name (copyable):

`extract_data_from_html`

Steps:

1. Parse HTML with BeautifulSoup (e.g. `"lxml"`).
2. Find the stats table (e.g. selector `.stats-data-table-player table`).
3. Get the top-ranked player with a helper (e.g. `get_first_rank_player(soup)`) and add to records.
4. Loop over `tr` in the table; for each row get `td`s; skip if too few.
5. From first column get rank (number); from second get name. Build a row dict with `Rank`, `Name`.
6. For remaining columns: get value and label elements; use `normalize_label(label)` for key and `coerce_value(value)` for value; add to row dict.
7. Append each row to a list of records.
8. Fill missing keys (from `columns[kind]`) with `None` for every record.
9. Return the list of records.

### 5.2 First-rank player

Write a helper that takes the soup and returns one dict for the top player. Use this name (copyable):

`get_first_rank_player`

Find the name container and the ranking table; read name and each stat cell; use `normalize_label` and `coerce_value`; return dict with `Rank: 1`, `Name`, and stat keys.

---

## 6. Build DataFrames and save CSV

### 6.1 Loop over jobs and save

For each `(url, basename, discipline)` in `jobs`:

1. Get `html = html_store[basename]`.
2. Call `extract_data_from_html(html, discipline)` → `records`.
3. If no records, skip.
4. Build a DataFrame: `pd.DataFrame.from_records(records, columns=columns[discipline])`.
5. Optionally display the DataFrame.
6. Path: `os.path.join(OUT_DIR, f"{basename}.csv")`; save with `df.to_csv(path, index=False)`.
7. Append `path` to `saved_paths`.

---

## 7. Database setup

### 7.1 Database URL

- Create a PostgreSQL database (e.g. [Neon](https://console.neon.tech)).
- Get the connection URL (e.g. `postgresql://user:password@host/dbname?sslmode=require`).
- In Colab: Secrets → add secret name `DATABASE_URL`, value = that URL; enable notebook access.
- In local Python: use env var or a config; never commit the URL.

Store the URL in a variable named exactly (copyable):

`DATABASE_URL`

If not set, raise a clear error.

### 7.2 Create tables

Define a list of `CREATE TABLE IF NOT EXISTS` statements. Use this name (copyable):

`TABLES_TO_CREATE`

Tables to create (names must match your basenames and CSV usage):

- `bcci_odi_most_runs` — batting: rank (PK), name, matches, inns, avg, sr, hs, Fours, Sixes, Fifties, Centuries, runs.
- `bcci_odi_top_wickets` — bowling: rank (PK), name, matches, inns, avg, econ, sr, bbi, Four_w, Five_w, wkts.
- `bcci_test_most_runs` — same batting schema as ODI most runs.
- `bcci_test_top_wickets` — same bowling schema as ODI top wickets.

Use appropriate types: `INTEGER`, `TEXT`, `DOUBLE PRECISION` for numbers, `TEXT` for BBI.

Execute each statement in a single connection block: `psycopg.connect(DATABASE_URL, autocommit=True)`, then loop and `cur.execute(create_query)`.

---

## 8. Insert helpers

Define three small helpers for safe conversion from CSV/DataFrame to DB (copyable names):

`to_int` · `to_float` · `to_text`

- `to_int(v)`: try `int(v)`, else return `None`.
- `to_float(v)`: try `float(v)`, else return `None`.
- `to_text(v)`: strip; return `None` if empty or `"-"`.

---

## 9. Load CSV into PostgreSQL

### 9.1 Insert statement (ODI most runs)

Define the INSERT for `bcci_odi_most_runs` with column list and placeholders (e.g. `%(rank)s`, `%(name)s`, …). Use `ON CONFLICT (rank) DO UPDATE SET ...` so re-runs upsert. Store the SQL string in (copyable):

`INSERT_ODI_MOST_RUNS_SQL`

### 9.2 Run the insert

1. Set `csv_path` to the ODI most runs CSV (e.g. under `OUT_DIR`, file `bcci_odi_most_runs.csv`). Check `os.path.exists(csv_path)`.
2. Open DB connection and cursor; open CSV with `open(..., newline="", encoding="utf-8")`.
3. Read CSV with `pd.read_csv(f)`.
4. Build a list of dicts from each row: map DataFrame columns to keys expected by your INSERT (e.g. `rank`, `name`, `matches`, …) using `to_int`, `to_float`, `to_text`. Skip rows where `rank` is None.
5. `cur.executemany(INSERT_ODI_MOST_RUNS_SQL, records_to_insert)`.
6. Repeat the same pattern for Test most runs and for both wicket tables (write their INSERT SQL and use the same pattern).

---

## 10. Queries (practice)

After data is loaded, run SQL against the same connection (or new one):

- Top 10 run-scorers in Tests / ODIs.
- Top 10 wicket-takers in Tests / ODIs.
- Players with most centuries (batting tables).
- Bowlers with best economy (min wickets threshold).
- Join or compare across formats (e.g. same player in Test vs ODI).

Use `cur.execute(query)` and fetch results; print or display in a DataFrame.

---

## Copy-paste checklist

| What to use exactly | Variable / name |
|---------------------|-----------------|
| Test most runs URL  | `MOST_RUNS_TEST_URL` |
| ODI most runs URL   | `MOST_RUNS_ODI_URL` |
| ODI top wickets URL | `TOP_WICKET_TAKERS_ODI_URL` |
| Test top wickets URL| `TOP_WICKET_TAKERS_TEST_URL` |
| Output directory    | `OUT_DIR` |
| Job list            | `jobs` |
| Column definitions  | `columns` |
| Saved CSV paths     | `saved_paths` |
| HTML cache          | `html_store` |
| Label mapping       | `mapping` (inside `normalize_label`) |
| DB URL              | `DATABASE_URL` |
| Table DDL list      | `TABLES_TO_CREATE` |
| ODI most runs INSERT| `INSERT_ODI_MOST_RUNS_SQL` |

Keep these names identical so instructions and pasted snippets stay consistent.
