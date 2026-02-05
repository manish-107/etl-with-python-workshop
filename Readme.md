# ETL Workshop: Scrape BCCI Stats → PostgreSQL

Scraping cricket stats with Python, saving to CSV, loading into PostgreSQL, and running queries.

## What we'll cover today

- **Find the data source**: Open the BCCI stats page in the browser, use **Inspect → Network**, and watch the requests to discover the API calls that return the HTML stats data we need. https://www.bcci.tv/international/men/rankings/test
- **Fetch raw data from BCCI**: Use `requests` to call those URLs, get the JSON payload, and extract the embedded HTML.
- **Parse and clean stats**: Use `BeautifulSoup` plus helper functions to normalize labels, convert text to numbers, and build clean Python dictionaries.
- **Build DataFrames and CSVs**: Turn parsed records into `pandas` DataFrames for Test/ODI batting and bowling, then save them as CSV files.
- **Set up PostgreSQL**: Create a Neon database, store `DATABASE_URL` as a Colab secret, and define `CREATE TABLE` statements for all four stats tables.
- **Load CSVs into the database**: Write insert helpers and `INSERT ... ON CONFLICT` queries to load the cleaned stats into PostgreSQL.
- **Query and compare players**: Run SELECT queries to pull top players from ODI vs Test tables and visualize comparisons with `matplotlib`.

Client-server -> https://blog.algomaster.io/i/148038540/3-tier-architecture

|            |                             |
| ---------- | --------------------------- |
| **Part A** | Scrape → CSV                |
| **Part B** | Load → PostgreSQL → Queries |

---

## Prerequisites

- PostgreSQL (e.g. [Neon](https://neon.tech) free tier)
- Google Colab

---

## 1. Scrape the data

### 1.1 Install and import the necessary packages

```python
!pip install psycopg[binary]
```

### Imports

```python
import os
import sys
from typing import List, Literal, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
import psycopg
from psycopg.rows import dict_row
```

---

## 1.2 API Endpoints and other initializations

### URLs — copy exact names and values

```python
MOST_RUNS_TEST_URL = "https://www.bcci.tv/getStats?platform=international&type=men&s_type=batting&slug=batting_most_runs&format=test"
MOST_RUNS_ODI_URL = "https://www.bcci.tv/getStats?platform=international&type=men&s_type=batting&slug=batting_most_runs&format=odi"
TOP_WICKET_TAKERS_ODI_URL = "https://www.bcci.tv/getStats?platform=international&type=men&s_type=bowling&slug=bowling_top_wicket_takers&format=odi"
TOP_WICKET_TAKERS_TEST_URL = "https://www.bcci.tv/getStats?platform=international&type=men&s_type=bowling&slug=bowling_top_wicket_takers&format=test"
```

### Output directory

Create a constant for the folder where CSVs will be saved:

```python
OUT_DIR = os.path.join(os.getcwd(), "out")
```

### Discipline type

Define a type alias for `"batting"` or `"bowling"`:

```python
Discipline = Literal["batting", "bowling"]
```

### Jobs list

Create a list of tuples `(url, basename, discipline)` — one per stat. Use this variable name and structure:

```python
jobs: list[tuple[str, str, Discipline]] = [
    (MOST_RUNS_TEST_URL, "bcci_test_most_runs", "batting"),
    (MOST_RUNS_ODI_URL, "bcci_odi_most_runs", "batting"),
    (TOP_WICKET_TAKERS_TEST_URL, "bcci_test_top_wickets", "bowling"),
    (TOP_WICKET_TAKERS_ODI_URL, "bcci_odi_top_wickets", "bowling"),
]
```

### Column definitions

Create a dict mapping `"batting"` and `"bowling"` to lists of column names (order matters):

```python
columns: dict[Discipline, list[str]] = {
"batting": [
        "Rank",
        "Name",
        "Matches",
        "Inns",
        "Avg",
        "SR",
        "HS",
        "Fours",
        "Sixes",
        "Fifties",
        "Centuries",
        "Runs",
    ],
    "bowling": [
        "Rank",
        "Name",
        "Matches",
        "Inns",
        "Avg",
        "Econ",
        "SR",
        "BBI",
        "Four_w",
        "Five_w",
        "Wkts",
    ],
}
```

### Paths list

Create a list of strings to hold paths of saved CSV files:

```python
saved_paths: List[str] = []
```

---

## 1.3 Let's get the data in it's raw format

### Boilerplate for fetching HTML

Copy this block and fill in the logic under each comment:

```python
# Create output directory if it doesn't exist (use OUT_DIR as the folder name)


# Create an empty dictionary html_store to hold raw HTML
#   - key: basename (str)
#   - value: HTML string (str)


# Loop over all (url, basename, kind) items in jobs


# For each job: make HTTP GET request to the BCCI API (with timeout and raise_for_status)


# Parse JSON response (resp.json()) and read the "html" field


# If the "html" field is missing or empty, print a message and skip this job (continue)


# Otherwise, store the HTML in html_store using basename as the key


# Print a short success message for each basename you processed

# See the HTML if you wish (⚠️ The contents are too long!)
    # print(html)
```

---

## 1.4 Helper functions for cleaning the data

### Helper boilerplate

Copy this block and then fill in the TODO parts yourself:

```python
def normalize_label(text: str) -> str:
    """Normalize cricket statistics labels to a consistent format."""
    # TODO: clean up the raw label text (strip, lower, remove dots, handle smart quotes)
    # low = ...

    # Mapping dictionary to standardize label names
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

    # TODO: return the standardized label using mapping and the cleaned string
    # return ...


def coerce_value(text: str):
    """Convert string values to appropriate data types (int, float, or string)."""
    # TODO: implement try/except that:
    #   - if "." in text, returns float(text)
    #   - otherwise returns int(text)
    #   - on failure, returns text.strip()
    ...
```

---

## 1.5 Extract the data from the HTML

### Main extractor (boilerplate)

Copy this function and then fill in the TODO parts at the top:

```python
def extract_data_from_html(html: str, kind: Discipline) -> tuple[list[dict], list[str]]:
    """Parse rows from the getStats API HTML snippet."""

    # TODO: parse the HTML string using BeautifulSoup and the "lxml" parser
    # soup = BeautifulSoup(html, "lxml")

    # TODO: find the statistics table element (use the correct CSS selector)
    # table = soup.select_one(".stats-data-table-player table")
    # if table is None:
    #     return []

    records: list[dict] = []

    # TODO: get the first ranking player (separate section in the HTML) and append
    # records.append(get_first_rank_player(soup))

    # Process each row in the table
    for tr in table.select("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:  # Skip rows with insufficient data
            continue

        # Extract rank (serial number) from first column
        sn_el = tds[0].find(["h5", "h6"]) or tds[0]
        name_el = tds[1].find("h6") or tds[1]
        try:
            sn = int((sn_el.get_text(strip=True) or "0").strip())
        except Exception:
            sn = None

        # Extract player name from second column
        name = name_el.get_text(strip=True)
        row: dict = {"Rank": sn, "Name": name}

        # Extract statistics from remaining columns (matches, innings, avg, etc.)
        for td in tds[2:]:
            val_el = td.find("h6")  # Value element
            lab_el = td.find("span")  # Label element
            if not val_el or not lab_el:
                continue

            val_txt = val_el.get_text(strip=True)
            lab_txt = lab_el.get_text(strip=True)
            key = normalize_label(lab_txt)  # Normalize label (e.g., "4's" -> "Fours")
            row[key] = coerce_value(val_txt)  # Convert value to appropriate type

        records.append(row)

    # Ensure all records have all columns (fill missing with None)
    for r in records:
        for c in columns[kind]:
            if c not in r:
                r[c] = None

    return records
```

### First-rank player

Copy this helper and then fill in the TODOs:

```python
def get_first_rank_player(soup: BeautifulSoup):
    """Return stats dict for the top-ranked player (Rank = 1)."""

    # TODO: select the name container for the top player
    # name_container = soup.select_one(".player-name-trw")

    # TODO: select the small stats table for the top player
    # stat_table = soup.select_one(".ranking-top-table table")

    # TODO: handle the case when either element is missing (print and return None)
    # if not name_container or not stat_table:
    #     ...

    # TODO: build the player's name from the text inside name_container
    # name = " ".join(name_container.stripped_strings)

    stats = {"Rank": 1, "Name": name}
    for td in stat_table.select("td"):
        label = td.find("span").get_text(strip=True)
        value = td.find("p").get_text(strip=True)
        key = normalize_label(label)
        stats[key] = coerce_value(value)

    return stats
```

---

## 1.6 Store and display the data

### Loop over jobs and save

Copy this boilerplate and fill in the TODO parts:

```python
from IPython.display import display

# Extract records and column names from HTML for each job
for url, basename, discipline in jobs:
    # TODO: get the HTML string you stored earlier for this basename (from html_store)

    # TODO: parse the HTML into a list of records for this discipline

    # If no records, print a message and skip this job
    # if not records:
    #     print(f"No records found for {basename}!")
    #     continue

    # TODO: build a DataFrame using the correct columns list for this discipline

    # If you wish to see the file
    print(f"--------{basename}----------")
    display(df)
    print("\n\n\n")

    # TODO: build the CSV path for this basename inside OUT_DIR
    # (example: use os.path.join with OUT_DIR and basename)

    # TODO: save the DataFrame to CSV (without the index column)

    # TODO: append the saved CSV path into saved_paths for later steps
```

---

## 2. Storing the data to the Database

### 2.1 Get the secrets

- Create a PostgreSQL database (e.g. [Neon](https://console.neon.tech)).
- Get the connection URL. In Colab: Secrets → add secret name `DATABASE_URL`, value = that URL; enable notebook access.

Store the URL in a variable. Name it exactly:

```python
from google.colab import userdata

# TODO: read the DATABASE_URL secret from Colab userdata


if not DATABASE_URL:
    raise Exception("DATABASE_URL not found! Follow the documentation above")
```

If not set, raise a clear error (see the exception above).

### 2.2 Create necessary tables

Use this boilerplate to define all required tables:

```python
TABLES_TO_CREATE = [
    """
      CREATE TABLE IF NOT EXISTS bcci_odi_most_runs (
        rank INTEGER PRIMARY KEY,
        name TEXT NULL,
        matches INTEGER NULL,
        inns INTEGER NULL,
        avg DOUBLE PRECISION NULL,
        sr DOUBLE PRECISION NULL,
        hs DOUBLE PRECISION NULL,
        Fours INTEGER NULL,
        Sixes INTEGER NULL,
        Fifties INTEGER NULL,
        Centuries INTEGER NULL,
        runs INTEGER NULL
      );
    """,

    """
    CREATE TABLE IF NOT EXISTS bcci_odi_top_wickets (
      rank INTEGER PRIMARY KEY,
      name TEXT NULL,
      matches INTEGER NULL,
      inns INTEGER NULL,
      avg DOUBLE PRECISION NULL,
      econ DOUBLE PRECISION NULL,
      sr DOUBLE PRECISION NULL,
      bbi TEXT NULL,
      Four_w INTEGER NULL,
      Five_w INTEGER NULL,
      wkts INTEGER NULL
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS bcci_test_most_runs (
      -- TODO: define columns for Test most runs
      -- Hint: use the SAME columns as bcci_odi_most_runs
      --   (rank, name, matches, inns, avg, sr, hs, Fours, Sixes, Fifties, Centuries, runs)
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS bcci_test_top_wickets (
      -- TODO: define columns for Test top wickets
      -- Hint: use the SAME columns as bcci_odi_top_wickets
      --   (rank, name, matches, inns, avg, econ, sr, bbi, Four_w, Five_w, wkts)
    );
    """
]

# Finally, connect to the database and execute each CREATE TABLE statement:
# - Open a connection using `psycopg.connect(DATABASE_URL, autocommit=True)`.
# - Create a cursor from the connection.
# - Loop over every SQL string in `TABLES_TO_CREATE`.
# - For each SQL string, run it on the cursor to create the table if it does not exist.
# - Optionally print a short success message after each table is created.
```

---

## 2.3 Utility functions

Define three small helpers for safe conversion from CSV/DataFrame to DB:

```python
def to_int(v: str):
    try:
        return int(v)
    except Exception:
        return None

def to_float(v: str):
    try:
        return float(v)
    except Exception:
        return None

def to_text(v: str):
    v = v.strip()
    return v if v and v != "-" else None
```

---

## 2.4 Insert ODI Most Runs to the Database

Copy this boilerplate and fill in the TODO parts for the ODI most runs table:

```python
INSERT_ODI_MOST_RUNS_SQL = """
  INSERT INTO bcci_odi_most_runs
    (rank, name, matches, inns, avg, sr, hs, Fours, Sixes, Fifties, Centuries, runs)
  VALUES
    (%(rank)s, %(name)s, %(matches)s, %(inns)s, %(avg)s, %(sr)s, %(hs)s, %(fours)s, %(sixes)s, %(fifties)s, %(centuries)s, %(runs)s)
  ON CONFLICT (rank) DO UPDATE SET
    name = EXCLUDED.name,
    matches = EXCLUDED.matches,
    inns = EXCLUDED.inns,
    avg = EXCLUDED.avg,
    sr = EXCLUDED.sr,
    hs = EXCLUDED.hs,
    Fours = EXCLUDED.Fours,
    Sixes = EXCLUDED.Sixes,
    Fifties = EXCLUDED.Fifties,
    Centuries = EXCLUDED.Centuries,
    runs = EXCLUDED.runs;
"""

# TODO: set the CSV path for ODI most runs inside OUT_DIR
# csv_path = os.path.join(os.getcwd(), OUT_DIR, "bcci_odi_most_runs.csv")

# TODO: check if the file exists; if not, raise an exception
# if not os.path.exists(csv_path):
#     raise Exception(f"CSV not found: {csv_path}", file=sys.stderr)

# Optional: print a message when the CSV is found
# print(f"CSV found {csv_path}")

# TODO: connect to the database and open the CSV file
# with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
#     with conn.cursor() as cur, open(csv_path, newline="", encoding="utf-8") as f:
#         # TODO: read the CSV into a DataFrame (e.g. odi_most_runs_df = pd.read_csv(f))


#         # TODO: create an empty list records_to_insert


#         # TODO: loop over rows (e.g. odi_most_runs_df.itertuples(index=False))
#         #       build a payload dict using to_int/to_float/to_text for each column
#         #       skip rows where payload["rank"] is None
#         #       append each valid payload to records_to_insert


#         # TODO: if records_to_insert is not empty,
#         #       call cur.executemany(INSERT_ODI_MOST_RUNS_SQL, records_to_insert)
#         #       and print how many records were inserted/updated



# TODO: repeat a similar pattern for:
#   - ODI top wickets
#   - Test top wickets
```

---

## 2.5 Insert Test Most Runs to the Database

```python
INSERT_TEST_MOST_RUNS_SQL = """
  INSERT INTO bcci_test_most_runs
    (rank, name, matches, inns, avg, sr, hs, fours, sixes, fifties, centuries, runs)
  VALUES
    (%(rank)s, %(name)s, %(matches)s, %(inns)s, %(avg)s, %(sr)s, %(hs)s, %(fours)s, %(sixes)s, %(fifties)s, %(centuries)s, %(runs)s)
  ON CONFLICT (rank) DO UPDATE SET
    name = EXCLUDED.name,
    matches = EXCLUDED.matches,
    inns = EXCLUDED.inns,
    avg = EXCLUDED.avg,
    sr = EXCLUDED.sr,
    hs = EXCLUDED.hs,
    fours = EXCLUDED.fours,
    sixes = EXCLUDED.sixes,
    fifties = EXCLUDED.fifties,
    centuries = EXCLUDED.centuries,
    runs = EXCLUDED.runs;
"""

csv_path = os.path.join(os.getcwd(), OUT_DIR, "bcci_test_most_runs.csv")

# Check if the file exists
if not os.path.exists(csv_path):
    print(f"CSV not found: {csv_path}", file=sys.stderr)
    sys.exit(1)

print(f"CSV found {csv_path}")

# Connect to the Database
with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
    # Create a cursor and open the CSV file
    with conn.cursor() as cur, open(csv_path, newline="", encoding="utf-8") as f:
        # Read the CSV file using pandas
        test_most_runs_df = pd.read_csv(f)
        records_to_insert = []
        for row in test_most_runs_df.itertuples(index=False):
            payload = {
                "rank": to_int(row.Rank),
                "name": to_text(row.Name),
                "matches": to_int(row.Matches),
                "inns": to_int(row.Inns),
                "avg": to_float(row.Avg),
                "sr": to_float(row.SR),
                "hs": to_float(row.HS),
                "fours": to_int(row.Fours),
                "sixes": to_int(row.Sixes),
                "fifties": to_int(row.Fifties),
                "centuries": to_int(row.Centuries),
                "runs": to_int(row.Runs),
            }

            # rank is required; skip rows without a valid rank
            if payload["rank"] is None:
                continue
            records_to_insert.append(payload)

        if records_to_insert:
            cur.executemany(INSERT_TEST_MOST_RUNS_SQL, records_to_insert)
            print(
                f"Successfully inserted {len(records_to_insert)} records into bcci_test_most_runs."
            )
```

---

## 2.6 Get the data from the database

Copy this boilerplate and then write the actual SQL and code yourself:

```python
# Configuration section
# TODO: decide how many top players to show (e.g. TOP_N = 10)

# TODO: open a database connection using psycopg.connect and DATABASE_URL

# TODO (inside a cursor):
#   - write and execute a SELECT query for ODI most runs (top TOP_N by rank)
#   - capture ODI column names and rows
#
#   - write and execute a SELECT query for Test most runs (same columns, from bcci_test_most_runs)
#   - capture Test column names and rows
#
#   - build and display results (as DataFrames or nicely formatted prints)
```

You can also visualize ODI vs Test stats with matplotlib:

```python
import matplotlib.pyplot as plt

# Convert fetched data (from the database) into pandas DataFrames for easier manipulation and plotting.
odi_df = pd.DataFrame(odi_data, columns=odi_columns)
test_df = pd.DataFrame(test_data, columns=test_columns)

# Common variables for charts
x_pos = range(len(odi_df))  # X-axis positions for the bars, based on the number of players
width = 0.35  # Width of each bar in the bar chart for side-by-side comparison

# --- First figure: Runs and Average Comparison ---
# Create a figure and a set of subplots (1 row, 2 columns) for these two charts.
fig1, axes1 = plt.subplots(1, 2, figsize=(16, 6))
# Set the overall title for the first figure.
fig1.suptitle(
    f"Top {TOP_N} Players Comparison: Test vs ODI - Runs & Average",
    fontsize=16,
    fontweight="bold",
)

# TODO: for Chart 1, create side-by-side bars on ax1 comparing ODI vs Test runs
#       (hint: use axes1[0], x_pos, width, and the "runs" column from both DataFrames)
ax1 = axes1[0]
ax1.set_xlabel("Player")
ax1.set_ylabel("Runs")
ax1.set_title("Total Runs Comparison")
ax1.set_xticks(x_pos)
ax1.set_xticklabels(odi_df["name"], rotation=90, ha="right")
ax1.legend()
ax1.grid(True, alpha=0.3)

# TODO: for Chart 2, create side-by-side bars on ax2 comparing ODI vs Test batting averages
#       (hint: use axes1[1], x_pos, width, and the "avg" column from both DataFrames)
ax2 = axes1[1]
ax2.set_xlabel("Player")
ax2.set_ylabel("Average")
ax2.set_title("Batting Average Comparison")
ax2.set_xticks(x_pos)
ax2.set_xticklabels(odi_df["name"], rotation=90, ha="right")
ax2.legend()
ax2.grid(True, alpha=0.3)

plt.show()
```

## 2.7 Plot the runs comparision of the players

Use this code to plot strike rate and centuries comparison:

```python
# Second figure: Strike Rate and Centuries Comparison
fig2, axes2 = plt.subplots(1, 2, figsize=(16, 6))
fig2.suptitle(
    f"Top {TOP_N} Players Comparison: Test vs ODI - Strike Rate & Centuries",
    fontsize=16,
    fontweight="bold",
)

# Chart 3: Strike Rate Comparison
ax3 = axes2[0]
# TODO: create side-by-side bars on ax3 comparing ODI vs Test strike rate ("sr")
#       (hint: use x_pos, width, and odi_df["sr"] / test_df["sr"])
ax3.set_xlabel("Player")
ax3.set_ylabel("Strike Rate")
ax3.set_title("Strike Rate Comparison")
ax3.set_xticks(x_pos)
ax3.set_xticklabels(odi_df["name"], rotation=90, ha="right")
ax3.legend()
ax3.grid(True, alpha=0.3)

# Chart 4: Centuries Comparison
ax4 = axes2[1]
# TODO: create side-by-side bars on ax4 comparing ODI vs Test centuries ("centuries")
#       (hint: use x_pos, width, and odi_df["centuries"] / test_df["centuries"])
ax4.set_xlabel("Player")
ax4.set_ylabel("Centuries")
ax4.set_title("Centuries Comparison")
ax4.set_xticks(x_pos)
ax4.set_xticklabels(odi_df["name"], rotation=90, ha="right")
ax4.legend()
ax4.grid(True, alpha=0.3)

plt.show()
```
