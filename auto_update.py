#!/usr/bin/env python3
"""
auto_update.py — Pull latest match data from Jeff Sackmann's GitHub repos,
import new matches into tennis.db, and recalculate Elo ratings.

Fetches ATP and WTA CSVs for the current year (and one year back) directly
from JeffSackmann/tennis_atp and JeffSackmann/tennis_wta on GitHub.
Only rows with tourney_date newer than the existing DB max are imported.
"""

import csv
import io
import logging
import sqlite3
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
DB_PATH = SCRIPT_DIR / "tennis.db"
LOG_PATH = SCRIPT_DIR / "auto_update.log"

ATP_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/"
    "atp_matches_{year}.csv"
)
WTA_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/"
    "wta_matches_{year}.csv"
)

# Sackmann CSV columns (in order) — 49 columns
SACKMANN_COLUMNS = [
    "tourney_id", "tourney_name", "surface", "draw_size", "tourney_level",
    "tourney_date", "match_num",
    "winner_id", "winner_seed", "winner_entry", "winner_name", "winner_hand",
    "winner_ht", "winner_ioc", "winner_age",
    "loser_id", "loser_seed", "loser_entry", "loser_name", "loser_hand",
    "loser_ht", "loser_ioc", "loser_age",
    "score", "best_of", "round", "minutes",
    "w_ace", "w_df", "w_svpt", "w_1stIn", "w_1stWon", "w_2ndWon",
    "w_SvGms", "w_bpSaved", "w_bpFaced",
    "l_ace", "l_df", "l_svpt", "l_1stIn", "l_1stWon", "l_2ndWon",
    "l_SvGms", "l_bpSaved", "l_bpFaced",
    "winner_rank", "winner_rank_points", "loser_rank", "loser_rank_points",
]

TOURS = [
    {
        "label": "ATP",
        "match_table": "atp_matches",
        "elo_table": "atp_elo_ratings",
        "url_template": ATP_URL_TEMPLATE,
    },
    {
        "label": "WTA",
        "match_table": "wta_matches",
        "elo_table": "wta_elo_ratings",
        "url_template": WTA_URL_TEMPLATE,
    },
]

# Elo constants (must match calc_elo.py)
K = 32
START_ELO = 1500


# ── Logging ─────────────────────────────────────────────────────────────────
def setup_logging():
    fmt = "%(asctime)s [%(levelname)s] %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(LOG_PATH),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ── Fetch ───────────────────────────────────────────────────────────────────
def fetch_csv(url):
    """Download a CSV from a URL and return a list of dicts. Returns None on error."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "auto_update.py/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8")
        rows = list(csv.DictReader(io.StringIO(text)))
        return rows
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None  # File doesn't exist yet (e.g. future year)
        logging.warning("HTTP %s fetching %s", e.code, url)
        return None
    except Exception as e:
        logging.warning("Failed to fetch %s: %s", url, e)
        return None


# ── Import ──────────────────────────────────────────────────────────────────
def import_tour(conn, tour, current_max):
    """Fetch recent CSVs for a tour and insert rows newer than current_max."""
    label = tour["label"]
    match_table = tour["match_table"]
    url_template = tour["url_template"]

    current_year = datetime.now().year
    # Check current year and one year back (Sackmann sometimes lags; also catches
    # late additions to the previous year's file)
    years_to_fetch = [current_year - 1, current_year]

    all_new_rows = []
    for year in years_to_fetch:
        url = url_template.format(year=year)
        logging.info("%s %d — fetching %s", label, year, url)
        rows = fetch_csv(url)
        if rows is None:
            logging.info("%s %d — not available, skipping", label, year)
            continue
        new = [r for r in rows if r.get("tourney_date", "") > current_max]
        logging.info(
            "%s %d — %d total rows, %d newer than %s",
            label, year, len(rows), len(new), current_max,
        )
        all_new_rows.extend(new)

    if not all_new_rows:
        logging.info("%s — nothing new to import", label)
        return 0

    col_names = ", ".join(SACKMANN_COLUMNS)
    placeholders = ", ".join(["?"] * len(SACKMANN_COLUMNS))
    sql = f"INSERT INTO {match_table} ({col_names}) VALUES ({placeholders})"

    def to_tuple(row):
        return tuple(row.get(col) or None for col in SACKMANN_COLUMNS)

    db_rows = [to_tuple(r) for r in all_new_rows]

    cur = conn.cursor()
    cur.executemany(sql, db_rows)
    conn.commit()

    # Report which tournaments were imported
    tournaments = sorted(set(
        (r["tourney_date"][:6], r["tourney_name"]) for r in all_new_rows
    ))
    for date_prefix, name in tournaments:
        count = sum(1 for r in all_new_rows if r["tourney_name"] == name)
        yr, mo = date_prefix[:4], date_prefix[4:]
        logging.info("  Imported: %s-%s  %s  (%d matches)", yr, mo, name, count)

    logging.info("%s — imported %d new matches", label, len(db_rows))
    return len(db_rows)


# ── Elo ─────────────────────────────────────────────────────────────────────
def expected(ra, rb):
    return 1.0 / (1.0 + 10 ** ((rb - ra) / 400.0))


def calc_tour_elo(conn, tour):
    """Recalculate Elo for a tour from scratch and write to the elo table."""
    match_table = tour["match_table"]
    elo_table = tour["elo_table"]
    label = tour["label"]

    cur = conn.cursor()
    cur.execute(f"""
        SELECT winner_id, winner_name, loser_id, loser_name, surface, tourney_date
        FROM {match_table}
        WHERE surface IN ('Hard', 'Clay', 'Grass', 'Carpet')
        ORDER BY tourney_date, match_num
    """)
    matches = cur.fetchall()
    logging.info("%s Elo — processing %d matches", label, len(matches))

    elo = {}
    ids = {}

    def get_elo(name, surface=None):
        if name not in elo:
            elo[name] = {"overall": START_ELO, "Hard": START_ELO, "Clay": START_ELO, "Grass": START_ELO}
        return elo[name][surface] if surface else elo[name]["overall"]

    for m in matches:
        wname = m[1]  # winner_name
        lname = m[3]  # loser_name
        if m[0]:      # winner_id
            ids[wname] = m[0]
        if m[2]:      # loser_id
            ids[lname] = m[2]
        raw_surface = m[4]
        surface = "Hard" if raw_surface == "Carpet" else raw_surface

        # Overall Elo
        w_ov, l_ov = get_elo(wname), get_elo(lname)
        exp_w = expected(w_ov, l_ov)
        elo[wname]["overall"] = w_ov + K * (1 - exp_w)
        elo[lname]["overall"] = l_ov + K * (0 - (1 - exp_w))

        # Surface-specific Elo
        if surface in ("Hard", "Clay", "Grass"):
            w_s, l_s = get_elo(wname, surface), get_elo(lname, surface)
            exp_ws = expected(w_s, l_s)
            elo[wname][surface] = w_s + K * (1 - exp_ws)
            elo[lname][surface] = l_s + K * (0 - (1 - exp_ws))

    cur.execute(f"DROP TABLE IF EXISTS {elo_table}")
    cur.execute(f"""
        CREATE TABLE {elo_table} (
            player_id TEXT,
            player_name TEXT,
            elo_overall REAL,
            elo_hard REAL,
            elo_clay REAL,
            elo_grass REAL
        )
    """)
    rows = [
        (ids.get(name), name, r["overall"], r["Hard"], r["Clay"], r["Grass"])
        for name, r in elo.items()
    ]
    cur.executemany(f"INSERT INTO {elo_table} VALUES (?,?,?,?,?,?)", rows)
    conn.commit()
    logging.info("%s Elo — saved %d player ratings to %s", label, len(rows), elo_table)


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    setup_logging()
    logging.info("=== auto_update.py started ===")

    conn = sqlite3.connect(DB_PATH)

    any_new = False
    for tour in TOURS:
        match_table = tour["match_table"]
        label = tour["label"]

        row = conn.execute(f"SELECT MAX(tourney_date) FROM {match_table}").fetchone()
        current_max = row[0] or "00000000"
        logging.info("%s current max tourney_date: %s", label, current_max)

        imported = import_tour(conn, tour, current_max)
        if imported:
            any_new = True

        new_max = conn.execute(f"SELECT MAX(tourney_date) FROM {match_table}").fetchone()[0]
        total = conn.execute(f"SELECT COUNT(*) FROM {match_table}").fetchone()[0]
        logging.info("%s — total rows: %d, new max date: %s", label, total, new_max)

    if any_new:
        logging.info("Recalculating Elo ratings for both tours...")
        for tour in TOURS:
            calc_tour_elo(conn, tour)
    else:
        logging.info("No new matches — skipping Elo recalculation")

    conn.close()
    logging.info("=== auto_update.py finished ===")


if __name__ == "__main__":
    main()
