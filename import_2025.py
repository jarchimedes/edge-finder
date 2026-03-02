#!/usr/bin/env python3
"""
import_2025.py — Import 2025 ATP match results from TML-Database into tennis.db.
Maps TML column order to our Sackmann-compatible schema.
"""

import csv
import io
import sqlite3
import urllib.request

DB_PATH = "tennis.db"
TML_URLS = [
    "https://raw.githubusercontent.com/Tennismylife/TML-Database/master/2025.csv",
    "https://raw.githubusercontent.com/Tennismylife/TML-Database/master/2026.csv",
]

# Our DB schema column order (Sackmann format, 49 columns)
DB_COLUMNS = [
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
NUM_COLS = 49


def tml_row_to_db_row(tml_row):
    """Map a TML CSV row (dict) to our 49-column DB tuple.

    TML has 'indoor' column (dropped) and rank columns in different positions.
    Our schema puts rank columns at the end.
    """
    return (
        tml_row["tourney_id"], tml_row["tourney_name"], tml_row["surface"],
        tml_row["draw_size"], tml_row["tourney_level"],
        tml_row["tourney_date"], tml_row["match_num"],
        tml_row["winner_id"], tml_row["winner_seed"], tml_row["winner_entry"],
        tml_row["winner_name"], tml_row["winner_hand"], tml_row["winner_ht"],
        tml_row["winner_ioc"], tml_row["winner_age"],
        tml_row["loser_id"], tml_row["loser_seed"], tml_row["loser_entry"],
        tml_row["loser_name"], tml_row["loser_hand"], tml_row["loser_ht"],
        tml_row["loser_ioc"], tml_row["loser_age"],
        tml_row["score"], tml_row["best_of"], tml_row["round"],
        tml_row["minutes"],
        tml_row["w_ace"], tml_row["w_df"], tml_row["w_svpt"],
        tml_row["w_1stIn"], tml_row["w_1stWon"], tml_row["w_2ndWon"],
        tml_row["w_SvGms"], tml_row["w_bpSaved"], tml_row["w_bpFaced"],
        tml_row["l_ace"], tml_row["l_df"], tml_row["l_svpt"],
        tml_row["l_1stIn"], tml_row["l_1stWon"], tml_row["l_2ndWon"],
        tml_row["l_SvGms"], tml_row["l_bpSaved"], tml_row["l_bpFaced"],
        tml_row["winner_rank"], tml_row["winner_rank_points"],
        tml_row["loser_rank"], tml_row["loser_rank_points"],
    )


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Get current max date
    cur.execute("SELECT MAX(tourney_date) FROM atp_matches")
    current_max = cur.fetchone()[0]
    print(f"Current max tourney_date in atp_matches: {current_max}")

    # Download TML CSVs
    new_rows = []
    for url in TML_URLS:
        year = url.split("/")[-1].replace(".csv", "")
        print(f"Downloading TML {year} data from GitHub...")
        try:
            with urllib.request.urlopen(url) as resp:
                text = resp.read().decode("utf-8")
        except Exception as e:
            print(f"  Skipping {year}: {e}")
            continue

        reader = csv.DictReader(io.StringIO(text))
        all_rows = list(reader)
        batch = [r for r in all_rows if r["tourney_date"] > current_max]
        print(f"  {year}: {len(all_rows)} total rows, {len(batch)} new")
        new_rows.extend(batch)

    print(f"\nTotal new matches to import: {len(new_rows)}")

    if not new_rows:
        print("Nothing to import.")
        conn.close()
        return

    # Convert and insert using explicit column names
    col_names = ", ".join(DB_COLUMNS)
    placeholders = ", ".join(["?"] * len(DB_COLUMNS))
    db_rows = [tml_row_to_db_row(r) for r in new_rows]

    cur.executemany(
        f"INSERT INTO atp_matches ({col_names}) VALUES ({placeholders})", db_rows
    )
    conn.commit()

    # Verify
    cur.execute("SELECT MAX(tourney_date) FROM atp_matches")
    new_max = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM atp_matches")
    total = cur.fetchone()[0]

    print(f"\nImported {len(db_rows)} matches.")
    print(f"New max tourney_date: {new_max}")
    print(f"Total atp_matches rows: {total}")

    # Show imported tournaments
    dates_and_tourneys = sorted(set((r["tourney_date"][:6], r["tourney_name"]) for r in new_rows))
    print(f"\nTournaments imported:")
    for date_prefix, name in dates_and_tourneys:
        count = sum(1 for r in new_rows if r["tourney_name"] == name)
        yr, mo = date_prefix[:4], date_prefix[4:]
        print(f"  {yr}-{mo}  {name} ({count} matches)")

    conn.close()


if __name__ == "__main__":
    main()
