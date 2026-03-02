#!/usr/bin/env python3
"""
import_td.py — Import tennis-data.co.uk xlsx files into tennis.db.
Handles name mapping from abbreviated ("Sinner J.") to full Sackmann names.
"""

import sqlite3
import sys
from datetime import datetime

import openpyxl

DB_PATH = "/Users/minikarl/Desktop/tennis-project/tennis.db"

# Round mapping from tennis-data to Sackmann format
ROUND_MAP = {
    "1st Round": "R128",
    "2nd Round": "R64",
    "3rd Round": "R32",
    "4th Round": "R16",
    "Quarterfinals": "QF",
    "Semifinals": "SF",
    "The Final": "F",
    "Round Robin": "RR",
}

# Tourney level mapping
TIER_MAP_WTA = {
    "WTA250": "I",
    "WTA500": "P",
    "WTA1000": "PM",
    "Grand Slam": "G",
    "WTA Finals": "F",
}
TIER_MAP_ATP = {
    "ATP250": "A",
    "ATP500": "A",
    "Masters 1000": "M",
    "Grand Slam": "G",
    "Masters Cup": "F",
}


def build_name_map(conn, table):
    """Build mapping from 'LastName F.' -> full name using existing DB names."""
    rows = conn.execute(f"SELECT DISTINCT winner_name FROM {table} UNION SELECT DISTINCT loser_name FROM {table}").fetchall()
    name_map = {}
    for (full_name,) in rows:
        if not full_name:
            continue
        parts = full_name.split()
        if len(parts) < 2:
            continue
        first = parts[0]
        last = parts[-1]
        # Standard: "FirstName LastName" -> "LastName F."
        abbr = f"{last} {first[0]}."
        # Don't overwrite if already set (handle duplicates by keeping first)
        if abbr not in name_map:
            name_map[abbr] = full_name
        # Also handle multi-word last names: "Carlos Alcaraz" -> "Alcaraz C."
        # And "Alexander De Minaur" -> "De Minaur A."
        if len(parts) > 2:
            # Try last-name = everything after first name
            last_multi = " ".join(parts[1:])
            abbr2 = f"{last_multi} {first[0]}."
            if abbr2 not in name_map:
                name_map[abbr2] = full_name
    return name_map


def resolve_name(abbr, name_map, unknown_names):
    """Resolve abbreviated name to full name."""
    if abbr in name_map:
        return name_map[abbr]
    # Try case-insensitive
    abbr_lower = abbr.lower()
    for k, v in name_map.items():
        if k.lower() == abbr_lower:
            name_map[abbr] = v  # Cache
            return v
    unknown_names.add(abbr)
    return abbr  # Return as-is if not found


def build_score(row, is_atp):
    """Build score string from set scores like '6-3 6-4'."""
    sets = []
    if is_atp:
        set_pairs = [('W1','L1'), ('W2','L2'), ('W3','L3'), ('W4','L4'), ('W5','L5')]
    else:
        set_pairs = [('W1','L1'), ('W2','L2'), ('W3','L3')]
    for w_key, l_key in set_pairs:
        w = row.get(w_key)
        l = row.get(l_key)
        if w is not None and l is not None:
            sets.append(f"{int(w)}-{int(l)}")
    return " ".join(sets)


def read_xlsx(path):
    """Read xlsx and return list of dicts."""
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    rows_iter = ws.iter_rows(values_only=True)
    headers = next(rows_iter)
    data = []
    for row in rows_iter:
        d = dict(zip(headers, row))
        data.append(d)
    wb.close()
    return data


def import_tour(conn, xlsx_paths, match_table, is_atp, min_date=None):
    """Import matches from tennis-data xlsx files into DB."""
    tier_map = TIER_MAP_ATP if is_atp else TIER_MAP_WTA
    tier_col = "Series" if is_atp else "Tier"

    # Build name map from existing data
    name_map = build_name_map(conn, match_table)
    print(f"  Name map: {len(name_map)} abbreviated -> full name entries")

    unknown_names = set()
    inserted = 0
    skipped = 0
    match_counter = {}  # tourney_id -> counter

    for xlsx_path in xlsx_paths:
        data = read_xlsx(xlsx_path)
        print(f"  Reading {xlsx_path}: {len(data)} rows")

        for row in data:
            # Skip incomplete matches
            comment = row.get("Comment", "")
            if comment and comment != "Completed":
                skipped += 1
                continue

            dt = row.get("Date")
            if not dt:
                skipped += 1
                continue
            if isinstance(dt, datetime):
                tourney_date = dt.strftime("%Y%m%d")
            else:
                tourney_date = str(dt).replace("-", "")[:8]

            if min_date and tourney_date <= min_date:
                skipped += 1
                continue

            winner_abbr = row.get("Winner", "")
            loser_abbr = row.get("Loser", "")
            winner_name = resolve_name(winner_abbr, name_map, unknown_names)
            loser_name = resolve_name(loser_abbr, name_map, unknown_names)

            surface = row.get("Surface", "Hard")
            tourney_name = row.get("Tournament", "")
            location = row.get("Location", "")
            tier = row.get(tier_col, "")
            tourney_level = tier_map.get(tier, "A" if is_atp else "I")
            rd = row.get("Round", "")
            round_code = ROUND_MAP.get(rd, rd)
            best_of = row.get("Best of", 3)
            score = build_score(row, is_atp)

            def safe_int(v):
                if v is None or v == "" or v == "N/A":
                    return None
                try:
                    return str(int(float(v)))
                except (ValueError, TypeError):
                    return None

            w_rank = safe_int(row.get("WRank"))
            l_rank = safe_int(row.get("LRank"))
            w_pts = safe_int(row.get("WPts"))
            l_pts = safe_int(row.get("LPts"))

            # Generate tourney_id
            year = tourney_date[:4]
            tid_base = f"{year}-td-{location}".replace(" ", "_")
            tourney_id = tid_base

            # Generate match_num
            if tourney_id not in match_counter:
                match_counter[tourney_id] = 0
            match_counter[tourney_id] += 1
            match_num = str(match_counter[tourney_id])

            conn.execute(
                f"""INSERT INTO {match_table}
                    (tourney_id, tourney_name, surface, draw_size, tourney_level,
                     tourney_date, match_num, winner_id, winner_seed, winner_entry,
                     winner_name, winner_hand, winner_ht, winner_ioc, winner_age,
                     loser_id, loser_seed, loser_entry, loser_name, loser_hand,
                     loser_ht, loser_ioc, loser_age, score, best_of, round, minutes,
                     w_ace, w_df, w_svpt, w_1stIn, w_1stWon, w_2ndWon, w_SvGms,
                     w_bpSaved, w_bpFaced, l_ace, l_df, l_svpt, l_1stIn, l_1stWon,
                     l_2ndWon, l_SvGms, l_bpSaved, l_bpFaced,
                     winner_rank, winner_rank_points, loser_rank, loser_rank_points)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (tourney_id, tourney_name, surface, None, tourney_level,
                 tourney_date, match_num, None, None, None,
                 winner_name, None, None, None, None,
                 None, None, None, loser_name, None,
                 None, None, None, score, str(best_of), round_code, None,
                 None, None, None, None, None, None, None,
                 None, None, None, None, None, None, None,
                 None, None, None, None,
                 w_rank, w_pts, l_rank, l_pts),
            )
            inserted += 1

    conn.commit()
    print(f"  Inserted: {inserted}, Skipped: {skipped}")
    if unknown_names:
        print(f"  Unknown names ({len(unknown_names)}): {sorted(unknown_names)[:20]}")
        if len(unknown_names) > 20:
            print(f"  ... and {len(unknown_names) - 20} more")
    return inserted, unknown_names


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Current max dates
    atp_max = conn.execute("SELECT MAX(tourney_date) FROM atp_matches").fetchone()[0]
    wta_max = conn.execute("SELECT MAX(tourney_date) FROM wta_matches").fetchone()[0]
    print(f"Current ATP max date: {atp_max}")
    print(f"Current WTA max date: {wta_max}")

    # Import WTA 2025 + 2026
    print(f"\n=== Importing WTA (after {wta_max}) ===")
    wta_ins, wta_unknown = import_tour(
        conn,
        ["/tmp/wta_2025.xlsx", "/tmp/wta_2026.xlsx"],
        "wta_matches",
        is_atp=False,
        min_date=wta_max,
    )

    # Import ATP 2026 (fill gap from Jan 17 to now)
    print(f"\n=== Importing ATP (after {atp_max}) ===")
    atp_ins, atp_unknown = import_tour(
        conn,
        ["/tmp/atp_2026_td.xlsx"],
        "atp_matches",
        is_atp=True,
        min_date=atp_max,
    )

    # Final counts
    atp_new_max = conn.execute("SELECT MAX(tourney_date) FROM atp_matches").fetchone()[0]
    wta_new_max = conn.execute("SELECT MAX(tourney_date) FROM wta_matches").fetchone()[0]
    atp_count = conn.execute("SELECT COUNT(*) FROM atp_matches").fetchone()[0]
    wta_count = conn.execute("SELECT COUNT(*) FROM wta_matches").fetchone()[0]

    print(f"\n=== Summary ===")
    print(f"ATP: {atp_count} matches, latest: {atp_new_max}")
    print(f"WTA: {wta_count} matches, latest: {wta_new_max}")

    conn.close()


if __name__ == "__main__":
    main()
