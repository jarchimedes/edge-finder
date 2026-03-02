#!/usr/bin/env python3
"""
calc_elo.py — Tournament-weighted Elo with inactivity decay.

K-factor multipliers by tournament level:
  Grand Slam (G)    : 1.5x  (+ 10% for best-of-5)
  Masters 1000 (M)  : 1.25x
  ATP 500           : 1.10x
  ATP 250           : 1.00x (baseline)
  Tour Finals (F)   : 1.10x
  Davis Cup (D)     : 0.50x
  Challenger/Other  : 0.85x

Inactivity decay:
  60+ days inactive -> Elo pulled toward 1500
  Rate: 3% per month beyond 60 days, capped at 20%.
"""

import sqlite3
from datetime import datetime

DB_PATH = "tennis.db"
BASE_K = 32
START_ELO = 1500
DECAY_START_DAYS = 60
DECAY_RATE_PER_MONTH = 0.03
DECAY_CAP = 0.20
RETIRED_DAYS = 1000  # 1000+ days = treat as retired, reset to 1500


def expected(ra, rb):
    return 1.0 / (1.0 + 10 ** ((rb - ra) / 400.0))


def get_k_multiplier(tourney_level, draw_size, best_of):
    level = str(tourney_level or '').strip()
    try:
        ds = int(draw_size)
    except (TypeError, ValueError):
        ds = 0
    try:
        bo = int(best_of)
    except (TypeError, ValueError):
        bo = 3

    if level == 'G':
        base = 1.5
    elif level == 'M':
        base = 1.25
    elif level in ('500', 'F'):
        base = 1.10
    elif level == '250':
        base = 1.00
    elif level == 'A':
        base = 1.10 if ds >= 48 else 1.00
    elif level == 'D':
        base = 0.50
    else:
        base = 0.85

    if bo == 5:
        base *= 1.10

    return base


def apply_decay(elo, player, match_date_str, last_date_str):
    try:
        curr = datetime.strptime(str(match_date_str), "%Y%m%d")
        last = datetime.strptime(str(last_date_str), "%Y%m%d")
        days = (curr - last).days
    except (ValueError, TypeError):
        return 0

    # Retired: 1000+ days inactive = hard reset to 1500
    if days >= RETIRED_DAYS:
        for s in ['overall', 'Hard', 'Clay', 'Grass']:
            elo[player][s] = START_ELO
        return 1

    if days < DECAY_START_DAYS:
        return 0

    months_beyond = (days - DECAY_START_DAYS) / 30.0
    rate = min(DECAY_RATE_PER_MONTH * months_beyond, DECAY_CAP)

    for s in ['overall', 'Hard', 'Clay', 'Grass']:
        elo[player][s] = elo[player][s] + rate * (START_ELO - elo[player][s])

    return 1


TOURS = [
    {"label": "ATP", "match_table": "atp_matches", "elo_table": "atp_elo_ratings"},
    {"label": "WTA", "match_table": "wta_matches", "elo_table": "wta_elo_ratings"},
]


def calc_tour_elo(conn, tour):
    cur = conn.cursor()
    mt = tour["match_table"]
    et = tour["elo_table"]
    label = tour["label"]

    cur.execute(f"""
        SELECT winner_id, winner_name, loser_id, loser_name,
               surface, tourney_date, tourney_level, draw_size, best_of
        FROM {mt}
        WHERE surface IN ('Hard', 'Clay', 'Grass', 'Carpet')
        ORDER BY tourney_date, match_num
    """)
    matches = cur.fetchall()
    print(f"\n{label}: {len(matches)} matches...")

    elo = {}
    ids = {}
    last_date = {}
    decay_count = 0

    def init_player(name):
        if name not in elo:
            elo[name] = {"overall": START_ELO, "Hard": START_ELO, "Clay": START_ELO, "Grass": START_ELO}

    for m in matches:
        wname, lname = m["winner_name"], m["loser_name"]
        date = m["tourney_date"]

        if m["winner_id"]: ids[wname] = m["winner_id"]
        if m["loser_id"]:  ids[lname] = m["loser_id"]

        surface = "Hard" if m["surface"] == "Carpet" else m["surface"]

        init_player(wname)
        init_player(lname)

        # Inactivity decay before match
        for p in [wname, lname]:
            if p in last_date:
                decay_count += apply_decay(elo, p, date, last_date[p])

        k = BASE_K * get_k_multiplier(m["tourney_level"], m["draw_size"], m["best_of"])

        # Overall update
        ew = expected(elo[wname]["overall"], elo[lname]["overall"])
        elo[wname]["overall"] += k * (1 - ew)
        elo[lname]["overall"] += k * (0 - (1 - ew))

        # Surface update
        if surface in ("Hard", "Clay", "Grass"):
            es = expected(elo[wname][surface], elo[lname][surface])
            elo[wname][surface] += k * (1 - es)
            elo[lname][surface] += k * (0 - (1 - es))

        last_date[wname] = date
        last_date[lname] = date

    print(f"  Decay events: {decay_count}")

    # Post-processing: retire players who haven't played in 1000+ days
    # Use the latest match date in the dataset as "today"
    latest_date_str = max(last_date.values()) if last_date else None
    retired_count = 0
    if latest_date_str:
        try:
            latest_dt = datetime.strptime(str(latest_date_str), "%Y%m%d")
            for player, last_str in last_date.items():
                last_dt = datetime.strptime(str(last_str), "%Y%m%d")
                days = (latest_dt - last_dt).days
                if days >= RETIRED_DAYS and player in elo:
                    for s in ['overall', 'Hard', 'Clay', 'Grass']:
                        elo[player][s] = START_ELO
                    retired_count += 1
        except (ValueError, TypeError):
            pass
    print(f"  Retired/reset: {retired_count} players (1000+ days inactive)")

    cur.execute(f"DROP TABLE IF EXISTS {et}")
    cur.execute(f"""
        CREATE TABLE {et} (
            player_id TEXT, player_name TEXT,
            elo_overall REAL, elo_hard REAL, elo_clay REAL, elo_grass REAL,
            last_match_date TEXT
        )
    """)
    rows = [(ids.get(n), n, r["overall"], r["Hard"], r["Clay"], r["Grass"], last_date.get(n))
            for n, r in elo.items()]
    cur.executemany(f"INSERT INTO {et} VALUES (?,?,?,?,?,?,?)", rows)
    conn.commit()
    print(f"  Saved {len(rows)} players.")

    print(f"\n{'='*75}")
    print(f"{label} TOP 20 — Tournament-Weighted Elo + Inactivity Decay")
    print(f"{'='*75}")
    cur.execute(f"""
        SELECT player_name, ROUND(elo_overall,1), ROUND(elo_hard,1),
               ROUND(elo_clay,1), ROUND(elo_grass,1), last_match_date
        FROM {et} ORDER BY elo_overall DESC LIMIT 20
    """)
    print(f"{'#':<4} {'Player':<26} {'Overall':<10} {'Hard':<10} {'Clay':<10} {'Grass':<10} Last Match")
    print("-"*80)
    for i, r in enumerate(cur.fetchall(), 1):
        print(f"{i:<4} {r[0]:<26} {r[1]:<10} {r[2]:<10} {r[3]:<10} {r[4]:<10} {r[5] or 'N/A'}")


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    for tour in TOURS:
        calc_tour_elo(conn, tour)
    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
