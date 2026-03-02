#!/usr/bin/env python3
"""
daily_edge_finder.py — Interactive tennis betting edge finder.
Compares Elo-based win probabilities against market odds to find value.
Supports both ATP and WTA tours. Auto-pulls live odds from The Odds API.
"""

import json
import sqlite3
import urllib.request

DB_PATH = "tennis.db"
ODDS_API_KEY = "3637b9c15b5c926ba3f72ba70d58dfa0"
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

SURFACES = {"1": "Hard", "2": "Clay", "3": "Grass"}
SURFACE_COL = {"Hard": "elo_hard", "Clay": "elo_clay", "Grass": "elo_grass"}

TOURS = {
    "1": {"label": "ATP", "elo_table": "atp_elo_ratings", "match_table": "atp_matches"},
    "2": {"label": "WTA", "elo_table": "wta_elo_ratings", "match_table": "wta_matches"},
}


def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── Odds API ────────────────────────────────────────────────────────────────

def api_get(endpoint, params=None):
    """Fetch JSON from The Odds API."""
    params = params or {}
    params["apiKey"] = ODDS_API_KEY
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{ODDS_API_BASE}/{endpoint}?{qs}"
    try:
        with urllib.request.urlopen(url) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  API error: {e}")
        return None


def fetch_tennis_sports():
    """Get all active tennis sport keys."""
    data = api_get("sports")
    if not data:
        return []
    return [
        s for s in data
        if "tennis" in s["key"] and s["active"]
    ]


def fetch_match_odds(sport_key):
    """Fetch h2h odds for all matches in a sport."""
    data = api_get(
        f"sports/{sport_key}/odds",
        {"regions": "us", "markets": "h2h", "oddsFormat": "american"},
    )
    return data or []


def get_consensus_odds(match):
    """Average odds across all bookmakers for a match."""
    all_odds = {}  # player_name -> list of american odds
    for bk in match.get("bookmakers", []):
        for market in bk.get("markets", []):
            if market["key"] != "h2h":
                continue
            for outcome in market["outcomes"]:
                name = outcome["name"]
                all_odds.setdefault(name, []).append(outcome["price"])

    avg = {}
    for name, prices in all_odds.items():
        avg[name] = sum(prices) / len(prices)
    return avg


def show_live_odds_menu(conn):
    """Fetch live tennis odds, let user pick a match. Returns (tour, match_info) or None."""
    print("\n  Fetching live tennis odds...")
    sports = fetch_tennis_sports()
    if not sports:
        print("  No active tennis tournaments found.")
        return None

    print(f"\n  Active tournaments:")
    for i, s in enumerate(sports, 1):
        tour_type = "ATP" if "atp" in s["key"] else "WTA"
        print(f"    {i}. [{tour_type}] {s['title']}")
    print(f"    0. Skip — enter manually")

    choice = input("  Pick tournament: ").strip()
    if choice == "0" or not choice:
        return None
    if not choice.isdigit() or int(choice) < 1 or int(choice) > len(sports):
        print("  Invalid choice.")
        return None

    sport = sports[int(choice) - 1]
    tour_type = "1" if "atp" in sport["key"] else "2"

    print(f"\n  Fetching odds for {sport['title']}...")
    matches = fetch_match_odds(sport["key"])
    if not matches:
        print("  No matches with odds found.")
        return None

    print(f"\n  Matches with odds:")
    match_list = []
    for m in matches:
        odds = get_consensus_odds(m)
        if len(odds) < 2:
            continue
        players = list(odds.keys())
        match_list.append({"match": m, "odds": odds, "players": players})

    for i, item in enumerate(match_list, 1):
        p1, p2 = item["players"]
        o1, o2 = item["odds"][p1], item["odds"][p2]
        print(f"    {i}. {p1} ({format_american(o1)}) vs {p2} ({format_american(o2)})")

    choice = input("  Pick match: ").strip()
    if not choice.isdigit() or int(choice) < 1 or int(choice) > len(match_list):
        return None

    selected = match_list[int(choice) - 1]
    return tour_type, selected


def format_american(odds):
    """Format a numeric odds value as American string."""
    if odds >= 0:
        return f"+{odds:.0f}"
    return f"{odds:.0f}"


# ── Elo lookup ──────────────────────────────────────────────────────────────

def find_player(conn, name, elo_table):
    """Fuzzy-match a player name in the Elo table. Returns the row or None."""
    row = conn.execute(
        f"SELECT * FROM {elo_table} WHERE player_name = ? COLLATE NOCASE", (name,)
    ).fetchone()
    if row:
        return row

    rows = conn.execute(
        f"SELECT * FROM {elo_table} WHERE player_name LIKE ? COLLATE NOCASE",
        (f"%{name}%",),
    ).fetchall()

    if len(rows) == 1:
        return rows[0]
    if len(rows) > 1:
        print(f"\n  Multiple matches for '{name}':")
        for i, r in enumerate(rows, 1):
            print(f"    {i}. {r['player_name']} (Elo {r['elo_overall']:.0f})")
        while True:
            choice = input("  Pick a number: ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(rows):
                return rows[int(choice) - 1]
    return None


# ── Probability math ────────────────────────────────────────────────────────

def elo_win_prob(elo_a, elo_b):
    return 1.0 / (1.0 + 10 ** ((elo_b - elo_a) / 400.0))


def american_to_decimal(odds):
    if odds > 0:
        return 1.0 + odds / 100.0
    else:
        return 1.0 + 100.0 / abs(odds)


def parse_odds(raw):
    raw = raw.strip()
    if raw.startswith("+") or raw.startswith("-"):
        dec = american_to_decimal(float(raw))
    else:
        dec = float(raw)
    return 1.0 / dec, dec


# ── Head-to-head ────────────────────────────────────────────────────────────

def head_to_head(conn, name_a, name_b, match_table):
    query = f"""
        SELECT tourney_name, surface, tourney_date, round, score,
               winner_name, loser_name
        FROM {match_table}
        WHERE (winner_name = ? AND loser_name = ?)
           OR (winner_name = ? AND loser_name = ?)
        ORDER BY tourney_date DESC
    """
    rows = conn.execute(query, (name_a, name_b, name_b, name_a)).fetchall()
    wins_a = sum(1 for r in rows if r["winner_name"] == name_a)
    return wins_a, len(rows) - wins_a, rows[:5]


# ── Recent form ─────────────────────────────────────────────────────────────

def recent_form(conn, player_name, match_table, n=10):
    query = f"""
        SELECT tourney_name, surface, tourney_date, round, score,
               winner_name, loser_name, winner_rank, loser_rank
        FROM (
            SELECT *, 'W' AS result FROM {match_table} WHERE winner_name = ?
            UNION ALL
            SELECT *, 'L' AS result FROM {match_table} WHERE loser_name = ?
        )
        ORDER BY tourney_date DESC, CAST(match_num AS INTEGER) DESC
        LIMIT ?
    """
    return conn.execute(query, (player_name, player_name, n)).fetchall()


# ── Display helpers ─────────────────────────────────────────────────────────

def pct(p):
    return f"{p * 100:.1f}%"


def print_section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def print_elo_card(label, player):
    print(f"\n  {label}: {player['player_name']}")
    print(f"    Overall Elo:  {player['elo_overall']:>8.1f}")
    print(f"    Hard Elo:     {player['elo_hard']:>8.1f}")
    print(f"    Clay Elo:     {player['elo_clay']:>8.1f}")
    print(f"    Grass Elo:    {player['elo_grass']:>8.1f}")


def print_h2h(name_a, name_b, wins_a, wins_b, meetings):
    print_section(f"HEAD-TO-HEAD: {name_a} vs {name_b}")
    print(f"  Record: {name_a} {wins_a} – {wins_b} {name_b}")
    if not meetings:
        print("  No meetings found in database.")
        return
    print(f"\n  Last {len(meetings)} meeting(s):")
    for m in meetings:
        date = m["tourney_date"]
        yr, mo = date[:4], date[4:6]
        print(
            f"    {yr}-{mo}  {m['tourney_name']:<22} {m['surface']:<6} "
            f"{m['round']:<4}  {m['winner_name']} d. {m['loser_name']}  {m['score']}"
        )


def print_form(player_name, matches):
    print_section(f"LAST {len(matches)} MATCHES: {player_name}")
    for m in matches:
        is_win = m["winner_name"] == player_name
        result = "W" if is_win else "L"
        opponent = m["loser_name"] if is_win else m["winner_name"]
        opp_rank = m["loser_rank"] if is_win else m["winner_rank"]
        opp_rank = opp_rank if opp_rank else "?"
        date = m["tourney_date"]
        yr, mo = date[:4], date[4:6]
        print(
            f"    {result}  {yr}-{mo}  {m['tourney_name']:<22} {m['surface']:<6} "
            f"vs {opponent:<24} (rank {opp_rank:>4})  {m['score']}"
        )


def print_edge(label, model_prob, implied_prob, name_a, decimal_odds):
    print_section(label)
    edge = model_prob - implied_prob
    print(f"  Model win prob:    {pct(model_prob):>8}")
    print(f"  Market implied:    {pct(implied_prob):>8}")
    print(f"  Edge:              {edge * 100:>+7.1f}pp")

    if edge > 0.02:
        ev = model_prob * (decimal_odds - 1) - (1 - model_prob)
        kelly = edge / (decimal_odds - 1) if decimal_odds > 1 else 0
        print(f"\n  >>> VALUE BET on {name_a}")
        print(f"      Expected value per $1:  ${ev:+.3f}")
        print(f"      Kelly fraction:         {pct(kelly)}")
    elif edge < -0.02:
        print(f"\n  >>> NO EDGE — market has {name_a} cheaper than model suggests.")
    else:
        print(f"\n  >>> MARGINAL — within noise (~2pp).")


# ── Analysis runner ─────────────────────────────────────────────────────────

def run_analysis(conn, tour, name_a_input, name_b_input, preset_odds_a=None):
    """Run full analysis for a matchup. If preset_odds_a is set, skip manual entry."""
    elo_table = tour["elo_table"]
    match_table = tour["match_table"]
    tour_label = tour["label"]

    # Look up players
    p_a = find_player(conn, name_a_input, elo_table)
    if not p_a:
        print(f"  Could not find '{name_a_input}' in {tour_label} Elo ratings.")
        return
    p_b = find_player(conn, name_b_input, elo_table)
    if not p_b:
        print(f"  Could not find '{name_b_input}' in {tour_label} Elo ratings.")
        return

    name_a = p_a["player_name"]
    name_b = p_b["player_name"]

    # Elo cards
    print_section(f"ELO RATINGS ({tour_label})")
    print_elo_card("Player A", p_a)
    print_elo_card("Player B", p_b)

    # Overall win probability
    prob_a_overall = elo_win_prob(p_a["elo_overall"], p_b["elo_overall"])
    print(f"\n  Overall Elo win prob:")
    print(f"    {name_a}: {pct(prob_a_overall)}    {name_b}: {pct(1 - prob_a_overall)}")

    # Surface selection
    print(f"\n  Surface: 1) Hard  2) Clay  3) Grass")
    surf_choice = input("  Pick surface: ").strip()
    surface = SURFACES.get(surf_choice, "Hard")
    col = SURFACE_COL[surface]

    prob_a_surface = elo_win_prob(p_a[col], p_b[col])
    print(f"\n  {surface} Elo win prob:")
    print(f"    {name_a}: {pct(prob_a_surface)}    {name_b}: {pct(1 - prob_a_surface)}")

    # Blended probability (70% surface, 30% overall)
    prob_a_blended = 0.7 * prob_a_surface + 0.3 * prob_a_overall
    print(f"\n  Blended (70% surface / 30% overall):")
    print(f"    {name_a}: {pct(prob_a_blended)}    {name_b}: {pct(1 - prob_a_blended)}")

    # H2H
    wins_a, wins_b, meetings = head_to_head(conn, name_a, name_b, match_table)
    print_h2h(name_a, name_b, wins_a, wins_b, meetings)

    # Recent form
    form_a = recent_form(conn, name_a, match_table)
    form_b = recent_form(conn, name_b, match_table)
    print_form(name_a, form_a)
    print_form(name_b, form_b)

    # Odds
    print_section("BETTING ODDS")
    if preset_odds_a is not None:
        # preset_odds_a is American odds from API (numeric)
        decimal_a = american_to_decimal(preset_odds_a)
        implied_a = 1.0 / decimal_a
        print(f"  Odds for {name_a}: {format_american(preset_odds_a)} (consensus from API)")
    else:
        print(f"  Enter odds for {name_a} to win.")
        print(f"  (American: +150 / -200  |  Decimal: 2.50)")
        raw_odds = input(f"  Odds for {name_a}: ").strip()
        if not raw_odds:
            print("  Skipping edge calculation.")
            return
        try:
            implied_a, decimal_a = parse_odds(raw_odds)
        except ValueError:
            print("  Could not parse odds.")
            return

    print(f"  Decimal odds: {decimal_a:.3f}  →  Implied prob: {pct(implied_a)}")

    # Edge analysis
    print_edge("EDGE ANALYSIS — OVERALL ELO", prob_a_overall, implied_a, name_a, decimal_a)
    print_edge(f"EDGE ANALYSIS — {surface.upper()} ELO", prob_a_surface, implied_a, name_a, decimal_a)
    print_edge("EDGE ANALYSIS — BLENDED", prob_a_blended, implied_a, name_a, decimal_a)


# ── Main loop ───────────────────────────────────────────────────────────────

def main():
    conn = connect()

    print("=" * 60)
    print("  DAILY EDGE FINDER — ATP / WTA Tennis")
    print("=" * 60)

    while True:
        print("\n")
        print("  Mode:")
        print("    1) Live odds — pick from today's matches")
        print("    2) Manual — enter players and odds yourself")
        mode = input("  Pick mode (or 'q' to quit): ").strip()

        if mode.lower() == "q":
            break

        if mode == "1":
            result = show_live_odds_menu(conn)
            if result is None:
                continue

            tour_type, selected = result
            tour = TOURS[tour_type]
            p1, p2 = selected["players"]
            odds_p1 = selected["odds"][p1]

            print(f"\n  Selected: {p1} vs {p2}")
            print(f"  Consensus odds: {p1} {format_american(odds_p1)}")

            run_analysis(conn, tour, p1, p2, preset_odds_a=odds_p1)

        elif mode == "2":
            print("\n  Tour: 1) ATP  2) WTA")
            tour_choice = input("  Pick tour: ").strip()
            tour = TOURS.get(tour_choice)
            if not tour:
                print("  Invalid choice, defaulting to ATP.")
                tour = TOURS["1"]

            name_a_input = input("  Player A name: ").strip()
            if name_a_input.lower() == "q":
                break
            name_b_input = input("  Player B name: ").strip()
            if name_b_input.lower() == "q":
                break

            run_analysis(conn, tour, name_a_input, name_b_input)

        else:
            print("  Invalid mode.")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
