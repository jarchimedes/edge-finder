#!/usr/bin/env python3
"""
app.py — Tennis Edge Finder v2.
Signal-driven daily briefing. Surfaces only interesting matches with reasoning.
"""

import json
import os
import sqlite3
import urllib.request
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for
from dotenv import load_dotenv

from db import get_db
from signals import analyze_match
from db import get_db

load_dotenv()

app = Flask(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "tennis.db")
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "3637b9c15b5c926ba3f72ba70d58dfa0")
ODDS_API_BASE = "https://api.the-odds-api.com/v4"

TOUR_CONFIG = {
    "atp": {"elo_table": "atp_elo_ratings", "match_table": "atp_matches"},
    "wta": {"elo_table": "wta_elo_ratings", "match_table": "wta_matches"},
}

SURFACE_COL = {"Hard": "elo_hard", "Clay": "elo_clay", "Grass": "elo_grass"}


# ── Database ────────────────────────────────────────────────────────────────


def init_picks_table():
    conn = get_db()
    is_pg = os.environ.get("DATABASE_URL") is not None
    if is_pg:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS picks (
                id SERIAL PRIMARY KEY,
                created_at TEXT DEFAULT (to_char(now(), 'YYYY-MM-DD HH24:MI:SS')),
                match_date TEXT, tournament TEXT, tour TEXT, surface TEXT,
                player_a TEXT, player_b TEXT, bet_type TEXT, bet_description TEXT,
                odds REAL, units REAL DEFAULT 1, result TEXT DEFAULT 'pending',
                profit REAL DEFAULT 0, notes TEXT, source TEXT DEFAULT 'self'
            )
        """)
        conn.commit()
        cur.close()
    else:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS picks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT DEFAULT (datetime('now')),
                match_date TEXT, tournament TEXT, tour TEXT, surface TEXT,
                player_a TEXT, player_b TEXT, bet_type TEXT, bet_description TEXT,
                odds REAL, units REAL DEFAULT 1, result TEXT DEFAULT 'pending',
                profit REAL DEFAULT 0, notes TEXT, source TEXT DEFAULT 'self'
            )
        """)
        conn.commit()
    conn.close()


init_picks_table()


# ── Odds API ────────────────────────────────────────────────────────────────

def api_get(endpoint, params=None):
    params = params or {}
    params["apiKey"] = ODDS_API_KEY
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{ODDS_API_BASE}/{endpoint}?{qs}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def fetch_tennis_sports():
    data = api_get("sports")
    if not data:
        return []
    return [s for s in data if "tennis" in s["key"] and s["active"]]


def fetch_match_odds(sport_key):
    data = api_get(
        f"sports/{sport_key}/odds",
        {"regions": "us,eu", "markets": "h2h,spreads,totals", "oddsFormat": "decimal"},
    )
    return data or []


def classify_tournament(title, sport_key):
    title_lower = title.lower()
    if any(s in title_lower for s in ["grand slam", "australian open", "roland garros",
                                       "french open", "wimbledon", "us open"]):
        return "Grand Slam"
    if "1000" in sport_key or "masters" in title_lower:
        return "1000"
    if "500" in sport_key or "500" in title_lower:
        return "500"
    if "250" in sport_key or "250" in title_lower:
        return "250"
    if "125" in sport_key or "challenger" in title_lower:
        return "125/CH"
    return "Other"


def get_all_market_odds(match):
    markets = {"h2h": {}, "spreads": [], "totals": []}
    for bk in match.get("bookmakers", []):
        bk_name = bk.get("key", "")
        for market in bk.get("markets", []):
            mkey = market["key"]
            if mkey == "h2h":
                for outcome in market["outcomes"]:
                    name = outcome["name"]
                    markets["h2h"].setdefault(name, []).append({
                        "book": bk_name, "price": outcome["price"]
                    })
            elif mkey == "spreads":
                for outcome in market["outcomes"]:
                    markets["spreads"].append({
                        "player": outcome["name"], "book": bk_name,
                        "price": outcome["price"], "point": outcome.get("point", 0)
                    })
            elif mkey == "totals":
                for outcome in market["outcomes"]:
                    markets["totals"].append({
                        "label": outcome["name"], "book": bk_name,
                        "price": outcome["price"], "point": outcome.get("point", 0)
                    })
    return markets


def avg_odds(odds_list):
    if not odds_list:
        return None
    return sum(o["price"] for o in odds_list) / len(odds_list)


def best_odds(odds_list):
    if not odds_list:
        return None
    return max(odds_list, key=lambda x: x["price"])


def decimal_to_american(dec):
    if dec is None:
        return "—"
    if dec >= 2.0:
        return f"+{(dec - 1) * 100:.0f}"
    elif dec > 1.0:
        return f"-{100 / (dec - 1):.0f}"
    return "—"


def format_date(d):
    if not d or len(d) < 8:
        return d or ""
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


def find_player_elo(conn, name, elo_table):
    row = conn.execute(
        f"SELECT * FROM {elo_table} WHERE LOWER(player_name) = LOWER(%s)", (name,)
    ).fetchone()
    if row:
        return row
    rows = conn.execute(
        f"SELECT * FROM {elo_table} WHERE LOWER(player_name) LIKE LOWER(%s)",
        (f"%{name}%",),
    ).fetchall()
    if len(rows) == 1:
        return rows[0]
    if len(rows) > 1:
        return max(rows, key=lambda r: r["elo_overall"])
    return None


def get_rank(conn, name, match_table):
    r = conn.execute(
        f"SELECT winner_rank FROM {match_table} WHERE winner_name = %s ORDER BY tourney_date DESC LIMIT 1",
        (name,)).fetchone()
    if r and r["winner_rank"]:
        try:
            return int(r["winner_rank"])
        except:
            pass
    return None


def elo_win_prob(a, b):
    return 1.0 / (1.0 + 10 ** ((b - a) / 400.0))


# ── Routes ──────────────────────────────────────────────────────────────────

@app.route("/")
def home():
    sports = fetch_tennis_sports()
    conn = get_db()
    all_signals = []

    for sport in sports:
        tour_type = "atp" if "atp" in sport["key"] else "wta"
        cfg = TOUR_CONFIG[tour_type]
        tier = classify_tournament(sport["title"], sport["key"])
        matches_data = fetch_match_odds(sport["key"])

        for m in matches_data:
            mkt = get_all_market_odds(m)
            h2h = mkt["h2h"]
            if len(h2h) < 2:
                continue

            players = list(h2h.keys())
            p1, p2 = players[0], players[1]
            avg1 = avg_odds(h2h[p1])
            avg2 = avg_odds(h2h[p2])
            if not avg1 or not avg2:
                continue

            best1 = best_odds(h2h[p1])
            best2 = best_odds(h2h[p2])

            # Elo + rank
            pa = find_player_elo(conn, p1, cfg["elo_table"])
            pb = find_player_elo(conn, p2, cfg["elo_table"])
            elo_a = pa["elo_overall"] if pa else None
            elo_b = pb["elo_overall"] if pb else None

            name_a_db = pa["player_name"] if pa else p1
            name_b_db = pb["player_name"] if pb else p2

            rank_a = get_rank(conn, name_a_db, cfg["match_table"]) if pa else None
            rank_b = get_rank(conn, name_b_db, cfg["match_table"]) if pb else None

            # Surface — default Hard for now
            surface = "Hard"  # TODO: detect from tournament data

            # Run signal analysis
            sig = analyze_match(
                conn, p1, p2, avg1, avg2, surface, tour_type,
                sport["title"], tier, spreads=mkt["spreads"], totals=mkt["totals"],
                elo_a=elo_a, elo_b=elo_b, rank_a=rank_a, rank_b=rank_b,
            )

            if sig:
                # Build best odds for spreads/totals display
                spread_display = {}
                for s in mkt["spreads"]:
                    key = s["player"]
                    if key not in spread_display or s["price"] > spread_display[key]["price"]:
                        spread_display[key] = s

                total_display = {}
                for t in mkt["totals"]:
                    key = t["label"]
                    if key not in total_display or t["price"] > total_display[key]["price"]:
                        total_display[key] = t

                all_signals.append({
                    "tournament": sport["title"],
                    "tour": tour_type.upper(),
                    "tier": tier,
                    "player_a": p1,
                    "player_b": p2,
                    "name_a_db": name_a_db,
                    "name_b_db": name_b_db,
                    "odds_a": avg1,
                    "odds_b": avg2,
                    "odds_a_am": decimal_to_american(avg1),
                    "odds_b_am": decimal_to_american(avg2),
                    "best_a": best1,
                    "best_b": best2,
                    "best_a_am": decimal_to_american(best1["price"]) if best1 else "—",
                    "best_b_am": decimal_to_american(best2["price"]) if best2 else "—",
                    "best_a_book": best1["book"] if best1 else "",
                    "best_b_book": best2["book"] if best2 else "",
                    "elo_a": f"{elo_a:.0f}" if elo_a else "—",
                    "elo_b": f"{elo_b:.0f}" if elo_b else "—",
                    "rank_a": rank_a,
                    "rank_b": rank_b,
                    "surface": surface,
                    "signal": sig,
                    "spreads": list(spread_display.values()),
                    "totals": list(total_display.values()),
                })

    # Sort by signal score descending
    all_signals.sort(key=lambda x: x["signal"]["score"], reverse=True)

    conn.close()
    return render_template("home.html", page="home", signals=all_signals,
                           today=datetime.now().strftime("%A, %B %d"))


@app.route("/all")
def all_matches():
    """Show ALL matches (no signal filter) for browsing."""
    sports = fetch_tennis_sports()
    conn = get_db()
    tournaments = []

    for sport in sports:
        tour_type = "atp" if "atp" in sport["key"] else "wta"
        cfg = TOUR_CONFIG[tour_type]
        tier = classify_tournament(sport["title"], sport["key"])
        matches_data = fetch_match_odds(sport["key"])
        cards = []

        for m in matches_data:
            mkt = get_all_market_odds(m)
            h2h = mkt["h2h"]
            if len(h2h) < 2:
                continue
            players = list(h2h.keys())
            p1, p2 = players[0], players[1]
            avg1 = avg_odds(h2h[p1])
            avg2 = avg_odds(h2h[p2])
            if not avg1 or not avg2:
                continue

            cards.append({
                "player_a": p1, "player_b": p2,
                "odds_a_am": decimal_to_american(avg1),
                "odds_b_am": decimal_to_american(avg2),
                "odds_a": avg1, "odds_b": avg2,
            })

        if cards:
            tournaments.append({
                "title": sport["title"],
                "tour": tour_type.upper(),
                "tier": tier,
                "matches": cards,
            })

    conn.close()
    return render_template("all_matches.html", page="all", tournaments=tournaments)


@app.route("/picks")
def picks():
    conn = get_db()
    all_picks = conn.execute(
        "SELECT * FROM picks ORDER BY created_at DESC LIMIT 100"
    ).fetchall()
    resolved = [p for p in all_picks if p["result"] in ("win", "loss", "push")]
    total_picks = len(resolved)
    wins = sum(1 for p in resolved if p["result"] == "win")
    total_profit = sum(p["profit"] for p in resolved)
    total_wagered = sum(p["units"] for p in resolved)
    roi = (total_profit / total_wagered * 100) if total_wagered > 0 else 0
    pending = [p for p in all_picks if p["result"] == "pending"]
    conn.close()
    return render_template("picks.html", page="picks", picks=all_picks,
                           stats={"total": total_picks, "wins": wins,
                                  "profit": total_profit, "roi": roi,
                                  "pending": len(pending), "wagered": total_wagered})


@app.route("/picks/add", methods=["POST"])
def add_pick():
    conn = get_db()
    conn.execute("""
        INSERT INTO picks (match_date, tournament, tour, surface, player_a, player_b,
                           bet_type, bet_description, odds, units, notes, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        request.form.get("match_date", ""),
        request.form.get("tournament", ""),
        request.form.get("tour", ""),
        request.form.get("surface", ""),
        request.form.get("player_a", ""),
        request.form.get("player_b", ""),
        request.form.get("bet_type", "ML"),
        request.form.get("bet_description", ""),
        float(request.form.get("odds", 0) or 0),
        float(request.form.get("units", 1) or 1),
        request.form.get("notes", ""),
        request.form.get("source", "self"),
    ))
    conn.commit()
    conn.close()
    return redirect(url_for("picks"))


@app.route("/picks/<int:pick_id>/resolve", methods=["POST"])
def resolve_pick(pick_id):
    result = request.form.get("result")
    conn = get_db()
    pick = conn.execute("SELECT * FROM picks WHERE id = %s", (pick_id,)).fetchone()
    if pick:
        if result == "win":
            profit = pick["units"] * (pick["odds"] - 1)
        elif result == "loss":
            profit = -pick["units"]
        else:
            profit = 0
            if result not in ("push", "void"):
                result = "void"
        conn.execute("UPDATE picks SET result = %s, profit = %s WHERE id = %s",
                     (result, profit, pick_id))
        conn.commit()
    conn.close()
    return redirect(url_for("picks"))


@app.route("/matchup")
def matchup():
    a = request.args.get("a", "")
    b = request.args.get("b", "")
    surface = request.args.get("surface", "Hard")
    tour = request.args.get("tour", "atp")
    if tour not in TOUR_CONFIG:
        tour = "atp"
    cfg = TOUR_CONFIG[tour]
    result = None
    error = None

    if a and b:
        conn = get_db()
        pa = find_player_elo(conn, a, cfg["elo_table"])
        pb = find_player_elo(conn, b, cfg["elo_table"])
        if not pa:
            error = f"Could not find '{a}'."
        elif not pb:
            error = f"Could not find '{b}'."
        else:
            name_a = pa["player_name"]
            name_b = pb["player_name"]
            col = SURFACE_COL.get(surface, "elo_hard")
            prob_overall = elo_win_prob(pa["elo_overall"], pb["elo_overall"])
            prob_surface = elo_win_prob(pa[col], pb[col])
            prob_blended = 0.7 * prob_surface + 0.3 * prob_overall

            from signals import _head_to_head, _get_form, _calc_streak
            h2h_a, h2h_b, meetings = _head_to_head(conn, name_a, name_b, cfg["match_table"])
            h2h_list = [{
                "date": format_date(m["tourney_date"]),
                "tourney": m["tourney_name"],
                "surface": m["surface"] or "?",
                "winner": m["winner_name"],
                "score": m["score"],
            } for m in meetings[:5]]

            form_a_raw = _get_form(conn, name_a, cfg["match_table"], 10)
            form_b_raw = _get_form(conn, name_b, cfg["match_table"], 10)

            def build_form_display(form_raw):
                return [{
                    "result": f["result"],
                    "date": format_date(f["date"]),
                    "tourney": f["tourney"],
                    "surface": f["surface"],
                    "opponent": f["opponent"],
                    "opp_rank": f["opp_rank"] or "?",
                    "score": f["score"],
                } for f in form_raw]

            result = {
                "name_a": name_a, "name_b": name_b,
                "elo_a": {"overall": pa["elo_overall"], "hard": pa["elo_hard"],
                          "clay": pa["elo_clay"], "grass": pa["elo_grass"]},
                "elo_b": {"overall": pb["elo_overall"], "hard": pb["elo_hard"],
                          "clay": pb["elo_clay"], "grass": pb["elo_grass"]},
                "prob_overall": prob_overall,
                "prob_surface": prob_surface,
                "prob_blended": prob_blended,
                "h2h_a": h2h_a, "h2h_b": h2h_b,
                "h2h_meetings": h2h_list,
                "form_a": build_form_display(form_a_raw),
                "form_b": build_form_display(form_b_raw),
                "streak_a": _calc_streak(form_a_raw),
                "streak_b": _calc_streak(form_b_raw),
            }
        conn.close()

    return render_template("matchup.html", page="matchup",
                           a=a, b=b, surface=surface, tour=tour,
                           result=result, error=error)


@app.route("/rankings")
def rankings():
    tour = request.args.get("tour", "atp")
    if tour not in TOUR_CONFIG:
        tour = "atp"
    cfg = TOUR_CONFIG[tour]
    conn = get_db()
    players = conn.execute(
        f"SELECT * FROM {cfg['elo_table']} ORDER BY elo_overall DESC"
    ).fetchall()
    conn.close()
    return render_template("rankings.html", page="rankings", players=players, tour=tour)


@app.route("/results")
def results():
    tour = request.args.get("tour", "all")
    conn = get_db()
    queries = []
    if tour in ("all", "atp"):
        queries.append(("ATP", "atp_matches"))
    if tour in ("all", "wta"):
        queries.append(("WTA", "wta_matches"))
    all_matches = []
    for tour_label, table in queries:
        rows = conn.execute(
            f"""SELECT tourney_name, surface, tourney_date, round, score,
                       winner_name, loser_name
                FROM {table}
                ORDER BY tourney_date DESC, COALESCE(NULLIF(match_num, '')::INTEGER, 0) DESC
                LIMIT 50"""
        ).fetchall()
        for r in rows:
            all_matches.append({
                "tour": tour_label,
                "date": format_date(r["tourney_date"]),
                "tourney": r["tourney_name"],
                "surface": r["surface"] or "?",
                "round": r["round"],
                "winner": r["winner_name"],
                "loser": r["loser_name"],
                "score": r["score"],
                "sort_date": r["tourney_date"],
            })
    all_matches.sort(key=lambda x: x["sort_date"], reverse=True)
    conn.close()
    return render_template("results.html", page="results", matches=all_matches[:50], tour=tour)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
