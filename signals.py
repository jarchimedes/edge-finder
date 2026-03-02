#!/usr/bin/env python3
"""
signals.py — Signal engine for tennis betting.
Analyzes matches and surfaces only the interesting ones with plain-English reasoning.
"""

from datetime import datetime, timedelta


def analyze_match(conn, player_a, player_b, odds_a, odds_b, surface, tour_type,
                  tournament, tier, spreads=None, totals=None,
                  elo_a=None, elo_b=None, rank_a=None, rank_b=None):
    """
    Analyze a single match and return a signal dict or None if not interesting.
    
    Returns: {
        "score": float,          # 0-100 composite interest score
        "signals": [str, ...],   # plain-English signal sentences
        "edge_player": str,      # who the edge favors (or None)
        "edge_type": str,        # "strong" / "moderate" / "lean"  
        "bet_angles": [str, ...],# specific bet suggestions
        "flags": [str, ...],     # warning flags (e.g. "inactivity")
    }
    """
    cfg = {
        "atp": {"elo_table": "atp_elo_ratings", "match_table": "atp_matches"},
        "wta": {"elo_table": "wta_elo_ratings", "match_table": "wta_matches"},
    }
    tour_key = tour_type.lower()
    if tour_key not in cfg:
        return None
    match_table = cfg[tour_key]["match_table"]

    score = 0.0
    signals = []
    flags = []
    bet_angles = []
    edge_player = None

    implied_a = 1.0 / odds_a if odds_a and odds_a > 1 else 0.5
    implied_b = 1.0 / odds_b if odds_b and odds_b > 1 else 0.5

    # Normalize player names for DB lookup
    name_a_db = _find_player_name(conn, player_a, match_table)
    name_b_db = _find_player_name(conn, player_b, match_table)

    # ── Form analysis ───────────────────────────────────────
    form_a = _get_form(conn, name_a_db, match_table, 10) if name_a_db else []
    form_b = _get_form(conn, name_b_db, match_table, 10) if name_b_db else []

    streak_a = _calc_streak(form_a)
    streak_b = _calc_streak(form_b)

    form_score_a = _form_quality(form_a, surface)
    form_score_b = _form_quality(form_b, surface)

    # Hot streak detection
    if streak_a["type"] == "W" and streak_a["count"] >= 3:
        score += 8 + streak_a["count"] * 2
        surface_note = ""
        surface_wins = sum(1 for f in form_a[:streak_a["count"]] if f["surface"] == surface)
        if surface_wins >= 2:
            surface_note = f", {surface_wins} on {surface.lower()}"
            score += 5
        signals.append(f"{player_a} is on a {streak_a['count']}-match win streak{surface_note}.")
    
    if streak_b["type"] == "W" and streak_b["count"] >= 3:
        score += 8 + streak_b["count"] * 2
        surface_note = ""
        surface_wins = sum(1 for f in form_b[:streak_b["count"]] if f["surface"] == surface)
        if surface_wins >= 2:
            surface_note = f", {surface_wins} on {surface.lower()}"
            score += 5
        signals.append(f"{player_b} is on a {streak_b['count']}-match win streak{surface_note}.")

    # Cold streak / bad form
    if streak_a["type"] == "L" and streak_a["count"] >= 2:
        score += 5
        signals.append(f"{player_a} has lost {streak_a['count']} straight.")
        flags.append(f"{player_a} cold")

    if streak_b["type"] == "L" and streak_b["count"] >= 2:
        score += 5
        signals.append(f"{player_b} has lost {streak_b['count']} straight.")
        flags.append(f"{player_b} cold")

    # ── Inactivity detection ────────────────────────────────
    days_since_a = _days_since_last_match(form_a)
    days_since_b = _days_since_last_match(form_b)

    if days_since_a and days_since_a > 30:
        score += 8
        signals.append(f"{player_a} hasn't played in {days_since_a} days — rust risk.")
        flags.append(f"{player_a} inactive {days_since_a}d")

    if days_since_b and days_since_b > 30:
        score += 8
        signals.append(f"{player_b} hasn't played in {days_since_b} days — rust risk.")
        flags.append(f"{player_b} inactive {days_since_b}d")

    # ── Head-to-head ────────────────────────────────────────
    h2h_a, h2h_b, h2h_matches = _head_to_head(conn, name_a_db, name_b_db, match_table)
    
    if h2h_a + h2h_b >= 3:
        # Significant H2H
        dominant = None
        if h2h_a >= h2h_b * 2 and h2h_a >= 3:
            dominant = player_a
            dominated = player_b
            dom_record = f"{h2h_a}-{h2h_b}"
        elif h2h_b >= h2h_a * 2 and h2h_b >= 3:
            dominant = player_b
            dominated = player_a
            dom_record = f"{h2h_b}-{h2h_a}"

        if dominant:
            score += 10
            # Check if market reflects this
            signals.append(f"{dominant} owns the H2H {dom_record} against {dominated}.")

        # Surface-specific H2H
        surface_h2h = _surface_h2h(h2h_matches, name_a_db, name_b_db, surface)
        if surface_h2h and surface_h2h["total"] >= 2:
            if surface_h2h["a_wins"] > surface_h2h["b_wins"]:
                signals.append(f"On {surface.lower()}: {player_a} leads {surface_h2h['a_wins']}-{surface_h2h['b_wins']}.")
            elif surface_h2h["b_wins"] > surface_h2h["a_wins"]:
                signals.append(f"On {surface.lower()}: {player_b} leads {surface_h2h['b_wins']}-{surface_h2h['a_wins']}.")

    # ── Quality of wins analysis ────────────────────────────
    quality_a = _quality_of_wins(form_a, 5)
    quality_b = _quality_of_wins(form_b, 5)

    if quality_a["ranked_wins"] >= 2:
        score += 6
        signals.append(f"{player_a} has beaten {quality_a['ranked_wins']} ranked opponents in last 5 (best: #{quality_a['best_scalp']}).")
    
    if quality_b["ranked_wins"] >= 2:
        score += 6
        signals.append(f"{player_b} has beaten {quality_b['ranked_wins']} ranked opponents in last 5 (best: #{quality_b['best_scalp']}).")

    # ── Straight sets tendency ──────────────────────────────
    straights_a = _straight_sets_rate(form_a, 6)
    straights_b = _straight_sets_rate(form_b, 6)

    # ── Fatigue / scheduling ────────────────────────────────
    played_yesterday_a = _played_recently(form_a, 1)
    played_yesterday_b = _played_recently(form_b, 1)

    if played_yesterday_a and not played_yesterday_b:
        score += 4
        last = form_a[0]
        if last.get("minutes") and int(last["minutes"] or 0) > 120:
            score += 4
            signals.append(f"{player_a} played a {last['minutes']}-minute match yesterday — potential fatigue.")
            flags.append(f"{player_a} fatigue risk")
        else:
            signals.append(f"{player_a} played yesterday; {player_b} is fresh.")

    if played_yesterday_b and not played_yesterday_a:
        score += 4
        last = form_b[0]
        if last.get("minutes") and int(last["minutes"] or 0) > 120:
            score += 4
            signals.append(f"{player_b} played a {last['minutes']}-minute match yesterday — potential fatigue.")
            flags.append(f"{player_b} fatigue risk")
        else:
            signals.append(f"{player_b} played yesterday; {player_a} is fresh.")

    # ── Form vs market mismatch ─────────────────────────────
    # If one player is in great form and the other isn't, but market doesn't reflect it
    form_gap = form_score_a - form_score_b  # positive = A in better form
    
    if abs(form_gap) > 0.3:
        better_form = player_a if form_gap > 0 else player_b
        worse_form = player_a if form_gap < 0 else player_b
        better_implied = implied_a if form_gap > 0 else implied_b
        
        # Is the market undervaluing the in-form player?
        if better_implied < 0.55:  # market doesn't have them as strong fav
            score += 12
            signals.append(f"{better_form} is in significantly better form than {worse_form}, but the market isn't pricing it in strongly.")

    # ── Elo edge analysis ───────────────────────────────────
    if elo_a and elo_b:
        elo_diff = elo_a - elo_b
        elo_prob_a = 1.0 / (1.0 + 10 ** ((elo_b - elo_a) / 400.0))
        elo_edge_a = elo_prob_a - implied_a
        elo_edge_b = (1 - elo_prob_a) - implied_b

        if abs(elo_edge_a) > 0.05:
            score += 5
            if elo_edge_a > 0:
                signals.append(f"Elo rates {player_a} at {elo_prob_a*100:.0f}% — market implies {implied_a*100:.0f}%. Gap of {elo_edge_a*100:.1f}pp.")
            else:
                signals.append(f"Elo rates {player_b} at {(1-elo_prob_a)*100:.0f}% — market implies {implied_b*100:.0f}%. Gap of {elo_edge_b*100:.1f}pp.")

    # ── Tier bonus (250/500 = softer lines) ─────────────────
    if tier in ("250", "125/CH"):
        score += 5  # lines are softer in smaller events
    elif tier == "500":
        score += 3

    # ── Determine edge player ───────────────────────────────
    # Combine all signals into an overall lean
    lean_a = 0
    lean_b = 0

    if streak_a["type"] == "W" and streak_a["count"] >= 3: lean_a += streak_a["count"]
    if streak_b["type"] == "W" and streak_b["count"] >= 3: lean_b += streak_b["count"]
    if streak_a["type"] == "L" and streak_a["count"] >= 2: lean_b += streak_a["count"]
    if streak_b["type"] == "L" and streak_b["count"] >= 2: lean_a += streak_b["count"]
    if days_since_a and days_since_a > 30: lean_b += 3
    if days_since_b and days_since_b > 30: lean_a += 3
    if form_gap > 0.3: lean_a += 4
    elif form_gap < -0.3: lean_b += 4
    if h2h_a > h2h_b: lean_a += min(h2h_a - h2h_b, 3)
    elif h2h_b > h2h_a: lean_b += min(h2h_b - h2h_a, 3)

    if lean_a > lean_b + 2:
        edge_player = player_a
    elif lean_b > lean_a + 2:
        edge_player = player_b

    edge_strength = abs(lean_a - lean_b)
    edge_type = "strong" if edge_strength >= 8 else "moderate" if edge_strength >= 5 else "lean"

    # ── Generate bet angles ─────────────────────────────────
    if edge_player:
        ep = edge_player
        ep_odds = odds_a if ep == player_a else odds_b
        opp = player_b if ep == player_a else player_a

        # ML angle
        if ep_odds >= 1.60:
            bet_angles.append(f"{ep} ML @ {ep_odds:.2f} — value on the in-form / favorable side.")
        elif ep_odds >= 1.30:
            bet_angles.append(f"{ep} ML @ {ep_odds:.2f} — shorter price but signals align.")

        # Straight sets angle
        ep_straights = straights_a if ep == player_a else straights_b
        if ep_straights >= 0.5 and edge_strength >= 5:
            bet_angles.append(f"{ep} -1.5 set handicap — wins in straights {ep_straights*100:.0f}% of recent matches.")

        # If edge player is the underdog — juicy
        if ep_odds >= 2.0:
            score += 8
            bet_angles.insert(0, f"🔥 {ep} is the DOG at {ep_odds:.2f} — signals favor them over {opp}.")

        # Parlay angle — pair with a heavy fav from another match
        if ep_odds < 1.50:
            bet_angles.append(f"Consider parlaying {ep} ML with another lean to boost juice.")

    # ── Totals angle ────────────────────────────────────────
    if totals:
        over_totals = [t for t in totals if t.get("label") == "Over"]
        if over_totals and form_a and form_b:
            # If both players are in long matches recently
            long_matches_a = sum(1 for f in form_a[:5] if f.get("sets_total", 0) >= 3)
            long_matches_b = sum(1 for f in form_b[:5] if f.get("sets_total", 0) >= 3)
            if long_matches_a >= 3 and long_matches_b >= 3:
                score += 5
                best_over = max(over_totals, key=lambda t: t.get("odds", 0))
                bet_angles.append(f"Over {best_over.get('point', '?')} games — both players trending long matches.")

    # ── Final threshold ─────────────────────────────────────
    if score < 15 or len(signals) < 2:
        return None

    return {
        "score": round(score, 1),
        "signals": signals,
        "edge_player": edge_player,
        "edge_type": edge_type if edge_player else None,
        "bet_angles": bet_angles,
        "flags": flags,
        "form_a": form_a[:5],
        "form_b": form_b[:5],
        "streak_a": streak_a,
        "streak_b": streak_b,
        "h2h": {"a": h2h_a, "b": h2h_b, "matches": h2h_matches[:3]} if name_a_db and name_b_db else None,
        "form_score_a": form_score_a,
        "form_score_b": form_score_b,
    }


# ── Helper functions ────────────────────────────────────────────────────────

def _find_player_name(conn, api_name, match_table):
    """Try to match an Odds API player name to our DB."""
    if not api_name:
        return None
    # Direct match
    row = conn.execute(
        f"SELECT winner_name FROM {match_table} WHERE LOWER(winner_name) = LOWER(%s) LIMIT 1",
        (api_name,)
    ).fetchone()
    if row:
        return row[0]
    
    # Fuzzy: try last name
    parts = api_name.split()
    if parts:
        last = parts[-1]
        rows = conn.execute(
            f"SELECT winner_name FROM {match_table} WHERE LOWER(winner_name) LIKE LOWER(%s) GROUP BY winner_name",
            (f"%{last}%",)
        ).fetchall()
        if len(rows) == 1:
            return rows[0][0]
        # Try first + last
        if len(parts) >= 2:
            first_initial = parts[0][0]
            for r in rows:
                if r[0].startswith(first_initial) or f" {first_initial}" in r[0]:
                    return r[0]
            # Try full first name
            first = parts[0]
            for r in rows:
                if first.lower() in r[0].lower():
                    return r[0]
    return None


def _get_form(conn, player_name, match_table, n=10):
    """Get recent match form for a player."""
    if not player_name:
        return []
    rows = conn.execute(
        f"""SELECT tourney_name, surface, tourney_date, round, score,
                   winner_name, loser_name, winner_rank, loser_rank, minutes,
                   best_of
            FROM (
                SELECT * FROM {match_table} WHERE winner_name = %s
                UNION ALL
                SELECT * FROM {match_table} WHERE loser_name = %s
            )
            ORDER BY tourney_date DESC, COALESCE(NULLIF(match_num, '')::INTEGER, 0) DESC
            LIMIT %s""",
        (player_name, player_name, n),
    ).fetchall()
    
    form = []
    for r in rows:
        is_win = r["winner_name"] == player_name
        opponent = r["loser_name"] if is_win else r["winner_name"]
        opp_rank = r["loser_rank"] if is_win else r["winner_rank"]
        
        # Parse score for sets
        score = r["score"] or ""
        sets = score.count("-")
        
        form.append({
            "result": "W" if is_win else "L",
            "date": r["tourney_date"],
            "tourney": r["tourney_name"],
            "surface": r["surface"] or "Hard",
            "round": r["round"],
            "opponent": opponent,
            "opp_rank": opp_rank,
            "score": score,
            "minutes": r["minutes"],
            "sets_total": sets,
        })
    return form


def _calc_streak(form):
    """Calculate current win/loss streak."""
    if not form:
        return {"type": "", "count": 0}
    streak_type = form[0]["result"]
    count = 0
    for f in form:
        if f["result"] == streak_type:
            count += 1
        else:
            break
    return {"type": streak_type, "count": count}


def _form_quality(form, target_surface, n=8):
    """
    Rate form quality on a 0-1 scale.
    Considers: win rate, surface relevance, opponent quality, recency.
    """
    if not form:
        return 0.5
    
    recent = form[:n]
    if not recent:
        return 0.5
    
    wins = sum(1 for f in recent if f["result"] == "W")
    win_rate = wins / len(recent)
    
    # Surface bonus
    surface_wins = sum(1 for f in recent if f["result"] == "W" and f["surface"] == target_surface)
    surface_matches = sum(1 for f in recent if f["surface"] == target_surface)
    surface_rate = surface_wins / surface_matches if surface_matches > 0 else win_rate
    
    # Blend
    return 0.6 * win_rate + 0.4 * surface_rate


def _days_since_last_match(form):
    """Days since most recent match."""
    if not form or not form[0].get("date"):
        return None
    try:
        last = datetime.strptime(form[0]["date"], "%Y%m%d")
        now = datetime.now()
        return (now - last).days
    except:
        return None


def _played_recently(form, days=1):
    """Did the player play within the last N days?"""
    ds = _days_since_last_match(form)
    return ds is not None and ds <= days


def _head_to_head(conn, name_a, name_b, match_table):
    """Get H2H record."""
    if not name_a or not name_b:
        return 0, 0, []
    rows = conn.execute(
        f"""SELECT tourney_name, surface, tourney_date, round, score,
                   winner_name, loser_name
            FROM {match_table}
            WHERE (winner_name = %s AND loser_name = %s)
               OR (winner_name = %s AND loser_name = %s)
            ORDER BY tourney_date DESC""",
        (name_a, name_b, name_b, name_a),
    ).fetchall()
    wins_a = sum(1 for r in rows if r["winner_name"] == name_a)
    return wins_a, len(rows) - wins_a, rows


def _surface_h2h(h2h_matches, name_a, name_b, surface):
    """Filter H2H to specific surface."""
    surface_matches = [m for m in h2h_matches if m["surface"] == surface]
    if not surface_matches:
        return None
    a_wins = sum(1 for m in surface_matches if m["winner_name"] == name_a)
    return {"a_wins": a_wins, "b_wins": len(surface_matches) - a_wins, "total": len(surface_matches)}


def _quality_of_wins(form, n=5):
    """Analyze quality of recent wins by opponent ranking."""
    recent = form[:n]
    wins = [f for f in recent if f["result"] == "W"]
    ranked_wins = 0
    best_scalp = 999
    for w in wins:
        try:
            rank = int(w["opp_rank"])
            if rank <= 50:
                ranked_wins += 1
            if rank < best_scalp:
                best_scalp = rank
        except (ValueError, TypeError):
            pass
    return {"ranked_wins": ranked_wins, "best_scalp": best_scalp if best_scalp < 999 else None}


def _straight_sets_rate(form, n=6):
    """What % of recent wins were in straight sets?"""
    wins = [f for f in form[:n] if f["result"] == "W"]
    if not wins:
        return 0
    straights = 0
    for w in wins:
        score = w.get("score", "")
        sets = score.count("-")
        # In best-of-3, straight sets = 2 sets played
        if sets == 2:
            straights += 1
    return straights / len(wins)
