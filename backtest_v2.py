#!/usr/bin/env python3
"""
backtest_v2.py — Improved Elo tennis betting model backtest.

Improvements over v1:
1. Dynamic K-factor: higher K for players with fewer matches (faster adaptation)
2. Recency/form weighting: recent match bonus — hot players get an Elo boost
3. Inactivity decay: players who haven't played in 60+ days get penalized

Uses tennis-data.co.uk historical data (2010-2025) with Pinnacle closing odds.
"""

import glob
import os
import sys
import csv
from collections import defaultdict
from datetime import datetime, timedelta

ODDS_DIR = "odds_data"
START_ELO = 1500
EDGE_THRESHOLD = 0.02
STAKE = 100.0

# Dynamic K-factor params
K_BASE = 32
K_NEW_PLAYER = 64       # K for players with < 30 matches
K_ESTABLISHED = 24      # K for players with > 200 matches
K_MATCH_THRESHOLD_LOW = 30
K_MATCH_THRESHOLD_HIGH = 200

# Recency/form params
FORM_WINDOW = 10         # last N matches for form calculation
FORM_RECENCY_DAYS = 90   # only count matches within this many days
FORM_WEIGHT = 0.15       # how much form adjusts the blended probability

# Inactivity params
INACTIVITY_DAYS = 60     # start decaying after this many days
INACTIVITY_DECAY_PER_DAY = 0.5  # Elo points lost per day of inactivity beyond threshold
INACTIVITY_MAX_DECAY = 75  # cap the total decay


# ── Load odds data (same as v1) ────────────────────────────────────────────

def load_odds_files():
    """Load all ATP and WTA odds files into a list of match dicts."""
    try:
        import openpyxl
    except ImportError:
        print("Need openpyxl: pip install openpyxl"); sys.exit(1)
    try:
        import xlrd
    except ImportError:
        print("Need xlrd: pip install xlrd"); sys.exit(1)

    matches = []
    files = sorted(glob.glob(os.path.join(ODDS_DIR, "*.xlsx")) +
                   glob.glob(os.path.join(ODDS_DIR, "*.xls")))

    for fpath in files:
        fname = os.path.basename(fpath)
        tour = "ATP" if fname.startswith("atp_") else "WTA"

        if fpath.endswith(".xlsx"):
            wb = openpyxl.load_workbook(fpath, read_only=True, data_only=True)
            ws = wb.active
            rows = list(ws.iter_rows(values_only=True))
            wb.close()
            if not rows: continue
            headers = [str(h).strip() if h else "" for h in rows[0]]
            data_rows = rows[1:]
        else:
            wb = xlrd.open_workbook(fpath)
            ws = wb.sheet_by_index(0)
            headers = [str(ws.cell_value(0, c)).strip() for c in range(ws.ncols)]
            data_rows = [[ws.cell_value(r, c) for c in range(ws.ncols)] for r in range(1, ws.nrows)]

        col = {h: i for i, h in enumerate(headers)}
        required = ["Winner", "Loser", "Surface"]
        if not all(r in col for r in required): continue

        for row in data_rows:
            try:
                winner = row[col["Winner"]]
                loser = row[col["Loser"]]
                surface = row[col["Surface"]]
                if not winner or not loser or not surface: continue

                date_val = row[col["Date"]] if "Date" in col else None
                if date_val is None: continue

                if isinstance(date_val, datetime):
                    date_str = date_val.strftime("%Y%m%d")
                elif isinstance(date_val, str):
                    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
                        try:
                            date_str = datetime.strptime(date_val.strip(), fmt).strftime("%Y%m%d")
                            break
                        except ValueError: continue
                    else: continue
                elif isinstance(date_val, (int, float)):
                    if fpath.endswith(".xls"):
                        try:
                            dt = xlrd.xldate_as_datetime(date_val, wb.datemode)
                            date_str = dt.strftime("%Y%m%d")
                        except: continue
                    else: continue
                else: continue

                psw = _safe_float(row[col["PSW"]]) if "PSW" in col else None
                psl = _safe_float(row[col["PSL"]]) if "PSL" in col else None
                b365w = _safe_float(row[col["B365W"]]) if "B365W" in col else None
                b365l = _safe_float(row[col["B365L"]]) if "B365L" in col else None
                avgw = _safe_float(row[col["AvgW"]]) if "AvgW" in col else None
                avgl = _safe_float(row[col["AvgL"]]) if "AvgL" in col else None

                winner_odds = psw or b365w or avgw
                loser_odds = psl or b365l or avgl
                if not winner_odds or not loser_odds: continue
                if winner_odds <= 1.0 or loser_odds <= 1.0: continue

                rnd = str(row[col["Round"]]) if "Round" in col else ""
                tourney = str(row[col.get("Tournament", col.get("Location", 0))]) if "Tournament" in col or "Location" in col else ""
                comment = str(row[col["Comment"]]) if "Comment" in col else "Completed"
                series = str(row[col["Series"]]) if "Series" in col else ""

                matches.append({
                    "date": date_str, "tour": tour, "tournament": tourney,
                    "surface": str(surface).strip(), "round": rnd,
                    "winner": str(winner).strip(), "loser": str(loser).strip(),
                    "winner_odds": winner_odds, "loser_odds": loser_odds,
                    "comment": comment, "series": series,
                })
            except (IndexError, TypeError, ValueError): continue

    matches.sort(key=lambda m: m["date"])
    return matches


def _safe_float(val):
    if val is None: return None
    try:
        f = float(val)
        return f if f > 0 else None
    except (ValueError, TypeError): return None


# ── Enhanced Elo engine ─────────────────────────────────────────────────────

class EloEngineV2:
    def __init__(self):
        self.ratings = {}       # name -> {"overall", "Hard", "Clay", "Grass"}
        self.match_count = {}   # name -> total matches played
        self.last_played = {}   # name -> date string (YYYYMMDD)
        self.recent_results = defaultdict(list)  # name -> list of (date_str, won: bool)

    def _ensure(self, name):
        if name not in self.ratings:
            self.ratings[name] = {
                "overall": START_ELO, "Hard": START_ELO,
                "Clay": START_ELO, "Grass": START_ELO,
            }
            self.match_count[name] = 0

    def _get_k(self, name):
        """Dynamic K-factor based on matches played."""
        n = self.match_count.get(name, 0)
        if n < K_MATCH_THRESHOLD_LOW:
            return K_NEW_PLAYER
        elif n > K_MATCH_THRESHOLD_HIGH:
            return K_ESTABLISHED
        else:
            # Linear interpolation
            ratio = (n - K_MATCH_THRESHOLD_LOW) / (K_MATCH_THRESHOLD_HIGH - K_MATCH_THRESHOLD_LOW)
            return K_NEW_PLAYER - ratio * (K_NEW_PLAYER - K_ESTABLISHED)

    def _inactivity_penalty(self, name, current_date_str):
        """Calculate Elo penalty for inactivity."""
        if name not in self.last_played:
            return 0
        try:
            last = datetime.strptime(self.last_played[name], "%Y%m%d")
            current = datetime.strptime(current_date_str, "%Y%m%d")
            days_inactive = (current - last).days
            if days_inactive <= INACTIVITY_DAYS:
                return 0
            excess_days = days_inactive - INACTIVITY_DAYS
            decay = excess_days * INACTIVITY_DECAY_PER_DAY
            return min(decay, INACTIVITY_MAX_DECAY)
        except:
            return 0

    def _form_score(self, name, current_date_str):
        """
        Calculate form score from recent results.
        Returns a value between -1.0 (terrible form) and +1.0 (great form).
        Weighted by recency — more recent matches count more.
        """
        results = self.recent_results[name]
        if not results:
            return 0.0

        try:
            current = datetime.strptime(current_date_str, "%Y%m%d")
        except:
            return 0.0

        # Filter to form window within recency days
        recent = []
        for date_str, won in results[-FORM_WINDOW:]:
            try:
                d = datetime.strptime(date_str, "%Y%m%d")
                days_ago = (current - d).days
                if days_ago <= FORM_RECENCY_DAYS:
                    recent.append((days_ago, won))
            except:
                continue

        if not recent:
            return 0.0

        # Weight by recency: more recent = higher weight
        total_weight = 0.0
        weighted_score = 0.0
        for days_ago, won in recent:
            weight = 1.0 / (1.0 + days_ago / 30.0)  # decays with time
            total_weight += weight
            weighted_score += weight * (1.0 if won else -1.0)

        if total_weight == 0:
            return 0.0
        
        return weighted_score / total_weight  # normalized to [-1, 1]

    def win_prob(self, name_a, name_b, surface, date_str):
        """
        Enhanced win probability with:
        - Surface-weighted Elo blend (70/30)
        - Inactivity penalty
        - Form adjustment
        """
        self._ensure(name_a)
        self._ensure(name_b)

        # Apply inactivity penalty (temporary, for prediction only)
        penalty_a = self._inactivity_penalty(name_a, date_str)
        penalty_b = self._inactivity_penalty(name_b, date_str)

        elo_a_ov = self.ratings[name_a]["overall"] - penalty_a
        elo_b_ov = self.ratings[name_b]["overall"] - penalty_b
        prob_overall = 1.0 / (1.0 + 10 ** ((elo_b_ov - elo_a_ov) / 400.0))

        s = surface if surface in ("Hard", "Clay", "Grass") else None
        if s:
            elo_a_s = self.ratings[name_a][s] - penalty_a
            elo_b_s = self.ratings[name_b][s] - penalty_b
            prob_surface = 1.0 / (1.0 + 10 ** ((elo_b_s - elo_a_s) / 400.0))
            base_prob = 0.7 * prob_surface + 0.3 * prob_overall
        else:
            base_prob = prob_overall

        # Form adjustment
        form_a = self._form_score(name_a, date_str)
        form_b = self._form_score(name_b, date_str)
        form_diff = form_a - form_b  # range [-2, 2]

        # Shift probability by form — capped to keep it sane
        form_adj = FORM_WEIGHT * (form_diff / 2.0)  # normalize to [-FORM_WEIGHT, +FORM_WEIGHT]
        adjusted_prob = base_prob + form_adj
        adjusted_prob = max(0.02, min(0.98, adjusted_prob))

        return adjusted_prob

    def update(self, winner, loser, surface, date_str):
        """Update ratings after a match."""
        self._ensure(winner)
        self._ensure(loser)

        k_w = self._get_k(winner)
        k_l = self._get_k(loser)
        k = (k_w + k_l) / 2.0  # average K for the match

        # Overall
        w_ov = self.ratings[winner]["overall"]
        l_ov = self.ratings[loser]["overall"]
        exp_w = 1.0 / (1.0 + 10 ** ((l_ov - w_ov) / 400.0))
        self.ratings[winner]["overall"] = w_ov + k * (1 - exp_w)
        self.ratings[loser]["overall"] = l_ov + k * (0 - (1 - exp_w))

        # Surface
        s = surface if surface in ("Hard", "Clay", "Grass") else None
        if s:
            w_s = self.ratings[winner][s]
            l_s = self.ratings[loser][s]
            exp_ws = 1.0 / (1.0 + 10 ** ((l_s - w_s) / 400.0))
            self.ratings[winner][s] = w_s + k * (1 - exp_ws)
            self.ratings[loser][s] = l_s + k * (0 - (1 - exp_ws))

        # Update metadata
        self.match_count[winner] = self.match_count.get(winner, 0) + 1
        self.match_count[loser] = self.match_count.get(loser, 0) + 1
        self.last_played[winner] = date_str
        self.last_played[loser] = date_str
        self.recent_results[winner].append((date_str, True))
        self.recent_results[loser].append((date_str, False))

        # Trim recent results to keep memory bounded
        if len(self.recent_results[winner]) > FORM_WINDOW * 3:
            self.recent_results[winner] = self.recent_results[winner][-FORM_WINDOW * 2:]
        if len(self.recent_results[loser]) > FORM_WINDOW * 3:
            self.recent_results[loser] = self.recent_results[loser][-FORM_WINDOW * 2:]


# ── Backtest ────────────────────────────────────────────────────────────────

def run_backtest(matches, edge_threshold=EDGE_THRESHOLD, stake=STAKE, warmup_matches=2000):
    elo = EloEngineV2()
    bets = []
    total_pnl = 0.0
    peak_pnl = 0.0
    max_drawdown = 0.0
    yearly_stats = defaultdict(lambda: {"bets": 0, "wins": 0, "pnl": 0.0})
    skipped_retirements = 0

    for i, m in enumerate(matches):
        surface = m["surface"]
        if surface == "Carpet": surface = "Hard"
        winner = m["winner"]
        loser = m["loser"]
        date_str = m["date"]

        comment = m.get("comment", "Completed")
        if comment and "Completed" not in str(comment):
            skipped_retirements += 1
            elo.update(winner, loser, surface, date_str)
            continue

        if i >= warmup_matches:
            prob_winner = elo.win_prob(winner, loser, surface, date_str)
            prob_loser = 1.0 - prob_winner
            implied_winner = 1.0 / m["winner_odds"]
            implied_loser = 1.0 / m["loser_odds"]
            year = date_str[:4]

            # Edge on winner
            edge_w = prob_winner - implied_winner
            if edge_w > edge_threshold:
                profit = stake * (m["winner_odds"] - 1)
                total_pnl += profit
                peak_pnl = max(peak_pnl, total_pnl)
                max_drawdown = max(max_drawdown, peak_pnl - total_pnl)
                bets.append({
                    "date": date_str, "tour": m["tour"], "tournament": m["tournament"],
                    "surface": surface, "bet_on": winner, "opponent": loser,
                    "model_prob": prob_winner, "implied_prob": implied_winner,
                    "edge": edge_w, "odds": m["winner_odds"],
                    "result": "W", "profit": profit, "cumulative_pnl": total_pnl,
                })
                yearly_stats[year]["bets"] += 1
                yearly_stats[year]["wins"] += 1
                yearly_stats[year]["pnl"] += profit

            # Edge on loser
            edge_l = prob_loser - implied_loser
            if edge_l > edge_threshold:
                profit = -stake
                total_pnl += profit
                max_drawdown = max(max_drawdown, peak_pnl - total_pnl)
                bets.append({
                    "date": date_str, "tour": m["tour"], "tournament": m["tournament"],
                    "surface": surface, "bet_on": loser, "opponent": winner,
                    "model_prob": prob_loser, "implied_prob": implied_loser,
                    "edge": edge_l, "odds": m["loser_odds"],
                    "result": "L", "profit": profit, "cumulative_pnl": total_pnl,
                })
                yearly_stats[year]["bets"] += 1
                yearly_stats[year]["pnl"] += profit

        elo.update(winner, loser, surface, date_str)

    return {
        "bets": bets, "total_pnl": total_pnl, "peak_pnl": peak_pnl,
        "max_drawdown": max_drawdown, "yearly_stats": dict(yearly_stats),
        "matches_processed": len(matches), "warmup_matches": warmup_matches,
        "skipped_retirements": skipped_retirements,
    }


# ── Reporting ───────────────────────────────────────────────────────────────

def print_report(results, label="V2"):
    bets = results["bets"]
    total_bets = len(bets)
    if total_bets == 0:
        print("\nNo bets triggered.")
        return

    wins = sum(1 for b in bets if b["result"] == "W")
    losses = total_bets - wins
    total_wagered = total_bets * STAKE
    roi = (results["total_pnl"] / total_wagered) * 100

    print(f"\n{'=' * 70}")
    print(f"  BACKTEST RESULTS — ELO TENNIS MODEL {label}")
    print(f"{'=' * 70}")
    print(f"\n  Config:")
    print(f"    K-factor:          dynamic ({K_NEW_PLAYER} new → {K_ESTABLISHED} established)")
    print(f"    Blend:             70% surface / 30% overall")
    print(f"    Form window:       last {FORM_WINDOW} matches within {FORM_RECENCY_DAYS} days")
    print(f"    Form weight:       {FORM_WEIGHT}")
    print(f"    Inactivity decay:  {INACTIVITY_DECAY_PER_DAY}/day after {INACTIVITY_DAYS} days (max {INACTIVITY_MAX_DECAY})")
    print(f"    Edge threshold:    {EDGE_THRESHOLD * 100:.1f}pp")
    print(f"    Stake:             ${STAKE:.0f} flat")
    print(f"    Warmup:            {results['warmup_matches']}")

    print(f"\n  Summary:")
    print(f"    Matches processed: {results['matches_processed']:,}")
    print(f"    Total bets:        {total_bets:,}")
    print(f"    Wins:              {wins:,} ({wins/total_bets*100:.1f}%)")
    print(f"    Losses:            {losses:,} ({losses/total_bets*100:.1f}%)")
    print(f"    Total wagered:     ${total_wagered:,.0f}")
    print(f"    Total P&L:         ${results['total_pnl']:+,.0f}")
    print(f"    ROI:               {roi:+.2f}%")
    print(f"    Peak P&L:          ${results['peak_pnl']:+,.0f}")
    print(f"    Max drawdown:      ${results['max_drawdown']:,.0f}")

    print(f"\n  {'Year':<6} {'Bets':>6} {'Wins':>6} {'Win%':>7} {'P&L':>10} {'ROI':>8}")
    print("  " + "-" * 45)
    for year in sorted(results["yearly_stats"].keys()):
        ys = results["yearly_stats"][year]
        yr_roi = (ys["pnl"] / (ys["bets"] * STAKE)) * 100 if ys["bets"] > 0 else 0
        win_pct = (ys["wins"] / ys["bets"]) * 100 if ys["bets"] > 0 else 0
        print(f"  {year:<6} {ys['bets']:>6} {ys['wins']:>6} {win_pct:>6.1f}% ${ys['pnl']:>+9,.0f} {yr_roi:>+7.2f}%")

    # Edge buckets
    print(f"\n  Edge Bucket Analysis:")
    print(f"  {'Edge Range':<15} {'Bets':>6} {'Wins':>6} {'Win%':>7} {'P&L':>10} {'ROI':>8}")
    print("  " + "-" * 55)
    for lo, hi in [(0.02, 0.05), (0.05, 0.10), (0.10, 0.15), (0.15, 0.20), (0.20, 1.0)]:
        bb = [b for b in bets if lo <= b["edge"] < hi]
        if not bb: continue
        bw = sum(1 for b in bb if b["result"] == "W")
        bp = sum(b["profit"] for b in bb)
        br = (bp / (len(bb) * STAKE)) * 100
        print(f"  {f'{lo*100:.0f}-{hi*100:.0f}pp':<15} {len(bb):>6} {bw:>6} {bw/len(bb)*100:>6.1f}% ${bp:>+9,.0f} {br:>+7.2f}%")

    # Tour breakdown
    print(f"\n  Tour Breakdown:")
    for tour_name in ["ATP", "WTA"]:
        tb = [b for b in bets if b["tour"] == tour_name]
        if not tb: continue
        tw = sum(1 for b in tb if b["result"] == "W")
        tp = sum(b["profit"] for b in tb)
        tr = (tp / (len(tb) * STAKE)) * 100
        print(f"    {tour_name}: {len(tb):,} bets, {tw/len(tb)*100:.1f}% win, ${tp:+,.0f} P&L, {tr:+.2f}% ROI")


def save_csv(bets, filename="backtest_v2_results.csv"):
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "date", "tour", "tournament", "surface", "bet_on", "opponent",
            "model_prob", "implied_prob", "edge", "odds", "result", "profit", "cumulative_pnl"
        ])
        writer.writeheader()
        for b in bets:
            row = dict(b)
            for k in ("model_prob", "implied_prob", "edge"): row[k] = f"{b[k]:.4f}"
            row["odds"] = f"{b['odds']:.3f}"
            row["profit"] = f"{b['profit']:.2f}"
            row["cumulative_pnl"] = f"{b['cumulative_pnl']:.2f}"
            writer.writerow(row)
    print(f"\n  Saved {len(bets)} bets to {filename}")


def save_chart(bets, filename="backtest_v2_pnl.png"):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("  matplotlib not installed — skipping chart")
        return

    dates = [datetime.strptime(b["date"], "%Y%m%d") for b in bets]
    pnls = [b["cumulative_pnl"] for b in bets]

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(dates, pnls, linewidth=0.8, color="#2196F3")
    ax.axhline(y=0, color="gray", linestyle="--", linewidth=0.5)
    ax.fill_between(dates, pnls, 0, where=[p >= 0 for p in pnls], alpha=0.15, color="#4CAF50")
    ax.fill_between(dates, pnls, 0, where=[p < 0 for p in pnls], alpha=0.15, color="#F44336")
    ax.set_title(f"Elo Tennis Model V2 — Cumulative P&L (${STAKE:.0f} flat, >{EDGE_THRESHOLD*100:.0f}pp edge)", fontsize=14)
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative P&L ($)")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(filename, dpi=150)
    plt.close()
    print(f"  Saved P&L chart to {filename}")


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    print("Loading historical odds data...")
    matches = load_odds_files()
    print(f"  Loaded {len(matches):,} matches with odds data")
    if not matches:
        print("No matches found."); return

    print(f"  Date range: {matches[0]['date']} — {matches[-1]['date']}")
    atp = sum(1 for m in matches if m["tour"] == "ATP")
    wta = sum(1 for m in matches if m["tour"] == "WTA")
    print(f"  ATP: {atp:,}  |  WTA: {wta:,}")

    print("\nRunning V2 backtest...")
    results = run_backtest(matches)
    print_report(results)
    save_csv(results["bets"])
    save_chart(results["bets"])
    print("\nDone.")


if __name__ == "__main__":
    main()
