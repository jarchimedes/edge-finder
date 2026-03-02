#!/usr/bin/env python3
"""
backtest.py — Backtest the Elo-based tennis betting model against historical odds.
Uses tennis-data.co.uk historical data (2010-2025) with Pinnacle closing odds.

Model: Elo ratings (K=32, start 1500), blended 70% surface / 30% overall.
Bet trigger: edge > 2pp (model prob - implied market prob).
Stake: flat $100 per bet.
"""

import glob
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime

DB_PATH = "tennis.db"
ODDS_DIR = "odds_data"
K = 32
START_ELO = 1500
EDGE_THRESHOLD = 0.02  # 2 percentage points
STAKE = 100.0

# ── Load odds data ──────────────────────────────────────────────────────────

def load_odds_files():
    """Load all ATP and WTA odds files into a list of match dicts."""
    matches = []

    # Try openpyxl for xlsx, xlrd for xls
    try:
        import openpyxl
    except ImportError:
        print("Need openpyxl: pip install openpyxl")
        sys.exit(1)
    try:
        import xlrd
    except ImportError:
        print("Need xlrd: pip install xlrd")
        sys.exit(1)

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
            if not rows:
                continue
            headers = [str(h).strip() if h else "" for h in rows[0]]
            data_rows = rows[1:]
        else:
            wb = xlrd.open_workbook(fpath)
            ws = wb.sheet_by_index(0)
            headers = [str(ws.cell_value(0, c)).strip() for c in range(ws.ncols)]
            data_rows = []
            for r in range(1, ws.nrows):
                data_rows.append([ws.cell_value(r, c) for c in range(ws.ncols)])

        col = {h: i for i, h in enumerate(headers)}

        # Required columns
        required = ["Winner", "Loser", "Surface"]
        if not all(r in col for r in required):
            print(f"  Skipping {fname}: missing columns {[r for r in required if r not in col]}")
            continue

        for row in data_rows:
            try:
                winner = row[col["Winner"]]
                loser = row[col["Loser"]]
                surface = row[col["Surface"]]
                
                if not winner or not loser or not surface:
                    continue

                # Parse date
                date_val = row[col["Date"]] if "Date" in col else None
                if date_val is None:
                    continue
                
                if isinstance(date_val, datetime):
                    date_str = date_val.strftime("%Y%m%d")
                elif isinstance(date_val, str):
                    # Try various formats
                    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
                        try:
                            date_str = datetime.strptime(date_val.strip(), fmt).strftime("%Y%m%d")
                            break
                        except ValueError:
                            continue
                    else:
                        continue
                elif isinstance(date_val, (int, float)):
                    # Excel date serial or xlrd float
                    if fpath.endswith(".xls"):
                        try:
                            dt = xlrd.xldate_as_datetime(date_val, wb.datemode)
                            date_str = dt.strftime("%Y%m%d")
                        except Exception:
                            continue
                    else:
                        continue
                else:
                    continue

                # Get odds — prefer Pinnacle, fallback to Bet365, then Avg
                psw = _safe_float(row[col["PSW"]]) if "PSW" in col else None
                psl = _safe_float(row[col["PSL"]]) if "PSL" in col else None
                b365w = _safe_float(row[col["B365W"]]) if "B365W" in col else None
                b365l = _safe_float(row[col["B365L"]]) if "B365L" in col else None
                avgw = _safe_float(row[col["AvgW"]]) if "AvgW" in col else None
                avgl = _safe_float(row[col["AvgL"]]) if "AvgL" in col else None

                winner_odds = psw or b365w or avgw
                loser_odds = psl or b365l or avgl

                if not winner_odds or not loser_odds:
                    continue
                if winner_odds <= 1.0 or loser_odds <= 1.0:
                    continue

                # Get round and tournament
                rnd = str(row[col["Round"]]) if "Round" in col else ""
                tourney = str(row[col.get("Tournament", col.get("Location", 0))]) if "Tournament" in col or "Location" in col else ""
                comment = str(row[col["Comment"]]) if "Comment" in col else "Completed"

                matches.append({
                    "date": date_str,
                    "tour": tour,
                    "tournament": tourney,
                    "surface": str(surface).strip(),
                    "round": rnd,
                    "winner": str(winner).strip(),
                    "loser": str(loser).strip(),
                    "winner_odds": winner_odds,
                    "loser_odds": loser_odds,
                    "comment": comment,
                })

            except (IndexError, TypeError, ValueError):
                continue

    # Sort by date
    matches.sort(key=lambda m: m["date"])
    return matches


def _safe_float(val):
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if f > 0 else None
    except (ValueError, TypeError):
        return None


# ── Elo engine ──────────────────────────────────────────────────────────────

class EloEngine:
    def __init__(self, k=K, start=START_ELO):
        self.k = k
        self.start = start
        self.ratings = {}  # name -> {"overall": float, "Hard": float, "Clay": float, "Grass": float}

    def _ensure(self, name):
        if name not in self.ratings:
            self.ratings[name] = {
                "overall": self.start,
                "Hard": self.start,
                "Clay": self.start,
                "Grass": self.start,
            }

    def get(self, name, surface=None):
        self._ensure(name)
        if surface and surface in self.ratings[name]:
            return self.ratings[name][surface]
        return self.ratings[name]["overall"]

    def win_prob(self, name_a, name_b, surface=None):
        """Blended win probability: 70% surface, 30% overall."""
        self._ensure(name_a)
        self._ensure(name_b)

        elo_a_ov = self.ratings[name_a]["overall"]
        elo_b_ov = self.ratings[name_b]["overall"]
        prob_overall = 1.0 / (1.0 + 10 ** ((elo_b_ov - elo_a_ov) / 400.0))

        if surface and surface in ("Hard", "Clay", "Grass"):
            elo_a_s = self.ratings[name_a][surface]
            elo_b_s = self.ratings[name_b][surface]
            prob_surface = 1.0 / (1.0 + 10 ** ((elo_b_s - elo_a_s) / 400.0))
            return 0.7 * prob_surface + 0.3 * prob_overall
        return prob_overall

    def update(self, winner, loser, surface=None):
        """Update ratings after a match result."""
        self._ensure(winner)
        self._ensure(loser)

        # Overall
        w_ov = self.ratings[winner]["overall"]
        l_ov = self.ratings[loser]["overall"]
        exp_w = 1.0 / (1.0 + 10 ** ((l_ov - w_ov) / 400.0))
        self.ratings[winner]["overall"] = w_ov + self.k * (1 - exp_w)
        self.ratings[loser]["overall"] = l_ov + self.k * (0 - (1 - exp_w))

        # Surface
        s = surface if surface in ("Hard", "Clay", "Grass") else None
        if s:
            w_s = self.ratings[winner][s]
            l_s = self.ratings[loser][s]
            exp_ws = 1.0 / (1.0 + 10 ** ((l_s - w_s) / 400.0))
            self.ratings[winner][s] = w_s + self.k * (1 - exp_ws)
            self.ratings[loser][s] = l_s + self.k * (0 - (1 - exp_ws))


# ── Backtest ────────────────────────────────────────────────────────────────

def run_backtest(matches, edge_threshold=EDGE_THRESHOLD, stake=STAKE, warmup_matches=2000):
    """
    Walk through matches chronologically.
    First `warmup_matches` are used to build initial Elo ratings (no betting).
    After warmup, simulate bets where model edge > threshold.
    """
    elo = EloEngine()
    
    bets = []
    total_pnl = 0.0
    peak_pnl = 0.0
    max_drawdown = 0.0
    yearly_stats = defaultdict(lambda: {"bets": 0, "wins": 0, "pnl": 0.0})
    
    skipped_retirements = 0
    matches_processed = 0

    for i, m in enumerate(matches):
        surface = m["surface"]
        if surface == "Carpet":
            surface = "Hard"
        
        winner = m["winner"]
        loser = m["loser"]

        # Skip retirements and walkovers for betting (unreliable)
        comment = m.get("comment", "Completed")
        if comment and "Completed" not in str(comment):
            skipped_retirements += 1
            # Still update Elo for completed portion
            elo.update(winner, loser, surface)
            matches_processed += 1
            continue

        # After warmup, evaluate bets
        if i >= warmup_matches:
            # Model probability for each player
            prob_winner = elo.win_prob(winner, loser, surface)
            prob_loser = 1.0 - prob_winner

            # Market implied probabilities
            implied_winner = 1.0 / m["winner_odds"]
            implied_loser = 1.0 / m["loser_odds"]

            year = m["date"][:4]

            # Check edge on winner side
            edge_w = prob_winner - implied_winner
            if edge_w > edge_threshold:
                # Bet on winner — they won, so this bet wins
                profit = stake * (m["winner_odds"] - 1)
                total_pnl += profit
                peak_pnl = max(peak_pnl, total_pnl)
                dd = peak_pnl - total_pnl
                max_drawdown = max(max_drawdown, dd)
                
                bets.append({
                    "date": m["date"], "tour": m["tour"], "tournament": m["tournament"],
                    "surface": surface, "bet_on": winner, "opponent": loser,
                    "model_prob": prob_winner, "implied_prob": implied_winner,
                    "edge": edge_w, "odds": m["winner_odds"],
                    "result": "W", "profit": profit, "cumulative_pnl": total_pnl,
                })
                yearly_stats[year]["bets"] += 1
                yearly_stats[year]["wins"] += 1
                yearly_stats[year]["pnl"] += profit

            # Check edge on loser side
            edge_l = prob_loser - implied_loser
            if edge_l > edge_threshold:
                # Bet on loser — they lost, so this bet loses
                profit = -stake
                total_pnl += profit
                dd = peak_pnl - total_pnl
                max_drawdown = max(max_drawdown, dd)
                
                bets.append({
                    "date": m["date"], "tour": m["tour"], "tournament": m["tournament"],
                    "surface": surface, "bet_on": loser, "opponent": winner,
                    "model_prob": prob_loser, "implied_prob": implied_loser,
                    "edge": edge_l, "odds": m["loser_odds"],
                    "result": "L", "profit": profit, "cumulative_pnl": total_pnl,
                })
                yearly_stats[year]["bets"] += 1
                yearly_stats[year]["pnl"] += profit

        # Always update Elo after evaluating (important: predict BEFORE updating)
        elo.update(winner, loser, surface)
        matches_processed += 1

    return {
        "bets": bets,
        "total_pnl": total_pnl,
        "peak_pnl": peak_pnl,
        "max_drawdown": max_drawdown,
        "yearly_stats": dict(yearly_stats),
        "matches_processed": matches_processed,
        "warmup_matches": warmup_matches,
        "skipped_retirements": skipped_retirements,
    }


# ── Reporting ───────────────────────────────────────────────────────────────

def print_report(results):
    bets = results["bets"]
    total_bets = len(bets)
    
    if total_bets == 0:
        print("\nNo bets triggered. Try lowering edge threshold.")
        return

    wins = sum(1 for b in bets if b["result"] == "W")
    losses = total_bets - wins
    total_wagered = total_bets * STAKE
    roi = (results["total_pnl"] / total_wagered) * 100

    print("\n" + "=" * 70)
    print("  BACKTEST RESULTS — ELO TENNIS MODEL")
    print("=" * 70)
    print(f"\n  Config:")
    print(f"    K-factor:        {K}")
    print(f"    Blend:           70% surface / 30% overall")
    print(f"    Edge threshold:  {EDGE_THRESHOLD * 100:.1f}pp")
    print(f"    Stake:           ${STAKE:.0f} flat")
    print(f"    Warmup matches:  {results['warmup_matches']}")
    print(f"    Retirements skipped: {results['skipped_retirements']}")

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

    # Yearly breakdown
    print(f"\n  {'Year':<6} {'Bets':>6} {'Wins':>6} {'Win%':>7} {'P&L':>10} {'ROI':>8}")
    print("  " + "-" * 45)
    for year in sorted(results["yearly_stats"].keys()):
        ys = results["yearly_stats"][year]
        yr_roi = (ys["pnl"] / (ys["bets"] * STAKE)) * 100 if ys["bets"] > 0 else 0
        win_pct = (ys["wins"] / ys["bets"]) * 100 if ys["bets"] > 0 else 0
        print(f"  {year:<6} {ys['bets']:>6} {ys['wins']:>6} {win_pct:>6.1f}% ${ys['pnl']:>+9,.0f} {yr_roi:>+7.2f}%")

    # Edge bucket analysis
    print(f"\n  Edge Bucket Analysis:")
    print(f"  {'Edge Range':<15} {'Bets':>6} {'Wins':>6} {'Win%':>7} {'P&L':>10} {'ROI':>8}")
    print("  " + "-" * 55)
    
    buckets = [(0.02, 0.05), (0.05, 0.10), (0.10, 0.15), (0.15, 0.20), (0.20, 1.0)]
    for lo, hi in buckets:
        bucket_bets = [b for b in bets if lo <= b["edge"] < hi]
        if not bucket_bets:
            continue
        bw = sum(1 for b in bucket_bets if b["result"] == "W")
        bp = sum(b["profit"] for b in bucket_bets)
        br = (bp / (len(bucket_bets) * STAKE)) * 100
        bwp = (bw / len(bucket_bets)) * 100
        label = f"{lo*100:.0f}-{hi*100:.0f}pp"
        print(f"  {label:<15} {len(bucket_bets):>6} {bw:>6} {bwp:>6.1f}% ${bp:>+9,.0f} {br:>+7.2f}%")


def save_csv(bets, filename="backtest_results.csv"):
    """Save all bets to CSV."""
    import csv
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "date", "tour", "tournament", "surface", "bet_on", "opponent",
            "model_prob", "implied_prob", "edge", "odds", "result", "profit", "cumulative_pnl"
        ])
        writer.writeheader()
        for b in bets:
            row = dict(b)
            row["model_prob"] = f"{b['model_prob']:.4f}"
            row["implied_prob"] = f"{b['implied_prob']:.4f}"
            row["edge"] = f"{b['edge']:.4f}"
            row["odds"] = f"{b['odds']:.3f}"
            row["profit"] = f"{b['profit']:.2f}"
            row["cumulative_pnl"] = f"{b['cumulative_pnl']:.2f}"
            writer.writerow(row)
    print(f"\n  Saved {len(bets)} bets to {filename}")


def save_chart(bets, filename="backtest_pnl.png"):
    """Generate cumulative P&L chart."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("  matplotlib not installed — skipping chart. pip install matplotlib")
        return

    dates = []
    pnls = []
    for b in bets:
        d = b["date"]
        dates.append(datetime.strptime(d, "%Y%m%d"))
        pnls.append(b["cumulative_pnl"])

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(dates, pnls, linewidth=0.8, color="#2196F3")
    ax.axhline(y=0, color="gray", linestyle="--", linewidth=0.5)
    ax.fill_between(dates, pnls, 0, alpha=0.15, color="#2196F3")
    ax.set_title(f"Elo Tennis Model — Cumulative P&L (${STAKE:.0f} flat, >{EDGE_THRESHOLD*100:.0f}pp edge)", fontsize=14)
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
        print("No matches found. Check odds_data/ directory.")
        return

    # Show date range
    print(f"  Date range: {matches[0]['date']} — {matches[-1]['date']}")
    
    # Tour breakdown
    atp = sum(1 for m in matches if m["tour"] == "ATP")
    wta = sum(1 for m in matches if m["tour"] == "WTA")
    print(f"  ATP: {atp:,}  |  WTA: {wta:,}")

    print("\nRunning backtest...")
    results = run_backtest(matches)

    print_report(results)
    save_csv(results["bets"])
    save_chart(results["bets"])

    print("\nDone.")


if __name__ == "__main__":
    main()
