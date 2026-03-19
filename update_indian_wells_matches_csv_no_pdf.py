import argparse
import csv
import re
import sys
from typing import List, Dict, Tuple
import requests
from bs4 import BeautifulSoup


DRAW_URL = "https://www.atptour.com/en/scores/current/indian-wells/404/draws"
RESULTS_URL = "https://www.atptour.com/en/scores/current/indian-wells/404/results"


# =========================
# NAME FORMATTING
# =========================

def format_name(name: str) -> str:
    if not name:
        return name

    name = name.strip()

    # Fix McDonald
    if name.upper().startswith("MCDONALD"):
        return "M. McDonald"

    parts = name.split()

    if len(parts) == 1:
        return name

    first = parts[0]
    last = " ".join(parts[1:])

    # fix van de etc
    last = last.replace("Van ", "van ")

    return f"{first[0]}. {last}"


# =========================
# SCORE PARSING (FIXED)
# =========================

def count_sets_won_from_score(score_raw: str) -> Tuple[int, int]:
    if not score_raw:
        return 0, 0

    # remove tie-breaks
    cleaned = re.sub(r"\(\d+\)", "", score_raw)

    cleaned = cleaned.replace("–", "-").replace("—", "-")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    sets_a = 0
    sets_b = 0

    for token in cleaned.split():
        m = re.fullmatch(r"(\d+)-(\d+)", token)
        if not m:
            continue

        a = int(m.group(1))
        b = int(m.group(2))

        if a > b:
            sets_a += 1
        elif b > a:
            sets_b += 1

    return sets_a, sets_b


# =========================
# FETCH DRAW (FIRST ROUND)
# =========================

def fetch_draw(debug=False) -> List[str]:
    r = requests.get(DRAW_URL)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    text = soup.get_text("\n")
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    players = []

    capture = False
    for line in lines:
        if "Round of" in line:
            capture = True
            continue

        if capture:
            if re.match(r"Round of \d+", line):
                break

            if re.match(r"[A-Z]\.", line) or "Bye" in line:
                players.append(line)

    if debug:
        print("[debug] first 12 slots:", players[:12])

    return players


# =========================
# FETCH RESULTS
# =========================

def fetch_results(debug=False) -> List[Dict]:
    r = requests.get(RESULTS_URL)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    text = soup.get_text("\n")
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    results = []

    current_round = None
    buffer = []

    for line in lines:
        if "Round of" in line or line in ["Semifinals", "Final"]:
            current_round = line
            continue

        buffer.append(line)

        if "wins the match" in line:
            joined = " ".join(buffer)

            m = re.search(
                r"Game Set and Match (.+?)\..+?wins the match (.+?)\.",
                joined,
            )

            if m:
                winner = format_name(m.group(1))
                score = m.group(2)

                results.append({
                    "round": current_round,
                    "winner": winner,
                    "score": score,
                })

            buffer = []

    if debug:
        print("[debug] results parsed:", len(results))

    return results


# =========================
# BUILD MATCHES
# =========================

def build_matches(players: List[str], results: List[Dict]) -> List[Dict]:
    matches = []

    for i in range(0, len(players), 2):
        a = players[i]
        b = players[i + 1] if i + 1 < len(players) else "Bye"

        player_a = format_name(a)
        player_b = format_name(b)

        winner = ""
        score_a = ""
        score_b = ""

        if "Bye" in b:
            winner = player_a

        matches.append({
            "Round": "1° turno",
            "Player A": player_a,
            "Player B": player_b,
            "Winner": winner,
            "Score A": score_a,
            "Score B": score_b,
        })

    return matches


# =========================
# CSV
# =========================

def write_csv(rows: List[Dict], output: str):
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


# =========================
# MAIN
# =========================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    players = fetch_draw(debug=args.debug)
    results = fetch_results(debug=args.debug)

    rows = build_matches(players, results)

    write_csv(rows, args.output)


if __name__ == "__main__":
    main()
