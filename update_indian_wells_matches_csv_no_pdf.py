#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup

DEFAULT_DRAWS_URL = "https://www.atptour.com/en/scores/current/indian-wells/404/draws"
DEFAULT_RESULTS_URL = "https://www.atptour.com/en/scores/current/indian-wells/404/results"
USER_AGENT = "Mozilla/5.0 (compatible; ATPDrawCSV/1.0)"
ROUND_HEADER_RE = re.compile(r"^(Round of (?:128|96|64|48|32|24|16)|Quarterfinals|Semifinals|Final)\b")
DAY_HEADER_RE = re.compile(r"^[A-Z][a-z]{2},\s+\d{2}\s+[A-Z][a-z]{2},\s+\d{4}\s+Day\s+\(\d+\)$")
SEED_RE = re.compile(r"^\((\d+)\)$")
SCORE_LINE_RE = re.compile(r"^[0-9()\s]+$")
ABBREV_NAME_RE = re.compile(r"^[A-Z]\.\s+.+$")
FULLISH_NAME_RE = re.compile(r"^[A-Z][A-Za-zÀ-ÖØ-öø-ÿ'’.-]+(?:\s+[A-Za-zÀ-ÖØ-öø-ÿ'’.-]+)+$")
IGNORE_LINES = {
    "Print",
    "Previous",
    "Next",
    "Versus",
    "Singles",
    "Doubles",
    "Qual Singles",
}
IGNORE_PREFIXES = (
    "Ump:",
    "Game Set and Match",
    "1st serve",
    "2nd serve",
    "Match point",
    "Break point",
    "Ace",
    "Double fault",
)
ROUND_KEY_ORDER = ["R128", "R96", "R64", "R48", "R32", "R24", "R16", "QF", "SF", "F"]


@dataclass
class MatchResult:
    round_key: str
    player1_raw: str = ""
    player2_raw: str = ""
    winner_raw: str = ""
    outcome_type: str = ""
    score_raw: str = ""
    p1_score_lines: list[str] = field(default_factory=list)
    p2_score_lines: list[str] = field(default_factory=list)


@dataclass
class MatchRow:
    round_label: str
    player_a: str
    player_b: str
    winner: str = ""
    participant_a_score: str = ""
    participant_b_score: str = ""


class ParseError(RuntimeError):
    pass


def fetch_html(url: str, session: requests.Session) -> str:
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def html_to_lines(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    out: list[str] = []
    for raw in text.splitlines():
        line = re.sub(r"\s+", " ", raw).strip()
        if not line:
            continue
        if line in IGNORE_LINES:
            continue
        out.append(line)
    return out


def round_header_to_key(line: str) -> str | None:
    m = ROUND_HEADER_RE.match(line)
    if not m:
        return None
    header = m.group(1)
    if header.startswith("Round of "):
        return f"R{header.split()[-1]}"
    if header == "Quarterfinals":
        return "QF"
    if header == "Semifinals":
        return "SF"
    if header == "Final":
        return "F"
    return None


def key_from_slots_count(slots_count: int) -> str:
    if slots_count in {128, 96, 64, 48, 32, 24, 16}:
        return f"R{slots_count}"
    if slots_count == 8:
        return "QF"
    if slots_count == 4:
        return "SF"
    if slots_count == 2:
        return "F"
    raise ValueError(f"Unsupported slots count: {slots_count}")


def label_for_round(round_index: int, total_rounds: int) -> str:
    remaining = total_rounds - round_index + 1
    if remaining == 4:
        return "Ottavi di finale"
    if remaining == 3:
        return "Quarti di finale"
    if remaining == 2:
        return "Semifinali"
    if remaining == 1:
        return "Finale"
    return f"{round_index}° turno"


def strip_seed(display_name: str) -> str:
    return re.sub(r"\s*\[\d+\]$", "", display_name).strip()


def normalize_ascii(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text


def tokenize_name(text: str) -> list[str]:
    text = normalize_ascii(text).lower()
    text = re.sub(r"\[[^\]]+\]", " ", text)
    text = text.replace("…", " ")
    text = re.sub(r"[^a-z0-9'’ .-]", " ", text)
    text = text.replace("’", "'")
    tokens = [t for t in re.split(r"[\s.-]+", text) if t]
    return tokens


def display_matches_full(display_name: str, full_name: str) -> bool:
    if display_name == "Bye":
        return full_name.lower() == "bye"
    d_tokens = tokenize_name(strip_seed(display_name))
    f_tokens = tokenize_name(full_name)
    if len(d_tokens) < 2 or len(f_tokens) < 2:
        return False
    display_initial = d_tokens[0][0]
    if f_tokens[0][0] != display_initial:
        return False
    display_tail = d_tokens[1:]
    if len(display_tail) > len(f_tokens) - 1:
        return False
    return f_tokens[-len(display_tail):] == display_tail


def parse_score_tokens(lines: Iterable[str]) -> list[int]:
    tokens: list[int] = []
    for line in lines:
        for token in re.findall(r"\d+", line):
            tokens.append(int(token))
    return tokens


def sets_won_from_score_lines(p1_lines: list[str], p2_lines: list[str]) -> tuple[int, int] | None:
    p1 = parse_score_tokens(p1_lines)
    p2 = parse_score_tokens(p2_lines)
    if not p1 or not p2:
        return None
    set_count = min(len(p1), len(p2), 5)
    if set_count <= 0:
        return None
    p1 = p1[:set_count]
    p2 = p2[:set_count]
    wins1 = wins2 = 0
    for a, b in zip(p1, p2):
        if a > b:
            wins1 += 1
        elif b > a:
            wins2 += 1
    if wins1 == wins2 == 0:
        return None
    return wins1, wins2


def sets_won_from_score_raw(score_raw: str) -> tuple[int, int] | None:
    score_raw = score_raw.strip()
    if not score_raw:
        return None
    wins1 = wins2 = 0
    pairs = re.findall(r"(\d+)-(\d+)(?:\([^)]*\))?", score_raw)
    if not pairs:
        return None
    for left, right in pairs:
        a = int(left)
        b = int(right)
        if a > b:
            wins1 += 1
        elif b > a:
            wins2 += 1
    if wins1 == wins2 == 0:
        return None
    return wins1, wins2


def looks_like_draw_player_line(line: str) -> bool:
    if line == "Bye":
        return True
    if not ABBREV_NAME_RE.match(line):
        return False
    if any(line.startswith(prefix) for prefix in IGNORE_PREFIXES):
        return False
    if "H2H" in line or "Stats" in line:
        return False
    return True


def looks_like_full_name_line(line: str) -> bool:
    if line == "Bye":
        return True
    if not FULLISH_NAME_RE.match(line):
        return False
    if line.startswith("ATP "):
        return False
    if DAY_HEADER_RE.match(line):
        return False
    if ROUND_HEADER_RE.match(line):
        return False
    if any(line.startswith(prefix) for prefix in IGNORE_PREFIXES):
        return False
    if "H2H" in line or "Stats" in line:
        return False
    return True


def attach_seed(base_name: str, maybe_seed_line: str | None) -> str:
    if not maybe_seed_line:
        return base_name
    m = SEED_RE.match(maybe_seed_line)
    if not m:
        return base_name
    return f"{base_name} [{m.group(1)}]"


def parse_draw_slots(lines: list[str], debug: bool = False) -> list[str]:
    start_idx = -1
    initial_key = None
    for i, line in enumerate(lines):
        key = round_header_to_key(line)
        if key and key in {"R128", "R96", "R64", "R48", "R32"}:
            start_idx = i
            initial_key = key
            break
    if start_idx < 0:
        raise ParseError("Could not find the first round on the draws page")

    slots: list[str] = []
    i = start_idx + 1
    while i < len(lines):
        line = lines[i]
        if i > start_idx + 1 and round_header_to_key(line):
            break
        if looks_like_draw_player_line(line):
            seed_line = lines[i + 1] if i + 1 < len(lines) else None
            slots.append(attach_seed(line, seed_line))
        i += 1

    expected_slots = int(initial_key[1:])
    if len(slots) < expected_slots:
        raise ParseError(f"Found only {len(slots)} draw slots on the draws page, expected at least {expected_slots}")
    if len(slots) > expected_slots:
        slots = slots[:expected_slots]

    if debug:
        print(f"[debug] initial draw key={initial_key} slots={len(slots)}", file=sys.stderr)
        print(f"[debug] first 12 slots={slots[:12]}", file=sys.stderr)
    return slots


def parse_results_blocks(lines: list[str], debug: bool = False) -> dict[str, list[MatchResult]]:
    by_round: dict[str, list[MatchResult]] = {}
    i = 0
    while i < len(lines):
        round_key = round_header_to_key(lines[i])
        if not round_key:
            i += 1
            continue
        block: list[str] = []
        j = i + 1
        while j < len(lines):
            line = lines[j]
            if round_header_to_key(line) or DAY_HEADER_RE.match(line):
                break
            block.append(line)
            j += 1
        result = parse_single_result_block(round_key, block)
        if result.player1_raw and result.player2_raw:
            by_round.setdefault(round_key, []).append(result)
        i = j
    if debug:
        summary = {k: len(v) for k, v in by_round.items()}
        print(f"[debug] parsed results per round={summary}", file=sys.stderr)
    return by_round


def parse_single_result_block(round_key: str, block_lines: list[str]) -> MatchResult:
    result = MatchResult(round_key=round_key)
    state = 0
    pending_seed_allowed = False
    for idx, line in enumerate(block_lines):
        if not line or line in IGNORE_LINES:
            continue
        if any(line.startswith(prefix) for prefix in IGNORE_PREFIXES):
            if line.startswith("Game Set and Match"):
                m = re.search(r"Game Set and Match\s+(.+?)\.\s+.+?wins the match\s+(.+?)\s*\.?", line)
                if m:
                    result.winner_raw = m.group(1).strip()
                    result.score_raw = m.group(2).strip()
            continue
        if line.startswith("Winner:"):
            winner_text = line[len("Winner:"):].strip()
            if "walkover" in winner_text.lower() or "w/o" in winner_text.lower():
                result.outcome_type = "W/O"
            winner_text = re.sub(r"\s+by\s+Walkover.*$", "", winner_text, flags=re.I).strip()
            result.winner_raw = winner_text
            continue
        if "H2H" in line or "Stats" in line:
            continue
        if SEED_RE.match(line):
            pending_seed_allowed = False
            continue
        if state == 1 and SCORE_LINE_RE.match(line):
            result.p1_score_lines.append(line)
            continue
        if state == 2 and SCORE_LINE_RE.match(line):
            result.p2_score_lines.append(line)
            continue
        if looks_like_full_name_line(line):
            if not result.player1_raw:
                result.player1_raw = line
                state = 1
                pending_seed_allowed = True
                continue
            if not result.player2_raw:
                result.player2_raw = line
                state = 2
                pending_seed_allowed = True
                continue
        pending_seed_allowed = False

    if not result.score_raw:
        # leave score_raw empty; sets can still be derived later from score lines
        pass
    return result


def find_matching_result(row: MatchRow, candidates: list[MatchResult], used: set[int]) -> MatchResult | None:
    matches: list[tuple[int, int]] = []
    for idx, candidate in enumerate(candidates):
        if idx in used:
            continue
        direct = display_matches_full(row.player_a, candidate.player1_raw) and display_matches_full(row.player_b, candidate.player2_raw)
        swapped = display_matches_full(row.player_a, candidate.player2_raw) and display_matches_full(row.player_b, candidate.player1_raw)
        if direct or swapped:
            score = 2 if direct else 1
            matches.append((score, idx))
    if not matches:
        return None
    matches.sort(reverse=True)
    chosen_idx = matches[0][1]
    used.add(chosen_idx)
    return candidates[chosen_idx]


def resolve_winner_display(row: MatchRow, result: MatchResult) -> str:
    if result.winner_raw:
        if display_matches_full(row.player_a, result.winner_raw):
            return row.player_a
        if display_matches_full(row.player_b, result.winner_raw):
            return row.player_b
    sets = sets_for_row_order(row, result)
    if sets:
        a_sets, b_sets = sets
        if a_sets > b_sets:
            return row.player_a
        if b_sets > a_sets:
            return row.player_b
    return ""


def sets_for_row_order(row: MatchRow, result: MatchResult) -> tuple[int, int] | None:
    if result.outcome_type == "W/O":
        return None

    raw_sets = sets_won_from_score_raw(result.score_raw)
    if raw_sets:
        winner_display = ""
        if result.winner_raw:
            if display_matches_full(row.player_a, result.winner_raw):
                winner_display = row.player_a
            elif display_matches_full(row.player_b, result.winner_raw):
                winner_display = row.player_b
        if winner_display == row.player_a:
            return raw_sets
        if winner_display == row.player_b:
            return (raw_sets[1], raw_sets[0])

    line_sets = sets_won_from_score_lines(result.p1_score_lines, result.p2_score_lines)
    if line_sets:
        row_a_is_p1 = display_matches_full(row.player_a, result.player1_raw)
        row_b_is_p2 = display_matches_full(row.player_b, result.player2_raw)
        if row_a_is_p1 and row_b_is_p2:
            return line_sets
        row_a_is_p2 = display_matches_full(row.player_a, result.player2_raw)
        row_b_is_p1 = display_matches_full(row.player_b, result.player1_raw)
        if row_a_is_p2 and row_b_is_p1:
            return (line_sets[1], line_sets[0])
    return None


def apply_results_to_rows(rows: list[MatchRow], round_results: list[MatchResult]) -> None:
    used: set[int] = set()
    for row in rows:
        if row.player_a == "TBD" or row.player_b == "TBD":
            continue
        if row.player_b == "Bye":
            row.winner = row.player_a
            continue
        if row.player_a == "Bye":
            row.winner = row.player_b
            continue
        result = find_matching_result(row, round_results, used)
        if not result:
            continue
        row.winner = resolve_winner_display(row, result)
        if result.outcome_type == "W/O":
            if row.winner == row.player_a:
                row.participant_a_score = "W/O"
                row.participant_b_score = ""
            elif row.winner == row.player_b:
                row.participant_a_score = ""
                row.participant_b_score = "W/O"
            continue
        sets = sets_for_row_order(row, result)
        if sets:
            row.participant_a_score = str(sets[0])
            row.participant_b_score = str(sets[1])


def build_rows(slots: list[str], results_by_round: dict[str, list[MatchResult]]) -> list[MatchRow]:
    all_rows: list[MatchRow] = []
    current_slots = slots[:]
    total_rounds = len(current_slots).bit_length() - 1
    round_index = 1
    while len(current_slots) >= 2:
        round_key = key_from_slots_count(len(current_slots))
        round_label = label_for_round(round_index, total_rounds)
        rows: list[MatchRow] = []
        for i in range(0, len(current_slots), 2):
            player_a = current_slots[i]
            player_b = current_slots[i + 1]
            rows.append(MatchRow(round_label=round_label, player_a=player_a, player_b=player_b))
        apply_results_to_rows(rows, results_by_round.get(round_key, []))
        all_rows.extend(rows)
        next_slots: list[str] = []
        for row in rows:
            if row.winner:
                next_slots.append(row.winner)
            elif row.player_a == "Bye":
                next_slots.append(row.player_b)
            elif row.player_b == "Bye":
                next_slots.append(row.player_a)
            else:
                next_slots.append("TBD")
        current_slots = next_slots
        round_index += 1
        if len(current_slots) == 1:
            break
    return all_rows


def write_csv(rows: list[MatchRow], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Round",
            "Player A",
            "Player B",
            "Winner",
            "Participant A score",
            "Participant B score",
        ])
        for row in rows:
            writer.writerow([
                row.round_label,
                row.player_a,
                row.player_b,
                row.winner,
                row.participant_a_score,
                row.participant_b_score,
            ])


def run_once(output: Path, draws_url: str, results_url: str, debug: bool = False) -> int:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    draws_html = fetch_html(draws_url, session)
    results_html = fetch_html(results_url, session)
    draw_lines = html_to_lines(draws_html)
    result_lines = html_to_lines(results_html)

    slots = parse_draw_slots(draw_lines, debug=debug)
    results_by_round = parse_results_blocks(result_lines, debug=debug)
    rows = build_rows(slots, results_by_round)
    write_csv(rows, output)
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ATP tournament match CSV without using the draw PDF")
    parser.add_argument("--output", required=True, help="Output CSV path")
    parser.add_argument("--draws-url", default=DEFAULT_DRAWS_URL, help="ATP draws page URL")
    parser.add_argument("--results-url", default=DEFAULT_RESULTS_URL, help="ATP results page URL")
    parser.add_argument("--debug", action="store_true", help="Print parser diagnostics to stderr")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        return run_once(Path(args.output), args.draws_url, args.results_url, debug=args.debug)
    except Exception as exc:  # pragma: no cover
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

