import csv
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Tag

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    )
}

DRAW_TO_RESULTS_ROUND = {
    "R128": "Round of 128",
    "R64": "Round of 64",
    "R32": "Round of 32",
    "R16": "Round of 16",
    "QF": "Quarterfinals",
    "SF": "Semifinals",
    "F": "Final",
}

ROUND_CODES_IN_ORDER = ["R128", "R64", "R32", "R16", "QF", "SF", "F"]

ROUND_NAME_COUNTS = {
    "R128": 128,
    "R64": 64,
    "R32": 32,
    "R16": 16,
    "QF": 8,
    "SF": 4,
    "F": 2,
}

SPECIAL_COUNTRY_CODES = {"JPN", "CHN", "KOR", "TPE", "HKG"}
SPECIAL_NAME_EXCEPTION = {"n. osaka"}

INLINE_LABEL_PATTERN = re.compile(r"^(.*?)(?:\s*\((\d{1,2}|Q|WC|LL|Alt|PR)\))?$", re.I)


@dataclass
class PlayerRow:
    display_name: str
    score_values: List[int]
    has_ret: bool
    has_wo: bool
    winner_marker: bool


@dataclass
class MatchRow:
    round_code: str
    player_a: str
    player_b: str
    winner: str
    participant_a_score: str
    participant_b_score: str


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_special_slot(base_name: str) -> str:
    lowered = base_name.strip().lower()

    if lowered == "bye":
        return "bye"
    if lowered == "tba":
        return ""
    if lowered == "qualifier":
        return "Qualifier"
    if lowered == "qualifier / lucky loser":
        return "Qualifier / Lucky Loser"
    if lowered == "lucky loser":
        return "Lucky Loser"

    return base_name.strip()


def clean_name_and_label(raw_name_text: str) -> Tuple[str, Optional[str]]:
    text = normalize_space(raw_name_text)
    match = INLINE_LABEL_PATTERN.match(text)
    if not match:
        return text, None

    base_name = normalize_space(match.group(1))
    label = match.group(2)
    return base_name, label


def extract_country_code(stats_item: Tag) -> Optional[str]:
    country_div = stats_item.select_one("div.country")
    if not country_div:
        return None

    href = ""
    a = country_div.select_one("a[href]")
    if a:
        href = a.get("href", "") or ""
    else:
        href = country_div.get("href", "") or ""

    match = re.search(r"([A-Z]{3})(?:/)?$", href)
    if match:
        return match.group(1)

    return None


def invert_name_for_special_country(name: str, country_code: Optional[str]) -> str:
    if not country_code or country_code not in SPECIAL_COUNTRY_CODES:
        return name

    if name.strip().lower() in SPECIAL_NAME_EXCEPTION:
        return name

    parts = name.split()
    if len(parts) != 2:
        return name

    first, last = parts
    if not first.endswith("."):
        return name

    return f"{last} {first}"


def build_display_name(stats_item: Tag) -> Optional[str]:
    name_div = stats_item.select_one("div.name")
    if not name_div:
        return None

    raw_name_text = normalize_space(name_div.get_text(" ", strip=True))
    if not raw_name_text:
        return None

    base_name, inline_label = clean_name_and_label(raw_name_text)
    normalized = normalize_special_slot(base_name)

    if normalized == "":
        return ""

    if normalized in {"bye", "Qualifier", "Qualifier / Lucky Loser", "Lucky Loser"}:
        return normalized

    country_code = extract_country_code(stats_item)
    normalized = invert_name_for_special_country(normalized, country_code)

    if inline_label:
        normalized = f"{normalized} [{inline_label}]"

    return normalized


def detect_first_round_code(draw_html: str) -> str:
    for code in ("R128", "R64", "R32"):
        long_label = DRAW_TO_RESULTS_ROUND[code]
        if long_label in draw_html or f">{code}<" in draw_html:
            return code
    raise ValueError("Impossibile determinare il primo turno dal draw.")


def slice_draw_html_for_round(draw_html: str, round_code: str) -> str:
    start_label = DRAW_TO_RESULTS_ROUND[round_code]
    start = draw_html.find(start_label)
    if start == -1:
        start = draw_html.find(round_code)
    if start == -1:
        raise ValueError(f"Round {round_code} non trovato nel draw HTML.")

    end = len(draw_html)
    start_idx = ROUND_CODES_IN_ORDER.index(round_code)

    for next_code in ROUND_CODES_IN_ORDER[start_idx + 1:]:
        next_label = DRAW_TO_RESULTS_ROUND[next_code]
        next_pos = draw_html.find(next_label, start + len(start_label))
        if next_pos != -1:
            end = min(end, next_pos)
            break

    return draw_html[start:end]


def extract_score_values_from_stats_item(stats_item: Tag, raw_name_text: str) -> Tuple[List[int], bool, bool]:
    item_text = normalize_space(stats_item.get_text(" ", strip=True))

    name_div = stats_item.select_one("div.name")
    if name_div:
        item_text = item_text.replace(normalize_space(name_div.get_text(" ", strip=True)), " ")

    country_div = stats_item.select_one("div.country")
    if country_div:
        item_text = item_text.replace(normalize_space(country_div.get_text(" ", strip=True)), " ")

    # Rimuove il nome grezzo se ancora presente
    item_text = item_text.replace(raw_name_text, " ")
    item_text = normalize_space(item_text)

    upper_text = item_text.upper()
    has_ret = "RET" in upper_text
    has_wo = "W/O" in upper_text or "WALKOVER" in upper_text

    score_values: List[int] = []

    for score_item in stats_item.select("div.score-item"):
        score_text = normalize_space(score_item.get_text(" ", strip=True))

        # prende tutti i numeri presenti nello score-item
        numbers = re.findall(r"\d+", score_text)

        if not numbers:
            continue

        # prende solo il primo numero:
        # es. "7 4" -> 7   (4 è il tie-break e viene ignorato)
        score_values.append(int(numbers[0]))

    return score_values, has_ret, has_wo


def stats_item_has_winner_marker(stats_item: Tag) -> bool:
    html = str(stats_item).lower()
    return any(
        marker in html
        for marker in [
            "icon-checkmark",
            "checkmark",
            "is-winner",
            "winner",
            "selected",
        ]
    )


def build_player_row(stats_item: Tag) -> Optional[PlayerRow]:
    name_div = stats_item.select_one("div.name")
    if not name_div:
        return None

    raw_name_text = normalize_space(name_div.get_text(" ", strip=True))
    display_name = build_display_name(stats_item)
    if display_name is None:
        return None

    score_values, has_ret, has_wo = extract_score_values_from_stats_item(stats_item, raw_name_text)
    winner_marker = stats_item_has_winner_marker(stats_item)

    return PlayerRow(
        display_name=display_name,
        score_values=score_values,
        has_ret=has_ret,
        has_wo=has_wo,
        winner_marker=winner_marker,
    )


def extract_round_player_rows(draw_html: str, round_code: str) -> List[PlayerRow]:
    expected_count = ROUND_NAME_COUNTS[round_code]

    try:
        round_fragment = slice_draw_html_for_round(draw_html, round_code)
    except ValueError:
        return []

    soup = BeautifulSoup(round_fragment, "html.parser")

    rows: List[PlayerRow] = []

    for stats_item in soup.select("div.stats-item"):
        player_row = build_player_row(stats_item)
        if player_row is None:
            continue

        if player_row.display_name == "":
            continue

        rows.append(player_row)

        if len(rows) == expected_count:
            break

    # Se il round non contiene abbastanza giocatori/match, lo consideriamo non disponibile.
    # In questo caso il CSV deve rimanere vuoto per quel turno, senza alzare errori.
    if len(rows) != expected_count:
        return []

    return rows


def is_complete_set(a_games: int, b_games: int) -> bool:
    if a_games == 7 or b_games == 7:
        return True
    if a_games == 6 and b_games < 5:
        return True
    if b_games == 6 and a_games < 5:
        return True
    return False


def count_complete_sets(a_scores: List[int], b_scores: List[int]) -> Tuple[int, int]:
    a_sets = 0
    b_sets = 0

    for a_games, b_games in zip(a_scores, b_scores):
        if not is_complete_set(a_games, b_games):
            continue
        if a_games > b_games:
            a_sets += 1
        elif b_games > a_games:
            b_sets += 1

    return a_sets, b_sets

def has_incomplete_final_set(a_scores: List[int], b_scores: List[int]) -> bool:
    if not a_scores or not b_scores:
        return False

    if len(a_scores) != len(b_scores):
        return False

    last_a = a_scores[-1]
    last_b = b_scores[-1]

    return not is_complete_set(last_a, last_b)

def determine_winner(
    player_a: PlayerRow,
    player_b: PlayerRow,
    a_sets: int,
    b_sets: int,
) -> str:
    if player_a.display_name == "bye" and player_b.display_name != "bye":
        return player_b.display_name
    if player_b.display_name == "bye" and player_a.display_name != "bye":
        return player_a.display_name

    if player_a.winner_marker and not player_b.winner_marker:
        return player_a.display_name
    if player_b.winner_marker and not player_a.winner_marker:
        return player_b.display_name

    if a_sets > b_sets:
        return player_a.display_name
    if b_sets > a_sets:
        return player_b.display_name

    # Walkover senza punteggi: prova a capire dal marker, altrimenti lascia vuoto.
    if player_a.has_wo and not player_b.has_wo:
        return player_a.display_name
    if player_b.has_wo and not player_a.has_wo:
        return player_b.display_name

    return ""


def build_match_row_from_pair(round_code: str, player_a: PlayerRow, player_b: PlayerRow) -> MatchRow:
    # bye
    if player_a.display_name == "bye" and player_b.display_name != "bye":
        return MatchRow(
            round_code=round_code,
            player_a=player_a.display_name,
            player_b=player_b.display_name,
            winner=player_b.display_name,
            participant_a_score="",
            participant_b_score="",
        )

    if player_b.display_name == "bye" and player_a.display_name != "bye":
        return MatchRow(
            round_code=round_code,
            player_a=player_a.display_name,
            player_b=player_b.display_name,
            winner=player_a.display_name,
            participant_a_score="",
            participant_b_score="",
        )

    # walkover senza game
    if (player_a.has_wo or player_b.has_wo) and not player_a.score_values and not player_b.score_values:
        if player_a.winner_marker and not player_b.winner_marker:
            return MatchRow(
                round_code=round_code,
                player_a=player_a.display_name,
                player_b=player_b.display_name,
                winner=player_a.display_name,
                participant_a_score="W/O",
                participant_b_score="",
            )
        if player_b.winner_marker and not player_a.winner_marker:
            return MatchRow(
                round_code=round_code,
                player_a=player_a.display_name,
                player_b=player_b.display_name,
                winner=player_b.display_name,
                participant_a_score="",
                participant_b_score="W/O",
            )

    a_sets, b_sets = count_complete_sets(player_a.score_values, player_b.score_values)
    winner = determine_winner(player_a, player_b, a_sets, b_sets)

    # ritiro dedotto:
    # ultimo set incompleto + winner marker
    if (
        player_a.display_name != "bye"
        and player_b.display_name != "bye"
        and has_incomplete_final_set(player_a.score_values, player_b.score_values)
    ):
        if player_a.winner_marker and not player_b.winner_marker:
            return MatchRow(
                round_code=round_code,
                player_a=player_a.display_name,
                player_b=player_b.display_name,
                winner=player_a.display_name,
                participant_a_score=str(a_sets),
                participant_b_score=f"(rit.) {b_sets}",
            )
        if player_b.winner_marker and not player_a.winner_marker:
            return MatchRow(
                round_code=round_code,
                player_a=player_a.display_name,
                player_b=player_b.display_name,
                winner=player_b.display_name,
                participant_a_score=f"(rit.) {a_sets}",
                participant_b_score=str(b_sets),
            )

    # ritiro con label esplicita RET nel testo
    has_ret = player_a.has_ret or player_b.has_ret
    if has_ret:
        if winner == player_a.display_name:
            return MatchRow(
                round_code=round_code,
                player_a=player_a.display_name,
                player_b=player_b.display_name,
                winner=winner,
                participant_a_score=str(a_sets),
                participant_b_score=f"(rit.) {b_sets}",
            )
        if winner == player_b.display_name:
            return MatchRow(
                round_code=round_code,
                player_a=player_a.display_name,
                player_b=player_b.display_name,
                winner=winner,
                participant_a_score=f"(rit.) {a_sets}",
                participant_b_score=str(b_sets),
            )

    # walkover anche senza marker esplicito: se non ci sono punteggi completi ma c'è un winner
    if (player_a.has_wo or player_b.has_wo) and winner:
        if winner == player_a.display_name:
            return MatchRow(
                round_code=round_code,
                player_a=player_a.display_name,
                player_b=player_b.display_name,
                winner=winner,
                participant_a_score="W/O",
                participant_b_score="",
            )
        return MatchRow(
            round_code=round_code,
            player_a=player_a.display_name,
            player_b=player_b.display_name,
            winner=winner,
            participant_a_score="",
            participant_b_score="W/O",
        )

    return MatchRow(
        round_code=round_code,
        player_a=player_a.display_name,
        player_b=player_b.display_name,
        winner=winner,
        participant_a_score=str(a_sets) if winner else "",
        participant_b_score=str(b_sets) if winner else "",
    )


def build_round_rows_from_draw(draw_html: str, round_code: str) -> List[MatchRow]:
    player_rows = extract_round_player_rows(draw_html, round_code)

    expected_matches = ROUND_NAME_COUNTS[round_code] // 2
    if not player_rows:
        return [
            MatchRow(
                round_code=round_code,
                player_a="",
                player_b="",
                winner="",
                participant_a_score="",
                participant_b_score="",
            )
            for _ in range(expected_matches)
        ]

    match_rows: List[MatchRow] = []
    for i in range(0, len(player_rows), 2):
        match_rows.append(build_match_row_from_pair(round_code, player_rows[i], player_rows[i + 1]))

    return match_rows


def available_round_codes(draw_html: str, first_round_code: str) -> List[str]:
    start_idx = ROUND_CODES_IN_ORDER.index(first_round_code)
    rounds: List[str] = []

    for code in ROUND_CODES_IN_ORDER[start_idx:]:
        label = DRAW_TO_RESULTS_ROUND[code]
        if label in draw_html or f">{code}<" in draw_html:
            rounds.append(code)

    return rounds


def export_csv(rows: List[MatchRow], output_path: str) -> None:
    with open(output_path, "w", newline="", encoding="utf-8") as f:
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
                row.round_code,
                row.player_a,
                row.player_b,
                row.winner,
                row.participant_a_score,
                row.participant_b_score,
            ])


def propagate_winners_to_next_round(round_rows_map: dict[str, List[MatchRow]], rounds: List[str]) -> None:
    for prev_round_code, next_round_code in zip(rounds, rounds[1:]):
        prev_rows = round_rows_map.get(prev_round_code, [])
        next_rows = round_rows_map.get(next_round_code, [])

        if not prev_rows or not next_rows:
            continue

        propagated_winners = [row.winner for row in prev_rows]

        for match_idx, next_row in enumerate(next_rows):
            winner_a_idx = match_idx * 2
            winner_b_idx = winner_a_idx + 1

            next_row.player_a = propagated_winners[winner_a_idx] if winner_a_idx < len(propagated_winners) else ""
            next_row.player_b = propagated_winners[winner_b_idx] if winner_b_idx < len(propagated_winners) else ""


def build_full_tournament_csv_from_draw(draw_url: str, output_csv: str) -> None:
    draw_html = fetch_html(draw_url)
    first_round_code = detect_first_round_code(draw_html)
    rounds = available_round_codes(draw_html, first_round_code)

    round_rows_map: dict[str, List[MatchRow]] = {}
    for round_code in rounds:
        round_rows_map[round_code] = build_round_rows_from_draw(draw_html, round_code)

    propagate_winners_to_next_round(round_rows_map, rounds)

    all_rows: List[MatchRow] = []
    for round_code in rounds:
        all_rows.extend(round_rows_map[round_code])

    export_csv(all_rows, output_csv)


if __name__ == "__main__":
    DRAW_URL = "https://www.atptour.com/en/scores/current/monte-carlo/410/draws"

    BASE_DIR = Path(__file__).resolve().parent
    OUTPUT_CSV = BASE_DIR / "montecarlo_full_draw.csv"

    print("=== DEBUG START ===")
    print("Working dir:", os.getcwd())
    print("Saving CSV in:", OUTPUT_CSV)
    print("Script starting...")

    build_full_tournament_csv_from_draw(DRAW_URL, str(OUTPUT_CSV))

    print("CSV creato:", OUTPUT_CSV)
    print("File exists:", OUTPUT_CSV.exists())
    print("=== DEBUG END ===")

