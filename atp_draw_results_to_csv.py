import csv
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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


@dataclass
class MatchRow:
    round_code: str
    player_a: str
    player_b: str
    winner: str
    participant_a_score: str
    participant_b_score: str


@dataclass
class ResultMatch:
    round_label: str
    player1_full_name: str
    player2_full_name: str
    winner_full_name: str
    raw_score: str
    is_walkover: bool
    is_retirement: bool


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


def extract_round_player_rows(draw_html: str, round_code: str) -> List[PlayerRow]:
    expected_count = ROUND_NAME_COUNTS[round_code]
    round_fragment = slice_draw_html_for_round(draw_html, round_code)
    soup = BeautifulSoup(round_fragment, "html.parser")

    rows: List[PlayerRow] = []
    seen_names: List[str] = []

    for stats_item in soup.select("div.stats-item"):
        display_name = build_display_name(stats_item)
        if display_name is None:
            continue
        if display_name == "":
            continue

        rows.append(PlayerRow(display_name=display_name))
        seen_names.append(display_name)

        if len(rows) == expected_count:
            break

    if len(rows) != expected_count:
        preview = ", ".join(seen_names[:20])
        raise ValueError(
            f"Estratti {len(rows)} nomi per {round_code}, attesi {expected_count}. "
            f"Primi nomi letti: {preview}"
        )

    return rows


def build_round_rows_from_draw(draw_html: str, round_code: str) -> List[MatchRow]:
    player_rows = extract_round_player_rows(draw_html, round_code)

    rows: List[MatchRow] = []
    for i in range(0, len(player_rows), 2):
        player_a = player_rows[i].display_name
        player_b = player_rows[i + 1].display_name

        winner = ""
        score_a = ""
        score_b = ""

        if player_a == "bye" and player_b != "bye":
            winner = player_b
        elif player_b == "bye" and player_a != "bye":
            winner = player_a

        rows.append(
            MatchRow(
                round_code=round_code,
                player_a=player_a,
                player_b=player_b,
                winner=winner,
                participant_a_score=score_a,
                participant_b_score=score_b,
            )
        )

    return rows


def available_round_codes(draw_html: str, first_round_code: str) -> List[str]:
    start_idx = ROUND_CODES_IN_ORDER.index(first_round_code)
    rounds: List[str] = []

    for code in ROUND_CODES_IN_ORDER[start_idx:]:
        label = DRAW_TO_RESULTS_ROUND[code]
        if label in draw_html or f">{code}<" in draw_html:
            rounds.append(code)

    return rounds


def extract_result_matches(results_html: str) -> Dict[str, List[ResultMatch]]:
    """
    Costruisce match dei results per round.
    Ogni match viene identificato da:
    - 2 giocatori completi
    - winner
    - score grezzo
    """
    soup = BeautifulSoup(results_html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [normalize_space(line) for line in text.splitlines()]
    lines = [line for line in lines if line]

    round_labels = list(DRAW_TO_RESULTS_ROUND.values())
    results_by_round: Dict[str, List[ResultMatch]] = {label: [] for label in round_labels}

    current_round: Optional[str] = None
    current_block: List[str] = []

    def flush_block() -> None:
        nonlocal current_round, current_block

        if not current_round or not current_block:
            current_block = []
            return

        block_text = "\n".join(current_block)

        # Cerca i due giocatori nella forma:
        # "First Last" su linee consecutive nel blocco
        candidate_player_lines = []
        for line in current_block:
            if re.match(r"^[A-Z][a-zA-Z'`\-\.]+(?:\s+[A-Z][a-zA-Z'`\-\.]+)+$", line):
                candidate_player_lines.append(line)

        player1_full_name = ""
        player2_full_name = ""

        # In molti blocchi ATP i primi due nomi completi sono i due giocatori
        if len(candidate_player_lines) >= 2:
            player1_full_name = candidate_player_lines[0]
            player2_full_name = candidate_player_lines[1]

        winner_match = re.search(r"Game Set and Match\s+(.+?)\.", block_text, flags=re.I)
        walkover_match = re.search(r"Winner:\s*(.+?)\s+by\s+Walkover", block_text, flags=re.I)

        winner_full_name = ""
        raw_score = ""
        is_walkover = False
        is_retirement = False

        if winner_match:
            winner_full_name = normalize_space(winner_match.group(1))

            score_match = re.search(r"wins the match\s+(.+?)(?:\.|$)", block_text, flags=re.I)
            if score_match:
                raw_score = normalize_space(score_match.group(1))

            is_retirement = "RET" in raw_score.upper()
            is_walkover = "W/O" in raw_score.upper() or "WALKOVER" in raw_score.upper()

        elif walkover_match:
            winner_full_name = normalize_space(walkover_match.group(1))
            raw_score = "W/O"
            is_walkover = True

        if winner_full_name and player1_full_name and player2_full_name:
            results_by_round[current_round].append(
                ResultMatch(
                    round_label=current_round,
                    player1_full_name=player1_full_name,
                    player2_full_name=player2_full_name,
                    winner_full_name=winner_full_name,
                    raw_score=raw_score,
                    is_walkover=is_walkover,
                    is_retirement=is_retirement,
                )
            )

        current_block = []

    for line in lines:
        matched_round = None
        for round_label in round_labels:
            if line.startswith(round_label):
                matched_round = round_label
                break

        if matched_round:
            flush_block()
            current_round = matched_round
            current_block = [line]
            continue

        if current_round is None:
            continue

        current_block.append(line)

        if "Game Set and Match" in line or line.startswith("Winner:"):
            flush_block()

    flush_block()
    return results_by_round


def remove_labels(name: str) -> str:
    return re.sub(r"\s*\[[^\]]+\]\s*$", "", name).strip()


def split_name_parts(name: str) -> Tuple[str, str]:
    """
    Restituisce (initial, surname_like_display)
    Supporta:
    - C. Alcaraz
    - Shimabukuro S.
    """
    base = remove_labels(name)
    parts = base.split()
    if not parts:
        return "", ""

    # Formato invertito: "Shimabukuro S."
    if len(parts) == 2 and parts[-1].endswith("."):
        return parts[-1][0].lower(), parts[0].lower()

    # Formato standard: "C. Alcaraz" / "B. van de Zandschulp"
    if parts[0].endswith("."):
        return parts[0][0].lower(), " ".join(parts[1:]).lower()

    return parts[0][0].lower(), parts[-1].lower()


def winner_matches_player(winner_full_name: str, player_display_name: str) -> bool:
    winner_lower = winner_full_name.lower()
    initial, surname = split_name_parts(player_display_name)

    if not initial or not surname:
        return False

    if surname not in winner_lower:
        return False

    # Nome completo nei results: es. "Grigor Dimitrov"
    # Accettiamo se compare il cognome e il nome inizia con la stessa iniziale.
    tokens = winner_full_name.split()
    if not tokens:
        return False

    return tokens[0][0].lower() == initial

def normalize_for_matching(name: str) -> str:
    return normalize_space(name).lower()


def surname_key_from_display_name(name: str) -> str:
    base = remove_labels(name)
    parts = base.split()
    if not parts:
        return ""

    # Shimabukuro S.
    if len(parts) == 2 and parts[-1].endswith("."):
        return parts[0].lower()

    # B. van de Zandschulp
    if parts[0].endswith("."):
        return " ".join(parts[1:]).lower()

    return parts[-1].lower()


def surname_key_from_full_name(name: str) -> str:
    parts = normalize_space(name).split()
    if not parts:
        return ""

    if len(parts) == 1:
        return parts[0].lower()

    return " ".join(parts[1:]).lower()


def result_match_matches_row(result_match: ResultMatch, row: MatchRow) -> bool:
    row_surnames = {
        surname_key_from_display_name(row.player_a),
        surname_key_from_display_name(row.player_b),
    }
    result_surnames = {
        surname_key_from_full_name(result_match.player1_full_name),
        surname_key_from_full_name(result_match.player2_full_name),
    }
    return row_surnames == result_surnames
    

def parse_score_tokens(raw_score: str) -> Tuple[List[Tuple[int, int]], bool, bool]:
    text = normalize_space(raw_score)
    upper = text.upper()

    has_ret = "RET" in upper
    has_wo = "W/O" in upper or "WALKOVER" in upper

    text = re.sub(r"\bRET\.?\b", "", text, flags=re.I)
    text = re.sub(r"\bW/O\b", "", text, flags=re.I)
    text = re.sub(r"\bWALKOVER\b", "", text, flags=re.I)

    # Tie-break: 7-6(5) / 6-7(5) -> 7-6 / 6-7
    text = re.sub(r"(\d+)\((\d+)\)", r"\1", text)

    sets_found: List[Tuple[int, int]] = []
    for left, right in re.findall(r"(\d+)\s*-\s*(\d+)", text):
        sets_found.append((int(left), int(right)))

    return sets_found, has_ret, has_wo


def is_complete_set(a_games: int, b_games: int) -> bool:
    if a_games == 7 or b_games == 7:
        return True
    if a_games == 6 and b_games < 5:
        return True
    if b_games == 6 and a_games < 5:
        return True
    return False


def convert_raw_score_to_csv_scores(raw_score: str, winner_is_a: bool) -> Tuple[str, str]:
    """
    Nei results ATP lo score è dal punto di vista del vincitore.
    """
    sets_found, has_ret, has_wo = parse_score_tokens(raw_score)

    if has_wo:
        return ("W/O", "") if winner_is_a else ("", "W/O")

    winner_sets = 0
    loser_sets = 0

    for left_games, right_games in sets_found:
        if not is_complete_set(left_games, right_games):
            continue

        if left_games > right_games:
            winner_sets += 1
        elif right_games > left_games:
            loser_sets += 1

    if has_ret:
        if winner_is_a:
            return str(winner_sets), f"(rit.) {loser_sets}"
        return f"(rit.) {loser_sets}", str(winner_sets)

    if winner_is_a:
        return str(winner_sets), str(loser_sets)
    return str(loser_sets), str(winner_sets)


def overlay_results_on_round_rows(
    round_code: str,
    round_rows: List[MatchRow],
    results_by_round: Dict[str, List[ResultMatch]],
) -> List[MatchRow]:
    round_label = DRAW_TO_RESULTS_ROUND[round_code]
    result_matches = results_by_round.get(round_label, [])

    # Solo i match reali vanno presi dai results.
    real_rows = [row for row in round_rows if row.player_a != "bye" and row.player_b != "bye"]

    if len(real_rows) > len(result_matches):
        raise ValueError(
            f"Nei results ho trovato solo {len(result_matches)} match per {round_code}, "
            f"ma nel draw ci sono {len(real_rows)} match reali."
        )

    for row, result_match in zip(real_rows, result_matches):
        winner = ""

        if winner_matches_player(result_match.winner_full_name, row.player_a):
            winner = row.player_a
            winner_is_a = True
        elif winner_matches_player(result_match.winner_full_name, row.player_b):
            winner = row.player_b
            winner_is_a = False
        else:
            raise ValueError(
                f"Impossibile associare winner '{result_match.winner_full_name}' "
                f"a '{row.player_a}' / '{row.player_b}' in {round_code}"
            )

        score_a, score_b = convert_raw_score_to_csv_scores(result_match.raw_score, winner_is_a)

        row.winner = winner
        row.participant_a_score = score_a
        row.participant_b_score = score_b

    return round_rows


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


def build_full_tournament_csv(draw_url: str, results_url: str, output_csv: str) -> None:
    draw_html = fetch_html(draw_url)
    results_html = fetch_html(results_url)

    first_round_code = detect_first_round_code(draw_html)
    rounds = available_round_codes(draw_html, first_round_code)
    results_by_round = extract_result_matches(results_html)

    all_rows: List[MatchRow] = []

    for round_code in rounds:
        round_rows = build_round_rows_from_draw(draw_html, round_code)
        round_rows = overlay_results_on_round_rows(round_code, round_rows, results_by_round)
        all_rows.extend(round_rows)

    export_csv(all_rows, output_csv)


if __name__ == "__main__":
    DRAW_URL = "https://www.atptour.com/en/scores/current/indian_wells/404/draws"
    RESULTS_URL = "https://www.atptour.com/en/scores/current/indian_wells/404/results"

    BASE_DIR = Path(__file__).resolve().parent
    OUTPUT_CSV = BASE_DIR / "indian_wells_full_draw.csv"

    print("=== DEBUG START ===")
    print("Working dir:", os.getcwd())
    print("Saving CSV in:", OUTPUT_CSV)
    print("Script starting...")

    build_full_tournament_csv(DRAW_URL, RESULTS_URL, str(OUTPUT_CSV))

    print("CSV creato:", OUTPUT_CSV)
    print("File exists:", OUTPUT_CSV.exists())
    print("=== DEBUG END ===")
