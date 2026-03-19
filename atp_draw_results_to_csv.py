import csv
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Pattern, Tuple

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

FIRST_ROUND_NAME_COUNTS = {
    "R128": 128,
    "R64": 64,
    "R32": 32,
}

SPECIAL_COUNTRY_CODES = {"JPN", "CHN", "KOR", "TPE", "HKG"}
SPECIAL_NAME_EXCEPTION = {"n. osaka"}

LOWERCASE_SURNAME_PARTICLES = {
    "da", "de", "del", "della", "di", "du", "la", "le",
    "van", "von", "der", "den", "ten", "ter", "dos"
}

INLINE_LABEL_PATTERN = re.compile(r"^(.*?)(?:\s*\((\d{1,2}|Q|WC|LL|Alt|PR)\))?$", re.I)


@dataclass
class Entrant:
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
class ResultChunk:
    round_label: str
    text: str


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def strip_display_label(name: str) -> str:
    return re.sub(r"\s*\[[^\]]+\]\s*$", "", name).strip()


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


def extract_first_round_entrants(draw_html: str) -> Tuple[str, List[Entrant]]:
    first_round_code = detect_first_round_code(draw_html)
    expected_count = FIRST_ROUND_NAME_COUNTS[first_round_code]

    round_fragment = slice_draw_html_for_round(draw_html, first_round_code)
    soup = BeautifulSoup(round_fragment, "html.parser")

    entrants: List[Entrant] = []
    seen_sequence: List[str] = []

    for stats_item in soup.select("div.stats-item"):
        display_name = build_display_name(stats_item)
        if display_name is None:
            continue

        # Scarta solo slot davvero privi di nome.
        if display_name == "":
            continue

        entrants.append(Entrant(display_name=display_name))
        seen_sequence.append(display_name)

        if len(entrants) == expected_count:
            break

    if len(entrants) != expected_count:
        preview = ", ".join(seen_sequence[:20])
        raise ValueError(
            f"Estratti {len(entrants)} nomi per {first_round_code}, attesi {expected_count}. "
            f"Primi nomi letti: {preview}"
        )

    return first_round_code, entrants


def pair_entrants(round_code: str, entrants: List[Entrant]) -> List[MatchRow]:
    rows: List[MatchRow] = []

    for i in range(0, len(entrants), 2):
        player_a = entrants[i].display_name
        player_b = entrants[i + 1].display_name

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


def remove_labels_and_parens(name: str) -> str:
    name = re.sub(r"\s*\[[^\]]+\]\s*$", "", name).strip()
    name = re.sub(r"\s*\((?:\d{1,2}|Q|WC|LL|Alt|PR)\)\s*$", "", name).strip()
    return normalize_space(name)


def split_name_for_matching(name: str) -> Tuple[str, str]:
    """
    Restituisce:
    - initial
    - surname
    """
    name = remove_labels_and_parens(name)
    if not name:
        return "", ""

    parts = name.split()

    # Formato invertito: "Tseng C."
    if len(parts) == 2 and parts[-1].endswith("."):
        initial = parts[-1][0].lower()
        surname = parts[0].lower()
        return initial, surname

    # Formato draw standard: "B. van de Zandschulp"
    if parts[0].endswith("."):
        initial = parts[0][0].lower()
        surname = " ".join(parts[1:]).lower()
        return initial, surname

    # Formato results completo: "Botic van de Zandschulp"
    initial = parts[0][0].lower()

    first_particle_idx = None
    for idx, token in enumerate(parts[1:], start=1):
        if token.lower() in LOWERCASE_SURNAME_PARTICLES:
            first_particle_idx = idx
            break

    if first_particle_idx is not None:
        surname = " ".join(parts[first_particle_idx:]).lower()
    else:
        surname = parts[-1].lower()

    return initial, surname


def build_player_regex(display_name: str) -> Optional[Pattern[str]]:
    base = remove_labels_and_parens(display_name)
    lowered = base.lower()

    if lowered in {"", "bye", "qualifier", "qualifier / lucky loser", "lucky loser"}:
        return None

    initial, surname = split_name_for_matching(base)
    if not initial or not surname:
        return None

    surname_pattern = re.escape(surname).replace(r"\ ", r"\s+")
    regex = rf"\b{re.escape(initial)}[a-z\-']*(?:\s+[a-z\-']+)*\s+{surname_pattern}\b"
    return re.compile(regex, re.I)


def build_winner_regex_from_chunk(chunk_text: str) -> Optional[str]:
    match = re.search(r"Game Set and Match\s+(.+?)\.", chunk_text, flags=re.I)
    if match:
        return normalize_space(match.group(1))

    match = re.search(r"Winner:\s*(.+?)\s+by\s+Walkover", chunk_text, flags=re.I)
    if match:
        return normalize_space(match.group(1))

    match = re.search(r"(.+?)\s+wins the match\b", chunk_text, flags=re.I)
    if match:
        return normalize_space(match.group(1))

    return None


def extract_score_from_chunk(chunk_text: str) -> str:
    if re.search(r"\bWalkover\b|\bW/O\b", chunk_text, flags=re.I):
        return "W/O"

    match = re.search(r"wins the match\s+(.+?)(?:\.|$)", chunk_text, flags=re.I)
    if match:
        return normalize_space(match.group(1))

    return ""


def parse_score_tokens(raw_score: str) -> Tuple[List[Tuple[int, int]], bool, bool]:
    text = normalize_space(raw_score)
    upper = text.upper()

    has_ret = "RET" in upper
    has_wo = "W/O" in upper or "WALKOVER" in upper

    text = re.sub(r"\bRET\.?\b", "", text, flags=re.I)
    text = re.sub(r"\bW/O\b", "", text, flags=re.I)
    text = re.sub(r"\bWALKOVER\b", "", text, flags=re.I)

    # 7-6(5) -> 7-6 ; 6(5)-7 -> 6-7
    text = re.sub(r"(\d+)\(\d+\)", r"\1", text)

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


def convert_raw_score_to_csv_scores(
    raw_score: str,
    winner_is_a: bool,
) -> Tuple[str, str]:
    sets_found, has_ret, has_wo = parse_score_tokens(raw_score)

    if has_wo:
        return ("W/O", "") if winner_is_a else ("", "W/O")

    a_sets = 0
    b_sets = 0

    for a_games, b_games in sets_found:
        if not is_complete_set(a_games, b_games):
            continue
        if a_games > b_games:
            a_sets += 1
        elif b_games > a_games:
            b_sets += 1

    if has_ret:
        return (str(a_sets), f"(rit.) {b_sets}") if winner_is_a else (f"(rit.) {a_sets}", str(b_sets))

    return str(a_sets), str(b_sets)


def extract_result_chunks(results_html: str) -> List[ResultChunk]:
    soup = BeautifulSoup(results_html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [normalize_space(line) for line in text.splitlines()]
    lines = [line for line in lines if line]

    result_round_labels = list(DRAW_TO_RESULTS_ROUND.values())

    chunks: List[ResultChunk] = []
    current_round: Optional[str] = None
    current_lines: List[str] = []

    def flush_current() -> None:
        nonlocal current_round, current_lines, chunks
        if current_round and current_lines:
            chunks.append(ResultChunk(round_label=current_round, text="\n".join(current_lines)))
        current_round = None
        current_lines = []

    for line in lines:
        matched_round = None
        for round_label in result_round_labels:
            if line.startswith(f"{round_label} -"):
                matched_round = round_label
                break

        if matched_round:
            flush_current()
            current_round = matched_round
            current_lines = [line]
            continue

        if current_round:
            current_lines.append(line)

    flush_current()
    return chunks


def chunk_matches_players(chunk_text: str, player_a: str, player_b: str) -> bool:
    regex_a = build_player_regex(player_a)
    regex_b = build_player_regex(player_b)

    if not regex_a or not regex_b:
        return False

    return bool(regex_a.search(chunk_text) and regex_b.search(chunk_text))


def chunk_matches_players_fallback(chunk_text: str, player_a: str, player_b: str) -> bool:
    _, surname_a = split_name_for_matching(player_a)
    _, surname_b = split_name_for_matching(player_b)

    if not surname_a or not surname_b:
        return False

    text = chunk_text.lower()
    return surname_a in text and surname_b in text


def winner_from_chunk_for_row(chunk_text: str, player_a: str, player_b: str) -> str:
    winner_text = build_winner_regex_from_chunk(chunk_text)
    if not winner_text:
        return ""

    regex_a = build_player_regex(player_a)
    regex_b = build_player_regex(player_b)

    if regex_a and regex_a.search(winner_text):
        return player_a
    if regex_b and regex_b.search(winner_text):
        return player_b

    # fallback cognome
    _, surname_a = split_name_for_matching(player_a)
    _, surname_b = split_name_for_matching(player_b)

    lowered_winner = winner_text.lower()
    if surname_a and surname_a in lowered_winner:
        return player_a
    if surname_b and surname_b in lowered_winner:
        return player_b

    return ""


def enrich_rows_with_results(rows: List[MatchRow], result_chunks: List[ResultChunk]) -> List[MatchRow]:
    for row in rows:
        # bye già completato
        if row.winner and "W/O" not in row.participant_a_score and "W/O" not in row.participant_b_score:
            continue

        target_round = DRAW_TO_RESULTS_ROUND[row.round_code]

        exact_chunk = None
        fallback_chunk = None

        for chunk in result_chunks:
            if chunk.round_label != target_round:
                continue

            if chunk_matches_players(chunk.text, row.player_a, row.player_b):
                exact_chunk = chunk
                break

            if fallback_chunk is None and chunk_matches_players_fallback(chunk.text, row.player_a, row.player_b):
                fallback_chunk = chunk

        chosen_chunk = exact_chunk or fallback_chunk
        if not chosen_chunk:
            continue

        winner = winner_from_chunk_for_row(chosen_chunk.text, row.player_a, row.player_b)
        if not winner:
            continue

        raw_score = extract_score_from_chunk(chosen_chunk.text)
        winner_is_a = winner == row.player_a
        score_a, score_b = convert_raw_score_to_csv_scores(raw_score, winner_is_a)

        row.winner = winner
        row.participant_a_score = score_a
        row.participant_b_score = score_b

    return rows


def winners_to_entrants(rows: List[MatchRow]) -> List[Entrant]:
    winners: List[Entrant] = []
    for row in rows:
        if not row.winner:
            raise ValueError(f"Manca il winner nel turno {row.round_code}: {row.player_a} vs {row.player_b}")
        winners.append(Entrant(display_name=row.winner))
    return winners


def next_round_code(current_round_code: str) -> str:
    idx = ROUND_CODES_IN_ORDER.index(current_round_code)
    return ROUND_CODES_IN_ORDER[idx + 1]


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


def build_tournament_csv_up_to_r64(draw_url: str, results_url: str, output_csv: str) -> None:
    draw_html = fetch_html(draw_url)
    results_html = fetch_html(results_url)

    first_round_code, first_round_entrants = extract_first_round_entrants(draw_html)
    first_round_rows = pair_entrants(first_round_code, first_round_entrants)

    result_chunks = extract_result_chunks(results_html)
    first_round_rows = enrich_rows_with_results(first_round_rows, result_chunks)

    # Se il torneo parte già da R64 o R32, costruiamo comunque solo il turno successivo.
    second_round_code = next_round_code(first_round_code)
    second_round_entrants = winners_to_entrants(first_round_rows)
    second_round_rows = pair_entrants(second_round_code, second_round_entrants)
    second_round_rows = enrich_rows_with_results(second_round_rows, result_chunks)

    all_rows = first_round_rows + second_round_rows
    export_csv(all_rows, output_csv)


if __name__ == "__main__":
    DRAW_URL = "https://www.atptour.com/en/scores/current/indian_wells/404/draws"
    RESULTS_URL = "https://www.atptour.com/en/scores/current/indian_wells/404/results"

    BASE_DIR = Path(__file__).resolve().parent
    OUTPUT_CSV = BASE_DIR / "indian_wells_up_to_r64.csv"

    print("=== DEBUG START ===")
    print("Working dir:", os.getcwd())
    print("Saving CSV in:", OUTPUT_CSV)
    print("Script starting...")

    build_tournament_csv_up_to_r64(DRAW_URL, RESULTS_URL, str(OUTPUT_CSV))

    print("CSV creato:", OUTPUT_CSV)
    print("File exists:", OUTPUT_CSV.exists())
    print("=== DEBUG END ===")
