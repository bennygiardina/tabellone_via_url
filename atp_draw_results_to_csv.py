import csv
import re
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple

import requests
from bs4 import BeautifulSoup, Tag


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    )
}

SPECIAL_COUNTRY_CODES = {"JPN", "CHN", "KOR", "TPE", "HKG"}
NAME_LABEL_MAP = {
    "Q": "Q",
    "WC": "WC",
    "LL": "LL",
    "Alt": "Alt",
    "PR": "PR",
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


@dataclass
class Entrant:
    raw_name: str
    display_name: str
    surname_key: str


@dataclass
class MatchRow:
    round_code: str
    player_a: str
    player_b: str
    winner: str
    participant_a_score: str
    participant_b_score: str


@dataclass
class ParsedResult:
    round_label: str
    player_a: str
    player_b: str
    winner: str
    raw_score: str
    participant_a_score: str
    participant_b_score: str


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def surname_from_display_name(name: str) -> str:
    name = re.sub(r"\s*\[[^\]]+\]\s*$", "", name).strip()
    if name.lower() in {"bye", "qualifier", "lucky loser", "qualifier / lucky loser", ""}:
        return name.lower()

    parts = name.split()
    if not parts:
        return ""

    # Gestisce già sia "C. Alcaraz" sia "Shimabukuro S."
    return parts[0].lower() if parts[0].endswith(".") is False and len(parts) == 2 and parts[1].endswith(".") else parts[-1].lower()


def extract_country_code(stats_item: Tag) -> Optional[str]:
    country_div = stats_item.select_one("div.country a[href]")
    if not country_div:
        country_div = stats_item.select_one("div.country[href]")

    href = ""
    if country_div:
        href = country_div.get("href", "")

    match = re.search(r"/([A-Z]{3})(?:/)?$", href)
    if match:
        return match.group(1)

    match = re.search(r"([A-Z]{3})$", href)
    if match:
        return match.group(1)

    return None


def extract_label(stats_item: Tag) -> Optional[str]:
    text = normalize_space(stats_item.get_text(" ", strip=True))

    seed_match = re.search(r"\((\d{1,2})\)\s*$", text)
    if seed_match:
        return seed_match.group(1)

    for raw, final in NAME_LABEL_MAP.items():
        if re.search(rf"\({re.escape(raw)}\)\s*$", text):
            return final

    return None


def invert_name_for_special_country(name: str, country_code: Optional[str]) -> str:
    if not country_code or country_code not in SPECIAL_COUNTRY_CODES:
        return name

    if name.strip().lower() == "n. osaka":
        return name

    parts = name.split()
    if len(parts) != 2:
        return name

    first, last = parts
    if not first.endswith("."):
        return name

    return f"{last} {first}"


def normalize_special_slot(name: str) -> str:
    lowered = name.strip().lower()

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

    return name.strip()


def build_display_name(stats_item: Tag) -> Entrant:
    name_div = stats_item.select_one("div.name")
    if not name_div:
        raise ValueError("stats-item senza div.name")

    raw_name = normalize_space(name_div.get_text(" ", strip=True))
    normalized = normalize_special_slot(raw_name)

    if normalized in {"bye", "", "Qualifier", "Qualifier / Lucky Loser", "Lucky Loser"}:
        display_name = normalized
        return Entrant(
            raw_name=raw_name,
            display_name=display_name,
            surname_key=surname_from_display_name(display_name),
        )

    country_code = extract_country_code(stats_item)
    normalized = invert_name_for_special_country(normalized, country_code)

    label = extract_label(stats_item)
    if label:
        normalized = f"{normalized} [{label}]"

    return Entrant(
        raw_name=raw_name,
        display_name=normalized,
        surname_key=surname_from_display_name(normalized),
    )


def detect_first_round(draw_html: str) -> str:
    # Priorità: R128 > R64 > R32
    for code in ("R128", "R64", "R32"):
        if re.search(rf"\b{re.escape(code)}\b", draw_html):
            return code
    raise ValueError("Impossibile determinare il primo turno dal draw.")


def expected_first_round_name_count(first_round_code: str) -> int:
    return {"R128": 128, "R64": 64, "R32": 32}[first_round_code]


def extract_first_round_entrants(draw_html: str) -> Tuple[str, List[Entrant]]:
    soup = BeautifulSoup(draw_html, "html.parser")
    first_round_code = detect_first_round(draw_html)
    expected = expected_first_round_name_count(first_round_code)

    stats_items = soup.select("div.stats-item")
    entrants: List[Entrant] = []

    for item in stats_items:
        if not item.select_one("div.name"):
            continue
        entrants.append(build_display_name(item))
        if len(entrants) == expected:
            break

    if len(entrants) != expected:
        raise ValueError(
            f"Estratti {len(entrants)} nomi, ma per {first_round_code} me ne aspettavo {expected}."
        )

    return first_round_code, entrants


def pair_entrants(round_code: str, entrants: List[Entrant]) -> List[MatchRow]:
    rows: List[MatchRow] = []

    for i in range(0, len(entrants), 2):
        a = entrants[i].display_name
        b = entrants[i + 1].display_name

        if a == "bye" and b != "bye":
            winner = b
            score_a = ""
            score_b = ""
        elif b == "bye" and a != "bye":
            winner = a
            score_a = ""
            score_b = ""
        else:
            winner = ""
            score_a = ""
            score_b = ""

        rows.append(
            MatchRow(
                round_code=round_code,
                player_a=a,
                player_b=b,
                winner=winner,
                participant_a_score=score_a,
                participant_b_score=score_b,
            )
        )

    return rows


def canonical_name(name: str) -> str:
    name = re.sub(r"\s*\[[^\]]+\]\s*$", "", name).strip()
    return normalize_space(name).lower()


def parse_score_tokens(raw_score: str) -> Tuple[List[Tuple[int, int]], bool, bool]:
    """
    Restituisce:
    - lista di set come tuple (a_games, b_games)
    - has_ret
    - has_wo
    """
    text = normalize_space(raw_score)
    upper = text.upper()

    has_ret = "RET" in upper
    has_wo = "W/O" in upper or "WALKOVER" in upper

    text = re.sub(r"\bRET\.?\b", "", text, flags=re.I)
    text = re.sub(r"\bW/O\b", "", text, flags=re.I)
    text = re.sub(r"\bWALKOVER\b", "", text, flags=re.I)

    # Converte 7-6(5) oppure 6(5)-7 in soli game
    text = re.sub(r"(\d+)\(\d+\)", r"\1", text)

    sets_found = []
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
    winner_name: str,
    player_a: str,
    player_b: str,
) -> Tuple[str, str]:
    sets_found, has_ret, has_wo = parse_score_tokens(raw_score)

    if has_wo:
        if canonical_name(winner_name) == canonical_name(player_a):
            return "W/O", ""
        if canonical_name(winner_name) == canonical_name(player_b):
            return "", "W/O"
        return "W/O", ""

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
        if canonical_name(winner_name) == canonical_name(player_a):
            return str(a_sets), f"(rit.) {b_sets}"
        if canonical_name(winner_name) == canonical_name(player_b):
            return f"(rit.) {a_sets}", str(b_sets)

    return str(a_sets), str(b_sets)


def extract_results_round_blocks(results_html: str) -> Dict[str, str]:
    """
    Versione semplice:
    separa il testo della pagina results in blocchi per round.
    """
    soup = BeautifulSoup(results_html, "html.parser")
    text = normalize_space(soup.get_text(" ", strip=True))

    round_labels = [
        "Round of 128",
        "Round of 64",
        "Round of 32",
        "Round of 16",
        "Quarterfinals",
        "Semifinals",
        "Final",
    ]

    positions = []
    for label in round_labels:
        idx = text.find(label)
        if idx != -1:
            positions.append((label, idx))

    positions.sort(key=lambda x: x[1])

    blocks: Dict[str, str] = {}
    for i, (label, start_idx) in enumerate(positions):
        end_idx = positions[i + 1][1] if i + 1 < len(positions) else len(text)
        blocks[label] = text[start_idx:end_idx]

    return blocks


def split_round_block_into_match_chunks(round_block: str) -> List[str]:
    """
    Prima versione pragmatica:
    spezza usando la frase 'wins the match' / 'Winner:' come ancora.
    """
    chunks = []
    parts = re.split(r"(?=(?:Game Set and Match|Winner:))", round_block, flags=re.I)
    for part in parts:
        part = normalize_space(part)
        if "wins the match" in part.lower() or "winner:" in part.lower():
            chunks.append(part)
    return chunks


def extract_winner_from_chunk(chunk: str) -> Optional[str]:
    match = re.search(r"Game Set and Match (.+?)\.", chunk, flags=re.I)
    if match:
        return normalize_space(match.group(1))

    match = re.search(r"Winner:\s*(.+?)(?:\s+by\s+Walkover|\.)", chunk, flags=re.I)
    if match:
        return normalize_space(match.group(1))

    match = re.search(r"(.+?) wins the match", chunk, flags=re.I)
    if match:
        return normalize_space(match.group(1))

    return None


def extract_score_from_chunk(chunk: str) -> str:
    # Walkover
    if re.search(r"walkover|W/O", chunk, flags=re.I):
        return "W/O"

    # Prende la parte dopo "wins the match", se esiste
    match = re.search(r"wins the match\s+(.+?)(?:$|\.)", chunk, flags=re.I)
    if match:
        return normalize_space(match.group(1))

    # fallback: raccoglie pattern da set
    tokens = re.findall(r"\d+\(\d+\)-\d+|\d+-\d+\(\d+\)|\d+-\d+|RET\.?", chunk, flags=re.I)
    return " ".join(tokens)


def find_players_in_chunk(
    chunk: str,
    candidate_names: List[str],
) -> List[str]:
    found = []
    lowered_chunk = chunk.lower()

    for name in candidate_names:
        bare = canonical_name(name)
        if bare and bare in lowered_chunk:
            found.append(name)

    return found


def build_parsed_results(results_html: str, candidate_names: List[str]) -> List[ParsedResult]:
    blocks = extract_results_round_blocks(results_html)
    parsed: List[ParsedResult] = []

    for round_label, block in blocks.items():
        chunks = split_round_block_into_match_chunks(block)
        for chunk in chunks:
            winner = extract_winner_from_chunk(chunk)
            if not winner:
                continue

            score = extract_score_from_chunk(chunk)

            matched_names = find_players_in_chunk(chunk, candidate_names)
            matched_names = list(dict.fromkeys(matched_names))  # dedup preservando ordine

            if len(matched_names) < 2:
                continue

            player_a = matched_names[0]
            player_b = matched_names[1]

            a_score, b_score = convert_raw_score_to_csv_scores(score, winner, player_a, player_b)

            parsed.append(
                ParsedResult(
                    round_label=round_label,
                    player_a=player_a,
                    player_b=player_b,
                    winner=winner,
                    raw_score=score,
                    participant_a_score=a_score,
                    participant_b_score=b_score,
                )
            )

    return parsed


def surname_fallback_match(
    row_a: str,
    row_b: str,
    result_a: str,
    result_b: str,
) -> bool:
    row_keys = {surname_from_display_name(row_a), surname_from_display_name(row_b)}
    result_keys = {surname_from_display_name(result_a), surname_from_display_name(result_b)}
    return row_keys == result_keys


def enrich_rows_with_results(
    rows: List[MatchRow],
    parsed_results: List[ParsedResult],
    round_code: str,
) -> List[MatchRow]:
    round_label = DRAW_TO_RESULTS_ROUND[round_code]

    for row in rows:
        if row.winner:  # bye già gestito
            continue

        exact_match = None
        fallback_match = None

        for result in parsed_results:
            if result.round_label != round_label:
                continue

            same_exact = (
                canonical_name(row.player_a) == canonical_name(result.player_a)
                and canonical_name(row.player_b) == canonical_name(result.player_b)
            )

            if same_exact:
                exact_match = result
                break

            same_fallback = surname_fallback_match(
                row.player_a, row.player_b, result.player_a, result.player_b
            )
            if same_fallback and fallback_match is None:
                fallback_match = result

        chosen = exact_match or fallback_match
        if not chosen:
            continue

        row.winner = chosen.winner
        row.participant_a_score = chosen.participant_a_score
        row.participant_b_score = chosen.participant_b_score

    return rows


def rows_to_winners(rows: List[MatchRow]) -> List[Entrant]:
    winners: List[Entrant] = []
    for row in rows:
        winners.append(
            Entrant(
                raw_name=row.winner,
                display_name=row.winner,
                surname_key=surname_from_display_name(row.winner),
            )
        )
    return winners


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

    candidate_names = [e.display_name for e in first_round_entrants]
    parsed_results = build_parsed_results(results_html, candidate_names)
    first_round_rows = enrich_rows_with_results(first_round_rows, parsed_results, first_round_code)

    # Costruzione R64 dai winner del primo turno
    if first_round_code == "R128":
        next_round_code = "R64"
    elif first_round_code == "R64":
        next_round_code = "R32"
    elif first_round_code == "R32":
        next_round_code = "R16"
    else:
        raise ValueError(f"Primo turno non gestito: {first_round_code}")

    next_round_entrants = rows_to_winners(first_round_rows)
    next_round_rows = pair_entrants(next_round_code, next_round_entrants)

    candidate_names_next = [e.display_name for e in next_round_entrants]
    parsed_results_next = build_parsed_results(results_html, candidate_names_next)
    next_round_rows = enrich_rows_with_results(next_round_rows, parsed_results_next, next_round_code)

    all_rows = first_round_rows + next_round_rows
    export_csv(all_rows, output_csv)


if __name__ == "__main__":
    DRAW_URL = "https://www.atptour.com/en/scores/current/indian_wells/404/draws"
    RESULTS_URL = "https://www.atptour.com/en/scores/current/indian_wells/404/results"
    OUTPUT_CSV = "indian_wells_up_to_r64.csv"

    build_tournament_csv_up_to_r64(DRAW_URL, RESULTS_URL, OUTPUT_CSV)
    print(f"CSV creato: {OUTPUT_CSV}")
