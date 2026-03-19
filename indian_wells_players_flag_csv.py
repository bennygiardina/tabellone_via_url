#!/usr/bin/env python3
"""
Build a CSV with ATP draw players mapped to their flag URLs.

Output columns:
- Players: exact player string as it appears in the matches CSV
- Flags: flag URL looked up from NOC_flag.csv using ATP country code

Main workflow:
1. Download the ATP draw page and extract data-first, data-last, data-country-code
2. Download the matches CSV and collect player labels from Player A and Player B
3. Match ATP players to CSV labels with these rules:
   - case-insensitive
   - ignore bracketed suffixes like [1], [Q], [WC], [PR], [LL]
   - support compound surnames
   - for JPN/CHN/KOR allow inverted CSV format: "Surname F."
   - exception: Naomi Osaka is always treated in standard format: "N. Osaka"
4. Download NOC_flag.csv and map country codes to flag URLs
5. Write a two-column CSV: Players, Flags
"""

from __future__ import annotations

import argparse
import csv
import html
import io
import re
import sys
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set

import pandas as pd
import requests
from bs4 import BeautifulSoup

ATP_URL_DEFAULT = "https://www.atptour.com/en/scores/current/indian-wells/404/draws"
MATCHES_CSV_URL_DEFAULT = (
    "https://raw.githubusercontent.com/bennygiardina/tabellone_via_url/refs/heads/main/indian_wells_full_draw.csv"
)
FLAGS_CSV_URL_DEFAULT = (
    "https://raw.githubusercontent.com/bennygiardina/tabellone_via_url/refs/heads/main/NOC_flag.csv"
)
OUTPUT_CSV_DEFAULT = "indian_wells_players_flag.csv"

SPECIAL_INVERTED_COUNTRIES = {"JPN", "CHN", "KOR"}
EXACT_STANDARD_EXCEPTIONS = {("naomi", "osaka")}
IGNORED_PLAYER_VALUES = {"", "bye", "nan", "none", "null", "tbd", "unknown"}


@dataclass(frozen=True)
class AtpPlayer:
    first: str
    last: str
    country_code: str

    @property
    def normalized_first(self) -> str:
        return normalize_spaces(self.first).casefold()

    @property
    def normalized_last(self) -> str:
        return normalize_spaces(self.last).casefold()

    def key(self) -> tuple[str, str, str]:
        return (self.normalized_first, self.normalized_last, self.country_code.upper())


def normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_name_text(value: str) -> str:
    value = html.unescape(value or "")
    value = value.replace("\xa0", " ")
    value = value.replace("’", "'").replace("`", "'").replace("´", "'")
    return normalize_spaces(value)


def canonicalize_text(value: str) -> str:
    return normalize_name_text(value).casefold()


def strip_bracket_suffixes(player_label: str) -> str:
    cleaned = re.sub(r"\s*\[[^\]]*\]\s*", " ", player_label or "")
    return normalize_spaces(cleaned)


def looks_like_real_player(player_label: object) -> bool:
    if player_label is None:
        return False

    text = canonicalize_text(str(player_label))
    if not text:
        return False

    if text in IGNORED_PLAYER_VALUES:
        return False

    return not any(token in text for token in {"tbd", "bye", "unknown"})


def fetch_text(url: str, session: requests.Session, timeout: int = 30) -> str:
    response = session.get(
        url,
        timeout=timeout,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        },
    )
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    return response.text


def extract_players_from_atp_html(html_text: str) -> List[AtpPlayer]:
    """
    Extract players from ATP HTML.

    Primary path:
    - parse tags carrying data-first, data-last, data-country-code

    Fallback path:
    - regex over the raw HTML in case the attributes live inside a template/script block
    """
    players: "OrderedDict[tuple[str, str, str], AtpPlayer]" = OrderedDict()

    def add_player(first: str, last: str, country_code: str) -> None:
        first = normalize_name_text(first)
        last = normalize_name_text(last)
        country_code = normalize_name_text(country_code).upper()
        if not first or not last or not country_code:
            return
        if not looks_like_real_player(first) or not looks_like_real_player(last):
            return
        player = AtpPlayer(first=first, last=last, country_code=country_code)
        players.setdefault(player.key(), player)

    soup = BeautifulSoup(html_text, "html.parser")

    for tag in soup.find_all(attrs={"data-first": True, "data-last": True, "data-country-code": True}):
        add_player(
            str(tag.get("data-first", "")),
            str(tag.get("data-last", "")),
            str(tag.get("data-country-code", "")),
        )

    regex = re.compile(
        r'data-first="(?P<first>[^"]+)"[^>]*?'
        r'data-last="(?P<last>[^"]+)"[^>]*?'
        r'data-country-code="(?P<country>[^"]+)"',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in regex.finditer(html_text):
        add_player(match.group("first"), match.group("last"), match.group("country"))

    if not players:
        raise RuntimeError(
            "No ATP players found in the page HTML. The page structure may have changed."
        )

    return list(players.values())


def build_candidate_keys(player: AtpPlayer) -> Set[str]:
    first = player.normalized_first
    last = player.normalized_last
    if not first or not last:
        return set()

    initial = first[0]
    standard = f"{initial}. {last}"
    candidates = {standard}

    is_exception = (first, last) in EXACT_STANDARD_EXCEPTIONS
    if player.country_code.upper() in SPECIAL_INVERTED_COUNTRIES and not is_exception:
        candidates.add(f"{last} {initial}.")

    return candidates


def build_player_lookup(players: Sequence[AtpPlayer]) -> Dict[str, AtpPlayer]:
    lookup: Dict[str, AtpPlayer] = {}
    collisions: Dict[str, List[AtpPlayer]] = {}

    for player in players:
        for key in build_candidate_keys(player):
            if key in lookup and lookup[key] != player:
                collisions.setdefault(key, [lookup[key]]).append(player)
                continue
            lookup[key] = player

    if collisions:
        lines = []
        for key, conflict_players in collisions.items():
            pretty = ", ".join(
                f"{p.first} {p.last} ({p.country_code})" for p in conflict_players
            )
            lines.append(f"  - {key}: {pretty}")
        raise RuntimeError(
            "Ambiguous ATP player matching keys found:\n" + "\n".join(lines)
        )

    return lookup


def load_matches_csv(url: str, session: requests.Session) -> pd.DataFrame:
    text = fetch_text(url, session)
    return pd.read_csv(io.StringIO(text))


def collect_player_labels(matches_df: pd.DataFrame) -> List[str]:
    ordered: "OrderedDict[str, None]" = OrderedDict()
    candidate_columns = [col for col in ["Player A", "Player B"] if col in matches_df.columns]

    if not candidate_columns:
        raise RuntimeError("The matches CSV does not contain the required columns: Player A and/or Player B.")

    for column in candidate_columns:
        for value in matches_df[column].tolist():
            if not looks_like_real_player(value):
                continue
            label = normalize_spaces(str(value))
            ordered.setdefault(label, None)

    return list(ordered.keys())


def normalize_csv_player_label(player_label: str) -> str:
    return canonicalize_text(strip_bracket_suffixes(player_label))


def load_flag_lookup(url: str, session: requests.Session) -> Dict[str, str]:
    text = fetch_text(url, session)
    df = pd.read_csv(io.StringIO(text))

    if "NOCs" not in df.columns or "Flags" not in df.columns:
        raise RuntimeError("The flag CSV must contain columns named 'NOCs' and 'Flags'.")

    lookup: Dict[str, str] = {}
    for _, row in df.iterrows():
        code = normalize_spaces(str(row["NOCs"]))
        flag = normalize_spaces(str(row["Flags"]))
        if not code or code.casefold() == "nan" or not flag or flag.casefold() == "nan":
            continue
        lookup[code.upper()] = flag
    return lookup


def build_output_rows(
    player_labels: Sequence[str],
    player_lookup: Dict[str, AtpPlayer],
    flag_lookup: Dict[str, str],
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    unmatched_labels: List[str] = []
    for label in player_labels:
        normalized = normalize_csv_player_label(label)
        player = player_lookup.get(normalized)
        if player is None:
            unmatched_labels.append(label)
            continue

        flag_url = flag_lookup.get(player.country_code.upper(), "")
        rows.append({"Players": label, "Flags": flag_url})

    if unmatched_labels:
        preview = "\n".join(f"  - {item}" for item in unmatched_labels[:20])
        extra = "" if len(unmatched_labels) <= 20 else f"\n  ... and {len(unmatched_labels) - 20} more"
        raise RuntimeError(
            "Could not match these player labels from the matches CSV to ATP players:\n"
            f"{preview}{extra}"
        )

    return rows


def write_output_csv(rows: Sequence[Dict[str, str]], output_path: str) -> None:
    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["Players", "Flags"])
        writer.writeheader()
        writer.writerows(rows)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a CSV mapping tournament player labels to flag URLs."
    )
    parser.add_argument("--atp-url", default=ATP_URL_DEFAULT, help="ATP draw page URL")
    parser.add_argument(
        "--matches-csv-url",
        default=MATCHES_CSV_URL_DEFAULT,
        help="Matches CSV URL",
    )
    parser.add_argument(
        "--flags-csv-url",
        default=FLAGS_CSV_URL_DEFAULT,
        help="NOC flags CSV URL",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_CSV_DEFAULT,
        help="Output CSV path",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    try:
        with requests.Session() as session:
            atp_html = fetch_text(args.atp_url, session)
            atp_players = extract_players_from_atp_html(atp_html)
            player_lookup = build_player_lookup(atp_players)

            matches_df = load_matches_csv(args.matches_csv_url, session)
            player_labels = collect_player_labels(matches_df)

            flag_lookup = load_flag_lookup(args.flags_csv_url, session)
            rows = build_output_rows(player_labels, player_lookup, flag_lookup)

        write_output_csv(rows, args.output)
        print(f"Created {args.output} with {len(rows)} player rows.")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
