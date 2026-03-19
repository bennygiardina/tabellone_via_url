#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_TOURNAMENT_URL = "https://www.atptour.com/en/scores/current/indian-wells/404"
DEFAULT_DRAW_PAGE = ""
DEFAULT_RESULTS_PAGE = ""
DEFAULT_FALLBACK_PDF = "https://www.protennislive.com/posting/{year}/{tournament_id}/mds.pdf"
DEFAULT_TOURNAMENT_ID = "404"
SUPPORTED_DRAW_SIZES = {16, 24, 28, 32, 48, 56, 64, 96, 128}

LOWERCASE_PARTICLES = {
    "de", "del", "della", "di", "da", "dos", "das",
    "van", "von", "der", "den", "la", "le",
}

FAMILY_NAME_FIRST_COUNTRIES = {"JPN", "CHN", "KOR", "TPE", "HKG"}
FAMILY_NAME_FIRST_EXCEPTIONS = {"naomi osaka"}

STATUS_LABELS = {
    "WC": "[WC]",
    "Q": "[Q]",
    "LL": "[LL]",
    "PR": "[PR]",
    "ALT": "[Alt]",
}

FULL_NAME_FORMAT_OVERRIDES = {
    "botic van de zandschulp": "B. van de Zandschulp",
    "giovanni mpetshi perricard": "G. Mpetshi Perricard",
    "alejandro davidovich fokina": "A. Davidovich Fokina",
    "luca van assche": "L. Van Assche",
}

ROUND_HEADER_TO_CANONICAL = {
    "Round of 128": "R128",
    "Round of 96": "R96",
    "Round of 64": "R64",
    "Round of 48": "R48",
    "Round of 32": "R32",
    "Round of 24": "R24",
    "Round of 16": "R16",
    "Second Round": "R2",
    "Third Round": "R3",
    "Fourth Round": "R4",
    "Quarterfinals": "QF",
    "Quarter-Finals": "QF",
    "Semifinals": "SF",
    "Semi-Finals": "SF",
    "Final": "F",
}

RESULT_RETIREMENT_RE = re.compile(r"\b(?:RET|Ret|ret|retired|retirement|RIT\.?)\b", re.IGNORECASE)
RESULT_WALKOVER_RE = re.compile(r"\b(?:W/O|WO|walkover|walk-over)\b", re.IGNORECASE)
DRAW_START_MARKER_RE = re.compile(r"\bmain\s+draw\b", re.IGNORECASE)
SINGLES_MARKER_RE = re.compile(r"\bsingles?\b", re.IGNORECASE)
DRAW_POSITION_PREFIX_RE = re.compile(r"^(\d{1,3})(?:\s+|(?=WC\b|PR\b|Q\b|LL\b|ALT\b|Alt\b|alt\b))(.*)$")
DRAW_POSITION_ONLY_RE = re.compile(r"^(\d{1,3})(?:\s*(WC|PR|Q|LL|ALT|Alt|alt))?$")

STOP_MARKERS = {
    "Round of 32", "Round of 16", "Quarterfinals", "Semifinals", "Final", "Winner",
    "Last Direct Acceptance", "ATP Supervisor", "Released", "Seeded Players",
    "Alternates/Lucky Losers", "Withdrawals", "Retirements/W.O.", "Retirements/W.O",
}
STOP_MARKER_FRAGMENTS = (
    "seeded players",
    "alternates/lucky losers",
    "withdrawals",
    "retirements/w.o",
    "last direct acceptance",
    "atp supervisor",
    "released",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def smart_title_token(token: str) -> str:
    token = token.strip()
    if not token:
        return token
    if "-" in token:
        return "-".join(smart_title_token(part) for part in token.split("-"))
    if "'" in token:
        return "'".join(smart_title_token(part) for part in token.split("'"))
    lower = token.lower()
    if lower in LOWERCASE_PARTICLES:
        return lower
    if lower.startswith("mc") and len(token) > 2:
        return "Mc" + token[2:3].upper() + token[3:].lower()
    return token[:1].upper() + token[1:].lower()


def smart_join_tokens(tokens: list[str]) -> str:
    out: list[str] = []
    for i, tok in enumerate(tokens):
        if not tok:
            continue
        lower = tok.strip().lower()
        next_lower = tokens[i + 1].strip().lower() if i + 1 < len(tokens) and tokens[i + 1] else ""
        if lower == "van" and next_lower == "assche":
            out.append("Van")
            continue
        out.append(smart_title_token(tok))
    return " ".join(out)


def canonical_round_for_stage(current_slots: int) -> str:
    if current_slots <= 1:
        return ""
    bracket_size = current_slots
    if bracket_size > 96:
        pass
    elif bracket_size > 64:
        bracket_size = 128
    else:
        bracket_size = 64 if current_slots > 32 else 32 if current_slots > 16 else 16 if current_slots > 8 else 8 if current_slots > 4 else 4 if current_slots > 2 else 2

    matches_in_round = current_slots // 2
    players_in_round = matches_in_round * 2
    if players_in_round >= 32:
        return f"R{players_in_round}"
    if players_in_round == 16:
        return "R16"
    if players_in_round == 8:
        return "QF"
    if players_in_round == 4:
        return "SF"
    if players_in_round == 2:
        return "F"
    return f"R{players_in_round}"


def canonical_round_to_label(canonical_round: str) -> str:
    mapping = {
        "R128": "1° turno",
        "R96": "1° turno",
        "R64": "2° turno",
        "R48": "2° turno",
        "R32": "3° turno",
        "R24": "3° turno",
        "R16": "Ottavi di finale",
        "QF": "Quarti di finale",
        "SF": "Semifinali",
        "F": "Finale",
    }
    return mapping.get(canonical_round, canonical_round)


def get_round_label(round_size: int, initial_draw_size: int) -> str:
    current_slots = round_size * 2
    canonical = canonical_round_for_stage(current_slots)
    return canonical_round_to_label(canonical)


def map_atp_round_to_canonical(round_text: str) -> str | None:
    rt = normalize_spaces(round_text or "")
    if not rt:
        return None

    for header, canonical in ROUND_HEADER_TO_CANONICAL.items():
        if rt == header or rt.startswith(f"{header} ") or rt.startswith(f"{header} -"):
            return canonical

    m = re.match(r"^(Round of \d+|Quarterfinals|Quarter-Finals|Semifinals|Semi-Finals|Final|Second Round|Third Round|Fourth Round)(?:\b|\s*-)", rt, re.IGNORECASE)
    if m:
        matched = normalize_spaces(m.group(1))
        for header, canonical in ROUND_HEADER_TO_CANONICAL.items():
            if matched.lower() == header.lower():
                return canonical

    lower = rt.lower().replace("-", " ")
    aliases = {
        "first round": "R128",
        "second round": "R64",
        "third round": "R32",
        "fourth round": "R16",
        "quarter finals": "QF",
        "quarterfinal": "QF",
        "quarterfinals": "QF",
        "semi finals": "SF",
        "semifinal": "SF",
        "semifinals": "SF",
        "finals": "F",
        "championship": "F",
    }
    return aliases.get(lower)


def format_name(raw_name: str, seed: str = "", entry_status: str = "", country: str = "") -> str:
    raw_name = (raw_name or "").replace(",", "").strip()
    country = (country or "").strip().upper()
    normalized_raw_name = normalize_spaces(raw_name).lower()

    if normalized_raw_name in FULL_NAME_FORMAT_OVERRIDES:
        base_name = FULL_NAME_FORMAT_OVERRIDES[normalized_raw_name]
        extras = []
        if seed:
            extras.append(f"[{seed}]")
        if entry_status in STATUS_LABELS:
            extras.append(STATUS_LABELS[entry_status])
        return f"{base_name} {' '.join(extras)}".strip() if extras else base_name

    if not raw_name:
        return ""
    if raw_name == "Bye":
        return "bye"
    if raw_name == "Qualifier / Lucky Loser":
        return "[Q/LL]"
    if raw_name == "Qualifier":
        return "[Q]"
    if raw_name == "TBA":
        return "TBA"

    tokens = raw_name.split()
    if len(tokens) == 1:
        base_name = smart_title_token(tokens[0])
    else:
        surname_tokens: list[str] = []
        given_tokens: list[str] = []

        for i, tok in enumerate(tokens):
            if tok.isupper():
                surname_tokens.append(tok)
            else:
                given_tokens = tokens[i:]
                break

        if not surname_tokens or not given_tokens:
            if country in FAMILY_NAME_FIRST_COUNTRIES and normalized_raw_name not in FAMILY_NAME_FIRST_EXCEPTIONS and len(tokens) >= 2:
                surname_tokens = [tokens[0]]
                given_tokens = tokens[1:]
            else:
                given_tokens = tokens[:-1]
                surname_tokens = [tokens[-1]]

        surname = smart_join_tokens(surname_tokens)
        given_name = smart_join_tokens(given_tokens)
        first_initial = f"{given_name[0].upper()}." if given_name else ""
        base_name = f"{first_initial} {surname}".strip()

    extras = []
    if seed:
        extras.append(f"[{seed}]")
    if entry_status in STATUS_LABELS:
        extras.append(STATUS_LABELS[entry_status])

    return f"{base_name} {' '.join(extras)}".strip() if extras else base_name


def is_tournament_metadata(text: str) -> bool:
    if not text:
        return False

    t = text.strip().lower()
    month_words = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    ]
    if any(month in t for month in month_words):
        return True
    if "usd" in t:
        return True
    if "hard" in t or "clay" in t or "grass" in t:
        return True
    if "|" in t:
        return True
    return False


def normalize_tournament_url(url: str) -> str:
    url = (url or "").strip()
    return url.rstrip("/") if url else ""


def infer_tournament_id_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")

    for i in range(len(parts) - 1):
        if parts[i] in {"current", "current-challenger"} and i + 2 < len(parts):
            candidate = parts[i + 2]
            if candidate.isdigit():
                return candidate

    matches = re.findall(r"/(\d+)(?:/|$)", path)
    return matches[-1] if matches else ""


def infer_draw_page_url(tournament_url: str) -> str:
    tournament_url = normalize_tournament_url(tournament_url)
    if not tournament_url:
        return ""
    if tournament_url.endswith("/draws"):
        return tournament_url
    if tournament_url.endswith("/results"):
        return re.sub(r"/results$", "/draws", tournament_url)
    return f"{tournament_url}/draws"


def infer_results_page_url_from_tournament(tournament_url: str) -> str:
    tournament_url = normalize_tournament_url(tournament_url)
    if not tournament_url:
        return ""
    if tournament_url.endswith("/results"):
        return tournament_url
    if tournament_url.endswith("/draws"):
        return re.sub(r"/draws$", "/results", tournament_url)
    return f"{tournament_url}/results"


def infer_results_page_url_from_draw(draw_page_url: str) -> str:
    url = normalize_tournament_url(draw_page_url)
    if not url:
        return ""
    if url.endswith("/results"):
        return url
    if url.endswith("/draws"):
        return re.sub(r"/draws$", "/results", url)
    return f"{url}/results"


def make_requests_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0 Safari/537.36"
        ),
        "Accept": "text/html,application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
        "Connection": "keep-alive",
    })
    return session


def http_get(session: requests.Session, url: str, timeout: int = 30) -> requests.Response:
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp


def discover_pdf_url(session: requests.Session, draw_page_url: str, fallback_pdf_url: str) -> str:
    try:
        resp = http_get(session, draw_page_url, timeout=30)
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if "protennislive.com" in href and href.lower().endswith("mds.pdf"):
                return href
    except requests.RequestException as exc:
        print(
            f"[{utc_now_iso()}] WARN | draw page non raggiungibile, uso fallback PDF | url={draw_page_url} | err={exc}",
            file=sys.stderr,
            flush=True,
        )
    return fallback_pdf_url


def extract_pdf_text(pdf_bytes: bytes) -> list[str]:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return [(page.extract_text() or "") for page in reader.pages]


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def clean_lines(text: str) -> list[str]:
    return [normalize_spaces(line) for line in text.splitlines() if normalize_spaces(line)]


def coalesce_draw_lines(lines: list[str]) -> list[str]:
    merged: list[str] = []
    i = 0
    while i < len(lines):
        current = normalize_spaces(lines[i])
        if not current:
            i += 1
            continue

        m = DRAW_POSITION_ONLY_RE.match(current)
        if m and i + 1 < len(lines):
            nxt = normalize_spaces(lines[i + 1])
            if nxt and not DRAW_POSITION_PREFIX_RE.match(nxt) and not is_score_line(nxt):
                position = m.group(1)
                status = m.group(2) or ""
                combined_tail = f"{status} {nxt}".strip()
                merged.append(f"{position} {combined_tail}".strip())
                i += 2
                continue

        merged.append(current)
        i += 1
    return merged


def extract_released_at(pages_text: Iterable[str]) -> str:
    for page_text in pages_text:
        lines = clean_lines(page_text)
        for i, line in enumerate(lines):
            if line == "Released" and i + 1 < len(lines):
                return lines[i + 1]
    return ""


def is_score_line(text: str) -> bool:
    text = normalize_spaces(text)
    return bool(text and re.fullmatch(r"\d{1,2}\s+\d{1,2}(?:\s+\d{1,2})*", text))


def normalize_person_name_for_matching(name: str) -> str:
    name = (name or "").strip().lower()
    name = re.sub(r"\([^)]*\)", "", name)
    name = re.sub(r"\[[^\]]+\]", "", name)
    name = name.replace(".", " ")
    return normalize_spaces(name)


def surname_from_name(name: str) -> str:
    parts = normalize_person_name_for_matching(name).split()
    return parts[-1] if parts else ""


def first_initial_from_name(name: str) -> str:
    parts = normalize_person_name_for_matching(name).split()
    if len(parts) >= 2 and parts[0]:
        return parts[0][0]
    return ""


def parse_draw_line(line: str) -> dict | None:
    line = normalize_spaces(line)
    if not line or is_score_line(line):
        return None

    m = re.match(r"^(\d{1,3})\s+(.*)$", line)
    if not m:
        m = re.match(r"^(\d{1,3})(WC|PR|Q|LL|ALT|Alt|alt)\s+(.*)$", line)
        if not m:
            return None
        position = int(m.group(1))
        rest = f"{m.group(2)} {m.group(3)}".strip()
    else:
        position = int(m.group(1))
        rest = m.group(2).strip()

    if position < 1 or position > 128:
        return None

    entry_status = ""
    seed = ""
    country = ""
    slot_type = "player"
    raw_name = ""

    if rest == "Bye":
        raw_name = rest
        display_name = "bye"
        slot_type = "bye"
    elif rest == "Qualifier / Lucky Loser":
        raw_name = rest
        display_name = "[Q/LL]"
        slot_type = "qualifier_or_lucky_loser"
    elif rest == "Qualifier":
        raw_name = rest
        display_name = "[Q]"
        slot_type = "qualifier"
    else:
        tokens = rest.split()
        if tokens:
            cleaned = tokens[0].replace(".", "").upper()
            if cleaned in STATUS_LABELS:
                entry_status = cleaned
                tokens.pop(0)
        if tokens and re.fullmatch(r"\d{1,2}", tokens[0]):
            seed = tokens.pop(0)
        if tokens and re.fullmatch(r"[A-Z]{3}", tokens[-1]):
            country = tokens.pop()
        raw_name = " ".join(tokens).replace(",", "").strip()
        if not raw_name:
            return None
        display_name = format_name(raw_name, seed=seed, entry_status=entry_status, country=country)
        if is_tournament_metadata(display_name):
            display_name = "bye"
            slot_type = "bye"

    return {
        "draw_position": position,
        "seed": seed,
        "entry_status": entry_status,
        "player_name": display_name,
        "raw_name": raw_name,
        "country": country,
        "slot_type": slot_type,
    }


def is_draw_start_marker(line: str) -> bool:
    normalized = normalize_spaces(line)
    if normalized in {"Main Draw Singles", "Singles Main Draw", "Main Draw"}:
        return True
    return bool(DRAW_START_MARKER_RE.search(normalized) and SINGLES_MARKER_RE.search(normalized))


def is_draw_stop_marker(line: str) -> bool:
    normalized = normalize_spaces(line)
    lower = normalized.lower()
    if normalized in STOP_MARKERS:
        return True
    return any(fragment in lower for fragment in STOP_MARKER_FRAGMENTS)


def parse_draw_positions_from_lines(lines: list[str]) -> list[dict]:
    rows: list[dict] = []
    seen_positions: set[int] = set()
    for line in coalesce_draw_lines(lines):
        parsed = parse_draw_line(line)
        if not parsed:
            continue
        pos = parsed["draw_position"]
        if pos in seen_positions:
            continue
        seen_positions.add(pos)
        rows.append(parsed)
    rows.sort(key=lambda item: item["draw_position"])
    return rows


def parse_draw_positions(pages_text: list[str]) -> list[dict]:
    all_lines: list[str] = []
    scoped_lines: list[str] = []
    in_main_draw = False
    found_start_marker = False

    for page_text in pages_text:
        lines = clean_lines(page_text)
        all_lines.extend(lines)
        for line in lines:
            normalized = normalize_spaces(line)
            if is_draw_start_marker(normalized):
                in_main_draw = True
                found_start_marker = True
                continue
            if in_main_draw and is_draw_stop_marker(normalized):
                in_main_draw = False
                continue
            if in_main_draw:
                scoped_lines.append(normalized)

    primary_rows = parse_draw_positions_from_lines(scoped_lines)
    if len(primary_rows) in SUPPORTED_DRAW_SIZES:
        return primary_rows

    fallback_rows = parse_draw_positions_from_lines(all_lines)
    if len(fallback_rows) in SUPPORTED_DRAW_SIZES:
        return fallback_rows

    sample_scoped = scoped_lines[:20]
    sample_all = all_lines[:20]
    raise RuntimeError(
        "Impossibile riconoscere il main draw dal PDF. "
        f"start_marker={found_start_marker} | posizioni_scoped={len(primary_rows)} | posizioni_fallback={len(fallback_rows)} | "
        f"sample_scoped={sample_scoped} | sample_all={sample_all}"
    )


def classify_result_outcome(score_raw: str) -> str:
    score_raw = (score_raw or "").strip()
    if not score_raw:
        return "unknown"
    if RESULT_WALKOVER_RE.search(score_raw):
        return "walkover"
    if RESULT_RETIREMENT_RE.search(score_raw):
        return "retirement"
    return "completed"


def parse_score_pairs_from_score_raw(score_raw: str) -> list[tuple[int, int]]:
    score_raw = (score_raw or "").strip()
    if not score_raw:
        return []

    cleaned = re.sub(r"\(\d+\)", "", score_raw)
    cleaned = cleaned.replace("–", "-").replace("—", "-")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    pairs: list[tuple[int, int]] = []
    for tok in cleaned.split():
        m = re.fullmatch(r"(\d{1,2})-(\d{1,2})", tok)
        if m:
            pairs.append((int(m.group(1)), int(m.group(2))))
    return pairs


def is_completed_set_score(a: int, b: int) -> bool:
    if a == 6 and 0 <= b <= 4:
        return True
    if b == 6 and 0 <= a <= 4:
        return True
    return (a, b) in {(7, 5), (5, 7), (7, 6), (6, 7)}


def count_sets_from_pairs(pairs: list[tuple[int, int]]) -> tuple[int, int]:
    a_sets = 0
    b_sets = 0
    for a, b in pairs:
        if a > b:
            a_sets += 1
        elif b > a:
            b_sets += 1
    return a_sets, b_sets


def format_scores_from_result(player_a: str, player_b: str, winner: str, res: dict) -> tuple[str, str]:
    outcome = res.get("outcome_type", "unknown")
    pairs = parse_score_pairs_from_score_raw(res.get("score_raw", ""))

    res_p1 = (res.get("player1_name_raw", "") or "").strip()
    res_p2 = (res.get("player2_name_raw", "") or "").strip()

    if isinstance(res.get("player1_sets_won"), int) and isinstance(res.get("player2_sets_won"), int):
        a_sets = int(res["player1_sets_won"])
        b_sets = int(res["player2_sets_won"])
        if res_p1 and res_p2:
            if normalize_person_name_for_matching(res_p1) == normalize_person_name_for_matching(player_b) and normalize_person_name_for_matching(res_p2) == normalize_person_name_for_matching(player_a):
                a_sets, b_sets = b_sets, a_sets
        if winner == player_a and b_sets > a_sets:
            a_sets, b_sets = b_sets, a_sets
        if winner == player_b and a_sets > b_sets:
            a_sets, b_sets = b_sets, a_sets
        return (str(a_sets) if a_sets or b_sets else "", str(b_sets) if a_sets or b_sets else "")

    def same_player(x: str, y: str) -> bool:
        return normalize_person_name_for_matching(x) == normalize_person_name_for_matching(y)

    def orient_pairs_to_csv(raw_pairs: list[tuple[int, int]], use_completed_only: bool = False) -> list[tuple[int, int]]:
        if not raw_pairs:
            return raw_pairs
        if res_p1 and res_p2:
            if same_player(res_p1, player_a) and same_player(res_p2, player_b):
                return raw_pairs
            if same_player(res_p1, player_b) and same_player(res_p2, player_a):
                return [(b, a) for a, b in raw_pairs]

        pairs_for_check = raw_pairs
        if use_completed_only:
            pairs_for_check = [(a, b) for a, b in raw_pairs if is_completed_set_score(a, b)]

        a_sets, b_sets = count_sets_from_pairs(pairs_for_check)
        if winner == player_a and b_sets > a_sets:
            return [(b, a) for a, b in raw_pairs]
        if winner == player_b and a_sets > b_sets:
            return [(b, a) for a, b in raw_pairs]
        return raw_pairs

    if outcome == "walkover":
        if winner == player_a:
            return "W/O", ""
        if winner == player_b:
            return "", "W/O"
        return "", ""

    if outcome == "retirement":
        aligned_pairs = orient_pairs_to_csv(pairs, use_completed_only=True)
        completed_pairs = [(a, b) for a, b in aligned_pairs if is_completed_set_score(a, b)]
        incomplete_pairs = [(a, b) for a, b in aligned_pairs if not is_completed_set_score(a, b)]
        a_sets, b_sets = count_sets_from_pairs(completed_pairs)

        if incomplete_pairs and len(completed_pairs) == 2:
            a_sets, b_sets = 1, 1

        a_val = str(a_sets) if (completed_pairs or incomplete_pairs) else ""
        b_val = str(b_sets) if (completed_pairs or incomplete_pairs) else ""

        if winner == player_a:
            return a_val, f"(rit.) {b_val}" if b_val else "(rit.)"
        if winner == player_b:
            return f"(rit.) {a_val}" if a_val else "(rit.)", b_val
        return a_val, b_val

    aligned_pairs = orient_pairs_to_csv(pairs, use_completed_only=False)
    a_sets, b_sets = count_sets_from_pairs(aligned_pairs)
    a_val = str(a_sets) if aligned_pairs else ""
    b_val = str(b_sets) if aligned_pairs else ""
    return a_val, b_val


def extract_json_candidates_from_html(html: str) -> list[str]:
    candidates: list[str] = []
    patterns = [
        r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, html, flags=re.DOTALL | re.IGNORECASE):
            candidates.append(match.group(1))
    for match in re.finditer(r'(\{.*?(?:Game Set and Match|wins the match).*?\})', html, flags=re.DOTALL):
        candidates.append(match.group(1))
    return candidates


def _walk_json(obj):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _walk_json(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_json(item)


def dedupe_results(results: list[dict]) -> list[dict]:
    seen = set()
    out: list[dict] = []
    for r in results:
        key = (
            r.get("round", ""),
            normalize_person_name_for_matching(r.get("winner_name_raw", "")),
            normalize_spaces(r.get("score_raw", "")),
            normalize_person_name_for_matching(r.get("player1_name_raw", "")),
            normalize_person_name_for_matching(r.get("player2_name_raw", "")),
            r.get("outcome_type", "unknown"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _looks_like_player_name_line(line: str) -> bool:
    line = normalize_spaces(line)
    if not line:
        return False
    if line.startswith("Image:") or line.startswith("Ump:") or line.startswith("Winner:"):
        return False
    if line in {"H2H", "Stats", "Print", "Refresh"}:
        return False
    if map_atp_round_to_canonical(line):
        return False
    if re.match(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun),", line):
        return False
    if re.match(r"^Day\s*\(", line):
        return False
    if re.match(r"^\d{4}\.\d{2}\.\d{2}$", line):
        return False
    if re.fullmatch(r"\d{1,2}(?:[:.]\d{2}){0,2}", line):
        return False
    if re.fullmatch(r"\d{1,2}", line):
        return False
    if re.fullmatch(r"\d{1,2}\s+\d{1,2}", line):
        return False
    return bool(re.search(r"[A-Za-zÀ-ÿ]", line))


def _clean_results_player_name(line: str) -> str:
    line = normalize_spaces(line)
    return re.sub(r"\s*\(([^)]*)\)\s*$", "", line).strip()


def _is_numeric_score_line(line: str) -> bool:
    return bool(re.fullmatch(r"\d{1,2}(?:\s+\d{1,2})*", normalize_spaces(line)))


def _leading_score_number(line: str) -> int | None:
    m = re.match(r"^(\d{1,2})", normalize_spaces(line))
    return int(m.group(1)) if m else None


def _extract_sets_won_from_block(score1_lines: list[str], score2_lines: list[str]) -> tuple[int, int]:
    a_sets = 0
    b_sets = 0
    for left, right in zip(score1_lines, score2_lines):
        a = _leading_score_number(left)
        b = _leading_score_number(right)
        if a is None or b is None:
            continue
        if a > b:
            a_sets += 1
        elif b > a:
            b_sets += 1
    return a_sets, b_sets


def _parse_results_blocks_from_lines(lines: list[str]) -> list[dict]:
    results: list[dict] = []
    i = 0
    while i < len(lines):
        line = normalize_spaces(lines[i])
        canonical_round = map_atp_round_to_canonical(line)
        if not canonical_round:
            i += 1
            continue

        j = i + 1
        block: list[str] = []
        while j < len(lines):
            nxt = normalize_spaces(lines[j])
            if map_atp_round_to_canonical(nxt) or re.match(r"^####\s+", nxt):
                break
            block.append(nxt)
            j += 1

        player_lines: list[tuple[int, str]] = []
        for idx, item in enumerate(block):
            cleaned = _clean_results_player_name(item)
            lowered = cleaned.lower()
            if not _looks_like_player_name_line(cleaned):
                continue
            if cleaned.startswith("Game Set and Match ") or " wins the match " in cleaned:
                continue
            if cleaned.startswith("Winner:"):
                continue
            if any(fragment in lowered for fragment in ["serve fault", "match point", "break point", "set point", "ace", "double fault"]):
                continue
            player_lines.append((idx, cleaned))

        players: list[str] = []
        player_indexes: list[int] = []
        seen: set[str] = set()
        for idx, name in player_lines:
            if name and name not in seen:
                seen.add(name)
                players.append(name)
                player_indexes.append(idx)
            if len(players) == 2:
                break

        winner_name = ""
        score_raw = ""
        explicit_a_sets = None
        explicit_b_sets = None
        for item in block:
            m = re.search(
                r"Game Set and Match\s+([A-Za-zÀ-ÿ'`\-.\s]+?)\.\s+.*?wins the match\s+([0-9\-\(\)\sA-Za-z/.]+?)\s*\.?$",
                item,
                re.IGNORECASE,
            )
            if m:
                winner_name = normalize_spaces(m.group(1))
                score_raw = normalize_spaces(m.group(2))
                break
            m = re.search(r"Winner:\s+([A-Za-zÀ-ÿ'`\-.\s]+?)(?:\s+by\s+(Walkover))?$", item, re.IGNORECASE)
            if m:
                winner_name = normalize_spaces(m.group(1))
                if m.group(2):
                    score_raw = "W/O"
                break

        if len(players) == 2 and (not score_raw or not winner_name):
            idx1, idx2 = player_indexes[0], player_indexes[1]
            score1_lines = [x for x in block[idx1 + 1:idx2] if _is_numeric_score_line(x)]
            tail_end = len(block)
            for stop_idx in range(idx2 + 1, len(block)):
                if block[stop_idx].startswith("Ump:") or block[stop_idx].startswith("Winner:") or block[stop_idx].startswith("Game Set and Match"):
                    tail_end = stop_idx
                    break
            score2_lines = [x for x in block[idx2 + 1:tail_end] if _is_numeric_score_line(x)]
            if score1_lines and score2_lines:
                explicit_a_sets, explicit_b_sets = _extract_sets_won_from_block(score1_lines, score2_lines)
                if explicit_a_sets or explicit_b_sets:
                    if not winner_name:
                        winner_name = players[0] if explicit_a_sets > explicit_b_sets else players[1] if explicit_b_sets > explicit_a_sets else ""
                    if not score_raw:
                        score_raw = f"{explicit_a_sets}-{explicit_b_sets}"

        if players or winner_name or score_raw:
            result = {
                "round": canonical_round,
                "winner_name_raw": winner_name,
                "score_raw": score_raw,
                "player1_name_raw": players[0] if len(players) > 0 else "",
                "player2_name_raw": players[1] if len(players) > 1 else "",
                "outcome_type": classify_result_outcome(score_raw),
                "source": "results_page_text",
            }
            if explicit_a_sets is not None and explicit_b_sets is not None:
                result["player1_sets_won"] = explicit_a_sets
                result["player2_sets_won"] = explicit_b_sets
            results.append(result)
        i = j
    return results

def fetch_results_page(results_page_url: str, session: requests.Session | None = None) -> list[dict]:
    session = session or make_requests_session()
    resp = http_get(session, results_page_url, timeout=30)
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    results: list[dict] = []

    for candidate in extract_json_candidates_from_html(html):
        try:
            data = json.loads(candidate)
        except Exception:
            continue
        for obj in _walk_json(data):
            if not isinstance(obj, dict):
                continue
            round_text = winner_name = score_raw = player1 = player2 = None
            for key in ("round", "roundName", "Round", "matchRound"):
                if isinstance(obj.get(key), str):
                    round_text = obj[key]
                    break
            for key in ("winnerName", "WinnerName", "winningPlayerName", "winner"):
                if isinstance(obj.get(key), str):
                    winner_name = obj[key]
                    break
            for key in ("score", "Score", "matchScore", "result"):
                if isinstance(obj.get(key), str):
                    score_raw = obj[key]
                    break
            for key in ("player1Name", "Player1Name", "homePlayerName"):
                if isinstance(obj.get(key), str):
                    player1 = obj[key]
                    break
            for key in ("player2Name", "Player2Name", "awayPlayerName"):
                if isinstance(obj.get(key), str):
                    player2 = obj[key]
                    break
            canonical_round = map_atp_round_to_canonical(round_text or "")
            if not canonical_round:
                continue
            if not winner_name and not score_raw and not (player1 and player2):
                continue
            results.append({
                "round": canonical_round,
                "winner_name_raw": (winner_name or "").strip(),
                "score_raw": (score_raw or "").strip(),
                "player1_name_raw": (player1 or "").strip(),
                "player2_name_raw": (player2 or "").strip(),
                "outcome_type": classify_result_outcome(score_raw or ""),
                "source": "results_page_json",
            })

    text_compact = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    pattern = re.compile(
        r"(Round of 128|Round of 96|Round of 64|Round of 48|Round of 32|Round of 24|Round of 16|Quarterfinals|Semifinals|Final)"
        r".*?Game Set and Match\s+([A-Za-zÀ-ÿ'`\-.\s]+?)\.\s+"
        r"(?:\2)\s+wins the match\s+([0-9\-\(\)\sA-Za-z/.]+?)\s*\.",
        re.IGNORECASE,
    )
    for match in pattern.finditer(text_compact):
        round_text = match.group(1).strip()
        winner_name = " ".join(match.group(2).split())
        score_raw = " ".join(match.group(3).split())
        canonical_round = map_atp_round_to_canonical(round_text)
        if not canonical_round:
            continue
        results.append({
            "round": canonical_round,
            "winner_name_raw": winner_name,
            "score_raw": score_raw,
            "player1_name_raw": "",
            "player2_name_raw": "",
            "outcome_type": classify_result_outcome(score_raw),
            "source": "results_page_regex",
        })

    lines = [normalize_spaces(s) for s in soup.stripped_strings]
    results.extend(_parse_results_blocks_from_lines(lines))
    return dedupe_results(results)

def group_results_by_round(results: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for result in results:
        grouped.setdefault(result["round"], []).append(result)
    return grouped

def infer_current_results_page_url(results_page_url: str, draw_page_url: str) -> str:
    results_page_url = (results_page_url or "").strip()
    draw_page_url = (draw_page_url or "").strip()
    if results_page_url and "/current/" in results_page_url and results_page_url.rstrip('/').endswith('/results'):
        return results_page_url
    if results_page_url and "/archive/" in results_page_url:
        m = re.search(r"/archive/([^/]+)/([^/]+)/\d{4}/results/?$", results_page_url)
        if m:
            slug, tournament_id = m.group(1), m.group(2)
            return f"https://www.atptour.com/en/scores/current/{slug}/{tournament_id}/results"
    if draw_page_url and "/current/" in draw_page_url:
        return re.sub(r"/draws(?:/.*)?$", "/results", draw_page_url.rstrip('/'))
    return ""


def fetch_final_result_from_current_results(current_results_url: str, session: requests.Session | None = None) -> dict | None:
    current_results_url = (current_results_url or "").strip()
    if not current_results_url:
        return None
    session = session or make_requests_session()
    try:
        resp = http_get(session, current_results_url, timeout=30)
    except Exception:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    lines = [normalize_spaces(s) for s in soup.stripped_strings]
    final_results = [r for r in _parse_results_blocks_from_lines(lines) if r.get("round") == "F"]
    if final_results:
        for r in final_results:
            if r.get("player1_name_raw") and r.get("player2_name_raw") and r.get("winner_name_raw") and r.get("score_raw"):
                return r
        return final_results[0]

    text_compact = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    m = re.search(
        r"Final(?:\s*-\s*[^.]+?)?.*?([A-Z][A-Za-zÀ-ÿ'`.-]+(?:\s+[A-Z][A-Za-zÀ-ÿ'`.-]+)*)(?:\s*\((\d+)\))?.*?"
        r"([A-Z][A-Za-zÀ-ÿ'`.-]+(?:\s+[A-Z][A-Za-zÀ-ÿ'`.-]+)*)(?:\s*\((\d+)\))?.*?"
        r"Game Set and Match\s+([A-Za-zÀ-ÿ'`\-.\s]+?)\.\s+\5\s+wins the match\s+([0-9\-\(\)\sA-Za-z/.]+?)\s*\.",
        text_compact,
        re.IGNORECASE,
    )
    if not m:
        return None
    p1 = m.group(1).strip() + (f" ({m.group(2)})" if m.group(2) else "")
    p2 = m.group(3).strip() + (f" ({m.group(4)})" if m.group(4) else "")
    return {
        "round": "F",
        "player1_name_raw": p1,
        "player2_name_raw": p2,
        "winner_name_raw": " ".join(m.group(5).split()),
        "score_raw": " ".join(m.group(6).split()),
        "outcome_type": classify_result_outcome(m.group(6)),
        "source": "current_results_page",
    }


def find_formatted_name_match(name_raw: str, candidates: list[str]) -> str:
    target_norm = normalize_person_name_for_matching(name_raw)
    if not target_norm:
        return ""

    # 1) exact normalized match
    for candidate in candidates:
        if normalize_person_name_for_matching(candidate) == target_norm:
            return candidate

    target_parts = target_norm.split()
    target_surname = target_parts[-1] if target_parts else ""
    target_initial = target_parts[0][0] if target_parts and target_parts[0] else ""

    # 2) surname + first initial match
    strong_matches: list[str] = []
    for candidate in candidates:
        cand_norm = normalize_person_name_for_matching(candidate)
        cand_parts = cand_norm.split()
        if not cand_parts:
            continue
        cand_surname = cand_parts[-1]
        cand_initial = cand_parts[0][0] if cand_parts[0] else ""
        if cand_surname == target_surname and cand_initial == target_initial:
            strong_matches.append(candidate)
    if len(strong_matches) == 1:
        return strong_matches[0]

    # 3) token-overlap fallback for compound surnames / abbreviated names
    best = ""
    best_score = 0
    target_set = set(target_parts)
    for candidate in candidates:
        cand_parts = normalize_person_name_for_matching(candidate).split()
        if not cand_parts:
            continue
        score = len(target_set & set(cand_parts))
        if target_surname and cand_parts[-1] == target_surname:
            score += 2
        if target_initial and cand_parts[0] and cand_parts[0][0] == target_initial:
            score += 1
        if score > best_score:
            best_score = score
            best = candidate
    if best_score >= 3:
        return best

    return ""


def resolve_formatted_name_from_existing_rows(name_raw: str, candidates: list[str]) -> str:
    name_raw = normalize_spaces(name_raw)
    if not name_raw:
        return ""
    matched = find_formatted_name_match(name_raw, candidates)
    if matched:
        return matched
    return format_name(name_raw)


def apply_current_final_override(rows: list[dict], final_result: dict | None) -> list[dict]:
    if not rows or not final_result:
        return rows
    final_index = None
    for i, row in enumerate(rows):
        if row.get("Round") == "Finale":
            final_index = i
    if final_index is None:
        return rows

    semifinal_winners = []
    for row in rows:
        if row.get("Round") == "Semifinali" and row.get("Winner"):
            semifinal_winners.append(row["Winner"])
    semifinal_winners = list(dict.fromkeys(semifinal_winners))

    player_a = resolve_formatted_name_from_existing_rows(final_result.get("player1_name_raw", ""), semifinal_winners)
    player_b = resolve_formatted_name_from_existing_rows(final_result.get("player2_name_raw", ""), semifinal_winners)
    winner = resolve_winner_from_results_page(player_a, player_b, final_result.get("winner_name_raw", ""))

    sets_a, sets_b = format_scores_from_result(player_a, player_b, winner, final_result)

    row = rows[final_index]
    row["Player A"] = player_a or row.get("Player A", "")
    row["Player B"] = player_b or row.get("Player B", "")
    if winner:
        row["Winner"] = winner
    if sets_a is not None:
        row["Participant A score"] = sets_a
    if sets_b is not None:
        row["Participant B score"] = sets_b
    return rows


def build_results_name_lookup(results_list: list[dict]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for result in results_list:
        for key in ("player1_name_raw", "player2_name_raw", "winner_name_raw"):
            raw = normalize_spaces(result.get(key, ""))
            if not raw:
                continue
            if raw not in seen:
                seen.add(raw)
                names.append(raw)
    return names


def replace_truncated_pdf_names(positions: list[dict], results_list: list[dict]) -> list[dict]:
    manual = {
        "v. b…": "Botic van de Zandschulp",
        "m. g…": "Giovanni Mpetshi Perricard",
        "a. d…": "Alejandro Davidovich Fokina",
    }
    for pos in positions:
        current = normalize_spaces(pos.get("player_name", "")).lower()
        raw = normalize_spaces(pos.get("raw_name", "")).lower()
        full_name = manual.get(current) or manual.get(raw)
        if not full_name:
            continue
        pos["raw_name"] = full_name
        pos["player_name"] = format_name(
            full_name,
            seed=pos.get("seed", ""),
            entry_status=pos.get("entry_status", ""),
            country=pos.get("country", ""),
        )
    return positions


def resolve_winner_from_results_page(player_a: str, player_b: str, winner_full_name: str) -> str:
    a_norm = normalize_person_name_for_matching(player_a)
    b_norm = normalize_person_name_for_matching(player_b)
    w_norm = normalize_person_name_for_matching(winner_full_name)
    if a_norm and a_norm == w_norm:
        return player_a
    if b_norm and b_norm == w_norm:
        return player_b

    a_surname = surname_from_name(player_a)
    b_surname = surname_from_name(player_b)
    w_surname = surname_from_name(winner_full_name)
    if w_surname == a_surname and w_surname != b_surname:
        return player_a
    if w_surname == b_surname and w_surname != a_surname:
        return player_b

    a_initial = first_initial_from_name(player_a)
    b_initial = first_initial_from_name(player_b)
    w_initial = first_initial_from_name(winner_full_name)
    if w_surname == a_surname and w_initial and w_initial == a_initial and not (w_surname == b_surname and w_initial == b_initial):
        return player_a
    if w_surname == b_surname and w_initial and w_initial == b_initial and not (w_surname == a_surname and w_initial == a_initial):
        return player_b
    return ""


def match_result_to_players(player_a: str, player_b: str, res: dict) -> bool:
    p1 = normalize_person_name_for_matching(res.get("player1_name_raw", ""))
    p2 = normalize_person_name_for_matching(res.get("player2_name_raw", ""))
    a = normalize_person_name_for_matching(player_a)
    b = normalize_person_name_for_matching(player_b)
    if p1 and p2:
        return {a, b} == {p1, p2}
    winner = res.get("winner_name_raw", "")
    return bool(resolve_winner_from_results_page(player_a, player_b, winner))


def resolve_name_against_candidates(name_raw: str, candidates: set[str]) -> str:
    for candidate in candidates:
        if normalize_person_name_for_matching(candidate) == normalize_person_name_for_matching(name_raw):
            return candidate

    raw_surname = surname_from_name(name_raw)
    raw_initial = first_initial_from_name(name_raw)
    matches = []
    for candidate in candidates:
        if surname_from_name(candidate) != raw_surname:
            continue
        cand_initial = first_initial_from_name(candidate)
        if raw_initial and cand_initial and raw_initial != cand_initial:
            continue
        matches.append(candidate)
    return matches[0] if len(matches) == 1 else ""


def resolve_result_for_slots(slot_a: dict, slot_b: dict, res: dict) -> str:
    p1 = (res.get("player1_name_raw", "") or "").strip()
    p2 = (res.get("player2_name_raw", "") or "").strip()
    winner_raw = (res.get("winner_name_raw", "") or "").strip()

    a_candidates = slot_a["candidates"]
    b_candidates = slot_b["candidates"]

    if p1 and p2:
        a_p1 = resolve_name_against_candidates(p1, a_candidates)
        b_p2 = resolve_name_against_candidates(p2, b_candidates)
        a_p2 = resolve_name_against_candidates(p2, a_candidates)
        b_p1 = resolve_name_against_candidates(p1, b_candidates)

        if a_p1 and b_p2:
            return resolve_name_against_candidates(winner_raw, {a_p1, b_p2})
        if a_p2 and b_p1:
            return resolve_name_against_candidates(winner_raw, {a_p2, b_p1})
        return ""

    winner_in_a = resolve_name_against_candidates(winner_raw, a_candidates)
    winner_in_b = resolve_name_against_candidates(winner_raw, b_candidates)
    if winner_in_a and not winner_in_b:
        return winner_in_a
    if winner_in_b and not winner_in_a:
        return winner_in_b
    return ""


def slot_display_name(slot: dict) -> str:
    return slot.get("name", "") or ("TBD" if slot.get("candidates") else "")


def build_match_rows(positions: list[dict], round_results: dict[str, list[dict]]) -> list[dict]:
    current = []
    for p in positions:
        name = p["player_name"]
        current.append({
            "name": name if name != "bye" else "bye",
            "slot_type": p["slot_type"],
            "candidates": set() if name == "bye" else {name},
        })
    match_rows: list[dict] = []

    while len(current) > 1:
        current_slots = len(current)
        canonical_round = canonical_round_for_stage(current_slots)
        round_label = canonical_round_to_label(canonical_round)
        next_round = []
        results_for_round = round_results.get(canonical_round, [])
        used = [False] * len(results_for_round)

        for i in range(0, len(current), 2):
            slot_a = current[i]
            slot_b = current[i + 1]
            a_name = slot_display_name(slot_a)
            b_name = slot_display_name(slot_b)
            winner = ""
            a_sets = ""
            b_sets = ""

            if a_name == "bye" and b_name and b_name != "bye":
                winner = b_name
            elif b_name == "bye" and a_name and a_name != "bye":
                winner = a_name
            elif a_name == "bye" and b_name == "bye":
                winner = ""

            matched_res = None
            if not winner:
                for idx, res in enumerate(results_for_round):
                    if used[idx]:
                        continue
                    candidate_winner = resolve_result_for_slots(slot_a, slot_b, res)
                    if not candidate_winner:
                        continue
                    used[idx] = True
                    winner = candidate_winner
                    matched_res = res
                    break

            if matched_res is not None:
                a_sets, b_sets = format_scores_from_result(a_name, b_name, winner, matched_res)

            match_rows.append({
                "Round": round_label,
                "Player A": a_name,
                "Player B": b_name,
                "Winner": winner,
                "Participant A score": a_sets,
                "Participant B score": b_sets,
            })

            if winner:
                next_round.append({"name": winner, "slot_type": "player", "candidates": {winner}})
            else:
                next_candidates = set(slot_a.get("candidates", set())) | set(slot_b.get("candidates", set()))
                next_round.append({"name": "TBD" if next_candidates else "", "slot_type": "unknown", "candidates": next_candidates})
        current = next_round

    return match_rows


def csv_bytes(rows: list[dict]) -> bytes:
    fieldnames = ["Round", "Player A", "Player B", "Winner", "Participant A score", "Participant B score"]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8-sig")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def resolve_runtime_urls(
    tournament_url: str,
    draw_page_url: str,
    results_page_url: str,
    tournament_id: str,
    year: int,
) -> tuple[str, str, str, str]:
    tournament_url = normalize_tournament_url(tournament_url or DEFAULT_TOURNAMENT_URL)
    draw_page_url = normalize_tournament_url(draw_page_url or DEFAULT_DRAW_PAGE)
    results_page_url = normalize_tournament_url(results_page_url or DEFAULT_RESULTS_PAGE)
    tournament_id = (tournament_id or DEFAULT_TOURNAMENT_ID or "").strip()

    if tournament_url:
        if not draw_page_url:
            draw_page_url = infer_draw_page_url(tournament_url)
        if not results_page_url:
            results_page_url = infer_results_page_url_from_tournament(tournament_url)

    if not draw_page_url and results_page_url:
        draw_page_url = re.sub(r"/results$", "/draws", results_page_url)
    if not results_page_url and draw_page_url:
        results_page_url = infer_results_page_url_from_draw(draw_page_url)
    if not draw_page_url:
        raise ValueError("Devi specificare --tournament-url oppure --draw-page")
    if not tournament_id:
        tournament_id = infer_tournament_id_from_url(draw_page_url)
    if not tournament_id and tournament_url:
        tournament_id = infer_tournament_id_from_url(tournament_url)
    if not tournament_id:
        raise ValueError("Impossibile ricavare tournament_id dall'URL. Passa --tournament-id")
    fallback_pdf_url = DEFAULT_FALLBACK_PDF.format(year=year, tournament_id=tournament_id)
    return draw_page_url, results_page_url, tournament_id, fallback_pdf_url


def fetch_and_build_rows(draw_page_url: str, results_page_url: str, fallback_pdf_url: str, debug_pdf_preview: bool = False) -> tuple[list[dict], dict]:
    session = make_requests_session()
    pdf_url = discover_pdf_url(session, draw_page_url, fallback_pdf_url)
    pdf_resp = http_get(session, pdf_url, timeout=60)
    pages_text = extract_pdf_text(pdf_resp.content)

    if debug_pdf_preview:
        for index, page_text in enumerate(pages_text[:2], start=1):
            preview = "\n".join(clean_lines(page_text)[:40])
            print(f"\n--- PDF PAGE {index} PREVIEW ---\n{preview}\n", file=sys.stderr, flush=True)

    released_at = extract_released_at(pages_text)
    positions = parse_draw_positions(pages_text)
    results_list = fetch_results_page(results_page_url, session=session)
    positions = replace_truncated_pdf_names(positions, results_list)
    round_results = group_results_by_round(results_list)
    rows = build_match_rows(positions, round_results)
    current_results_url = infer_current_results_page_url(results_page_url, draw_page_url)
    current_final_result = fetch_final_result_from_current_results(current_results_url, session=session)
    rows = apply_current_final_override(rows, current_final_result)
    meta = {
        "source_draw_page": draw_page_url,
        "source_pdf": pdf_url,
        "source_results_page": results_page_url,
        "source_current_results_page": current_results_url,
        "released_at": released_at,
        "fetched_at": utc_now_iso(),
        "positions": len(positions),
        "matches": len(rows),
        "results_found": len(results_list),
    }
    return rows, meta


def write_csv_if_changed(output_path: Path, data: bytes) -> bool:
    if output_path.exists() and output_path.read_bytes() == data:
        return False
    output_path.write_bytes(data)
    return True


def run_once(
    output_path: Path,
    tournament_url: str,
    draw_page_url: str,
    results_page_url: str,
    tournament_id: str,
    year: int,
    debug_pdf_preview: bool = False,
) -> bool:
    draw_page_url, results_page_url, tournament_id, fallback_pdf_url = resolve_runtime_urls(
        tournament_url=tournament_url,
        draw_page_url=draw_page_url,
        results_page_url=results_page_url,
        tournament_id=tournament_id,
        year=year,
    )
    rows, meta = fetch_and_build_rows(draw_page_url, results_page_url, fallback_pdf_url, debug_pdf_preview=debug_pdf_preview)
    data = csv_bytes(rows)
    changed = write_csv_if_changed(output_path, data)
    status = "AGGIORNATO" if changed else "NESSUNA MODIFICA"
    print(
        f"[{utc_now_iso()}] {status} | file={output_path} | matches={meta['matches']} | "
        f"results_found={meta['results_found']} | released_at={meta['released_at'] or 'n/d'} | "
        f"sha256={sha256(data)[:12]} | pdf={meta['source_pdf']}",
        flush=True,
    )
    return changed


class RetirementWalkoverTests(unittest.TestCase):
    def test_retirement_incomplete_third_set_keeps_one_one(self) -> None:
        res = {
            "outcome_type": "retirement",
            "score_raw": "6-3 6-3 3-0 RET",
            "player1_name_raw": "L. Ambrogi [Alt]",
            "player2_name_raw": "S. Rodriguez Taverna",
        }
        a_score, b_score = format_scores_from_result(
            "S. Rodriguez Taverna",
            "L. Ambrogi [Alt]",
            "L. Ambrogi [Alt]",
            res,
        )
        self.assertEqual(a_score, "(rit.) 1")
        self.assertEqual(b_score, "1")

    def test_retirement_aligned_score_keeps_one_one(self) -> None:
        res = {
            "outcome_type": "retirement",
            "score_raw": "6-3 3-6 0-3 RET",
            "player1_name_raw": "S. Rodriguez Taverna",
            "player2_name_raw": "L. Ambrogi [Alt]",
        }
        a_score, b_score = format_scores_from_result(
            "S. Rodriguez Taverna",
            "L. Ambrogi [Alt]",
            "L. Ambrogi [Alt]",
            res,
        )
        self.assertEqual(a_score, "(rit.) 1")
        self.assertEqual(b_score, "1")

    def test_walkover_winner_player_a(self) -> None:
        res = {"outcome_type": "walkover", "score_raw": "W/O", "player1_name_raw": "A. Player", "player2_name_raw": "B. Player"}
        a_score, b_score = format_scores_from_result("A. Player", "B. Player", "A. Player", res)
        self.assertEqual(a_score, "W/O")
        self.assertEqual(b_score, "")

    def test_walkover_winner_player_b(self) -> None:
        res = {"outcome_type": "walkover", "score_raw": "W/O", "player1_name_raw": "A. Player", "player2_name_raw": "B. Player"}
        a_score, b_score = format_scores_from_result("A. Player", "B. Player", "B. Player", res)
        self.assertEqual(a_score, "")
        self.assertEqual(b_score, "W/O")

    def test_classify_result_outcome(self) -> None:
        self.assertEqual(classify_result_outcome("6-3 3-0 RET"), "retirement")
        self.assertEqual(classify_result_outcome("W/O"), "walkover")
        self.assertEqual(classify_result_outcome("6-4 7-6(5)"), "completed")

    def test_completed_match_scores_follow_csv_order(self) -> None:
        res = {
            "score_raw": "6-2 6-3",
            "outcome_type": "completed",
            "player1_name_raw": "J. Varillas",
            "player2_name_raw": "M. Kestelboim [Alt]",
        }
        a_score, b_score = format_scores_from_result(
            "M. Kestelboim [Alt]",
            "J. Varillas",
            "J. Varillas",
            res,
        )
        self.assertEqual(a_score, "0")
        self.assertEqual(b_score, "2")

    def test_parse_draw_positions_accepts_fallback_without_marker(self) -> None:
        lines = [
            "1 Bye",
            "2 WC 12 ALCARAZ Carlos ESP",
            "3 Qualifier",
            "4 DJOKOVIC Novak SRB",
            "5 Bye",
            "6 SINNER Jannik ITA",
            "7 Bye",
            "8 MEDVEDEV Daniil",
            "9 Bye",
            "10 ZVEREV Alexander GER",
            "11 Bye",
            "12 RUNE Holger DEN",
            "13 Bye",
            "14 PAUL Tommy USA",
            "15 Bye",
            "16 MUSETTI Lorenzo ITA",
        ]
        rows = parse_draw_positions(["\n".join(lines)])
        self.assertEqual(len(rows), 16)
        self.assertEqual(rows[1]["player_name"], "C. Alcaraz [12] [WC]")

    def test_tiebreak_score_counts_only_sets(self) -> None:
        self.assertEqual(parse_score_pairs_from_score_raw("7-6(3) 6-7(5) 6-3"), [(7, 6), (6, 7), (6, 3)])

    def test_tiebreak_score_sets_result_is_2_1(self) -> None:
        res = {"score_raw": "7-6(3) 6-7(5) 6-3", "outcome_type": "completed"}
        a_score, b_score = format_scores_from_result("J. Cerundolo", "B. van de Zandschulp", "J. Cerundolo", res)
        self.assertEqual((a_score, b_score), ("2", "1"))

    def test_mc_name_is_formatted_correctly(self) -> None:
        self.assertEqual(format_name("MCDONALD Mackenzie"), "M. McDonald")

    def test_family_name_first_rule_also_applies_to_tpe_and_hkg(self) -> None:
        self.assertEqual(format_name("Wang Tzu-Wei", country="TPE"), "T. Wang")
        self.assertEqual(format_name("Wong Hong Kit", country="HKG"), "H. Wong")

    def test_naomi_osaka_is_excluded_from_family_name_first_rule(self) -> None:
        self.assertEqual(format_name("Naomi Osaka", country="JPN"), "N. Osaka")

    def test_van_assche_keeps_capital_v(self) -> None:
        self.assertEqual(format_name("VAN ASSCHE Luca"), "L. Van Assche")

    def test_future_rounds_are_kept_as_tbd(self) -> None:
        positions = [
            {"player_name": "A. One", "slot_type": "player"},
            {"player_name": "B. Two", "slot_type": "player"},
            {"player_name": "C. Three", "slot_type": "player"},
            {"player_name": "D. Four", "slot_type": "player"},
        ]
        round_results = {
            "SF": [{
                "round": "SF",
                "winner_name_raw": "A. One",
                "score_raw": "6-4 6-4",
                "player1_name_raw": "A. One",
                "player2_name_raw": "B. Two",
                "outcome_type": "completed",
            }]
        }
        rows = build_match_rows(positions, round_results)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[-1]["Round"], "Finale")
        self.assertEqual(rows[-1]["Player A"], "A. One")
        self.assertEqual(rows[-1]["Player B"], "TBD")

    def test_canonical_round_mapping_for_128_draw(self) -> None:
        self.assertEqual(canonical_round_to_label(canonical_round_for_stage(128)), "1° turno")
        self.assertEqual(canonical_round_to_label(canonical_round_for_stage(64)), "2° turno")
        self.assertEqual(canonical_round_to_label(canonical_round_for_stage(32)), "3° turno")
        self.assertEqual(canonical_round_to_label(canonical_round_for_stage(16)), "Ottavi di finale")


def run_tests() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(RetirementWalkoverTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera un CSV match-by-match dal draw ATP ufficiale.")
    parser.add_argument("--output", default="matches_format.csv", help="Percorso del file CSV da creare/aggiornare")
    parser.add_argument("--tournament-url", default=DEFAULT_TOURNAMENT_URL, help="URL base del torneo")
    parser.add_argument("--draw-page", default=DEFAULT_DRAW_PAGE, help="URL della pagina ATP del draw")
    parser.add_argument("--results-page", default=DEFAULT_RESULTS_PAGE, help="URL della pagina ATP Results")
    parser.add_argument("--tournament-id", default=DEFAULT_TOURNAMENT_ID, help="ID torneo ATP/Challenger")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="Anno usato per il PDF fallback ProTennisLive")
    parser.add_argument("--watch", action="store_true", help="Resta in esecuzione e aggiorna il CSV a intervalli regolari")
    parser.add_argument("--interval", type=int, default=1800, help="Intervallo in secondi in modalità --watch")
    parser.add_argument("--run-tests", action="store_true", help="Esegue i test automatici RET/W/O ed esce")
    parser.add_argument("--debug-pdf-preview", action="store_true", help="Stampa su stderr un'anteprima del testo estratto dal PDF")
    args = parser.parse_args()

    if args.run_tests:
        return run_tests()

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.watch:
        run_once(
            output_path,
            args.tournament_url,
            args.draw_page,
            args.results_page,
            args.tournament_id,
            args.year,
            debug_pdf_preview=args.debug_pdf_preview,
        )
        return 0

    while True:
        try:
            run_once(
                output_path,
                args.tournament_url,
                args.draw_page,
                args.results_page,
                args.tournament_id,
                args.year,
                debug_pdf_preview=args.debug_pdf_preview,
            )
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            print(f"[{utc_now_iso()}] ERRORE | {exc}", file=sys.stderr, flush=True)
        time.sleep(max(30, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())

