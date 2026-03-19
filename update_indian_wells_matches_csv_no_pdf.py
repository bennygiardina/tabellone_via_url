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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

DEFAULT_TOURNAMENT_URL = "https://www.atptour.com/en/scores/current/indian-wells/404"
DEFAULT_DRAW_PAGE = ""
DEFAULT_RESULTS_PAGE = ""
DEFAULT_TOURNAMENT_ID = "404"

LOWERCASE_PARTICLES = {
    "de", "del", "della", "di", "da", "dos", "das",
    "van", "von", "der", "den", "la", "le"
}

STATUS_LABELS = {
    "WC": "[WC]",
    "Q": "[Q]",
    "LL": "[LL]",
    "PR": "[PR]",
    "ALT": "[Alt]",
}

ROUND_HEADER_TO_LABEL = {
    "Round of 128": "1° turno",
    "Round of 96": "1° turno",
    "Round of 64": "1° turno",
    "Round of 48": "1° turno",
    "Round of 32": "1° turno",
    "Round of 24": "1° turno",
    "Round of 16": "Ottavi di finale",
    "Quarterfinals": "Quarti di finale",
    "Semifinals": "Semifinali",
    "Final": "Finale",
}

RESULT_RETIREMENT_RE = re.compile(r"\b(?:RET|Ret|ret|retired|retirement|RIT\.?)\b", re.IGNORECASE)
RESULT_WALKOVER_RE = re.compile(r"\b(?:W/O|WO|walkover|walk-over)\b", re.IGNORECASE)


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
    return token[:1].upper() + token[1:].lower()


def smart_join_tokens(tokens: list[str]) -> str:
    return " ".join(smart_title_token(tok) for tok in tokens if tok)


def get_round_label(round_size: int, initial_draw_size: int) -> str:
    if round_size == 1:
        return "Finale"
    if round_size == 2:
        return "Semifinali"
    if round_size == 4:
        return "Quarti di finale"
    if round_size == 8:
        return "Ottavi di finale"

    known = {
        128: {64: "1° turno", 32: "2° turno", 16: "3° turno"},
        96:  {48: "1° turno", 32: "2° turno", 16: "3° turno"},
        64:  {32: "1° turno", 16: "2° turno"},
        56:  {32: "1° turno", 16: "2° turno"},
        48:  {24: "1° turno", 16: "2° turno"},
        32:  {16: "1° turno"},
        28:  {16: "1° turno"},
        24:  {12: "1° turno"},
        16:  {8: "1° turno"},
    }
    return known.get(initial_draw_size, {}).get(round_size, f"Round {round_size}")


def map_atp_round_to_label(round_text: str) -> str | None:
    return ROUND_HEADER_TO_LABEL.get((round_text or "").strip())


def format_name(raw_name: str, seed: str = "", entry_status: str = "", country: str = "") -> str:
    raw_name = (raw_name or "").replace(",", "").strip()
    country = (country or "").strip().upper()

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
        surname_tokens = []
        given_tokens = []

        for i, tok in enumerate(tokens):
            if tok.isupper():
                surname_tokens.append(tok)
            else:
                given_tokens = tokens[i:]
                break

        if not surname_tokens or not given_tokens:
            surname_tokens = tokens[:-1]
            given_tokens = [tokens[-1]]

        surname = smart_join_tokens(surname_tokens)
        given_name = smart_join_tokens(given_tokens)
        first_initial = f"{given_name[0].upper()}." if given_name else ""
        base_name = f"{first_initial} {surname}".strip()

    extras = []
    if seed:
        extras.append(f"[{seed}]")
    if entry_status in STATUS_LABELS:
        extras.append(STATUS_LABELS[entry_status])

    if extras:
        return f"{base_name} {' '.join(extras)}"
    return base_name


def is_tournament_metadata(text: str) -> bool:
    if not text:
        return False

    t = text.strip().lower()
    month_words = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december"
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
    if not url:
        return ""
    return url.rstrip("/")


def infer_tournament_id_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")

    for i in range(len(parts) - 1):
        if parts[i] in {"current", "current-challenger"} and i + 2 < len(parts):
            candidate = parts[i + 2]
            if candidate.isdigit():
                return candidate

    matches = re.findall(r"/(\d+)(?:/|$)", path)
    if matches:
        return matches[-1]
    return ""


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


def clean_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line and line.strip()]


def extract_released_at(pages_text: Iterable[str]) -> str:
    for page_text in pages_text:
        lines = clean_lines(page_text)
        for i, line in enumerate(lines):
            if line == "Released" and i + 1 < len(lines):
                return lines[i + 1]
    return ""


def is_score_line(text: str) -> bool:
    text = (text or "").strip()
    if not text:
        return False
    return bool(re.fullmatch(r"\d{1,2}\s+\d{1,2}(?:\s+\d{1,2})*", text))


def normalize_person_name_for_matching(name: str) -> str:
    name = (name or "").strip().lower()
    name = re.sub(r"\([^)]*\)", "", name)
    name = re.sub(r"\[[^\]]+\]", "", name)
    name = name.replace(".", " ")
    name = re.sub(r"\s+", " ", name).strip()
    return name


def surname_from_name(name: str) -> str:
    n = normalize_person_name_for_matching(name)
    parts = n.split()
    return parts[-1] if parts else ""


def first_initial_from_name(name: str) -> str:
    n = normalize_person_name_for_matching(name)
    parts = n.split()
    if len(parts) >= 2 and parts[0]:
        return parts[0][0]
    return ""


def parse_draw_line(line: str) -> dict | None:
    line = line.strip()
    if not line:
        return None
    if is_score_line(line):
        return None

    m = re.match(r"^(\d{1,3})\s+(.*)$", line)
    if not m:
        m = re.match(r"^(\d{1,3})(WC|PR|Q|LL|ALT|Alt|alt)\s+(.*)$", line)
        if m:
            position = int(m.group(1))
            if position < 1:
                return None
            rest = f"{m.group(2)} {m.group(3)}".strip()
        else:
            return None
    else:
        position = int(m.group(1))
        if position < 1:
            return None
        rest = m.group(2).strip()

    if position > 128:
        return None

    entry_status = ""
    seed = ""
    country = ""
    slot_type = "player"
    raw_name = ""
    display_name = ""

    if rest == "Bye":
        raw_name = rest
        display_name = "bye"
        slot_type = "bye"
    elif rest.strip() == "Qualifier / Lucky Loser":
        raw_name = rest
        display_name = "[Q/LL]"
        slot_type = "qualifier_or_lucky_loser"
    elif rest.strip() == "Qualifier":
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


def parse_draw_positions(pages_text: list[str]) -> list[dict]:
    rows: list[dict] = []
    seen_positions: set[int] = set()
    in_main_draw = False

    stop_markers = {
        "Round of 32", "Round of 16", "Quarterfinals", "Semifinals", "Final", "Winner",
        "Last Direct Acceptance", "ATP Supervisor", "Released", "Seeded Players",
        "Alternates/Lucky Losers", "Withdrawals", "Retirements/W.O.",
    }

    for page_text in pages_text:
        lines = clean_lines(page_text)
        for line in lines:
            normalized = line.strip()
            if normalized == "Main Draw Singles":
                in_main_draw = True
                continue
            if not in_main_draw:
                continue
            if normalized in stop_markers:
                in_main_draw = False
                break
            parsed = parse_draw_line(normalized)
            if not parsed:
                continue
            pos = parsed["draw_position"]
            if pos in seen_positions:
                continue
            seen_positions.add(pos)
            rows.append(parsed)

    rows.sort(key=lambda x: x["draw_position"])
    draw_size = len(rows)
    if draw_size not in {16, 24, 28, 32, 48, 56, 64, 96, 128}:
        raise RuntimeError(f"Draw non supportato: {draw_size} posizioni")
    return rows


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

    # Rimuove completamente i tie-break: 7-6(3) -> 7-6
    cleaned = re.sub(r"\(\d+\)", "", score_raw)
    cleaned = cleaned.replace("–", "-").replace("—", "-")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    pairs: list[tuple[int, int]] = []
    for tok in cleaned.split():
        m = re.fullmatch(r"(\d{1,2})-(\d{1,2})", tok)
        if not m:
            continue
        pairs.append((int(m.group(1)), int(m.group(2))))
    return pairs


def is_completed_set_score(a: int, b: int) -> bool:
    if a == 6 and 0 <= b <= 4:
        return True
    if b == 6 and 0 <= a <= 4:
        return True
    if (a, b) in {(7, 5), (5, 7), (7, 6), (6, 7)}:
        return True
    return False


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

    def same_player(x: str, y: str) -> bool:
        return normalize_person_name_for_matching(x) == normalize_person_name_for_matching(y)

    def orient_pairs_to_csv(raw_pairs: list[tuple[int, int]], use_completed_only: bool = False) -> list[tuple[int, int]]:
        if not raw_pairs:
            return raw_pairs

        # 1) Prova ad allineare usando player1/player2 del source
        if res_p1 and res_p2:
            if same_player(res_p1, player_a) and same_player(res_p2, player_b):
                return raw_pairs
            if same_player(res_p1, player_b) and same_player(res_p2, player_a):
                return [(b, a) for a, b in raw_pairs]

        # 2) Fallback: usa il winner per capire se i pair sono invertiti
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

        # Fix robusto per feed ATP incoerenti:
        # se ci sono 2 set completi + 1 set incompleto, in un best-of-3
        # il punteggio dopo i set completi deve essere 1-1
        if incomplete_pairs and len(completed_pairs) == 2:
            a_sets, b_sets = 1, 1

        a_val = str(a_sets) if (completed_pairs or incomplete_pairs) else ""
        b_val = str(b_sets) if (completed_pairs or incomplete_pairs) else ""

        if winner == player_a:
            return a_val, f"(rit.) {b_val}" if b_val else "(rit.)"
        if winner == player_b:
            return f"(rit.) {a_val}" if a_val else "(rit.)", b_val

        return a_val, b_val

    # completed / normal
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
        for m in re.finditer(pattern, html, flags=re.DOTALL | re.IGNORECASE):
            candidates.append(m.group(1))
    for m in re.finditer(r'(\{.*?(?:Game Set and Match|wins the match).*?\})', html, flags=re.DOTALL):
        candidates.append(m.group(1))
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
    out = []
    for r in results:
        key = (
            r.get("round", ""),
            normalize_person_name_for_matching(r.get("winner_name_raw", "")),
            re.sub(r"\s+", " ", (r.get("score_raw", "") or "").strip()),
            normalize_person_name_for_matching(r.get("player1_name_raw", "")),
            normalize_person_name_for_matching(r.get("player2_name_raw", "")),
            r.get("outcome_type", "unknown"),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


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
                if key in obj and isinstance(obj[key], str):
                    round_text = obj[key]
                    break
            for key in ("winnerName", "WinnerName", "winningPlayerName", "winner"):
                if key in obj and isinstance(obj[key], str):
                    winner_name = obj[key]
                    break
            for key in ("score", "Score", "matchScore", "result"):
                if key in obj and isinstance(obj[key], str):
                    score_raw = obj[key]
                    break
            for key in ("player1Name", "Player1Name", "homePlayerName"):
                if key in obj and isinstance(obj[key], str):
                    player1 = obj[key]
                    break
            for key in ("player2Name", "Player2Name", "awayPlayerName"):
                if key in obj and isinstance(obj[key], str):
                    player2 = obj[key]
                    break
            label = map_atp_round_to_label(round_text or "")
            if not label:
                continue
            if not winner_name and not score_raw and not (player1 and player2):
                continue
            results.append({
                "round": label,
                "winner_name_raw": (winner_name or "").strip(),
                "score_raw": (score_raw or "").strip(),
                "player1_name_raw": (player1 or "").strip(),
                "player2_name_raw": (player2 or "").strip(),
                "outcome_type": classify_result_outcome(score_raw or ""),
                "source": "results_page",
            })

    if results:
        return dedupe_results(results)

    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    pattern = re.compile(
        r"(Round of 128|Round of 96|Round of 64|Round of 48|Round of 32|Round of 24|Round of 16|Quarterfinals|Semifinals|Final)"
        r".*?Game Set and Match\s+([A-Za-zÀ-ÿ'`\-.\s]+?)\.\s+"
        r"(?:\2)\s+wins the match\s+([0-9\-\(\)\sA-Za-z/.]+?)\s*\.",
        re.IGNORECASE,
    )
    for m in pattern.finditer(text):
        round_text = m.group(1).strip()
        winner_name = " ".join(m.group(2).split())
        score_raw = " ".join(m.group(3).split())
        label = map_atp_round_to_label(round_text)
        if not label:
            continue
        results.append({
            "round": label,
            "winner_name_raw": winner_name,
            "score_raw": score_raw,
            "player1_name_raw": "",
            "player2_name_raw": "",
            "outcome_type": classify_result_outcome(score_raw),
            "source": "results_page",
        })
    return dedupe_results(results)


def group_results_by_round(results: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for r in results:
        grouped.setdefault(r["round"], []).append(r)
    return grouped


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


def build_match_rows(positions: list[dict], round_results: dict[str, list[dict]]) -> list[dict]:
    current = [{"name": p["player_name"], "slot_type": p["slot_type"]} for p in positions]
    match_rows: list[dict] = []
    initial_size = len(current)

    while len(current) > 1:
        round_size = len(current) // 2
        round_label = get_round_label(round_size, initial_size)
        next_round = []
        results_for_round = round_results.get(round_label, [])
        used = [False] * len(results_for_round)

        for i in range(0, len(current), 2):
            a_name = current[i]["name"]
            b_name = current[i + 1]["name"]
            winner = ""
            a_sets = ""
            b_sets = ""

            if a_name == "bye" and b_name and b_name != "bye":
                winner = b_name
            elif b_name == "bye" and a_name and a_name != "bye":
                winner = a_name
            elif a_name == "bye" and b_name == "bye":
                winner = ""

            if not winner:
                for idx, res in enumerate(results_for_round):
                    if used[idx]:
                        continue
                    if not match_result_to_players(a_name, b_name, res):
                        continue
                    candidate_winner = resolve_winner_from_results_page(
                        a_name, b_name, res.get("winner_name_raw", "")
                    )
                    if not candidate_winner:
                        continue
                    used[idx] = True
                    winner = candidate_winner
                    a_sets, b_sets = format_scores_from_result(a_name, b_name, winner, res)
                    break

            match_rows.append({
                "Round": round_label,
                "Player A": a_name,
                "Player B": b_name,
                "Winner": winner,
                "Participant A score": a_sets,
                "Participant B score": b_sets,
            })
            next_round.append({"name": winner, "slot_type": "player" if winner else "unknown"})
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
    return draw_page_url, results_page_url, tournament_id


def fetch_and_build_rows(draw_page_url: str, results_page_url: str: str) -> tuple[list[dict], dict]:
    session = make_requests_session()
    released_at = extract_released_at(pages_text)
    positions = parse_draw_positions(pages_text)
    results_list = fetch_results_page(results_page_url, session=session)
    round_results = group_results_by_round(results_list)
    rows = build_match_rows(positions, round_results)
    meta = {
        "source_draw_page": draw_page_url,
        "source_results_page": results_page_url,
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


def run_once(output_path: Path, tournament_url: str, draw_page_url: str, results_page_url: str, tournament_id: str, year: int) -> bool:
    draw_page_url, results_page_url, tournament_id = resolve_runtime_urls(
        tournament_url=tournament_url,
        draw_page_url=draw_page_url,
        results_page_url=results_page_url,
        tournament_id=tournament_id,
        year=year,
    )
    rows, meta = fetch_and_build_rows(draw_page_url, results_page_url)
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

    def test_completed_match_scores_follow_csv_order():
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
        assert a_score == "0"
        assert b_score == "2"

    def test_tiebreak_score_is_counted_correctly(self) -> None:
        self.assertEqual(
            parse_score_pairs_from_score_raw("7-6(3) 6-7(5) 6-3"),
            [(7, 6), (6, 7), (6, 3)],
        )
        self.assertEqual(
            count_sets_from_pairs(parse_score_pairs_from_score_raw("7-6(3) 6-7(5) 6-3")),
            (2, 1),
        )

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
    args = parser.parse_args()

    if args.run_tests:
        return run_tests()

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.watch:
        run_once(output_path, args.tournament_url, args.draw_page, args.results_page, args.tournament_id, args.year)
        return 0

    while True:
        try:
            run_once(output_path, args.tournament_url, args.draw_page, args.results_page, args.tournament_id, args.year)
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            print(f"[{utc_now_iso()}] ERRORE | {exc}", file=sys.stderr, flush=True)
        time.sleep(max(30, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())

