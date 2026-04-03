from __future__ import annotations

import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from assignment2.config import EARLY_CAREER_TERMS, KZ_CITIES, KZT_EXCHANGE_RATES, ROLE_KEYWORDS


# Shared parsing, normalization, and HTTP helper functions.


def slugify(text: str) -> str:
    value = re.sub(r"\s+", "-", text.strip().lower())
    value = re.sub(r"[^a-zA-Z0-9а-яА-ЯёЁ_-]+", "-", value)
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value or "query"


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_date(value: Any) -> pd.Timestamp | pd.NaT:
    text = clean_text(value)
    if not text:
        return pd.NaT
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%d.%m.%Y", "%d.%m.%Y %H:%M"):
        try:
            return pd.Timestamp(datetime.strptime(text, fmt))
        except ValueError:
            continue
    try:
        return pd.to_datetime(text, errors="coerce")
    except Exception:
        return pd.NaT


def parse_salary_numbers(text: str) -> tuple[float | None, float | None]:
    values = re.findall(r"\d[\d\s]*", clean_text(text))
    nums = [float(v.replace(" ", "")) for v in values if v.strip()]
    if not nums:
        return None, None
    if len(nums) == 1:
        return nums[0], nums[0]
    return nums[0], nums[1]


def currency_to_kzt(amount: float | None, currency: str | None) -> float | None:
    if amount is None:
        return None
    normalized = clean_text(currency).upper()
    rate = KZT_EXCHANGE_RATES.get(normalized, 1.0 if normalized == "KZT" else None)
    if rate is None:
        return None
    return round(amount * rate, 2)


def midpoint(low: float | None, high: float | None) -> float | None:
    if low is None and high is None:
        return None
    if low is None:
        return high
    if high is None:
        return low
    return round((low + high) / 2, 2)


def normalize_city(city: Any, region: Any = "") -> str:
    combined = " | ".join([clean_text(city), clean_text(region)])
    if not combined.strip("| "):
        return ""
    for known_city in KZ_CITIES:
        if known_city.lower() in combined.lower():
            if known_city in {"Қостанай", "Карағанды", "Көкшетау", "Талдықорған", "Ақтау", "Ақтөбе", "Қызылорда", "Өскемен", "Жезқазған", "Екібастұз", "Түркістан"}:
                return {
                    "Қостанай": "Костанай",
                    "Карағанды": "Караганда",
                    "Көкшетау": "Кокшетау",
                    "Талдықорған": "Талдыкорган",
                    "Ақтау": "Актау",
                    "Ақтөбе": "Актобе",
                    "Қызылорда": "Кызылорда",
                    "Өскемен": "Усть-Каменогорск",
                    "Жезқазған": "Жезказган",
                    "Екібастұз": "Экибастуз",
                    "Түркістан": "Туркестан",
                }[known_city]
            return known_city
    if "," in clean_text(city):
        return clean_text(city).split(",")[-1].replace("г.", "").strip()
    if "," in clean_text(region):
        return clean_text(region).split(",")[-1].replace("г.", "").strip()
    return clean_text(city) or clean_text(region)


def infer_role_category(*parts: Any) -> str:
    haystack = " ".join(clean_text(part).lower() for part in parts if clean_text(part))
    for label, keywords in ROLE_KEYWORDS.items():
        if any(keyword in haystack for keyword in keywords):
            return label
    return "General Early-Career"


def infer_student_friendly(*parts: Any) -> bool:
    haystack = " ".join(clean_text(part).lower() for part in parts if clean_text(part))
    return any(term in haystack for term in EARLY_CAREER_TERMS)


def normalize_employment(value: Any) -> str:
    text = clean_text(value).lower()
    mapping = {
        "полная занятость": "Full-time",
        "полная": "Full-time",
        "частичная занятость": "Part-time",
        "частичная": "Part-time",
        "проектная работа": "Project",
        "постоянная": "Permanent",
        "временная": "Temporary",
    }
    return mapping.get(text, clean_text(value))


def normalize_schedule(value: Any) -> str:
    text = clean_text(value).lower()
    mapping = {
        "полный день": "Full day",
        "полный рабочий день": "Full day",
        "гибкий график": "Flexible",
        "сменный график": "Shift",
        "удаленная работа": "Remote",
        "удалённая работа": "Remote",
        "вахта": "Rotation",
    }
    return mapping.get(text, clean_text(value))


def request_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Accept-Language": "ru,en;q=0.9",
            "Connection": "close",
        }
    )
    return session


def dump_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def dump_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def http_get(session: requests.Session, url: str, *, params: dict[str, Any] | None = None, timeout: int = 30) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt == 5:
                raise
            time.sleep(min(2 ** attempt, 8))
    assert last_error is not None
    raise last_error
