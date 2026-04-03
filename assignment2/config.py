from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


# Shared paths and constants used across the assignment pipeline.
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_HH_DIR = BASE_DIR / "data" / "raw" / "hh"
RAW_ENBEK_DIR = BASE_DIR / "data" / "raw" / "enbek"
RAW_ENBEK_DETAIL_DIR = RAW_ENBEK_DIR / "details"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
FIGURES_DIR = BASE_DIR / "output" / "figures"
DOC_DIR = BASE_DIR / "output" / "doc"

RUN_DATE = datetime.now(timezone.utc)
FRESHNESS_DAYS = 90

KZT_EXCHANGE_RATES = {
    "KZT": 1.0,
    "USD": 500.0,
    "EUR": 540.0,
    "RUR": 5.5,
    "RUB": 5.5,
}

KZ_CITIES = [
    "Алматы",
    "Астана",
    "Шымкент",
    "Уральск",
    "Караганда",
    "Карағанды",
    "Актобе",
    "Ақтөбе",
    "Павлодар",
    "Костанай",
    "Қостанай",
    "Кокшетау",
    "Көкшетау",
    "Усть-Каменогорск",
    "Өскемен",
    "Семей",
    "Тараз",
    "Талдыкорган",
    "Талдықорған",
    "Атырау",
    "Актау",
    "Ақтау",
    "Петропавловск",
    "Петропавл",
    "Кызылорда",
    "Қызылорда",
    "Туркестан",
    "Түркістан",
    "Жезказган",
    "Жезқазған",
    "Рудный",
    "Экибастуз",
    "Екібастұз",
]

ROLE_KEYWORDS = {
    "IT / Data": [
        "python",
        "data",
        "аналит",
        "developer",
        "разработ",
        "qa",
        "tester",
        "software",
        "machine learning",
        "devops",
        "backend",
        "frontend",
        "fullstack",
        "информатик",
    ],
    "Marketing / Sales": [
        "marketing",
        "smm",
        "sales",
        "продаж",
        "маркет",
        "brand",
        "content",
        "pr ",
        "реклам",
        "бариста",
        "продав",
    ],
    "Finance / Admin": [
        "finance",
        "account",
        "бухгал",
        "экономист",
        "офис",
        "hr",
        "кадр",
        "админист",
        "assistan",
    ],
    "Education / Support": [
        "teacher",
        "учител",
        "образован",
        "ментор",
        "support",
        "оператор",
        "консульт",
    ],
    "Operations / Logistics": [
        "операц",
        "logist",
        "warehouse",
        "склад",
        "закуп",
        "транспорт",
        "разнорабоч",
        "производ",
    ],
}

EARLY_CAREER_TERMS = [
    "стаж",
    "intern",
    "trainee",
    "graduate",
    "junior",
    "без опыта",
    "начинающ",
    "student",
    "студент",
    "practice",
    "практик",
]

HH_QUERIES = [
    "стажировка",
    "стажер",
    "intern",
    "internship",
    "trainee",
    "junior",
    "graduate",
    "студент",
    "без опыта",
    "первая работа",
    "начинающий специалист",
]

ENBEK_SEARCHES = [
    {"label": "youth", "params": {"tag": "youth", "experience": "e0"}, "pages": 12},
    {"label": "practice", "params": {"tag": "practice", "experience": "e0"}, "pages": 8},
    {"label": "intern", "params": {"prof": "intern", "tag": "youth", "experience": "e0"}, "pages": 4},
    {"label": "junior", "params": {"prof": "junior", "tag": "youth", "experience": "e0"}, "pages": 4},
    {"label": "trainee", "params": {"prof": "trainee", "tag": "youth", "experience": "e0"}, "pages": 4},
    {"label": "стажировка", "params": {"prof": "стажировка", "tag": "youth", "experience": "e0"}, "pages": 6},
    {"label": "без опыта", "params": {"prof": "без опыта", "tag": "youth", "experience": "e0"}, "pages": 6},
]

COMMON_COLUMNS = [
    "source",
    "source_id",
    "url",
    "title",
    "company",
    "city",
    "region",
    "published_at",
    "employment",
    "schedule",
    "experience",
    "education",
    "salary_from",
    "salary_to",
    "currency",
    "salary_mid_kzt",
    "query_group",
    "role_category",
    "is_student_friendly",
    "description_text",
    "industry",
    "subtitle",
    "skills",
    "languages",
    "work_format",
    "internship_flag",
    "source_listing_label",
    "price_segment",
    "salary_outlier_flag",
]


def ensure_dirs() -> None:
    """Create output and cache directories expected by the pipeline."""
    for path in [RAW_HH_DIR, RAW_ENBEK_DIR, RAW_ENBEK_DETAIL_DIR, PROCESSED_DIR, FIGURES_DIR, DOC_DIR]:
        path.mkdir(parents=True, exist_ok=True)
