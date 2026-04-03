from __future__ import annotations

import json
import re
import time
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from assignment2.config import COMMON_COLUMNS, ENBEK_SEARCHES, HH_QUERIES, RAW_ENBEK_DETAIL_DIR, RAW_ENBEK_DIR, RAW_HH_DIR
from assignment2.helpers import (
    clean_text,
    currency_to_kzt,
    dump_json,
    dump_text,
    http_get,
    infer_role_category,
    infer_student_friendly,
    midpoint,
    normalize_city,
    normalize_employment,
    normalize_schedule,
    parse_date,
    parse_salary_numbers,
    slugify,
)


# Source-specific data collection for hh.kz and Enbek.kz.


def fetch_hh_records(session: requests.Session, max_pages_per_query: int) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for query in HH_QUERIES:
        print(f"[hh] query={query}", flush=True)
        query_slug = slugify(query)
        for page in range(max_pages_per_query):
            raw_path = RAW_HH_DIR / f"{query_slug}_page_{page + 1}.json"
            params = {
                "text": query,
                "area": 40,
                "per_page": 100,
                "page": page,
                "experience": "noExperience",
                "order_by": "publication_time",
            }
            if raw_path.exists():
                payload = json.loads(raw_path.read_text(encoding="utf-8"))
            else:
                response = http_get(session, "https://api.hh.ru/vacancies", params=params, timeout=30)
                payload = response.json()
                dump_json(raw_path, payload)

            items = payload.get("items", [])
            if not items:
                break

            for item in items:
                salary = item.get("salary") or {}
                salary_from = salary.get("from")
                salary_to = salary.get("to")
                currency = salary.get("currency") or "KZT"
                description_text = " ".join(
                    [
                        clean_text(item.get("snippet", {}).get("requirement")),
                        clean_text(item.get("snippet", {}).get("responsibility")),
                    ]
                ).strip()
                work_format = ", ".join(clean_text(entry.get("name")) for entry in item.get("work_format", []))
                professional_roles = ", ".join(clean_text(entry.get("name")) for entry in item.get("professional_roles", []))

                records.append(
                    {
                        "source": "hh.kz",
                        "source_id": clean_text(item.get("id")),
                        "url": clean_text(item.get("alternate_url")),
                        "title": clean_text(item.get("name")),
                        "company": clean_text(item.get("employer", {}).get("name")),
                        "city": normalize_city(item.get("area", {}).get("name")),
                        "region": clean_text(item.get("area", {}).get("name")),
                        "published_at": parse_date(item.get("published_at")),
                        "employment": normalize_employment(item.get("employment", {}).get("name")),
                        "schedule": normalize_schedule(item.get("schedule", {}).get("name")),
                        "experience": clean_text(item.get("experience", {}).get("name")),
                        "education": "",
                        "salary_from": salary_from,
                        "salary_to": salary_to,
                        "currency": clean_text(currency or "KZT"),
                        "salary_mid_kzt": currency_to_kzt(midpoint(salary_from, salary_to), currency),
                        "query_group": query,
                        "role_category": infer_role_category(
                            item.get("name"), professional_roles, item.get("snippet", {}).get("requirement")
                        ),
                        "is_student_friendly": infer_student_friendly(
                            query,
                            item.get("name"),
                            item.get("snippet", {}).get("requirement"),
                            item.get("experience", {}).get("name"),
                        ),
                        "description_text": description_text,
                        "industry": professional_roles,
                        "subtitle": "",
                        "skills": "",
                        "languages": "",
                        "work_format": work_format,
                        "internship_flag": bool(item.get("internship")),
                        "source_listing_label": f"hh_search::{query}",
                        "price_segment": "",
                        "salary_outlier_flag": False,
                    }
                )

            if page + 1 >= payload.get("pages", 1):
                break
            time.sleep(0.2)

    return pd.DataFrame(records, columns=COMMON_COLUMNS)


def parse_enbek_listing_card(card: BeautifulSoup, query_group: str) -> dict[str, object]:
    link_tag = card.select_one(".title a.stretched")
    title = clean_text(link_tag.get_text()) if link_tag else ""
    href = link_tag.get("href", "") if link_tag else ""
    source_id_match = re.search(r"~(\d+)$", href)
    source_id = source_id_match.group(1) if source_id_match else clean_text(card.get("wire:key", "")).replace("item-", "")
    subtitle = clean_text(card.select_one(".subtitle").get_text()) if card.select_one(".subtitle") else ""
    industry = clean_text(card.select_one(".profobl").get_text()) if card.select_one(".profobl") else ""
    price_text = clean_text(card.select_one(".price").get_text()) if card.select_one(".price") else ""
    salary_from, salary_to = parse_salary_numbers(price_text)
    company = clean_text(card.select_one(".company").get_text()) if card.select_one(".company") else ""
    location = clean_text(card.select_one(".location").get_text()) if card.select_one(".location") else ""
    experience = clean_text(card.select_one(".experience").get_text()) if card.select_one(".experience") else ""
    schedule = clean_text(card.select_one(".time").get_text()) if card.select_one(".time") else ""
    education = clean_text(card.select_one(".education").get_text()) if card.select_one(".education") else ""
    published_text = clean_text(card.select_one(".right-content").get_text()) if card.select_one(".right-content") else ""
    published_match = re.search(r"(\d{2}\.\d{2}\.\d{4})", published_text)
    published_at = parse_date(published_match.group(1)) if published_match else pd.NaT

    return {
        "source": "enbek.kz",
        "source_id": source_id,
        "url": urljoin("https://www.enbek.kz", href),
        "title": title,
        "company": company,
        "city": normalize_city(location, location),
        "region": location,
        "published_at": published_at,
        "employment": "",
        "schedule": normalize_schedule(schedule),
        "experience": experience,
        "education": education,
        "salary_from": salary_from,
        "salary_to": salary_to,
        "currency": "KZT",
        "salary_mid_kzt": midpoint(salary_from, salary_to),
        "query_group": query_group,
        "role_category": infer_role_category(title, subtitle, industry),
        "is_student_friendly": infer_student_friendly(query_group, title, subtitle, experience, education),
        "description_text": "",
        "industry": industry,
        "subtitle": subtitle,
        "skills": "",
        "languages": "",
        "work_format": "",
        "internship_flag": False,
        "source_listing_label": f"enbek_search::{query_group}",
        "price_segment": "",
        "salary_outlier_flag": False,
    }


def fetch_enbek_search_records(session: requests.Session) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    seen_pages: set[str] = set()

    for search in ENBEK_SEARCHES:
        label = search["label"]
        print(f"[enbek] search={label}", flush=True)
        params = dict(search["params"])
        for page in range(1, search["pages"] + 1):
            params_with_page = dict(params)
            params_with_page["page"] = page
            page_key = f"{label}_{page}"
            if page_key in seen_pages:
                continue
            seen_pages.add(page_key)

            raw_path = RAW_ENBEK_DIR / f"{label}_page_{page}.html"
            if raw_path.exists():
                html = raw_path.read_text(encoding="utf-8")
            else:
                try:
                    response = http_get(session, "https://www.enbek.kz/ru/search/vacancy", params=params_with_page, timeout=30)
                except requests.RequestException:
                    break
                html = response.text
                dump_text(raw_path, html)

            soup = BeautifulSoup(html, "lxml")
            cards = soup.select("div.item-list[wire\\:key^='item-']")
            if not cards:
                break

            for card in cards:
                records.append(parse_enbek_listing_card(card, label))
            time.sleep(0.2)

    listing_df = pd.DataFrame(records, columns=COMMON_COLUMNS)
    if listing_df.empty:
        return listing_df
    return enrich_enbek_details(session, listing_df)


def parse_enbek_jsonld(soup: BeautifulSoup) -> dict[str, object]:
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    for script in scripts:
        raw = clean_text(script.string or script.get_text())
        if "@type" not in raw or "JobPosting" not in raw:
            continue
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            continue
    return {}


def fetch_enbek_detail(session: requests.Session, url: str) -> dict[str, object]:
    detail_id = re.search(r"~(\d+)$", url)
    cache_path = RAW_ENBEK_DETAIL_DIR / f"{detail_id.group(1) if detail_id else slugify(url)}.html"
    if cache_path.exists():
        html = cache_path.read_text(encoding="utf-8")
    else:
        response = http_get(session, url, timeout=30)
        html = response.text
        dump_text(cache_path, html)
        time.sleep(0.25)

    soup = BeautifulSoup(html, "lxml")
    jsonld = parse_enbek_jsonld(soup)
    detail: dict[str, object] = {
        "employment": "",
        "schedule": "",
        "experience": "",
        "education": "",
        "salary_from": None,
        "salary_to": None,
        "currency": "KZT",
        "salary_mid_kzt": None,
        "description_text": "",
        "skills": "",
        "languages": "",
        "region": "",
        "city": "",
        "internship_flag": False,
        "company": "",
    }

    price_text = clean_text(soup.select_one(".price").get_text()) if soup.select_one(".price") else ""
    salary_from, salary_to = parse_salary_numbers(price_text)
    detail["salary_from"] = salary_from
    detail["salary_to"] = salary_to
    detail["salary_mid_kzt"] = midpoint(salary_from, salary_to)

    field_map = {
        "Тип занятости": "employment",
        "График работы": "schedule",
        "Опыт работы": "experience",
        "Образование": "education",
        "Стажировка": "internship_flag",
        "Регион": "region",
    }
    for block in soup.select(".single-line"):
        label_tag = block.select_one(".label")
        value_tag = block.select_one(".value")
        if not label_tag or not value_tag:
            continue
        label = clean_text(label_tag.get_text())
        value = clean_text(value_tag.get_text(" "))
        if label in field_map:
            detail[field_map[label]] = value

    detail["company"] = clean_text(soup.select_one(".company-box .info a").get_text()) if soup.select_one(".company-box .info a") else ""
    detail["city"] = normalize_city(detail.get("region"), detail.get("region"))

    skills = [clean_text(tag.get_text()) for tag in soup.select(".single-line .list-inline .list-inline-item")]
    detail["skills"] = ", ".join(sorted(set(skill for skill in skills if skill)))

    language_values: list[str] = []
    for tag in soup.select(".single-line .list-unstyled li"):
        value = clean_text(tag.get_text())
        if "/" in value:
            language_values.append(value)
    detail["languages"] = ", ".join(sorted(set(language_values)))

    text_chunks = []
    title_tag = soup.select_one("h4.title strong")
    subtitle_tag = soup.select_one(".subtitle")
    if title_tag:
        text_chunks.append(clean_text(title_tag.get_text()))
    if subtitle_tag:
        text_chunks.append(clean_text(subtitle_tag.get_text()))
    if jsonld.get("description"):
        text_chunks.append(clean_text(jsonld.get("description")))
    if jsonld.get("skills"):
        text_chunks.append(clean_text(jsonld.get("skills")))
    detail["description_text"] = " ".join(chunk for chunk in text_chunks if chunk)

    if jsonld.get("baseSalary", {}).get("currency"):
        detail["currency"] = clean_text(jsonld["baseSalary"]["currency"])
    if jsonld.get("educationRequirements") and not detail["education"]:
        detail["education"] = clean_text(jsonld["educationRequirements"])
    if jsonld.get("experienceRequirements") and not detail["experience"]:
        detail["experience"] = clean_text(jsonld["experienceRequirements"])
    if jsonld.get("hiringOrganization", {}).get("name") and not detail["company"]:
        detail["company"] = clean_text(jsonld["hiringOrganization"]["name"])
    if jsonld.get("jobLocation", {}).get("address", {}).get("addressLocality"):
        locality = clean_text(jsonld["jobLocation"]["address"]["addressLocality"])
        detail["region"] = detail["region"] or locality
        detail["city"] = normalize_city(locality, locality)

    internship_value = clean_text(detail.get("internship_flag"))
    detail["internship_flag"] = internship_value.lower().startswith("предполага") or "practice" in clean_text(url).lower()
    detail["schedule"] = normalize_schedule(detail.get("schedule"))
    detail["employment"] = normalize_employment(detail.get("employment"))
    return detail


def enrich_enbek_details(session: requests.Session, listing_df: pd.DataFrame) -> pd.DataFrame:
    rows = listing_df.to_dict("records")
    unique_urls = sorted({row["url"] for row in rows if row.get("url")})[:120]
    detail_map: dict[str, dict[str, object]] = {}

    for index, url in enumerate(unique_urls, start=1):
        if index == 1 or index % 25 == 0:
            print(f"[enbek] detail {index}/{len(unique_urls)}", flush=True)
        try:
            detail_map[url] = fetch_enbek_detail(session, url)
        except Exception as exc:
            detail_map[url] = {"description_text": f"detail fetch failed: {exc}"}

    enriched_rows: list[dict[str, object]] = []
    for row in rows:
        detail = detail_map.get(row["url"], {})
        merged = dict(row)
        for key, value in detail.items():
            if key not in merged or not merged[key]:
                merged[key] = value
            elif key in {"description_text", "skills", "languages"} and value:
                merged[key] = clean_text(f"{merged[key]} {value}")

        merged["company"] = merged["company"] or detail.get("company", "")
        merged["city"] = normalize_city(merged.get("city"), merged.get("region"))
        merged["role_category"] = infer_role_category(
            merged.get("title"), merged.get("subtitle"), merged.get("industry"), merged.get("description_text")
        )
        merged["is_student_friendly"] = infer_student_friendly(
            merged.get("query_group"),
            merged.get("title"),
            merged.get("description_text"),
            merged.get("experience"),
            merged.get("education"),
        )
        if merged.get("salary_mid_kzt") is None:
            merged["salary_mid_kzt"] = midpoint(merged.get("salary_from"), merged.get("salary_to"))
        enriched_rows.append(merged)

    return pd.DataFrame(enriched_rows, columns=COMMON_COLUMNS)
