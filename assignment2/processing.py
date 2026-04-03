from __future__ import annotations

import math
from os import PathLike
from dataclasses import dataclass
from typing import Any

import pandas as pd

from assignment2.config import COMMON_COLUMNS, FRESHNESS_DAYS, RUN_DATE
from assignment2.helpers import (
    clean_text,
    currency_to_kzt,
    infer_role_category,
    infer_student_friendly,
    midpoint,
    normalize_city,
    normalize_employment,
    normalize_schedule,
    parse_date,
)


# Data standardization, cleaning, and summary helpers.


@dataclass
class QualityLogRow:
    step: str
    metric: str
    value: Any


def standardize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    for column in COMMON_COLUMNS:
        if column not in working.columns:
            working[column] = ""

    text_columns = [
        "source",
        "source_id",
        "url",
        "title",
        "company",
        "city",
        "region",
        "employment",
        "schedule",
        "experience",
        "education",
        "currency",
        "query_group",
        "role_category",
        "description_text",
        "industry",
        "subtitle",
        "skills",
        "languages",
        "work_format",
        "source_listing_label",
    ]
    for column in text_columns:
        working[column] = working[column].map(clean_text)

    working["city"] = working.apply(lambda row: normalize_city(row["city"], row["region"]), axis=1)
    working["employment"] = working["employment"].map(normalize_employment)
    working["schedule"] = working["schedule"].map(normalize_schedule)
    working["published_at"] = working["published_at"].map(parse_date)
    working["published_at"] = pd.to_datetime(working["published_at"], errors="coerce", utc=True).dt.tz_convert(None)
    working["salary_from"] = pd.to_numeric(working["salary_from"], errors="coerce")
    working["salary_to"] = pd.to_numeric(working["salary_to"], errors="coerce")
    working["salary_mid_kzt"] = pd.to_numeric(working["salary_mid_kzt"], errors="coerce")

    missing_mid = working["salary_mid_kzt"].isna()
    working.loc[missing_mid, "salary_mid_kzt"] = working.loc[missing_mid].apply(
        lambda row: currency_to_kzt(midpoint(row["salary_from"], row["salary_to"]), row["currency"]),
        axis=1,
    )
    working["is_student_friendly"] = working.apply(
        lambda row: bool(
            row["is_student_friendly"]
            or infer_student_friendly(
                row["query_group"], row["title"], row["description_text"], row["experience"], row["education"]
            )
        ),
        axis=1,
    )
    working["internship_flag"] = working["internship_flag"].astype(bool)
    working["role_category"] = working.apply(
        lambda row: row["role_category"]
        or infer_role_category(row["title"], row["subtitle"], row["industry"], row["description_text"]),
        axis=1,
    )
    return working[COMMON_COLUMNS]


def apply_quality_cleaning(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logs: list[QualityLogRow] = [QualityLogRow("raw", "rows", len(df))]
    working = df.copy()

    working = working.drop_duplicates(subset=["source", "source_id"], keep="first")
    logs.append(QualityLogRow("dedup", "after_source_id_dedup", len(working)))

    working["published_at"] = pd.to_datetime(working["published_at"], errors="coerce", utc=True).dt.tz_convert(None)
    freshness_cutoff = pd.Timestamp(RUN_DATE.date()) - pd.Timedelta(days=FRESHNESS_DAYS)
    before_stale = len(working)
    working = working[working["published_at"].notna()]
    working = working[working["published_at"] >= freshness_cutoff]
    logs.append(QualityLogRow("freshness", "removed_stale_or_invalid_dates", before_stale - len(working)))

    for column in ["title", "company", "city"]:
        working[column] = working[column].map(clean_text)
    working = working[working["title"].ne("") & working["company"].ne("")]
    logs.append(QualityLogRow("completeness", "after_required_fields_filter", len(working)))

    soft_key = (
        working["title"].str.lower()
        + "||"
        + working["company"].str.lower()
        + "||"
        + working["city"].str.lower()
        + "||"
        + working["published_at"].dt.strftime("%Y-%m-%d")
    )
    working = working.assign(_soft_key=soft_key)
    before_soft = len(working)
    working = working.drop_duplicates(subset=["_soft_key"], keep="first").drop(columns=["_soft_key"])
    logs.append(QualityLogRow("dedup", "removed_soft_duplicates", before_soft - len(working)))

    working["salary_mid_kzt"] = pd.to_numeric(working["salary_mid_kzt"], errors="coerce")
    positive_salaries = working.loc[working["salary_mid_kzt"].gt(0), "salary_mid_kzt"]
    q1 = positive_salaries.quantile(0.25) if not positive_salaries.empty else math.nan
    q3 = positive_salaries.quantile(0.75) if not positive_salaries.empty else math.nan
    iqr = q3 - q1 if pd.notna(q1) and pd.notna(q3) else math.nan
    lower_bound = q1 - 1.5 * iqr if pd.notna(iqr) else math.nan
    upper_bound = q3 + 1.5 * iqr if pd.notna(iqr) else math.nan
    working["salary_outlier_flag"] = False
    if pd.notna(lower_bound) and pd.notna(upper_bound):
        salary_mask = working["salary_mid_kzt"].notna() & (
            (working["salary_mid_kzt"] < lower_bound) | (working["salary_mid_kzt"] > upper_bound)
        )
        working.loc[salary_mask, "salary_outlier_flag"] = True
        working.loc[salary_mask, ["salary_from", "salary_to", "salary_mid_kzt"]] = pd.NA
        logs.append(QualityLogRow("salary_iqr", "salary_values_nullified_as_outliers", int(salary_mask.sum())))
        logs.append(QualityLogRow("salary_iqr", "iqr_lower_bound_kzt", round(lower_bound, 2)))
        logs.append(QualityLogRow("salary_iqr", "iqr_upper_bound_kzt", round(upper_bound, 2)))

    working["price_segment"] = ""
    clean_salary = working["salary_mid_kzt"].dropna()
    if not clean_salary.empty:
        q25, q50, q75 = clean_salary.quantile([0.25, 0.5, 0.75]).tolist()

        def assign_segment(value: Any) -> str:
            if pd.isna(value):
                return "unknown"
            if value <= q25:
                return "low-priced"
            if value <= q50:
                return "middle-priced"
            if value <= q75:
                return "high-priced"
            return "luxury"

        working["price_segment"] = working["salary_mid_kzt"].map(assign_segment)
        logs.append(QualityLogRow("pricing", "segment_q25_kzt", round(q25, 2)))
        logs.append(QualityLogRow("pricing", "segment_q50_kzt", round(q50, 2)))
        logs.append(QualityLogRow("pricing", "segment_q75_kzt", round(q75, 2)))

    logs.append(QualityLogRow("final", "rows", len(working)))
    return working[COMMON_COLUMNS], pd.DataFrame([row.__dict__ for row in logs])


def save_dataframe(df: pd.DataFrame, path: str | PathLike[str]) -> None:
    df.to_csv(path, index=False, encoding="utf-8")


def build_summary(clean_df: pd.DataFrame, raw_df: pd.DataFrame, quality_df: pd.DataFrame) -> dict[str, Any]:
    salary_coverage = round(float(clean_df["salary_mid_kzt"].notna().mean() * 100), 2) if len(clean_df) else 0.0
    student_share = round(float(clean_df["is_student_friendly"].mean() * 100), 2) if len(clean_df) else 0.0
    source_counts = clean_df["source"].value_counts().to_dict()
    role_counts = clean_df["role_category"].value_counts().head(6).to_dict()
    city_counts = clean_df["city"].value_counts().head(8).to_dict()
    segment_counts = clean_df["price_segment"].value_counts().to_dict()
    platform_coverage = (
        clean_df.groupby("source")["salary_mid_kzt"]
        .apply(lambda s: round(float(s.notna().mean() * 100), 2) if len(s) else 0.0)
        .to_dict()
    )
    top_segment = next(iter(pd.Series(segment_counts).sort_values(ascending=False).index), "unknown") if segment_counts else "unknown"
    perspective_segment = "middle-priced" if "middle-priced" in segment_counts else top_segment
    return {
        "run_date_utc": RUN_DATE.isoformat(),
        "raw_rows": int(len(raw_df)),
        "clean_rows": int(len(clean_df)),
        "source_counts": source_counts,
        "salary_coverage_pct": salary_coverage,
        "student_friendly_pct": student_share,
        "top_cities": city_counts,
        "top_role_categories": role_counts,
        "price_segments": segment_counts,
        "platform_salary_coverage_pct": platform_coverage,
        "freshness_window_days": FRESHNESS_DAYS,
        "perspective_segment": perspective_segment,
        "quality_log": quality_df.to_dict("records"),
    }
