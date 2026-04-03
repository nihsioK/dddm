from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from assignment2.config import DOC_DIR, PROCESSED_DIR, ensure_dirs
from assignment2.helpers import dump_json, dump_text, request_session
from assignment2.processing import apply_quality_cleaning, build_summary, save_dataframe, standardize_dataframe
from assignment2.reporting import build_report_docx, build_report_markdown, save_figures
from assignment2.sources import fetch_enbek_search_records, fetch_hh_records


# Top-level orchestration for the Assignment 2 data pipeline.


def run_pipeline(max_hh_pages: int) -> dict[str, Path]:
    ensure_dirs()
    session = request_session()

    hh_df = fetch_hh_records(session, max_pages_per_query=max_hh_pages)
    enbek_df = fetch_enbek_search_records(session)

    raw_df = pd.concat([hh_df, enbek_df], ignore_index=True)
    raw_df = standardize_dataframe(raw_df)
    clean_df, quality_df = apply_quality_cleaning(raw_df)

    summary = build_summary(clean_df, raw_df, quality_df)
    figure_paths = save_figures(clean_df)
    markdown_report = build_report_markdown(summary, clean_df, quality_df, figure_paths)

    raw_csv = PROCESSED_DIR / "assignment2_raw_combined.csv"
    clean_csv = PROCESSED_DIR / "assignment2_cleaned_market_intelligence.csv"
    quality_csv = PROCESSED_DIR / "assignment2_quality_log.csv"
    summary_json = PROCESSED_DIR / "assignment2_summary.json"
    report_md = DOC_DIR / "assignment2_report.md"
    report_docx = DOC_DIR / "assignment2_report.docx"

    save_dataframe(raw_df, raw_csv)
    save_dataframe(clean_df, clean_csv)
    save_dataframe(quality_df, quality_csv)
    dump_json(summary_json, summary)
    dump_text(report_md, markdown_report)
    build_report_docx(markdown_report, figure_paths, report_docx)

    return {
        "raw_csv": raw_csv,
        "clean_csv": clean_csv,
        "quality_csv": quality_csv,
        "summary_json": summary_json,
        "report_md": report_md,
        "report_docx": report_docx,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assignment 2 pipeline for Staj.kz market intelligence.")
    parser.add_argument(
        "--hh-pages",
        type=int,
        default=10,
        help="Maximum pages per hh.kz query (100 records per page). Default: 10",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    outputs = run_pipeline(max_hh_pages=args.hh_pages)
    summary = json.loads((PROCESSED_DIR / "assignment2_summary.json").read_text(encoding="utf-8"))

    print("Assignment 2 pipeline completed.")
    print(f"Clean rows: {summary['clean_rows']}")
    print(f"Source split: {summary['source_counts']}")
    for name, path in outputs.items():
        print(f"{name}: {path}")

    if summary["clean_rows"] < 2500:
        print("WARNING: Clean dataset is below the 2500-row target.")
