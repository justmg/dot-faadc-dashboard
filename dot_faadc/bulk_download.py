"""
Generate a DOT FABS assistance-awards CSV via USAspending's bulk_download endpoint.

The bulk_download/awards endpoint caps each request at a 1-year date range, so
this script loops fiscal-year-by-fiscal-year and concatenates the PrimeTransactions
CSVs into a single seed file. The Power BI dashboard's FAADC Data Pool reads this
snapshot and layers a recent spending_by_award API delta on top each refresh.

Usage:
    python bulk_download.py [--output PATH] [--fy-start YEAR] [--fy-end YEAR]

Requires: requests
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import sys
import time
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import requests

# ~30 columns that FAADC Data Pool downstream actually uses. Anything else gets
# dropped before commit to stay under GitHub's 100 MB per-file limit.
KEEP_COLUMNS = [
    "action_date",
    "fiscal_year",
    "period_of_performance_start_date",
    "period_of_performance_current_end_date",
    "assistance_type_code",
    "assistance_type_description",
    "award_description",
    "prime_award_base_transaction_description",
    "transaction_description",
    "award_id_fain",
    "fain",
    "total_obligated_amount",
    "total_dollars_obligated",
    "federal_action_obligation",
    "recipient_uei",
    "recipient_name",
    "cfda_number",
    "cfda_title",
    "assistance_listing_number",
    "assistance_listing_title",
    "primary_place_of_performance_congressional_district",
    "primary_place_of_performance_state_name",
    "primary_place_of_performance_country_name",
    "primary_place_of_performance_code",
    "awarding_agency_name",
    "awarding_sub_agency_name",
    "funding_agency_name",
    "funding_sub_agency_name",
    "funding_sub_agency_code",
    "funding_office_name",
    "funding_office_code",
    "funding_opportunity_number",
    "business_types",
    "business_types_code",
    "business_types_description",
    "last_modified_date",
    "indirect_federal_sharing",
    "research_and_development_funds_indicator",
    "type_of_research_and_development_funds_description",
    "modification_number",  # preserved if source is PrimeTransactions; blank otherwise
]

API_BASE = "https://api.usaspending.gov/"
BULK_DOWNLOAD_ENDPOINT = "/api/v2/bulk_download/awards/"

# Department of Transportation — toptier_agency_id returned by /bulk_download/list_agencies/
# (distinct from toptier_code "069" or the agencies-endpoint id 731).
DOT_AGENCY_ID = 62

# FABS prime-award types: Block Grant, Formula Grant, Project Grant, Cooperative Agreement
ASSISTANCE_AWARD_TYPES = ["02", "03", "04", "05"]

POLL_INTERVAL_SECONDS = 15
POLL_TIMEOUT_SECONDS = 60 * 60  # 1 hour per fiscal year
MAX_TRANSIENT_RETRIES = 6  # RemoteDisconnected etc. are common on long polls


def get_with_retry(url: str, **kwargs) -> requests.Response:
    """GET with backoff on connection-reset / remote-disconnect errors."""
    last_err: Exception | None = None
    for attempt in range(1, MAX_TRANSIENT_RETRIES + 1):
        try:
            return requests.get(url, **kwargs)
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
            last_err = e
            backoff = min(30, 2 ** attempt)
            print(f"  transient error ({type(e).__name__}), retry {attempt}/{MAX_TRANSIENT_RETRIES} in {backoff}s")
            time.sleep(backoff)
    raise RuntimeError(f"get_with_retry exhausted retries: {last_err}")


def fy_bounds(fy: int) -> tuple[str, str]:
    return f"{fy - 1}-10-01", f"{fy}-09-30"


def request_fy_download(fy: int) -> dict:
    start_date, end_date = fy_bounds(fy)
    payload = {
        "filters": {
            "agency": DOT_AGENCY_ID,
            "prime_award_types": ASSISTANCE_AWARD_TYPES,
            "date_type": "action_date",
            "date_range": {"start_date": start_date, "end_date": end_date},
        },
        "file_format": "csv",
    }
    url = urljoin(API_BASE, BULK_DOWNLOAD_ENDPOINT)
    print(f"POST {url}")
    print(f"  FY{fy}: {start_date} to {end_date}")
    r = requests.post(url, json=payload, timeout=60)
    if not r.ok:
        print("Response body:", r.text[:1000], file=sys.stderr)
    r.raise_for_status()
    return r.json()


def poll_until_ready(status_url: str, fy: int) -> dict:
    deadline = time.time() + POLL_TIMEOUT_SECONDS
    started = time.time()
    while time.time() < deadline:
        r = get_with_retry(status_url, timeout=30)
        r.raise_for_status()
        data = r.json()
        status = data.get("status")
        elapsed = int(time.time() - started)
        print(f"  FY{fy} [{elapsed:>4}s] status={status!r}")
        if status == "finished":
            return data
        if status == "failed":
            raise RuntimeError(f"bulk_download FY{fy} failed: {data}")
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"FY{fy} bulk_download did not finish within {POLL_TIMEOUT_SECONDS}s")


def download_and_extract_csv(url: str, workdir: Path, fy: int) -> Path:
    """Download the ZIP, extract the PrimeTransactions CSV, return its path."""
    print(f"  GET {url}")
    tmp_zip = workdir / f"fy{fy}.zip"
    with requests.get(url, stream=True, timeout=900) as r:
        r.raise_for_status()
        with open(tmp_zip, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    size_mb = tmp_zip.stat().st_size / (1024 * 1024)
    print(f"  Downloaded FY{fy} ZIP: {size_mb:.1f} MB")

    with zipfile.ZipFile(tmp_zip) as zf:
        csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_members:
            raise RuntimeError(f"No CSV in FY{fy} archive. Members: {zf.namelist()}")
        # Prefer PrimeAwardSummaries (award-level rollup — much smaller than per-action
        # PrimeTransactions, and matches the grain of the spending_by_award API delta).
        preferred = [n for n in csv_members if "primeawardsummaries" in n.lower().replace("_", "")]
        pick = preferred[0] if preferred else max(csv_members, key=lambda n: zf.getinfo(n).file_size)
        info = zf.getinfo(pick)
        print(f"  Extracting {pick} ({info.file_size / (1024 * 1024):.1f} MB)")
        out_csv = workdir / f"fy{fy}.csv"
        with zf.open(pick) as src, open(out_csv, "wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)

    tmp_zip.unlink()
    return out_csv


def concat_filter_gzip(parts: list[Path], output_path: Path) -> None:
    """Concat per-FY CSVs, keeping only KEEP_COLUMNS, writing gzipped output.

    Output schema is the intersection of KEEP_COLUMNS with the first part's
    header. Missing columns in later parts are written as empty strings.
    """
    print(f"Filter + gzip concat of {len(parts)} files -> {output_path}")
    total_rows = 0

    # Determine intersection of desired columns with the first file's header.
    with open(parts[0], "r", encoding="utf-8", newline="") as f0:
        first_header = next(csv.reader(f0))
    keep = [c for c in KEEP_COLUMNS if c in first_header]
    print(f"  Keeping {len(keep)}/{len(first_header)} columns")

    with gzip.open(output_path, "wt", encoding="utf-8", newline="", compresslevel=6) as gz:
        writer = csv.DictWriter(gz, fieldnames=keep, extrasaction="ignore")
        writer.writeheader()
        for p in parts:
            with open(p, "r", encoding="utf-8", newline="", errors="replace") as src:
                reader = csv.DictReader(src)
                rows_in_file = 0
                for row in reader:
                    writer.writerow(row)
                    rows_in_file += 1
                total_rows += rows_in_file
                print(f"  {p.name}: {rows_in_file:,} rows")

    final_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"Final: {output_path} - {total_rows:,} rows, {final_mb:.1f} MB gz")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", type=Path, default=Path("dot_faadc.csv.gz"))
    ap.add_argument("--fy-start", type=int, default=2020)
    ap.add_argument("--fy-end", type=int, default=2026)
    ap.add_argument("--workdir", type=Path, default=Path("_parts"))
    args = ap.parse_args()

    args.workdir.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    # Phase 1: kick off downloads for each FY (async — they generate in parallel on their side)
    jobs: list[tuple[int, str, str]] = []  # (fy, status_url, file_url)
    for fy in range(args.fy_start, args.fy_end + 1):
        resp = request_fy_download(fy)
        status_url = resp.get("status_url")
        file_url = resp.get("file_url")
        if not status_url or not file_url:
            print(f"ERROR: FY{fy} response missing URLs: {resp}", file=sys.stderr)
            return 1
        jobs.append((fy, status_url, file_url))

    # Phase 2: poll each in order, then download
    parts: list[Path] = []
    for fy, status_url, file_url in jobs:
        poll_until_ready(status_url, fy)
        parts.append(download_and_extract_csv(file_url, args.workdir, fy))

    concat_filter_gzip(parts, args.output)

    # Clean parts
    for p in parts:
        p.unlink()
    try:
        args.workdir.rmdir()
    except OSError:
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
