"""
ingest.py — Landing-to-Raw ingestion flow for daily network activity files.

Validates incoming files in data/landing/ and routes them to data/raw/
(valid) or data/rejected/ (invalid, with a stated reason). Writes an
audit record for every file seen, whether it passes or fails.

NOTE: data/reference/milano-grid.geojson is static reference data and is
NEVER processed by this module — it does not match the daily glob pattern
and is managed/validated separately.
"""

import os
import re
import shutil
import json
import logging
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ingestion")

REQUIRED_RAW_COLUMNS = [
    "datetime", "CellID", "countrycode",
    "smsin", "smsout", "callin", "callout", "internet"
]

# The "-mi-" is mandatory — a looser glob would admit a different city's
# files whose cell IDs collide numerically with Milan's.
FILENAME_PATTERN = re.compile(r"^sms-call-internet-mi-\d{4}-\d{2}-\d{2}\.csv$")


def detect_files(landing_dir):
    """Return a list of filenames in landing_dir that match the expected
    daily activity file pattern. Anything else (including the GeoJSON,
    if ever accidentally placed here) is ignored, not processed."""
    all_files = os.listdir(landing_dir)
    matched = [f for f in all_files if FILENAME_PATTERN.match(f)]
    ignored = [f for f in all_files if not FILENAME_PATTERN.match(f)]

    if ignored:
        logger.info(f"Ignored (does not match daily activity pattern): {ignored}")

    logger.info(f"Detected {len(matched)} candidate file(s) in {landing_dir}: {matched}")
    return matched


def validate_schema(filepath):
    """Check required columns exist. Returns (is_valid, reason)."""
    try:
        df = pd.read_csv(filepath, nrows=5)  # only need the header + a few rows
    except Exception as e:
        return False, f"FILE_UNREADABLE: {e}"

    missing_cols = [c for c in REQUIRED_RAW_COLUMNS if c not in df.columns]
    if missing_cols:
        return False, f"MISSING_COLUMNS: {missing_cols}"

    return True, None


def validate_minimum_quality(filepath):
    """Check basic quality thresholds. Returns (is_valid, reason, row_count)."""
    try:
        df = pd.read_csv(filepath, on_bad_lines="skip", engine="python")
    except Exception as e:
        return False, f"FILE_UNREADABLE: {e}", 0

    row_count = len(df)

    if row_count == 0:
        return False, "EMPTY_FILE: zero data rows", 0

    # Malformed timestamp check — every value must parse
    try:
        parsed = pd.to_datetime(df["datetime"], errors="coerce")
        unparseable = parsed.isnull().sum()
        if unparseable > 0:
            return False, f"MALFORMED_TIMESTAMP: {unparseable} row(s) failed to parse", row_count
    except KeyError:
        return False, "MISSING_COLUMNS: datetime column absent (should have failed schema check first)", row_count

    # Negative activity value check
    activity_cols = ["smsin", "smsout", "callin", "callout", "internet"]
    present_activity_cols = [c for c in activity_cols if c in df.columns]
    negative_counts = {c: int((df[c] < 0).sum()) for c in present_activity_cols}
    total_negative = sum(negative_counts.values())
    if total_negative > 0:
        return False, f"NEGATIVE_VALUES: {negative_counts}", row_count

    return True, None, row_count
def validate_partial_corruption(filepath, threshold=0.05):
    """Distinguishes 'a few garbage rows' from 'the whole file is
    unusable'. Uses pandas' on_bad_lines='skip' to count rows that
    couldn't be parsed at all (wrong field count, stray characters),
    separate from the full-file validity checks above.

    Returns (status, reason, good_row_count, bad_row_count) where
    status is one of: "OK" (no corruption), "WARN" (tolerable,
    below threshold), "REJECT" (too much corruption to trust)."""
    try:
        with open(filepath, "r") as f:
            total_lines = sum(1 for _ in f) - 1  # minus header

        good_df = pd.read_csv(filepath, on_bad_lines="skip", engine="python")
        good_rows = len(good_df)
        bad_rows = max(total_lines - good_rows, 0)

        if bad_rows == 0:
            return "OK", None, good_rows, 0

        bad_ratio = bad_rows / total_lines if total_lines > 0 else 1.0

        if bad_ratio <= threshold:
            reason = f"PARTIAL_CORRUPTION_TOLERATED: {bad_rows} of {total_lines} rows skipped ({bad_ratio:.1%})"
            return "WARN", reason, good_rows, bad_rows
        else:
            reason = f"PARTIAL_CORRUPTION_EXCEEDS_THRESHOLD: {bad_rows} of {total_lines} rows unparseable ({bad_ratio:.1%})"
            return "REJECT", reason, good_rows, bad_rows

    except Exception as e:
        return "REJECT", f"FILE_UNREADABLE: {e}", 0, 0


def route_file(filepath, filename, is_valid, raw_dir, rejected_dir):
    """Move the file to raw/ (valid) or rejected/ (invalid). Raw files
    are preserved unchanged — this is a move, not a copy-then-modify."""
    destination_dir = raw_dir if is_valid else rejected_dir
    destination_path = os.path.join(destination_dir, filename)
    shutil.move(filepath, destination_path)
    return destination_path


def already_processed(filename, log_path):
    """Check the audit log to see if this exact filename was already
    processed successfully in a prior run — required so re-running the
    DAG doesn't silently duplicate work."""
    if not os.path.exists(log_path):
        return False
    with open(log_path, "r") as f:
        for line in f:
            record = json.loads(line)
            if record["filename"] == filename and record["status"] == "ACCEPTED":
                return True
    return False


def write_audit_record(log_path, filename, status, row_count, reason):
    """Append one JSON-lines audit record per file processed."""
    record = {
        "filename": filename,
        "status": status,          # "ACCEPTED", "REJECTED", or "SKIPPED"
        "row_count": row_count,
        "reason": reason,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(log_path, "a") as f:
        f.write(json.dumps(record) + "\n")
    return record


def run_ingestion(landing_dir, raw_dir, rejected_dir, log_path):
    """Main entry point: detect, validate, route, and log every file."""
    results = []
    filenames = detect_files(landing_dir)

    for filename in filenames:
        filepath = os.path.join(landing_dir, filename)

        # Skip files already successfully processed in a prior run
        if already_processed(filename, log_path):
            logger.info(f"SKIPPED (already processed): {filename}")
            record = write_audit_record(log_path, filename, "SKIPPED", None,
                                          "Already processed in a prior run")
            results.append(record)
            continue

        schema_ok, schema_reason = validate_schema(filepath)
        if not schema_ok:
            dest = route_file(filepath, filename, False, raw_dir, rejected_dir)
            logger.warning(f"REJECTED: {filename} -> {schema_reason}")
            record = write_audit_record(log_path, filename, "REJECTED", None, schema_reason)
            results.append(record)
            continue
        quality_ok, quality_reason, row_count = validate_minimum_quality(filepath)
        if not quality_ok:
            dest = route_file(filepath, filename, False, raw_dir, rejected_dir)
            logger.warning(f"REJECTED: {filename} -> {quality_reason}")
            record = write_audit_record(log_path, filename, "REJECTED", row_count, quality_reason)
            results.append(record)
            continue

        # CONTROL #6: partial corruption check — tolerate a few bad
        # rows, reject only if corruption exceeds the threshold
        corruption_status, corruption_reason, good_rows, bad_rows = validate_partial_corruption(filepath)
        if corruption_status == "REJECT":
            dest = route_file(filepath, filename, False, raw_dir, rejected_dir)
            logger.warning(f"REJECTED: {filename} -> {corruption_reason}")
            record = write_audit_record(log_path, filename, "REJECTED", good_rows, corruption_reason)
            results.append(record)
            continue

        dest = route_file(filepath, filename, True, raw_dir, rejected_dir)
        status_label = "ACCEPTED_WITH_WARNINGS" if corruption_status == "WARN" else "ACCEPTED"
        log_reason = corruption_reason if corruption_status == "WARN" else None
        logger.info(f"{status_label}: {filename} -> {dest} ({row_count} rows)")
        record = write_audit_record(log_path, filename, status_label, row_count, log_reason)
        results.append(record)

    return results


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    LANDING_DIR = os.path.join(BASE_DIR, "..", "..", "data", "landing")
    RAW_DIR = os.path.join(BASE_DIR, "..", "..", "data", "raw")
    REJECTED_DIR = os.path.join(BASE_DIR, "..", "..", "data", "rejected")
    LOG_PATH = os.path.join(BASE_DIR, "..", "..", "logs", "ingestion_log.jsonl")

    results = run_ingestion(LANDING_DIR, RAW_DIR, REJECTED_DIR, LOG_PATH)

    print("\n=== Ingestion Summary ===")
    for r in results:
        print(f"{r['filename']}: {r['status']} — {r['reason'] or 'OK'}")