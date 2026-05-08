#!/usr/bin/env python3
"""
ZeroNorth Viscosity Patch Tool
--------------------------------
Reads a ZeroNorth backup JSON, filters reports by IMO and date range,
patches viscosity15Degrees for enabled fuel grades, and writes:

  1. One submission-ready JSON file per report  (always)
  2. A single consolidated backup JSON          (optional, see GENERATE_BACKUP_FILE)

Usage:
    Edit the CONFIGURATION block below, then:  python3 ingestion_viscosity.py
"""

import json
import os
import sys
from copy import deepcopy
from datetime import datetime, timezone

# =============================================================================
# CONFIGURATION — edit these values only
# =============================================================================

# Path to the backup JSON exported from the ZeroNorth online tool
BACKUP_FILE = "ZN-9381500-backup-from-online-2026-05-07T05_06_53.965Z.json"

# Directory where individual per-report JSON files will be written
OUTPUT_DIR = "output_reports"

# Set to True to also generate a single consolidated backup JSON file
# (same structure as the original backup, with viscosity patched).
# Individual JSON files are always generated first regardless of this setting.
GENERATE_BACKUP_FILE = True

# Filter by vessel IMO (integer). Set to None to include all IMOs in the backup.
IMO_FILTER = 9381500

# Report date range: include reports whose finishDateUTC falls between FROM and TO (both inclusive).
# Use ISO-8601 format.  Example: "2025-05-04T10:00:00+00:00"
DATE_FROM = "2025-12-31T11:00:00+00:00"
DATE_TO   = "2026-01-31T11:00:00+00:00"

# Viscosity values (cSt @ 15 °C) to apply for each fuel grade.
# Only grades listed here will be updated.  Grades not listed are left unchanged.
# Set a value to None to skip that grade even if it is listed.
VISCOSITY_VALUES = {
    "ulsfo":    19.60,
    "vlsfo":    0,    # <-- no existing value in the backup; set yours here
    "mgo":       2.65,
    "mdo":       0,
    "hsfo":    0,
    "hsmgo":     0,
    "ulsmgo":    0,
    "hsfoBio": 0,
    "ulsfoBio": 0,
    "mgoBio":    0,
}

# Offline app version string written into the meta block of each individual output JSON
OFFLINE_APP_VERSION = "6.2.1"

# Auth block — copied from the submission wrapper in an existing offline report JSON
AUTH = {
    "tenant": "vitol",
    "token":  "141774868d3033b41dd1293fcdeef1cd298fc26677787e21cb770ed9238d5364",
}

# =============================================================================
# Script logic — no need to edit below this line
# =============================================================================

REPORT_TYPES_WITH_CONSUMPTION = {"PORT", "SEA", "ARRIVAL", "DEPARTURE"}


def _parse_dt(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _make_report_id(finish_utc: str, report_type: str, submission_dt: datetime, index: int) -> str:
    """
    Build a reportId matching the ZeroNorth offline format:
        {YYMMDDHHmm}-{TYPE}_{YYMMDDHHmmss}{ii}
    The two-digit index suffix ensures uniqueness when multiple reports share
    the same finishDateUTC (common in dense port sequences).
    """
    fd = _parse_dt(finish_utc)
    fin_part = fd.strftime("%y%m%d%H%M")
    sub_part = submission_dt.strftime("%y%m%d%H%M%S")
    return f"{fin_part}-{report_type}_{sub_part}{index:02d}"


def _safe_date(finish_utc: str) -> str:
    """Make finishDateUTC filesystem-safe: 2025-05-04T10:00:00+00:00 → 2025-05-04T10-00-00Z"""
    s = finish_utc.replace(":", "-").replace("+00-00", "Z").replace("+00:00", "Z")
    return s.rstrip("Z") + "Z"


def _make_filename(imo: int, reference_id: str, report_type: str, finish_utc: str,
                   seen: dict) -> str:
    """
    Build output filename:  {imo}-{referenceId}-{type}-{finishDateUTC}.json
    When multiple reports produce the same base name (same referenceId, type and
    finishDateUTC within one port stay), a _N counter is appended so no file is
    silently overwritten.
    """
    ref  = reference_id or "noid"
    base = f"{imo}-{ref}-{report_type}-{_safe_date(finish_utc)}"
    count = seen.get(base, 0) + 1
    seen[base] = count
    suffix = f"_{count}" if count > 1 else ""
    return f"{base}{suffix}.json"


def _build_offline_meta(original: dict, submission_dt: datetime) -> dict:
    """Convert the online-app meta block to the offline-submission format."""
    return {
        "creationDate":            original.get("creationDate"),
        "submittedDate":           submission_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "version":                 OFFLINE_APP_VERSION,
        "browser":                 original.get("browser", "chrome"),
        "browserVersion":          original.get("browserVersion", "147.0.0"),
        "sendFromOfflineApp":      True,
        "offlineVesselAppVersion": OFFLINE_APP_VERSION,
        "customReport":            original.get("customReport", False),
        "template":                original.get("template"),
        "validationWarnings":      original.get("validationWarnings", {}),
    }


def _patch_viscosity(sections: list, viscosity_map: dict) -> int:
    """
    Update viscosity15Degrees in the CONSUMPTION section bunkerGrades list.
    Only touches entries where:
      - grade is in viscosity_map with a non-None value
      - enabled == True
    Returns the number of grade entries updated.
    """
    updated = 0
    for section in sections:
        if section["type"] != "CONSUMPTION":
            continue
        grades = section["data"].get("bunkerGrades")
        if not isinstance(grades, list):
            continue
        for grade in grades:
            gname = grade.get("grade")
            new_vis = viscosity_map.get(gname)
            if new_vis is None or not grade.get("enabled"):
                continue
            grade["viscosity15Degrees"] = new_vis
            updated += 1
    return updated


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # ------------------------------------------------------------------
    # Load backup
    # ------------------------------------------------------------------
    backup_path = os.path.join(script_dir, BACKUP_FILE)
    if not os.path.exists(backup_path):
        sys.exit(f"ERROR: Backup file not found: {backup_path}")

    print(f"Loading {backup_path} …")
    with open(backup_path, encoding="utf-8") as fh:
        source_data = json.load(fh)

    all_reports = source_data.get("report", [])
    print(f"  {len(all_reports)} total reports in backup")

    # ------------------------------------------------------------------
    # Filter
    # ------------------------------------------------------------------
    date_from = _parse_dt(DATE_FROM)
    date_to   = _parse_dt(DATE_TO)

    selected = []
    for r in all_reports:
        if r.get("deleted"):
            continue
        if IMO_FILTER is not None and r.get("imo") != IMO_FILTER:
            continue
        if r.get("type") not in REPORT_TYPES_WITH_CONSUMPTION:
            continue
        finish = _parse_dt(r["finishDateUTC"])
        if date_from <= finish <= date_to:
            selected.append(r)

    selected.sort(key=lambda r: r["finishDateUTC"])

    if not selected:
        sys.exit(
            "No matching reports found.\n"
            f"  IMO filter : {IMO_FILTER}\n"
            f"  Date range : {DATE_FROM}  →  {DATE_TO}\n"
            "Check your configuration."
        )

    print(f"  {len(selected)} reports match filter")

    # ------------------------------------------------------------------
    # Deduplicate by referenceId — keep only the latest submittedDate
    # ------------------------------------------------------------------
    _ref_best: dict = {}
    _no_ref: list = []
    for r in selected:
        ref_id = r.get("referenceId", "")
        if not ref_id:
            _no_ref.append(r)
            continue
        existing = _ref_best.get(ref_id)
        if existing is None:
            _ref_best[ref_id] = r
        else:
            existing_dt = _parse_dt(existing["meta"].get("submittedDate", "1970-01-01T00:00:00+00:00"))
            current_dt  = _parse_dt(r["meta"].get("submittedDate", "1970-01-01T00:00:00+00:00"))
            if current_dt > existing_dt:
                _ref_best[ref_id] = r

    duplicates_removed = len(selected) - len(_ref_best) - len(_no_ref)
    selected = sorted(list(_ref_best.values()) + _no_ref, key=lambda r: r["finishDateUTC"])
    if duplicates_removed:
        print(f"  {duplicates_removed} duplicate(s) removed (kept latest submittedDate per referenceId)")
    print(f"  {len(selected)} reports after deduplication")
    print(f"  Span: {selected[0]['finishDateUTC']}  →  {selected[-1]['finishDateUTC']}")

    # ------------------------------------------------------------------
    # Step 1 — Generate individual submission JSON files
    # ------------------------------------------------------------------
    out_dir = os.path.join(script_dir, OUTPUT_DIR)
    os.makedirs(out_dir, exist_ok=True)
    submission_dt = datetime.now(timezone.utc)

    stats = {"generated": 0, "viscosity_changes": 0, "no_enabled_grades": 0, "collisions": 0}

    # tracks seen base-names to append _N suffix on duplicates
    seen_filenames: dict = {}

    print(f"\n── Step 1: Individual JSON files ──────────────────────────────────────────────────────")
    print(f"{'Report type':<26} {'finishDateUTC':<32} {'Changes':>8}  Output file")
    print("-" * 110)

    for idx, r in enumerate(selected):
        # --- Patch viscosity on a copy for the individual submission JSON ---
        submission_report = deepcopy(r)
        changes = _patch_viscosity(submission_report["sections"], VISCOSITY_VALUES)
        if changes == 0:
            stats["no_enabled_grades"] += 1

        original_id = submission_report["reportId"]
        new_id      = _make_report_id(
            submission_report["finishDateUTC"], submission_report["type"], submission_dt, idx
        )

        submission_report.pop("local", None)
        submission_report["editedFromReportId"] = original_id
        submission_report["reportId"]           = new_id
        submission_report["meta"]               = _build_offline_meta(
            submission_report["meta"], submission_dt
        )

        output = {
            "item": {"body": submission_report, "type": "report"},
            "auth": {
                "tenant": AUTH["tenant"],
                "imo":    submission_report["imo"],
                "token":  AUTH["token"],
            },
            "type": "email-ingest-plain",
        }

        ref      = r.get("referenceId", "")
        base_key = f"{r['imo']}-{ref or 'noid'}-{r['type']}-{_safe_date(r['finishDateUTC'])}"
        filename = _make_filename(r["imo"], ref, r["type"], r["finishDateUTC"], seen_filenames)
        if seen_filenames[base_key] > 1:
            stats["collisions"] += 1
        filepath = os.path.join(out_dir, filename)
        with open(filepath, "w", encoding="utf-8") as fh:
            json.dump(output, fh, separators=(",", ":"), ensure_ascii=False)

        print(f"{r['type']:<26} {r['finishDateUTC']:<32} {changes:>8}  {filename}")
        stats["generated"] += 1
        stats["viscosity_changes"] += changes

    print("-" * 110)
    print(f"\n  Individual files generated       : {stats['generated']}")
    print(f"  Total viscosity fields set       : {stats['viscosity_changes']}")
    if stats["collisions"]:
        print(f"  Filename collisions (_N appended): {stats['collisions']}")
    if stats["no_enabled_grades"]:
        print(f"  Reports with no enabled grades   : {stats['no_enabled_grades']}")

    # ------------------------------------------------------------------
    # Step 2 — Generate consolidated backup file  (optional)
    # ------------------------------------------------------------------
    # Strategy: deep-copy the ENTIRE original source (all reports, all top-level
    # keys like reportsConfiguration/settings/source), then patch viscosity only
    # on the reports that match the filter.  Nothing else is touched — reportIds,
    # meta, and every other field remain byte-for-byte identical to the original.
    # ------------------------------------------------------------------
    if GENERATE_BACKUP_FILE:
        print(f"\n── Step 2: Consolidated backup file ───────────────────────────────────────────────────")
        print(f"  Deep-copying original backup ({len(all_reports)} reports) …", end=" ", flush=True)
        backup_data = deepcopy(source_data)
        print("done")

        # Build a lookup of reportIds that are in our filtered selection
        selected_ids = {r["reportId"] for r in selected}

        patched_count = 0
        for report in backup_data["report"]:
            if report.get("reportId") in selected_ids:
                _patch_viscosity(report["sections"], VISCOSITY_VALUES)
                patched_count += 1

        ts_str = submission_dt.strftime("%Y-%m-%dT%H_%M_%S.000Z")
        imo_str = str(IMO_FILTER) if IMO_FILTER else "all"
        backup_filename = f"ZN-{imo_str}-backup-patched-{ts_str}.json"
        backup_filepath = os.path.join(script_dir, backup_filename)

        print(f"  Reports viscosity-patched : {patched_count} of {len(all_reports)}")
        print(f"  Writing                   : {backup_filepath} …", end=" ", flush=True)

        with open(backup_filepath, "w", encoding="utf-8") as fh:
            json.dump(backup_data, fh, indent=4, ensure_ascii=False)

        size_mb = os.path.getsize(backup_filepath) / (1024 * 1024)
        print(f"done  ({size_mb:.1f} MB)")
    else:
        print(f"\n  Backup file generation skipped (GENERATE_BACKUP_FILE = False)")

    # ------------------------------------------------------------------
    # Final summary
    # ------------------------------------------------------------------
    print(f"\n── Summary ─────────────────────────────────────────────────────────────────────────────")
    print(f"  Individual JSONs  → {out_dir}/")
    if GENERATE_BACKUP_FILE:
        print(f"  Backup file       → {backup_filepath}")
    print()


if __name__ == "__main__":
    main()
