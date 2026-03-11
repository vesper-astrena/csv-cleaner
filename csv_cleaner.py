"""
CSV Cleaner Toolkit — Automatically detect and fix data quality issues.

Clean messy CSV/Excel files with one command:
  python csv_cleaner.py input.csv -o cleaned.csv

Full version with advanced features available at:
https://vesperfinch.gumroad.com/l/csv-cleaner
"""

import csv
import io
import re
import sys
import json
import argparse
import statistics
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from collections import Counter
from typing import Optional


@dataclass
class ColumnProfile:
    name: str
    original_name: str
    dtype_guess: str = "string"
    null_count: int = 0
    total_count: int = 0
    unique_count: int = 0
    duplicate_values: int = 0
    empty_strings: int = 0
    whitespace_issues: int = 0
    mixed_types: bool = False
    date_formats_found: list = field(default_factory=list)
    numeric_outliers: list = field(default_factory=list)
    sample_values: list = field(default_factory=list)


@dataclass
class CleaningReport:
    input_file: str
    output_file: str
    original_rows: int = 0
    cleaned_rows: int = 0
    original_columns: int = 0
    cleaned_columns: int = 0
    duplicates_removed: int = 0
    nulls_handled: int = 0
    types_fixed: int = 0
    whitespace_fixed: int = 0
    dates_normalized: int = 0
    encoding_issues: int = 0
    columns: list = field(default_factory=list)
    issues: list = field(default_factory=list)


# --- Detection ---

DATE_PATTERNS = [
    (r'^\d{4}-\d{2}-\d{2}$', '%Y-%m-%d'),
    (r'^\d{4}/\d{2}/\d{2}$', '%Y/%m/%d'),
    (r'^\d{2}/\d{2}/\d{4}$', '%m/%d/%Y'),
    (r'^\d{2}-\d{2}-\d{4}$', '%m-%d-%Y'),
    (r'^\d{2}/\d{2}/\d{2}$', '%m/%d/%y'),
    (r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', '%Y-%m-%dT%H:%M:%S'),
    (r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', '%Y-%m-%d %H:%M:%S'),
    (r'^\w+ \d{1,2}, \d{4}$', '%B %d, %Y'),
    (r'^\d{1,2} \w+ \d{4}$', '%d %B %Y'),
]

NULL_VARIANTS = {'', 'null', 'none', 'na', 'n/a', 'nan', '#n/a', '#na', '-', '--', '.', 'missing', 'undefined'}


def detect_encoding(file_path: str) -> str:
    """Detect file encoding by trying common encodings."""
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'shift_jis', 'euc-jp', 'iso-8859-1']
    for enc in encodings:
        try:
            with open(file_path, 'r', encoding=enc) as f:
                f.read(4096)
            return enc
        except (UnicodeDecodeError, UnicodeError):
            continue
    return 'utf-8'


def detect_delimiter(sample: str) -> str:
    """Auto-detect CSV delimiter."""
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample, delimiters=',\t;|')
        return dialect.delimiter
    except csv.Error:
        # Count occurrences
        counts = {d: sample.count(d) for d in [',', '\t', ';', '|']}
        return max(counts, key=counts.get) if any(counts.values()) else ','


def guess_type(values: list[str]) -> str:
    """Guess column data type from sample values."""
    non_null = [v for v in values if v.strip().lower() not in NULL_VARIANTS]
    if not non_null:
        return "empty"

    int_count = 0
    float_count = 0
    date_count = 0
    bool_count = 0

    for v in non_null[:200]:
        v_clean = v.strip().replace(',', '').replace('$', '').replace('€', '').replace('¥', '').replace('%', '')

        # Boolean
        if v.strip().lower() in ('true', 'false', 'yes', 'no', '1', '0', 'y', 'n'):
            bool_count += 1
            continue

        # Integer
        try:
            int(v_clean)
            int_count += 1
            continue
        except ValueError:
            pass

        # Float
        try:
            float(v_clean)
            float_count += 1
            continue
        except ValueError:
            pass

        # Date
        for pattern, _ in DATE_PATTERNS:
            if re.match(pattern, v.strip()):
                date_count += 1
                break

    total = len(non_null[:200])
    if total == 0:
        return "empty"

    # Need 80% consensus
    threshold = total * 0.8
    if int_count >= threshold:
        return "integer"
    if (int_count + float_count) >= threshold:
        return "float"
    if date_count >= threshold:
        return "date"
    if bool_count >= threshold:
        return "boolean"
    return "string"


def detect_date_format(values: list[str]) -> Optional[str]:
    """Detect the most common date format in a column."""
    format_counts = Counter()
    for v in values[:200]:
        v = v.strip()
        for pattern, fmt in DATE_PATTERNS:
            if re.match(pattern, v):
                try:
                    datetime.strptime(v.split('T')[0] if 'T' in v else v, fmt.split('T')[0])
                    format_counts[fmt] += 1
                    break
                except ValueError:
                    continue
    if format_counts:
        return format_counts.most_common(1)[0][0]
    return None


def detect_outliers_iqr(values: list[float], multiplier: float = 1.5) -> list[tuple[int, float]]:
    """Detect outliers using IQR method."""
    if len(values) < 10:
        return []
    sorted_vals = sorted(values)
    q1 = sorted_vals[len(sorted_vals) // 4]
    q3 = sorted_vals[3 * len(sorted_vals) // 4]
    iqr = q3 - q1
    lower = q1 - multiplier * iqr
    upper = q3 + multiplier * iqr
    return [(i, v) for i, v in enumerate(values) if v < lower or v > upper]


# --- Cleaning ---

def standardize_column_name(name: str) -> str:
    """Standardize column names to snake_case."""
    name = name.strip()
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_').lower()
    return name or "unnamed"


def normalize_null(value: str) -> str:
    """Normalize null variants to empty string."""
    if value.strip().lower() in NULL_VARIANTS:
        return ""
    return value


def clean_whitespace(value: str) -> str:
    """Strip and normalize whitespace."""
    return re.sub(r'\s+', ' ', value.strip())


def normalize_date(value: str, source_fmt: str, target_fmt: str = '%Y-%m-%d') -> str:
    """Convert date to standard format."""
    if not value.strip() or value.strip().lower() in NULL_VARIANTS:
        return ""
    try:
        dt = datetime.strptime(value.strip(), source_fmt)
        return dt.strftime(target_fmt)
    except ValueError:
        return value


def parse_number(value: str) -> Optional[float]:
    """Parse a number string, handling currency symbols and formatting."""
    v = value.strip()
    if not v or v.lower() in NULL_VARIANTS:
        return None
    v = re.sub(r'[$€¥£%]', '', v)
    v = v.replace(',', '')
    try:
        return float(v)
    except ValueError:
        return None


# --- Main Pipeline ---

def read_csv(file_path: str) -> tuple[list[str], list[list[str]]]:
    """Read CSV with auto-detected encoding and delimiter."""
    encoding = detect_encoding(file_path)
    with open(file_path, 'r', encoding=encoding, errors='replace') as f:
        sample = f.read(8192)

    delimiter = detect_delimiter(sample)

    with open(file_path, 'r', encoding=encoding, errors='replace') as f:
        reader = csv.reader(f, delimiter=delimiter)
        rows = list(reader)

    if not rows:
        return [], []

    headers = rows[0]
    data = rows[1:]
    return headers, data


def profile_columns(headers: list[str], data: list[list[str]]) -> list[ColumnProfile]:
    """Profile each column for data quality issues."""
    profiles = []
    for col_idx, header in enumerate(headers):
        values = [row[col_idx] if col_idx < len(row) else "" for row in data]
        non_null = [v for v in values if v.strip().lower() not in NULL_VARIANTS]

        profile = ColumnProfile(
            name=standardize_column_name(header),
            original_name=header,
            total_count=len(values),
            null_count=len(values) - len(non_null),
            empty_strings=sum(1 for v in values if v == ""),
            unique_count=len(set(values)),
            whitespace_issues=sum(1 for v in values if v != v.strip() or '  ' in v),
            sample_values=non_null[:5],
        )

        profile.dtype_guess = guess_type(values)

        if profile.dtype_guess == "date":
            fmt = detect_date_format(values)
            if fmt:
                profile.date_formats_found = [fmt]

        if profile.dtype_guess in ("integer", "float"):
            nums = []
            for v in values:
                n = parse_number(v)
                if n is not None:
                    nums.append(n)
            if nums:
                outliers = detect_outliers_iqr(nums)
                profile.numeric_outliers = outliers[:5]

        profiles.append(profile)
    return profiles


def clean_data(
    headers: list[str],
    data: list[list[str]],
    profiles: list[ColumnProfile],
    remove_duplicates: bool = True,
    standardize_names: bool = True,
    normalize_dates: bool = True,
    fix_whitespace: bool = True,
    handle_nulls: bool = True,
) -> tuple[list[str], list[list[str]], CleaningReport]:
    """Apply cleaning operations and generate report."""
    report = CleaningReport(
        input_file="", output_file="",
        original_rows=len(data),
        original_columns=len(headers),
    )

    # Standardize column names
    if standardize_names:
        new_headers = []
        seen = Counter()
        for p in profiles:
            name = p.name
            seen[name] += 1
            if seen[name] > 1:
                name = f"{name}_{seen[name]}"
            new_headers.append(name)
        headers = new_headers

    # Clean each cell
    cleaned = []
    for row in data:
        new_row = []
        for col_idx, profile in enumerate(profiles):
            val = row[col_idx] if col_idx < len(row) else ""

            if handle_nulls:
                val = normalize_null(val)

            if fix_whitespace and val:
                old_val = val
                val = clean_whitespace(val)
                if val != old_val:
                    report.whitespace_fixed += 1

            if normalize_dates and profile.dtype_guess == "date" and profile.date_formats_found and val:
                old_val = val
                val = normalize_date(val, profile.date_formats_found[0])
                if val != old_val:
                    report.dates_normalized += 1

            new_row.append(val)
        cleaned.append(new_row)

    # Remove duplicate rows
    if remove_duplicates:
        seen_rows = set()
        deduped = []
        for row in cleaned:
            key = tuple(row)
            if key not in seen_rows:
                seen_rows.add(key)
                deduped.append(row)
        report.duplicates_removed = len(cleaned) - len(deduped)
        cleaned = deduped

    report.cleaned_rows = len(cleaned)
    report.cleaned_columns = len(headers)
    report.columns = [asdict(p) for p in profiles]

    return headers, cleaned, report


def write_csv(file_path: str, headers: list[str], data: list[list[str]]):
    """Write cleaned CSV."""
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)


def print_report(report: CleaningReport):
    """Print human-readable cleaning report."""
    print(f"\n{'='*60}")
    print(f"  CSV Cleaner — Cleaning Report")
    print(f"{'='*60}")
    print(f"  Input:  {report.input_file}")
    print(f"  Output: {report.output_file}")
    print(f"\n  Rows:    {report.original_rows} → {report.cleaned_rows} ({report.duplicates_removed} duplicates removed)")
    print(f"  Columns: {report.original_columns} → {report.cleaned_columns}")
    print(f"\n  Changes:")
    print(f"    Whitespace fixed:    {report.whitespace_fixed}")
    print(f"    Dates normalized:    {report.dates_normalized}")
    print(f"    Duplicates removed:  {report.duplicates_removed}")

    if report.columns:
        print(f"\n  Column Profiles:")
        for col in report.columns:
            null_pct = (col['null_count'] / col['total_count'] * 100) if col['total_count'] > 0 else 0
            issues = []
            if null_pct > 10:
                issues.append(f"{null_pct:.0f}% null")
            if col['whitespace_issues'] > 0:
                issues.append(f"{col['whitespace_issues']} whitespace")
            if col['numeric_outliers']:
                issues.append(f"{len(col['numeric_outliers'])} outliers")
            issue_str = f" ⚠ {', '.join(issues)}" if issues else ""
            name_change = f" (was: {col['original_name']})" if col['name'] != col['original_name'].strip().lower() else ""
            print(f"    {col['name']}{name_change}: {col['dtype_guess']}, {col['unique_count']} unique{issue_str}")

    print(f"\n{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="CSV Cleaner — Fix messy data automatically")
    parser.add_argument("input", help="Input CSV file")
    parser.add_argument("-o", "--output", help="Output CSV file (default: input_cleaned.csv)")
    parser.add_argument("--report", help="Save JSON report to file")
    parser.add_argument("--profile-only", action="store_true", help="Only profile, don't clean")
    parser.add_argument("--keep-duplicates", action="store_true", help="Don't remove duplicates")
    parser.add_argument("--keep-names", action="store_true", help="Don't standardize column names")
    parser.add_argument("--no-date-normalize", action="store_true", help="Don't normalize dates")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: {input_path} not found")
        sys.exit(1)

    output_path = args.output or str(input_path.stem) + "_cleaned.csv"

    print(f"CSV Cleaner — Processing {input_path}")
    print(f"{'='*40}")

    # Read
    headers, data = read_csv(str(input_path))
    print(f"  Read {len(data)} rows, {len(headers)} columns")

    # Profile
    profiles = profile_columns(headers, data)

    if args.profile_only:
        report = CleaningReport(
            input_file=str(input_path), output_file="(profile only)",
            original_rows=len(data), original_columns=len(headers),
        )
        report.columns = [asdict(p) for p in profiles]
        print_report(report)
        return

    # Clean
    headers, cleaned, report = clean_data(
        headers, data, profiles,
        remove_duplicates=not args.keep_duplicates,
        standardize_names=not args.keep_names,
        normalize_dates=not args.no_date_normalize,
    )
    report.input_file = str(input_path)
    report.output_file = output_path

    # Write
    write_csv(output_path, headers, cleaned)
    print_report(report)

    # Save JSON report
    if args.report:
        with open(args.report, 'w') as f:
            json.dump(asdict(report), f, indent=2, default=str)
        print(f"\n  JSON report saved: {args.report}")

    print(f"\n  ✓ Cleaned file saved: {output_path}")


if __name__ == "__main__":
    main()
