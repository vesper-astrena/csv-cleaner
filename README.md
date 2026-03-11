# CSV Cleaner

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](#)

Automatically detect and fix data quality issues in CSV files. One command, clean data.

## Quick Start

```bash
python csv_cleaner.py messy_data.csv -o clean_data.csv
```

No external dependencies. Python 3.10+ only.

## What It Fixes

- **Duplicate rows** — exact match removal
- **Column names** — standardized to snake_case
- **Whitespace** — leading/trailing spaces, double spaces
- **Null variants** — NULL, N/A, None, -, nan, #N/A → normalized
- **Date formats** — auto-detects 9+ formats, normalizes to ISO 8601
- **Encoding** — auto-detects UTF-8, Latin-1, Shift-JIS, etc.
- **Type detection** — infers integer, float, date, boolean, string
- **Outlier detection** — IQR-based outlier identification

## Example

```
$ python csv_cleaner.py sales_export.csv -o sales_clean.csv

CSV Cleaner — Processing sales_export.csv
  Read 1,247 rows, 12 columns

  Rows:    1,247 → 1,198 (49 duplicates removed)
  Columns: 12 → 12

  Changes:
    Whitespace fixed:    156
    Dates normalized:    89
    Duplicates removed:  49

  Column Profiles:
    customer_name: string, 892 unique
    email: string, 1,043 unique ⚠ 3% null
    signup_date: date, 365 unique
    revenue: float, 1,102 unique ⚠ 4 outliers
    status: string, 3 unique
```

## Options

```bash
# Profile only (analyze without modifying)
python csv_cleaner.py input.csv --profile-only

# Keep original column names
python csv_cleaner.py input.csv --keep-names

# Save detailed JSON report
python csv_cleaner.py input.csv -o clean.csv --report report.json
```

## Pro Version

The [CSV Cleaner Pro](https://vesperfinch.gumroad.com/l/csv-cleaner) ($19) adds:

- Smart fill for missing values (mean, median, mode, forward-fill)
- Fuzzy deduplication (find similar-but-not-identical rows)
- Outlier handling (flag, cap, or remove)
- Custom validation rules (regex, ranges, required fields)
- Batch processing (clean entire directories)
- Column splitting and merging
- Beautiful HTML reports
- Excel (.xlsx) support

## Requirements

- Python 3.10+
- No external dependencies

## License

MIT — free for personal and commercial use.
