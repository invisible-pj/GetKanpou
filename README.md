# GetKanpou

This repository provides simple scripts for extracting naturalization notices from the Japanese government gazette (官報).

## Setup

Install the required dependencies using pip:

```bash
pip install -r requirements.txt
```

## Daily Fetch

`scripts/fetch_kika.py` checks the table of contents for today's issue. If the "官庁報告" section contains an entry beginning with `日本国に帰化を許可する件`, the script downloads the corresponding PDF, extracts naturalization records and saves them locally. When the `S3_BUCKET` environment variable is set, the PDF and CSV are also uploaded to S3.

Run it manually as:

```bash
python scripts/fetch_kika.py
```

## Backfill

To collect past data:

```bash
export S3_BUCKET=your-bucket-name
python scripts/backfill_kika_v2.py
```
