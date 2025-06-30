#!/usr/bin/env python3
import datetime
import os
import re
import requests
import io
import sys
import json
import pandas as pd
from pdfminer.high_level import extract_text
from bs4 import BeautifulSoup
import boto3

KANPOU_BASE = "https://kanpou.npb.go.jp"

def find_kika_pdf(date):
    """Return the PDF url when the naturalization notice appears in today's table of contents."""
    ymd = date.strftime("%Y%m%d")
    index_url = f"{KANPOU_BASE}/{ymd}/index.html"
    try:
        html = requests.get(index_url, timeout=20, headers={"User-Agent": "MUTSUMI-bot/1.0"}).text
    except Exception as e:
        print(f"failed to fetch {index_url}: {e}", file=sys.stderr)
        return None
    soup = BeautifulSoup(html, "html.parser")
    h2 = soup.find("h2", string=lambda t: t and "官庁報告" in t)
    if not h2:
        return None
    for el in h2.find_all_next():
        if el.name == "h2":
            break
        if el.name == "a" and el.get_text(strip=True).startswith("日本国に帰化を許可する件"):
            href = el.get("href")
            if href and href.endswith(".pdf"):
                return f"{KANPOU_BASE}/{ymd}/{href}"
    return None

def download(url):
    r = requests.get(url, timeout=30, headers={'User-Agent':'MUTSUMI-bot/1.0'})
    r.raise_for_status()
    return r.content

def extract_kika_records(pdf_bytes):
    text = extract_text(io.BytesIO(pdf_bytes))
    pattern = re.compile(r"帰化\s+([^\s　]+)\s+（([^）]+)）.*生.*")  # ざっくり
    rows=[]
    for line in text.splitlines():
        m = pattern.search(line)
        if m:
            rows.append({"new_name": m.group(1),
                         "old_name": m.group(2),
                         "raw": line})
    return rows

def save_s3(bucket, key, body, content_type):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)

def main():
    date = datetime.date.today()
    bucket = os.getenv("S3_BUCKET")
    all_rows = []

    url = find_kika_pdf(date)
    if not url:
        print("no naturalization notice today")
        return

    try:
        pdf = download(url)
    except Exception as e:
        print(f"skip {url}: {e}", file=sys.stderr)
        return

    rel = f"raw/{date}/{os.path.basename(url)}"
    os.makedirs(os.path.dirname(rel), exist_ok=True)
    with open(rel, "wb") as f:
        f.write(pdf)
    if bucket:
        save_s3(bucket, rel, pdf, "application/pdf")

    rows = extract_kika_records(pdf)
    all_rows.extend(rows)

    if all_rows:
        df = pd.DataFrame(all_rows)
        csv_path=f"data/csv/{date}_kika.csv"
        df.to_csv(csv_path,index=False)
        if bucket: save_s3(bucket, csv_path, df.to_csv(index=False), "text/csv")

if __name__ == "__main__":
    main()

