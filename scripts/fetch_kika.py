#!/usr/bin/env python3
import datetime, os, re, requests, io, sys, json
import pandas as pd
from pdfminer.high_level import extract_text
import boto3

KANPOU_BASE = "https://kanpou.npb.go.jp"

def list_today_pdfs(date):
    # 本紙・号外の URL パターン例（号外は適宜追加）
    ymd = date.strftime("%Y%m%d")
    return [
        f"{KANPOU_BASE}/{ymd}/{ymd}g00001/{ymd}g000010000.pdf",   # 本紙全ページ
    ]

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
    all_rows=[]
    for url in list_today_pdfs(date):
        try:
            pdf = download(url)
        except Exception as e:
            print(f"skip {url}: {e}", file=sys.stderr); continue
        # 保存
        rel = f"raw/{date}/{os.path.basename(url)}"
        os.makedirs(os.path.dirname(rel), exist_ok=True)
        with open(rel,"wb") as f: f.write(pdf)
        if bucket: save_s3(bucket, rel, pdf, "application/pdf")
        # 抽出
        rows = extract_kika_records(pdf)
        all_rows.extend(rows)

    if all_rows:
        df = pd.DataFrame(all_rows)
        csv_path=f"data/csv/{date}_kika.csv"
        df.to_csv(csv_path,index=False)
        if bucket: save_s3(bucket, csv_path, df.to_csv(index=False), "text/csv")

if __name__ == "__main__":
    main()

