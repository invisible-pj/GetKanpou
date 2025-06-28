import datetime, os, re, requests, io
import pandas as pd
from pathlib import Path
from pdfminer.high_level import extract_text
from bs4 import BeautifulSoup  # pip install beautifulsoup4
import boto3

KANPOU_BASE = "https://kanpou.npb.go.jp"
BUCKET = os.environ.get("S3_BUCKET")


def list_pdfs_from_index(date):
    ymd = date.strftime("%Y%m%d")
    index_url = f"{KANPOU_BASE}/{ymd}/index.html"
    try:
        html = requests.get(index_url, timeout=10).text
    except:
        print(f"skip {index_url}")
        return []
    soup = BeautifulSoup(html, "html.parser")
    pdfs = []
    for a in soup.find_all("a", href=True):
        href = a['href']
        if href.endswith(".pdf"):
            pdfs.append(f"{KANPOU_BASE}/{ymd}/{href}")
    return pdfs


def download_pdf(url):
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.content
    except:
        print(f"skip {url}")
        return None


def extract_kika(pdf_bytes):
    text = extract_text(io.BytesIO(pdf_bytes))
    pattern = re.compile(r"帰化\s+([^\s　]+)\s+（([^）]+)）.*生.*")
    rows = []
    for line in text.splitlines():
        m = pattern.search(line)
        if m:
            rows.append({
                "new_name": m.group(1),
                "old_name": m.group(2),
                "raw": line
            })
    return rows


def save(date, seq, pdf, rows):
    ymd = str(date)
    local_pdf = f"data/raw/{ymd}/kanpou_{seq}.pdf"
    local_csv = f"data/csv/{ymd}_kika.csv"

    Path(os.path.dirname(local_pdf)).mkdir(parents=True, exist_ok=True)
    Path("data/csv").mkdir(exist_ok=True)

    with open(local_pdf, "wb") as f:
        f.write(pdf)

    if rows:
        mode = "a" if os.path.exists(local_csv) else "w"
        df = pd.DataFrame(rows)
        df.to_csv(local_csv, mode=mode, header=not os.path.exists(local_csv), index=False)

    if BUCKET:
        s3 = boto3.client("s3")
        s3.upload_file(local_pdf, BUCKET, f"raw/{ymd}/kanpou_{seq}.pdf")
        if rows:
            s3.upload_file(local_csv, BUCKET, f"csv/{ymd}_kika.csv")


if __name__ == "__main__":
    start = datetime.date(2025, 4, 1)
    end = datetime.date.today()

    for i in range((end - start).days + 1):
        date = start + datetime.timedelta(days=i)
        ymd = str(date)
        csv_out = f"data/csv/{ymd}_kika.csv"
        if os.path.exists(csv_out):
            print(f"{ymd} already exists, skip")
            continue

        urls = list_pdfs_from_index(date)
        for j, url in enumerate(urls):
            pdf = download_pdf(url)
            if not pdf:
                continue
            rows = extract_kika(pdf)
            if rows:
                print(f"{ymd}: {len(rows)} rows from {url}")
            save(date, j+1, pdf, rows)

