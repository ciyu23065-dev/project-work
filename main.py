# ===== 事前インストール（必要なら最初に） =====
# pip install --upgrade google-api-python-client google-auth google-auth-httplib2 google-auth-oauthlib pandas

from __future__ import annotations
import io
import csv
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlparse, parse_qs
import re
import os
import json
import pandas as pd

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2.service_account import Credentials


# ========== 設定 ==========
# YouTube Data API の APIキー（公開データのみ取得に使用）
YOUTUBE_API_KEY = "AIzaSyC1AJIdp2eulKBv5SPVGgptMlmC7a1PQwI"   # ←あなたのAPIキーに置換

# Google Drive上の共有CSVのファイルID（指定済み）
DRIVE_FILE_ID = "1fZntJuEUcpTeXcbR5aGWvVj8WfsAE_Cb"

# サービスアカウントの認証ファイル（同一ディレクトリ）
SERVICE_ACCOUNT_JSON = "credentials.json"

# ローカルにバックアップとしても保存
LOCAL_CSV_PATH = "local_youtube_views.csv"

# 取得したい動画URLを指定（何本でもOK）
VIDEO_URLS = [
    # 例:
    "https://youtu.be/pv8A7eubPQQ?si=cAZ3HIwTN_q_evlH",
    "https://youtu.be/HcXduBwK5B4?si=SzkZxq1KKuMPcnRg",
    "https://youtu.be/ZfIXXgqxVn8?si=_61UUSlWh4aBeH7W",
    "https://youtu.be/mvBx-q8jnJc?si=r_Bn-GsdmydAhFTM",
    "https://youtu.be/Ca5cdthagBM?si=I4lxZcKMeZfP9ziB",
]

# 保存するカラム（要件：日付・動画名・再生数）
CSV_COLUMNS = ["date", "title", "views"]


# ========== ユーティリティ ==========
def extract_video_id(url: str) -> str | None:
    """
    YouTubeの各種URLから videoId を抽出。
      - https://www.youtube.com/watch?v=VIDEOID&...
      - https://youtu.be/VIDEOID?...
      - https://www.youtube.com/shorts/VIDEOID
    """
    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        path = parsed.path

        # youtu.be/VIDEOID
        if "youtu.be" in host:
            vid = path.strip("/").split("/")[0]
            return vid or None

        if "youtube.com" in host:
            # /shorts/VIDEOID
            m = re.match(r"^/shorts/([A-Za-z0-9_\-]{5,})", path)
            if m:
                return m.group(1)
            # /watch?v=VIDEOID
            qs = parse_qs(parsed.query)
            if "v" in qs and len(qs["v"]) > 0:
                return qs["v"][0]
        return None
    except Exception:
        return None


def now_date_jst() -> str:
    """JSTで YYYY-MM-DD を返す。"""
    return datetime.now(ZoneInfo("Asia/Tokyo")).date().isoformat()


# ========== YouTube API（APIキー） ==========
def fetch_video_stats(video_ids: list[str], api_key: str) -> dict[str, dict]:
    """
    video_ids について title と viewCount を取得。
    戻り値: {video_id: {"title": str, "views": int}}
    """
    yt = build("youtube", "v3", developerKey=api_key)
    results: dict[str, dict] = {}
    BATCH = 50
    for i in range(0, len(video_ids), BATCH):
        chunk = video_ids[i:i + BATCH]
        resp = yt.videos().list(part="snippet,statistics", id=",".join(chunk)).execute()
        for item in resp.get("items", []):
            vid = item["id"]
            title = item["snippet"]["title"]
            views = int(item.get("statistics", {}).get("viewCount", 0))
            results[vid] = {"title": title, "views": views}
    return results


# ========== Drive API（サービスアカウント） ==========
def build_drive_client(sa_json: str):
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(sa_json, scopes=scopes)
    return build("drive", "v3", credentials=creds), creds


def download_drive_csv_to_df(drive, file_id: str) -> pd.DataFrame:
    """
    DriveのCSV(file_id)をダウンロードしてDataFrameへ。
    壊れている/空の場合は空DataFrame（CSV_COLUMNS）を返す。
    """
    try:
        request = drive.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        data = fh.getvalue()
        if not data:
            return pd.DataFrame(columns=CSV_COLUMNS)
        df = pd.read_csv(io.BytesIO(data))
        # 欠けている列を追加し、必要列に揃える
        for c in CSV_COLUMNS:
            if c not in df.columns:
                df[c] = pd.NA
        return df[CSV_COLUMNS]
    except Exception as e:
        print(f"[WARN] Drive CSVを新規扱いにします: {e}")
        return pd.DataFrame(columns=CSV_COLUMNS)


def upload_df_to_drive_csv(drive, file_id: str, df: pd.DataFrame):
    """
    DataFrameをCSV化して、同じfile_idに上書きアップロード。
    """
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype="text/csv", resumable=False)
    drive.files().update(fileId=file_id, media_body=media).execute()


# ========== ローカルCSV ==========
def append_to_local_csv(rows: list[dict], local_path: str, columns: list[str]):
    """
    ローカルCSVに追記（ヘッダが無ければ付与）。
    """
    file_exists = os.path.isfile(local_path)
    with open(local_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        if not file_exists:
            writer.writeheader()
        for r in rows:
            writer.writerow(r)


# ========== メイン処理 ==========
def main():
    # --- 認証/共有チェック（Helpful出力） ---
    try:
        with open(SERVICE_ACCOUNT_JSON, "r", encoding="utf-8") as f:
            client_email = json.load(f).get("client_email", "")
        if client_email:
            print(f"[INFO] サービスアカウント: {client_email}")
            print("      ※ このメールをDriveの対象CSV(または親フォルダ)に『編集者』で共有してください。")
    except Exception:
        print("[WARN] credentials.json を読み取れませんでした。パスとファイル内容を確認してください。")

    # 1) URLからvideoId抽出
    video_ids, bad_urls = [], []
    for url in VIDEO_URLS:
        vid = extract_video_id(url)
        (video_ids if vid else bad_urls).append(vid or url)

    if bad_urls:
        print("※ videoIdを抽出できなかったURL:", bad_urls)
    if not video_ids:
        print("動画IDが0件です。VIDEO_URLSを確認してください。")
        return

    # 2) YouTubeから title / views を取得
    stats = fetch_video_stats(video_ids, YOUTUBE_API_KEY)
    today = now_date_jst()

    # 3) 追記行（date, title, views）
    new_rows = []
    for vid in video_ids:
        s = stats.get(vid)
        if not s:
            continue
        new_rows.append({"date": today, "title": s["title"], "views": s["views"]})
    if not new_rows:
        print("新規に追加できるデータがありませんでした。")
        return

    # 4) ローカルCSVに追記（バックアップ）
    append_to_local_csv(new_rows, LOCAL_CSV_PATH, CSV_COLUMNS)
    print(f"[OK] ローカルに追記しました: {LOCAL_CSV_PATH}")

    # 5) DriveのCSVを取得→追記→上書き
    drive, _ = build_drive_client(SERVICE_ACCOUNT_JSON)
    df_drive = download_drive_csv_to_df(drive, DRIVE_FILE_ID)
    df_append = pd.DataFrame(new_rows, columns=CSV_COLUMNS)
    df_out = pd.concat([df_drive, df_append], ignore_index=True)
    upload_df_to_drive_csv(drive, DRIVE_FILE_ID, df_out)
    print("[OK] Drive上の共有CSVに追記して上書きしました。")


if __name__ == "__main__":
    main()

