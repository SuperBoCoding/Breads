# prepare_franchises.py  (CSV -> web/franchises.json)
# pip install pandas
# 사용: python prepare_franchises.py

import os, json, math
import pandas as pd
import numpy as np

HERE = os.path.dirname(__file__)
DATA = os.path.join(HERE, "data")
OUT  = os.path.join(HERE, "web", "franchises.json")

# CSV 파일명 ↔ 라벨
FILES = {
    "dunkin":   ("Dunkin_compiled.csv",       "던킨"),
    "paris":    ("Paris_compiled.csv",        "파리바게뜨"),
    "pariscr":  ("ParisCr_compiled.csv",      "파리크라상"),
    "tlj":      ("TousLesJours_compiled.csv", "뚜레쥬르"),
}

def try_read_csv(path):
    for enc in ("utf-8-sig", "cp949", "euc-kr", "utf-8"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    # 마지막 시도(인코딩 지정 없이)
    return pd.read_csv(path)

def find_col(cols, candidates):
    norm = {c.strip().lower(): c for c in cols}
    for cand in candidates:
        lc = cand.strip().lower()
        if lc in norm: return norm[lc]
    return None

def to_float(x):
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return float("nan")

def plausible_korea(lat, lon):
    return 33.0 <= lat <= 39.5 and 124.0 <= lon <= 132.5

out = {}

for key, (fname, label) in FILES.items():
    path = os.path.join(DATA, fname)
    if not os.path.exists(path):
        print(f"[WARN] {label}: 파일 없음 -> {path}")
        out[key] = {"label": label, "stores": []}
        continue

    df = try_read_csv(path)

    # 필요한 컬럼 찾기 (대소문자/언어 무시)
    col_id   = find_col(df.columns, ["id", "place_id"])
    col_name = find_col(df.columns, ["place_name", "name", "매장명", "상호명"])
    col_addr = find_col(df.columns, ["address_name", "지번주소"])
    col_radr = find_col(df.columns, ["road_address_name", "도로명주소"])
    col_x    = find_col(df.columns, ["x", "lon", "lng", "경도"])
    col_y    = find_col(df.columns, ["y", "lat", "위도"])

    if not (col_name and col_x and col_y):
        print(f"[ERROR] {label}: 필수 컬럼 누락 (name/x/y). 실제 컬럼: {list(df.columns)}")
        out[key] = {"label": label, "stores": []}
        continue

    # 숫자화
    xs = df[col_x].map(to_float)
    ys = df[col_y].map(to_float)

    stores = []
    seen = set()  # 중복 제거용 (lat,lon,이름) 라운드 키

    for i, row in df.iterrows():
        name  = str(row[col_name]).strip() if pd.notna(row[col_name]) else ""
        addr  = str(row[col_addr]).strip() if (col_addr and pd.notna(row[col_addr])) else ""
        raddr = str(row[col_radr]).strip() if (col_radr and pd.notna(row[col_radr])) else ""
        lon   = to_float(row[col_x])
        lat   = to_float(row[col_y])

        if not name: continue
        if math.isnan(lat) or math.isnan(lon): continue
        if not plausible_korea(lat, lon):      continue

        dedup_key = (round(lat, 5), round(lon, 5), name)
        if dedup_key in seen:  # 근접 중복 제거
            continue
        seen.add(dedup_key)

        sid = str(row[col_id]) if (col_id and pd.notna(row[col_id])) else f"{key}_{i}"

        stores.append({
            "id": sid,
            "name": name,
            "address": addr,
            "road_address": raddr,
            "lat": round(lat, 6),   # lat = y
            "lon": round(lon, 6)    # lon = x
        })

    out[key] = {"label": label, "stores": stores}
    print(f"[OK] {label}: {len(stores)} stores")

os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, "w", encoding="utf-8") as f:
    json.dump(out, f, ensure_ascii=False, indent=2)

print(f"[DONE] wrote {OUT}")
