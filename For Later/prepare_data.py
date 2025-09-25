# prepare_data.py
# pip install pandas openpyxl pyproj
# 사용: python prepare_data.py
# 입력: ./data/bakeries.xlsx (예시 헤더: Number, Address1, Address2, Name, X, Y, Size)
# 출력: ./web/places.json  (schema: [{id,name,road_address,address,lat,lon,size}])

import os
import json
import math
import pandas as pd
from pyproj import Transformer

HERE = os.path.dirname(__file__)
IN_XLSX = os.path.join(HERE, "data", "bakeries.xlsx")
OUT_JSON = os.path.join(HERE, "web", "places.json")

# EPSG:5174(중부원점 Bessel) -> WGS84(4326)
TX_5174_TO_4326 = Transformer.from_crs("EPSG:5174", "EPSG:4326", always_xy=True)

def _find_col(cols, candidates):
    cl = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in cl:
            return cl[cand.lower()]
    return None

def plausible_korea(lat, lon):
    return (33.0 <= lat <= 39.5) and (124.0 <= lon <= 132.5)

def main():
    if not os.path.exists(IN_XLSX):
        raise SystemExit(f"[ERROR] 입력 엑셀이 없습니다: {IN_XLSX}")

    df = pd.read_excel(IN_XLSX)

    # 스샷 헤더 대응 + 유연 매칭
    col_id   = _find_col(df.columns, ["Number", "번호", "id"])
    col_name = _find_col(df.columns, ["Name", "매장명", "상호명", "store_name"])
    col_addr = _find_col(df.columns, ["Address1", "지번주소", "address1", "주소"])
    col_radr = _find_col(df.columns, ["Address2", "도로명주소", "road_address", "address2"])
    col_x    = _find_col(df.columns, ["X", "x", "x_5174", "EPSG5174_X"])
    col_y    = _find_col(df.columns, ["Y", "y", "y_5174", "EPSG5174_Y"])
    col_size = _find_col(df.columns, ["Size", "size", "면적", "규모"])

    missing = [k for k,v in {"Name":col_name, "X":col_x, "Y":col_y}.items() if v is None]
    if missing:
        raise SystemExit(f"[ERROR] 필수 컬럼 없음: {missing} / 현재 컬럼: {list(df.columns)}")

    rows_out = []
    skipped = 0

    for i, r in df.iterrows():
        try:
            name = str(r[col_name]).strip() if pd.notna(r[col_name]) else ""
            if not name:
                skipped += 1
                continue

            # 주소
            address = str(r[col_addr]).strip() if (col_addr and pd.notna(r[col_addr])) else ""
            road_address = str(r[col_radr]).strip() if (col_radr and pd.notna(r[col_radr])) else ""

            # 좌표 변환 (EPSG:5174 X,Y -> 4326 lon,lat)
            x = float(r[col_x]); y = float(r[col_y])
            lon, lat = TX_5174_TO_4326.transform(x, y)

            if not plausible_korea(lat, lon) or any(math.isnan(v) for v in (lat, lon)):
                skipped += 1
                continue

            size = None
            if col_size and pd.notna(r[col_size]):
                try:
                    size = float(r[col_size])
                except Exception:
                    size = None

            rid = int(r[col_id]) if (col_id and pd.notna(r[col_id])) else i + 1

            rows_out.append({
                "id": rid,
                "name": name,
                "road_address": road_address,
                "address": address,
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "size": size
            })
        except Exception:
            skipped += 1
            continue

    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(rows_out, f, ensure_ascii=False, indent=2)

    print(f"[DONE] {len(rows_out)}건 저장 → {OUT_JSON} (건너뜀 {skipped}건)")

if __name__ == "__main__":
    main()