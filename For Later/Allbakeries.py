import requests, time, csv, math

API_KEY = "KakaoAK 4e6433211bfdc437854dfe6e02825605"
BASE = "https://dapi.kakao.com/v2/local/search/category.json"

# 대한민국 대략 경계
LAT_MIN, LAT_MAX = 33.0, 38.7
LON_MIN, LON_MAX = 124.6, 131.9

# 격자 설정 (도시밀집 보정 원하면 step을 더 촘촘히)
LAT_STEP = 0.28
LON_STEP = 0.28
RADIUS = 20000  # meters (max 20000)
PAGE_MAX = 45   # 카카오가 안내하는 최대 페이지

headers = {"Authorization": API_KEY}

def is_bakery(item):
    # 세부 분류와 상호명으로 필터
    cn = (item.get("category_name") or "").lower()
    name = (item.get("place_name") or "").lower()
    # 대표 케이스: "음식점 > 카페 > 제과,베이커리"
    if "제과" in cn or "베이커리" in cn:
        return True
    # 보완 키워드
    kw = ["베이커리", "빵집", "제과점", "bakery", "bread", "boulangerie", "patisserie"]
    return any(k in name for k in kw)

def fetch_circle(lon, lat):
    results = []
    for page in range(1, PAGE_MAX + 1):
        params = {
            "category_group_code": "FD6",  # 음식점
            "x": lon,
            "y": lat,
            "radius": RADIUS,
            "page": page,
            "size": 15,
            "sort": "accuracy",
        }
        r = requests.get(BASE, headers=headers, params=params, timeout=10)
        if r.status_code == 429:
            # 쿼터/속도 제한 시 대기 후 재시도
            time.sleep(5); 
            r = requests.get(BASE, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        docs = data.get("documents", [])
        if not docs:
            break
        results.extend(docs)
        # meta.is_end가 True면 더 없음
        if data.get("meta", {}).get("is_end"):
            break
        time.sleep(0.25)  # rate limit 여유
    return results

seen = set()
rows = []

lat = LAT_MIN
while lat <= LAT_MAX:
    lon = LON_MIN
    while lon <= LON_MAX:
        try:
            docs = fetch_circle(lon, lat)
            for d in docs:
                if not is_bakery(d):
                    continue
                pid = d.get("id")
                if pid in seen:
                    continue
                seen.add(pid)
                rows.append({
                    "id": pid,
                    "place_name": d.get("place_name"),
                    "x": d.get("x"),  # lon
                    "y": d.get("y"),  # lat
                    "address_name": d.get("address_name"),
                    "road_address_name": d.get("road_address_name"),
                    "phone": d.get("phone"),
                    "category_name": d.get("category_name"),
                    "place_url": d.get("place_url"),
                })
        except Exception as e:
            print("error at", lon, lat, e)
            time.sleep(1)
        lon += LON_STEP
        time.sleep(0.1)
    lat += LAT_STEP

# CSV 저장 (UTF-8-SIG: 엑셀에서 한글깨짐 방지)
with open("korean_bakeries_kakao.csv", "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=[
        "id","place_name","x","y","address_name","road_address_name","phone","category_name","place_url"
    ])
    w.writeheader()
    w.writerows(rows)

print("saved:", len(rows))
