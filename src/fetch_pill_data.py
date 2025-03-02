import os
import requests
import json
from dotenv import load_dotenv
from urllib.parse import urlencode

# 프로젝트 루트 디렉토리 경로 구하기
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(ROOT_DIR, "configs", ".env")

# 환경 변수 로드
load_dotenv(dotenv_path)

# API 기본 정보
BASE_URL = "http://apis.data.go.kr/1471000/MdcinGrnIdntfcInfoService01/getMdcinGrnIdntfcInfoList01"
API_KEY = os.getenv("DATA_PORTAL_API_KEY_DECODED")  # .env 파일에서 API 키 로드
OUTPUT_JSON_FILE = os.path.join(ROOT_DIR, "data", "raw", "pill_raw_data.json")

# 필요한 필드 목록
REQUIRED_FIELDS = [
    "ITEM_SEQ", "ITEM_NAME", "ENTP_SEQ", "ENTP_NAME", "CHART", "ITEM_IMAGE", 
    "PRINT_FRONT", "PRINT_BACK", "DRUG_SHAPE", "COLOR_CLASS1", "COLOR_CLASS2",
    "LENG_LONG", "LENG_SHORT", "THICK", "CLASS_NO", "CLASS_NAME", "ETC_OTC_NAME", 
    "FORM_CODE_NAME", "MARK_CODE_FRONT_ANAL", "MARK_CODE_BACK_ANAL"
]

def fetch_pill_data():
    page_no = 1
    page_size = 100
    total_data = []

    while True:
        params = {
            "serviceKey": API_KEY,
            "pageNo": page_no,
            "numOfRows": page_size,
            "type": "json"
        }

        # 요청 URL 생성 (디버깅용)
        request_url = f"{BASE_URL}?{urlencode(params)}"
        print(f"🔗 Request URL: {request_url}")

        try:
            response = requests.get(request_url)
            
            # ✅ Debugging: API 응답 코드 출력
            print(f"🔍 API Response Code: {response.status_code}")
            
            # HTTP 응답이 200이 아닐 경우 오류 출력
            if response.status_code != 200:
                print(f"❌ API Request Failed! Status Code: {response.status_code}")
                print(f"⚠️ Response Text: {response.text}")
                break
            
            # ✅ Debugging : API 응답 미리보기
            print(f"🔍 Response Preview: {response.text[:200]}")  # 처음 200자만 출력

            data = response.json()  # JSON 파싱
            
            # 응답 구조 확인을 위한 디버깅 출력
            print(f"🔍 Response Structure Keys: {list(data.keys())}")
            
            # 수정: 올바른 경로로 데이터 접근
            if "body" not in data or not data["body"].get("items"):
                print(f"📢 No more data at page {page_no}. Stopping fetch.")
                break
            
            items = data["body"]["items"]
            
            # 수정: items가 리스트인지 확인 (단일 아이템일 경우 리스트로 변환)
            if not isinstance(items, list):
                items = [items]
            
            # 필요한 필드만 필터링
            filtered_items = [
                {key: item.get(key, "") for key in REQUIRED_FIELDS} for item in items
            ]

            total_data.extend(filtered_items)
            print(f"✅ Fetched {len(filtered_items)} records from page {page_no}.")
            
            # 데이터를 모두 가져왔는지 확인
            total_count = data["body"].get("totalCount", 0)
            if page_no * page_size >= total_count:
                print(f"📢 Retrieved all data. Total count: {total_count}")
                break

            page_no += 1

        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching data: {e}")
            break
        except json.JSONDecodeError:
            print(f"❌ JSON Decode Error! Response is not valid JSON: {response.text}")
            break
        except KeyError as e:
            print(f"❌ KeyError: {e}. Response structure: {data}")
            break

    # 절대 경로로 디렉토리 생성
    output_dir = os.path.dirname(OUTPUT_JSON_FILE)
    os.makedirs(output_dir, exist_ok=True)
    with open(OUTPUT_JSON_FILE, "w", encoding="utf-8") as json_file:
        json.dump(total_data, json_file, ensure_ascii=False, indent=4)
    
    print(f"📁 Data saved to {OUTPUT_JSON_FILE}. Total records: {len(total_data)}")

if __name__ == "__main__":
    fetch_pill_data()