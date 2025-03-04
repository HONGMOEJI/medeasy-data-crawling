import json
import aiohttp
import asyncio
import os
import time
import random
from datetime import datetime
from tqdm import tqdm  # 프로그레스 바
from bs4 import BeautifulSoup  # HTML 파싱

# 📌 파일 경로 설정
DATA_DIR = "data"
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
RAW_DIR = os.path.join(DATA_DIR, "raw")
FILTERED_DIR = os.path.join(DATA_DIR, "filtered")
LOGS_DIR = os.path.join(DATA_DIR, "logs")  # 로그 디렉토리 추가

DRUG_FILE = os.path.join(PROCESSED_DIR, "drug_approval_data_processed.json")
PILL_FILE = os.path.join(RAW_DIR, "pill_raw_data.json")

FILTERED_DRUG_FILE = os.path.join(FILTERED_DIR, "filtered_drug_approvals.json")
FILTERED_PILL_FILE = os.path.join(FILTERED_DIR, "filtered_pill_data.json")

# 📌 필요한 디렉토리 생성
for directory in [FILTERED_DIR, LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

# 📌 로깅 설정
def setup_logger():
    """로깅 설정 및 로그 파일 경로 반환"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOGS_DIR, f"filter_process_{timestamp}.log")
    
    with open(log_file, "w", encoding="utf-8") as f:
        f.write(f"=== 필터링 프로세스 로그 ({timestamp}) ===\n\n")
    
    print(f"📝 로그 파일 생성: {log_file}")
    return log_file

def log_message(message, log_file, print_to_console=True):
    """로그 메시지 기록"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    
    if print_to_console:
        print(log_entry)
    
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry + "\n")

# ✅ 1. JSON에서 데이터 로드 (+ 샘플 기능)
def load_json(file_path, log_file, sample_size=None):
    """JSON 파일 로드 및 샘플링"""
    log_message(f"📂 JSON 파일 로드 중: {file_path}", log_file)
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        if sample_size and len(data) > sample_size:
            data = random.sample(data, sample_size)
            log_message(f"🔍 샘플링 적용: {sample_size}개 데이터 선택됨", log_file)
        
        log_message(f"🔢 총 {len(data)}개의 항목 로드 완료", log_file)
        return data
    except Exception as e:
        log_message(f"❌ 파일 로드 중 오류 발생: {e}", log_file)
        return []

# ✅ 2. 비동기 요청 함수 (필터링 기준 개선)
async def fetch_status(session, item, log_file, request_index=None, total_requests=None):
    """식약처 웹사이트에서 ITEM_SEQ 등록 여부를 확인하는 비동기 함수"""
    item_seq = item.get("ITEM_SEQ")
    if not item_seq:
        log_message(f"⚠️ ITEM_SEQ 없음, 건너뜀", log_file, False)
        return item, False, "ITEM_SEQ_MISSING"
    
    url = f"https://nedrug.mfds.go.kr/searchDrug?searchYn=true&itemSeq={item_seq}"
    headers = {"User-Agent": "Mozilla/5.0"}

    progress_info = f"[{request_index}/{total_requests}]" if request_index and total_requests else ""
    item_name = item.get("ITEM_NAME", "이름 없음")
    log_message(f"🔍 {progress_info} 확인 중: {item_name} (ITEM_SEQ: {item_seq})", log_file, False)
    
    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")

            # ❌ "조회 결과가 없습니다." → 무조건 미등록
            if soup.find("span", string="조회 결과가 없습니다."):
                log_message(f"❌ {progress_info} 미등록: {item_name} (ITEM_SEQ: {item_seq})", log_file, False)
                return item, False, "NOT_REGISTERED"

            # ✅ 등록된 경우: <table> 내부에 `getItemDetail?itemSeq=` 포함 여부 확인
            result_table = soup.find("table", class_="dr_table2")
            if result_table and result_table.find("a", href=lambda x: x and "getItemDetail?itemSeq=" in x):
                log_message(f"✅ {progress_info} 등록됨: {item_name} (ITEM_SEQ: {item_seq})", log_file, False)
                return item, True, "REGISTERED"

            log_message(f"⚠️ {progress_info} 예외 상황 발생 (정확한 등록 여부 확인 불가)", log_file, False)
            return item, False, "UNKNOWN_RESPONSE"
    
    except Exception as e:
        log_message(f"⚠️ {progress_info} 요청 오류 발생: {str(e)} (ITEM_SEQ: {item_seq})", log_file, False)
        return item, False, f"ERROR: {str(e)}"

# ✅ 3. 비동기 방식으로 데이터 필터링
async def filter_data_async(data, batch_size=10, log_file=None):
    """비동기 방식으로 데이터 필터링"""
    valid_data = []
    total_items = len(data)
    
    log_message(f"🚀 총 {total_items}개 항목 필터링 시작 (배치 크기: {batch_size})", log_file)
    pbar = tqdm(total=total_items, desc="필터링 진행 중")

    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, item in enumerate(data):
            request_index = idx + 1
            tasks.append(fetch_status(session, item, log_file, request_index, total_items))
            
            if len(tasks) >= batch_size:
                results = await asyncio.gather(*tasks)
                valid_data.extend([item for item, is_valid, _ in results if is_valid])
                tasks = []
                pbar.update(batch_size)

        if tasks:
            results = await asyncio.gather(*tasks)
            valid_data.extend([item for item, is_valid, _ in results if is_valid])
            pbar.update(len(tasks))

    pbar.close()
    log_message(f"✅ 필터링 완료! 등록된 항목 수: {len(valid_data)}/{total_items}", log_file)
    return valid_data

# ✅ 4. 실행 (샘플링 적용 가능)
async def main(sample_size=None):
    log_file = setup_logger()
    start_time = time.time()

    log_message("🚀 데이터 필터링 프로세스 시작", log_file)

    log_message("\n🔎 허가정보 데이터 필터링 시작...", log_file)
    drug_data = load_json(DRUG_FILE, log_file, sample_size)
    filtered_drug_data = await filter_data_async(drug_data, batch_size=10, log_file=log_file)
    with open(FILTERED_DRUG_FILE, "w", encoding="utf-8") as f:
        json.dump(filtered_drug_data, f, ensure_ascii=False, indent=4)

    log_message("\n🔎 낱알정보 데이터 필터링 시작...", log_file)
    pill_data = load_json(PILL_FILE, log_file, sample_size)
    filtered_pill_data = await filter_data_async(pill_data, batch_size=15, log_file=log_file)
    with open(FILTERED_PILL_FILE, "w", encoding="utf-8") as f:
        json.dump(filtered_pill_data, f, ensure_ascii=False, indent=4)

    log_message("✅ 전체 필터링 완료!", log_file)

# 비동기 실행 (샘플 개수 지정 가능)
if __name__ == "__main__":
    asyncio.run(main(sample_size=None))  # 샘플링 적용 시: sample_size=100
