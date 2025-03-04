import os
import requests
import json
import xml.etree.ElementTree as ET
import re
import html
import time
from dotenv import load_dotenv
from urllib.parse import urlencode

# 환경 변수 파일 경로
dotenv_path = os.path.join(os.path.dirname(__file__), "..", "configs", ".env")

# 환경 변수 로드
load_dotenv(dotenv_path)

# API 기본 정보
BASE_URL = "http://apis.data.go.kr/1471000/DrugPrdtPrmsnInfoService06/getDrugPrdtPrmsnDtlInq05"
API_KEY = os.getenv("DATA_PORTAL_API_KEY_DECODED")  # .env 파일에서 API 키 로드

# 출력 파일 경로
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_OUTPUT_FILE = os.path.join(ROOT_DIR, "data", "raw", "drug_approval_data.json")
PROCESSED_OUTPUT_FILE = os.path.join(ROOT_DIR, "data", "processed", "drug_approval_data_processed.json")

# 필요한 필드 목록
REQUIRED_FIELDS = [
    "ITEM_SEQ", "ITEM_NAME", "ENTP_NAME", "ETC_OTC_CODE", "ETC_OTC_NAME", "CHART", 
    "EE_DOC_DATA", "UD_DOC_DATA", "NB_DOC_DATA", "STORAGE_METHOD", "VALID_TERM", "CANCEL_DATE"
]

def parse_xml_doc(xml_string):
    """
    CDATA 내의 XML 문서를 파싱하여 구조화된 형태로 변환합니다.
    """
    if not xml_string:
        return None
    
    try:
        # 1단계: XML을 파싱하기 전에 문제가 될 수 있는 부분을 정리
        
        # HTML 태그를 안전하게 처리 (CDATA 블록 내부 포함)
        xml_string = re.sub(r'<sub>(.*?)</sub>', r'\1', xml_string)
        xml_string = re.sub(r'<sup>(.*?)</sup>', r'\1', xml_string)
        
        # CDATA 내의 &amp; 처리 - 이미 &amp;로 인코딩된 것은 건너뛰고 일반 &만 변환
        # CDATA 블록을 임시로 추출
        cdata_blocks = re.findall(r'<!\[CDATA\[(.*?)\]\]>', xml_string, re.DOTALL)
        
        # 각 CDATA 블록 내에서 엔티티 처리
        for block in cdata_blocks:
            fixed_block = re.sub(r'&(?!(amp;|lt;|gt;|apos;|quot;|#\d+;|#x[0-9a-fA-F]+;))', '&amp;', block)
            # 원본 블록을 수정된 블록으로 대체
            xml_string = xml_string.replace('<![CDATA[' + block + ']]>', '<![CDATA[' + fixed_block + ']]>')
        
        # 2단계: XML 파싱 시도
        try:
            root = ET.fromstring(xml_string)
            
            # 문서 제목과 타입 추출
            doc_title = root.get('title', '')
            doc_type = root.get('type', '')
            
            # 결과 구조 초기화
            result = {
                'title': doc_title,
                'type': doc_type,
                'sections': []
            }
            
            # 각 섹션 처리
            for section in root.findall('SECTION'):
                section_data = {
                    'title': section.get('title', ''),
                    'articles': []
                }
                
                # 섹션 내 각 아티클 처리
                for article in section.findall('ARTICLE'):
                    article_data = {
                        'title': article.get('title', ''),
                        'paragraphs': []
                    }
                    
                    # 아티클 내 각 문단 처리
                    for paragraph in article.findall('PARAGRAPH'):
                        # 텍스트 콘텐츠 가져오기 - text가 None이면 빈 문자열 사용
                        text = paragraph.text or ''
                        
                        # CDATA 처리
                        if '![CDATA[' in text:
                            # CDATA 마커 제거
                            text = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', text, flags=re.DOTALL)
                        
                        # HTML 태그 제거 - 남아있을 수 있는 모든 HTML 태그
                        text = re.sub(r'<[^>]+>', '', text)
                        
                        # HTML 엔티티 디코딩 (예: &nbsp; -> 공백)
                        text = html.unescape(text)
                        
                        # 이스케이프 문자 및 특수 문자 정리
                        text = text.replace('\r', '').replace('\t', ' ')
                        
                        # 연속된 공백 하나로 치환
                        text = re.sub(r' +', ' ', text)
                        
                        # 문단 앞뒤 공백 제거
                        text = text.strip()
                        
                        # 비어있지 않은 문단만 추가
                        if text:
                            article_data['paragraphs'].append(text)
                    
                    # 내용이 있는 아티클만 추가
                    if article_data['paragraphs'] or article_data['title']:
                        section_data['articles'].append(article_data)
                
                # 내용이 있는 섹션만 추가
                if section_data['articles']:
                    result['sections'].append(section_data)
            
            # 텍스트 형식으로 변환
            text_content = extract_text_from_parsed_doc(result)
            result['text'] = text_content
            
            return result
            
        except ET.ParseError as xml_error:
            # 고급 복구 시도: 더 강력한 전처리 적용
            print(f"기본 XML 파싱 실패, 고급 복구 시도: {xml_error}")
            return handle_complex_xml_parsing(xml_string)
            
    except Exception as e:
        print(f"XML 처리 중 예상치 못한 오류: {e}")
        return {
            'title': '처리 오류',
            'type': 'error',
            'error': str(e),
            'text': f"처리 오류: {e}",
            'raw': xml_string[:500] + ('...' if len(xml_string) > 500 else '')
        }

def handle_complex_xml_parsing(xml_string):
    """
    더 강력한 XML 파싱 복구 로직을 적용합니다.
    """
    try:
        # CDATA 마커 제거 (모든 CDATA 태그를 텍스트로 변환)
        xml_string = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', xml_string, flags=re.DOTALL)
        
        # HTML 태그를 안전하게 처리
        xml_string = re.sub(r'<sub>(.*?)</sub>', r'\1', xml_string)
        xml_string = re.sub(r'<sup>(.*?)</sup>', r'\1', xml_string)
        xml_string = re.sub(r'<br\s*/?>', ' ', xml_string)
        
        # 모든 & 기호를 &amp;로 변환 (이미 인코딩된 엔티티는 제외)
        xml_string = re.sub(r'&(?!(amp;|lt;|gt;|apos;|quot;|#\d+;|#x[0-9a-fA-F]+;))', '&amp;', xml_string)
        
        # 닫히지 않은 태그 처리
        # 예: <PARAGRAPH> ... (닫는 태그 없음) -> <PARAGRAPH> ... </PARAGRAPH>
        opened_tags = re.findall(r'<(\w+)(?:\s+[^>]*)?>[^<]*$', xml_string)
        for tag in reversed(opened_tags):
            xml_string += f'</{tag}>'
        
        # 이제 XML 파싱 다시 시도
        try:
            root = ET.fromstring(xml_string)
            # 이후 로직은 기존 parse_xml_doc과 동일...
            
            # 문서 제목과 타입 추출
            doc_title = root.get('title', '')
            doc_type = root.get('type', '')
            
            # 결과 구조 초기화
            result = {
                'title': doc_title,
                'type': doc_type,
                'sections': []
            }
            
            # 각 섹션 처리
            for section in root.findall('SECTION'):
                section_data = {
                    'title': section.get('title', ''),
                    'articles': []
                }
                
                # 섹션 내 각 아티클 처리
                for article in section.findall('ARTICLE'):
                    article_data = {
                        'title': article.get('title', ''),
                        'paragraphs': []
                    }
                    
                    # 아티클 내 각 문단 처리
                    for paragraph in article.findall('PARAGRAPH'):
                        # 텍스트 콘텐츠 가져오기
                        text = paragraph.text or ''
                        
                        # HTML 태그 제거
                        text = re.sub(r'<[^>]+>', '', text)
                        
                        # HTML 엔티티 디코딩
                        text = html.unescape(text)
                        
                        # 특수 문자 정리
                        text = text.replace('\r', '').replace('\t', ' ')
                        text = re.sub(r' +', ' ', text)
                        text = text.strip()
                        
                        # 비어있지 않은 문단만 추가
                        if text:
                            article_data['paragraphs'].append(text)
                    
                    # 내용이 있는 아티클만 추가
                    if article_data['paragraphs'] or article_data['title']:
                        section_data['articles'].append(article_data)
                
                # 내용이 있는 섹션만 추가
                if section_data['articles']:
                    result['sections'].append(section_data)
            
            # 텍스트 형식으로 변환
            text_content = extract_text_from_parsed_doc(result)
            result['text'] = text_content
            
            return result
            
        except ET.ParseError:
            # 최종 대안: 비정규식 텍스트 추출 시도
            return extract_text_from_broken_xml(xml_string)
            
    except Exception as e:
        print(f"고급 XML 복구 중 오류: {e}")
        return extract_text_from_broken_xml(xml_string)

def extract_text_from_parsed_doc(doc_data):
    """파싱된 문서 구조에서 텍스트를 추출하는 헬퍼 함수"""
    result = []
    
    # 문서 제목 추가
    if doc_data.get('title'):
        result.append(f"【{doc_data['title']}】")
    
    # 각 섹션 처리
    for section in doc_data.get('sections', []):
        # 각 아티클 처리
        for article in section.get('articles', []):
            # 아티클 제목 추가 (있는 경우)
            if article.get('title'):
                result.append(f"\n■ {article['title']}")
            
            # 문단 추가
            for paragraph in article.get('paragraphs', []):
                result.append(f"- {paragraph}")
    
    return '\n'.join(result)

def extract_text_from_broken_xml(xml_string):
    """
    완전히 깨진 XML에서 텍스트 추출을 시도하는 함수
    """
    # 원본 XML 백업
    original_xml = xml_string
    
    try:
        # 모든 HTML/XML 태그 처리 전 CDATA 마커 제거
        xml_string = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', xml_string, flags=re.DOTALL)
        
        # HTML 태그 처리
        xml_string = re.sub(r'<sub>(.*?)</sub>', r'\1', xml_string)
        xml_string = re.sub(r'<sup>(.*?)</sup>', r'\1', xml_string)
        xml_string = re.sub(r'<br\s*/?>', ' ', xml_string)
        
        # 문서 제목 추출 시도
        title_match = re.search(r'title="([^"]+)"', xml_string)
        doc_title = title_match.group(1) if title_match else "제목 없음"
        
        # SECTION, ARTICLE, PARAGRAPH 태그 사이의 콘텐츠 추출
        sections = []
        
        # 아티클 제목 추출
        article_titles = re.findall(r'<ARTICLE title="([^"]+)"', xml_string)
        
        # PARAGRAPH 내용 추출
        paragraph_contents = re.findall(r'<PARAGRAPH[^>]*>(.*?)</PARAGRAPH>', xml_string, re.DOTALL)
        
        # 구조화된 결과 생성
        result = {
            'title': doc_title,
            'type': 'text_extraction',
            'text': ''
        }
        
        # 텍스트 구성
        text_parts = []
        text_parts.append(f"【{doc_title}】")
        
        # 아티클 제목 추가
        for title in article_titles:
            if title.strip():
                text_parts.append(f"\n■ {title}")
        
        # 문단 내용 추가
        for content in paragraph_contents:
            # HTML 태그 제거
            content = re.sub(r'<[^>]+>', '', content)
            # HTML 엔티티 디코딩
            content = html.unescape(content)
            # 특수 문자 정리
            content = content.replace('\r', '').replace('\t', ' ')
            content = re.sub(r' +', ' ', content)
            content = content.strip()
            
            if content:
                text_parts.append(f"- {content}")
        
        # 텍스트 조합
        result['text'] = '\n'.join(text_parts)
        
        # 텍스트가 없으면 다른 방법으로 추출 시도
        if result['text'] == f"【{doc_title}】":
            # 모든 XML 태그 제거
            plain_text = re.sub(r'<[^>]*>', ' ', xml_string)
            plain_text = html.unescape(plain_text)
            plain_text = re.sub(r'\s+', ' ', plain_text).strip()
            
            # 의미 있는 문장 추출
            sentences = re.split(r'[.!?]\s+', plain_text)
            meaningful_sentences = [s.strip() + '.' for s in sentences if len(s.strip()) > 10]
            
            if meaningful_sentences:
                result['text'] = f"【{doc_title}】\n\n" + '\n- '.join([''] + meaningful_sentences)
            else:
                result['text'] = f"【{doc_title}】\n\n- 텍스트 추출 실패"
        
        return result
        
    except Exception as e:
        print(f"텍스트 추출 중 오류: {e}")
        
        # 최후의 방법: 모든 XML 태그를 제거하고 단순 텍스트만 추출
        try:
            text = re.sub(r'<[^>]*>', ' ', original_xml)
            text = html.unescape(text)
            text = re.sub(r'\s+', ' ', text).strip()
            
            return {
                'title': '텍스트 추출',
                'type': 'raw_text',
                'text': text
            }
        except:
            return {
                'title': '추출 실패',
                'type': 'error',
                'text': '텍스트 추출에 실패했습니다.'
            }

def fetch_drug_approval_data():
    """
    의약품 허가 정보 데이터를 API에서 가져오고 XML 문서를 파싱합니다.
    허가 취소된 의약품과 수출용 의약품을 건너뜁니다.
    """
    page_no = 1
    page_size = 100
    total_data = []
    max_retries = 3  # API 요청 실패 시 최대 재시도 횟수
    last_page_items = -1  # 이전 페이지의 항목 수 (초기값은 -1로 설정)
    seen_item_sequences = set()  # 이미 수집한 아이템 일련번호 추적
    
    # 필터링 카운터 초기화
    filtered_canceled_count = 0
    filtered_export_count = 0

    while True:
        params = {
            "serviceKey": API_KEY,
            "pageNo": page_no,
            "numOfRows": page_size,
            "type": "json"
        }

        # 요청 URL 생성
        request_url = f"{BASE_URL}?{urlencode(params)}"
        print(f"🔗 요청 URL: {request_url}")

        retries = 0
        success = False
        
        while retries < max_retries and not success:
            try:
                response = requests.get(request_url, timeout=30)
                print(f"🔍 API 응답 코드: {response.status_code}")
                
                if response.status_code != 200:
                    print(f"❌ API 요청 실패! 상태 코드: {response.status_code}")
                    print(f"⚠️ 응답 텍스트: {response.text}")
                    retries += 1
                    if retries < max_retries:
                        print(f"재시도 중... ({retries}/{max_retries})")
                        time.sleep(2)
                        continue
                    break
                
                # 응답 확인을 위한 미리보기
                response_preview = response.text[:200] + "..." if len(response.text) > 200 else response.text
                print(f"🔍 응답 미리보기: {response_preview}")

                # 응답 데이터 파싱
                try:
                    data = response.json()
                except json.JSONDecodeError:
                    print(f"❌ JSON 파싱 오류! 응답이 올바른 JSON 형식이 아닙니다.")
                    print(f"응답 내용: {response.text[:500]}...")
                    retries += 1
                    if retries < max_retries:
                        time.sleep(2)
                        continue
                    break
                
                # 응답 구조 확인
                print(f"🔍 응답 구조 키: {list(data.keys())}")
                
                # 데이터 추출 로직
                items = None
                total_count = 0
                
                # 응답 구조 확인
                if "header" in data and "body" in data:
                    if "items" not in data["body"] or not data["body"]["items"]:
                        print(f"📢 페이지 {page_no}에서 항목이 없습니다. 데이터 수집을 종료합니다.")
                        break
                    
                    items = data["body"]["items"]
                    total_count = data["body"].get("totalCount", 0)
                    
                elif "response" in data and "body" in data["response"]:
                    if "items" not in data["response"]["body"] or not data["response"]["body"]["items"]:
                        print(f"📢 페이지 {page_no}에서 항목이 없습니다. 데이터 수집을 종료합니다.")
                        break
                    
                    items = data["response"]["body"]["items"]
                    total_count = data["response"]["body"].get("totalCount", 0)
                    
                else:
                    print(f"❌ 예상치 못한 API 응답 구조: {list(data.keys())}")
                    retries += 1
                    if retries < max_retries:
                        time.sleep(2)
                        continue
                    break
                
                # items가 단일 객체인 경우 리스트로 변환
                if isinstance(items, dict):
                    items = [items]
                
                # 중복 확인 로직
                if len(items) == last_page_items and last_page_items > 0:
                    # 항목 개수가 이전과 같은 경우, 내용도 중복인지 확인
                    current_item_sequences = [item.get("ITEM_SEQ", "") for item in items]
                    current_sequences_set = set(current_item_sequences)
                    
                    # 이미 처리한 아이템인지 확인
                    if current_sequences_set.issubset(seen_item_sequences):
                        print(f"🛑 페이지 {page_no}의 모든 항목이 이미 처리되었습니다. 데이터 수집을 종료합니다.")
                        break
                
                # 필요한 필드만 필터링하고 XML 파싱
                processed_items = []
                new_item_count = 0
                
                for item in items:
                    item_seq = item.get("ITEM_SEQ", "")
                    
                    # 이미 처리한 아이템 건너뛰기
                    if item_seq in seen_item_sequences:
                        continue
                    
                    # 허가 취소된 의약품 필터링
                    if item.get("CANCEL_DATE"):
                        print(f"📢 허가 취소된 의약품 제외: {item.get('ITEM_NAME', '이름 없음')} (취소일: {item['CANCEL_DATE']})")
                        filtered_canceled_count += 1
                        continue
                    
                    # 수출용 의약품 필터링
                    item_name = item.get("ITEM_NAME", "")
                    if "(수출용)" in item_name:
                        print(f"📢 수출용 의약품 제외: {item_name}")
                        filtered_export_count += 1
                        continue
                    
                    seen_item_sequences.add(item_seq)
                    new_item_count += 1
                    
                    # 필요한 필드만 추출
                    filtered_item = {}
                    for key in REQUIRED_FIELDS:
                        filtered_item[key] = item.get(key, "")
                    
                    # ETC_OTC_CODE 처리
                    if filtered_item.get("ETC_OTC_CODE") and not filtered_item.get("ETC_OTC_NAME"):
                        filtered_item["ETC_OTC_NAME"] = filtered_item["ETC_OTC_CODE"]
                    
                    # XML 문서 파싱 (개선된 파싱 로직 사용)
                    for field in ['EE_DOC_DATA', 'UD_DOC_DATA', 'NB_DOC_DATA']:
                        if filtered_item.get(field):
                            try:
                                # XML 파싱 시도
                                parsed_doc = parse_xml_doc(filtered_item[field])
                                filtered_item[f"{field}_PARSED"] = parsed_doc
                                
                                # 파싱 실패 시 로그 출력
                                if parsed_doc and parsed_doc.get('type') == 'error':
                                    print(f"⚠️ {field} 필드 파싱 실패: {parsed_doc.get('error')}")
                            except Exception as e:
                                print(f"❌ {field} 파싱 중 예외 발생: {e}")
                                # 백업 처리: 텍스트 추출 시도
                                filtered_item[f"{field}_PARSED"] = extract_text_from_broken_xml(filtered_item[field])
                    
                    processed_items.append(filtered_item)

                # 새로운 아이템이 없으면 종료
                if new_item_count == 0:
                    print(f"📢 페이지 {page_no}에서 새로운 항목이 없습니다. 데이터 수집을 종료합니다.")
                    break
                
                # 데이터 추가
                total_data.extend(processed_items)
                print(f"✅ 페이지 {page_no}에서 {len(processed_items)}개 레코드를 가져와 처리했습니다.")
                print(f"📊 현재까지 필터링된 의약품: 허가 취소 {filtered_canceled_count}개, 수출용 {filtered_export_count}개")
                
                # 마지막 페이지 도달 확인
                if total_count > 0:
                    estimated_pages = (total_count + page_size - 1) // page_size  # 올림 나눗셈
                    print(f"📊 현재 진행 상황: 페이지 {page_no}/{estimated_pages}, 총 데이터: {len(total_data)}/{total_count}")
                    
                    if page_no >= estimated_pages:
                        print(f"📢 마지막 페이지에 도달했습니다. 데이터 수집을 종료합니다.")
                        break
                
                # 페이지 크기보다 적은 데이터가 반환되면 마지막 페이지로 간주
                if len(items) < page_size:
                    print(f"📢 페이지 크기보다 적은 항목을 받았습니다. 마지막 페이지로 추정됩니다.")
                    break
                
                # 성공 표시
                success = True
                last_page_items = len(items)
                
            except requests.exceptions.RequestException as e:
                print(f"❌ 데이터 가져오기 오류: {e}")
                retries += 1
                if retries < max_retries:
                    print(f"재시도 중... ({retries}/{max_retries})")
                    time.sleep(2)
                else:
                    break
            except Exception as e:
                print(f"❌ 예상치 못한 오류: {e}")
                retries += 1
                if retries < max_retries:
                    print(f"재시도 중... ({retries}/{max_retries})")
                    time.sleep(2)
                else:
                    print("최대 재시도 횟수 초과. 진행을 중단합니다.")
                    return total_data
        
        # 성공하지 못했으면 종료
        if not success:
            print(f"⚠️ 페이지 {page_no} 처리 실패. 데이터 수집을 중단합니다.")
            break
        
        # 다음 페이지로 이동
        page_no += 1
    
    print(f"✅ 데이터 수집 완료. 총 {len(total_data)}개 항목 수집, 필터링된 항목: 허가 취소 {filtered_canceled_count}개, 수출용 {filtered_export_count}개")
    return total_data

def process_and_save_data(data, raw_file, processed_file):
    """
    데이터를 처리하고 저장합니다.
    이미 fetch_drug_approval_data에서 필터링을 했지만, 안전을 위해 한번 더 필터링합니다.
    """
    # 디렉토리 생성
    os.makedirs(os.path.dirname(raw_file), exist_ok=True)
    os.makedirs(os.path.dirname(processed_file), exist_ok=True)
    
    # 원본 데이터 저장
    print(f"📁 원본 데이터를 {raw_file}에 저장합니다...")
    with open(raw_file, "w", encoding="utf-8") as raw_json_file:
        json.dump(data, raw_json_file, ensure_ascii=False, indent=2)
    
    # 한번 더 필터링 적용 (안전 확인)
    filtered_data = []
    filtered_canceled_count = 0
    filtered_export_count = 0
    
    for item in data:
        # 허가 취소된 의약품 필터링
        if item.get("CANCEL_DATE"):
            filtered_canceled_count += 1
            continue
        
        # 수출용 의약품 필터링
        item_name = item.get("ITEM_NAME", "")
        if "(수출용)" in item_name:
            filtered_export_count += 1
            continue
        
        filtered_data.append(item)
    
    if filtered_canceled_count > 0 or filtered_export_count > 0:
        print(f"⚠️ 추가 필터링: 허가 취소 의약품 {filtered_canceled_count}개, 수출용 의약품 {filtered_export_count}개가 제외되었습니다.")
    
    # 텍스트 형식으로 추출한 데이터 생성
    text_data = []
    for item in filtered_data:
        # 기본 필드 추출
        text_item = {
            'ITEM_SEQ': item.get('ITEM_SEQ', ''),
            'ITEM_NAME': item.get('ITEM_NAME', ''),
            'ENTP_NAME': item.get('ENTP_NAME', ''),
            'ETC_OTC_NAME': item.get('ETC_OTC_NAME', ''),
            'CHART': item.get('CHART', ''),
            'STORAGE_METHOD': item.get('STORAGE_METHOD', '').strip(),
            'VALID_TERM': item.get('VALID_TERM', '')
        }
        
        # 각 XML 문서 필드 처리
        for field, target_field in [
            ('EE_DOC_DATA_PARSED', 'EFFECTIVENESS'),  # 효능효과
            ('UD_DOC_DATA_PARSED', 'USAGE_DOSAGE'),   # 용법용량
            ('NB_DOC_DATA_PARSED', 'PRECAUTIONS')     # 사용상의주의사항
        ]:
            # 파싱된 데이터에서 텍스트 추출
            if field in item and item[field]:
                # 파싱된 데이터에서 텍스트 필드 추출
                parsed_text = item[field].get('text', '')
                text_item[target_field] = parsed_text
            else:
                # 파싱된 데이터가 없는 경우 원본 XML에서 텍스트 추출 시도
                original_field = field.replace('_PARSED', '')
                if original_field in item and item[original_field]:
                    try:
                        extracted_text = extract_text_from_broken_xml(item[original_field]).get('text', '')
                        text_item[target_field] = extracted_text
                    except Exception as e:
                        print(f"❌ {original_field} 필드 텍스트 추출 오류: {e}")
                        text_item[target_field] = f"텍스트 추출 실패: {str(e)[:100]}"
                else:
                    text_item[target_field] = ''
        
        # 데이터 품질 확인
        has_content = False
        for field in ['EFFECTIVENESS', 'USAGE_DOSAGE', 'PRECAUTIONS']:
            if text_item.get(field) and len(text_item[field]) > 10:  # 의미 있는 내용이 있는지 확인
                has_content = True
                break
        
        # 의미 있는 내용이 있는 항목만 추가
        if has_content or (text_item.get('ITEM_NAME') and text_item.get('ENTP_NAME')):
            text_data.append(text_item)
        else:
            print(f"⚠️ 항목 '{text_item.get('ITEM_NAME', '이름 없음')}' (ID: {text_item.get('ITEM_SEQ', '일련번호 없음')})에 의미 있는 내용이 없어 제외합니다.")
    
    # 처리된 데이터 저장
    print(f"📁 처리된 데이터를 {processed_file}에 저장합니다...")
    with open(processed_file, "w", encoding="utf-8") as processed_json_file:
        json.dump(text_data, processed_json_file, ensure_ascii=False, indent=2)
    
    print(f"✅ 데이터 처리 완료. 원본 레코드 수: {len(data)}, 필터링 후 레코드 수: {len(filtered_data)}, 최종 유효 레코드 수: {len(text_data)}")
    
    # 샘플 데이터 출력
    if text_data:
        print("\n[처리된 데이터 샘플]")
        sample = text_data[0]
        print(f"의약품명: {sample.get('ITEM_NAME', '이름 없음')}")
        print(f"업체명: {sample.get('ENTP_NAME', '업체명 없음')}")
        print(f"성상: {sample.get('CHART', '성상 정보 없음')}")
        print(f"분류: {sample.get('ETC_OTC_NAME', '분류 정보 없음')}")
        
        # 효능효과, 용법용량, 주의사항 미리보기 출력
        for field, label in [
            ('EFFECTIVENESS', '효능효과'), 
            ('USAGE_DOSAGE', '용법용량'), 
            ('PRECAUTIONS', '주의사항')
        ]:
            preview = sample.get(field, '')
            if preview:
                # 긴 내용은 축약해서 보여줌
                preview_text = preview[:100] + ('...' if len(preview) > 100 else '')
                print(f"{label} 미리보기: {preview_text}")
            else:
                print(f"{label}: 정보 없음")
    else:
        print("⚠️ 처리된 데이터가 없습니다!")

    # 에러 보고서 생성 (선택적)
    error_records = []
    for item in filtered_data:
        error_fields = []
        
        # XML 파싱 오류 확인
        for field in ['EE_DOC_DATA_PARSED', 'UD_DOC_DATA_PARSED', 'NB_DOC_DATA_PARSED']:
            if field in item and item[field] and item[field].get('type') == 'error':
                error_fields.append({
                    'field': field.replace('_PARSED', ''),
                    'error': item[field].get('error', '알 수 없는 오류')
                })
        
        # 오류가 있는 레코드 정보 추가
        if error_fields:
            error_records.append({
                'ITEM_SEQ': item.get('ITEM_SEQ', ''),
                'ITEM_NAME': item.get('ITEM_NAME', ''),
                'error_fields': error_fields
            })
    
    # 오류 보고서 출력
    if error_records:
        error_report_file = os.path.join(os.path.dirname(processed_file), "error_report.json")
        with open(error_report_file, "w", encoding="utf-8") as error_file:
            json.dump(error_records, error_file, ensure_ascii=False, indent=2)
        print(f"⚠️ {len(error_records)}개의 레코드에서 XML 파싱 오류가 발생했습니다. 자세한 내용은 {error_report_file}을 참조하세요.")

if __name__ == "__main__":
    print("🔎 의약품 허가 정보 데이터를 가져오는 중...")
    drug_data = fetch_drug_approval_data()
    
    if drug_data:
        print(f"✅ {len(drug_data)}개의 의약품 레코드를 성공적으로 가져왔습니다.")
        process_and_save_data(drug_data, RAW_OUTPUT_FILE, PROCESSED_OUTPUT_FILE)
    else:
        print("❌ 데이터를 가져오지 못했습니다. API 연결 및 매개변수를 확인하세요.")