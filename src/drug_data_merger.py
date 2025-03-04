#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
의약품 허가정보와 낱알정보 병합 스크립트

파일 경로:
- 허가정보 데이터: ./data/processed/drug_approval_data_processed.json
- 낱알정보 데이터: ./data/raw/pill_raw_data.json
"""

import os
import json
import logging
from typing import Dict, List, Any, Tuple, Set
from pathlib import Path

# 프로젝트 루트 디렉토리 설정 (상대 경로 사용)
PROJECT_ROOT = Path(__file__).parent.parent.absolute()

# 파일 경로 설정 (상대 경로 사용)
APPROVAL_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "drug_approval_data_processed.json"
PILL_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "pill_raw_data.json"
OUTPUT_DIR = PROJECT_ROOT / "data" / "merged"
MERGED_OUTPUT_PATH = OUTPUT_DIR / "merged_drug_data.json"
UNMATCHED_PILLS_PATH = OUTPUT_DIR / "unmatched_pills.json"
UNMATCHED_APPROVALS_PATH = OUTPUT_DIR / "unmatched_approvals.json"

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def load_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """
    JSON 파일을 로드하는 함수
    
    Args:
        file_path: 로드할 JSON 파일 경로
        
    Returns:
        파싱된 JSON 데이터 (항상 리스트 형태로 반환)
    
    Raises:
        FileNotFoundError: 파일이 존재하지 않을 때
        json.JSONDecodeError: JSON 파싱 오류 발생 시
    """
    try:
        logger.info(f"파일 로드 중: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 데이터가 리스트가 아니면 리스트로 변환
        if not isinstance(data, list):
            data = [data]
            
        return data
    except FileNotFoundError:
        logger.error(f"파일을 찾을 수 없습니다: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"JSON 파싱 오류: {file_path}")
        raise
    except Exception as e:
        logger.error(f"파일 로드 중 예기치 않은 오류: {str(e)}")
        raise


def validate_and_preprocess_data(data_list: List[Dict[str, Any]], source_type: str) -> List[Dict[str, Any]]:
    """
    데이터 검증 및 전처리 함수
    
    Args:
        data_list: 전처리할 데이터 리스트
        source_type: 데이터 소스 유형 ('approval' 또는 'pill')
        
    Returns:
        전처리된 데이터 리스트
    """
    logger.info(f"{source_type} 데이터 검증 및 전처리 중...")
    
    if not data_list:
        logger.warning(f"{source_type} 데이터가 비어있습니다.")
        return []
    
    # 중복 ITEM_SEQ 확인을 위한 세트
    seen_item_seqs: Set[str] = set()
    processed_data = []
    
    for item in data_list:
        # ITEM_SEQ 확인
        if 'ITEM_SEQ' not in item or not item['ITEM_SEQ']:
            logger.warning(f"ITEM_SEQ가 없는 {source_type} 항목이 필터링됨")
            continue
        
        # ITEM_SEQ를 문자열로 통일
        item_seq = str(item['ITEM_SEQ'])
        
        # 중복 체크
        if item_seq in seen_item_seqs:
            logger.warning(f"중복된 ITEM_SEQ 발견: {item_seq}")
            continue
        
        seen_item_seqs.add(item_seq)
        
        # 기본 필드 표준화
        processed_item = {
            **item,
            'ITEM_SEQ': item_seq,
            'ITEM_NAME': item.get('ITEM_NAME', '').strip(),
            'ENTP_NAME': item.get('ENTP_NAME', '').strip(),
            'CHART': item.get('CHART', '').strip()
        }
        
        processed_data.append(processed_item)
    
    logger.info(f"{source_type} 데이터 전처리 완료: {len(processed_data)}개 유효 항목")
    return processed_data


def merge_drug_data(approval_data: List[Dict[str, Any]], pill_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    허가정보와 낱알정보 병합 함수
    
    Args:
        approval_data: 허가정보 데이터 리스트
        pill_data: 낱알정보 데이터 리스트
        
    Returns:
        병합 결과 딕셔너리 (merged, unmatchedPills, unmatchedApprovals 키 포함)
    """
    # 결과 저장할 딕셔너리
    result = {
        'merged': [],            # 매칭되어 병합된 데이터
        'unmatchedPills': [],    # 허가정보에 없는 낱알정보
        'unmatchedApprovals': [] # 낱알정보에 없는 허가정보
    }

    # 허가정보를 ITEM_SEQ를 키로 하는 딕셔너리로 변환하여 검색 효율 향상
    approval_map = {item['ITEM_SEQ']: item for item in approval_data}
    
    # 처리 여부 추적을 위한 세트
    processed_approvals = set()
    
    # 낱알정보 순회하며 매칭 찾기
    for pill in pill_data:
        item_seq = pill['ITEM_SEQ']
        
        if item_seq in approval_map:
            # 매칭된 경우: 데이터 병합
            approval_item = approval_map[item_seq]
            
            # 중복 필드 중 허가정보 우선 처리
            merged_data = {
                **approval_item,  # 허가정보 기본 데이터
                **pill,           # 낱알정보 추가
                
                # 중복 필드 처리 (허가정보 우선)
                'ITEM_NAME': approval_item.get('ITEM_NAME') or pill.get('ITEM_NAME', ''),
                'ENTP_NAME': approval_item.get('ENTP_NAME') or pill.get('ENTP_NAME', ''),
                'CHART': approval_item.get('CHART') or pill.get('CHART', ''),
                
                # 출처 표시
                '_source': 'both',
                '_matchType': 'exact_match_by_ITEM_SEQ'
            }
            
            result['merged'].append(merged_data)
            processed_approvals.add(item_seq)
        else:
            # 매칭되지 않은 낱알정보
            pill['_needsAdditionalInfo'] = True
            pill['_source'] = 'pill_only'
            result['unmatchedPills'].append(pill)
    
    # 매칭되지 않은 허가정보 수집
    for item_seq, approval in approval_map.items():
        if item_seq not in processed_approvals:
            approval['_source'] = 'approval_only'
            result['unmatchedApprovals'].append(approval)
    
    return result


def analyze_results(merge_result: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    병합 결과 분석 함수
    
    Args:
        merge_result: 병합 결과 딕셔너리
        
    Returns:
        분석 결과 딕셔너리
    """
    merged_count = len(merge_result['merged'])
    unmatched_pills_count = len(merge_result['unmatchedPills'])
    unmatched_approvals_count = len(merge_result['unmatchedApprovals'])
    total_items = merged_count + unmatched_pills_count + unmatched_approvals_count
    
    match_rate = (merged_count / total_items * 100) if total_items > 0 else 0
    
    return {
        'totalItems': total_items,
        'totalMerged': merged_count,
        'totalUnmatchedPills': unmatched_pills_count,
        'totalUnmatchedApprovals': unmatched_approvals_count,
        'matchRate': f"{match_rate:.2f}%",
        'actionNeeded': (
            f"낱알 의약품 중 {unmatched_pills_count}개 항목에 대한 추가 정보 입력 필요" 
            if unmatched_pills_count > 0 else 
            "모든 낱알 의약품 정보가 매칭되었습니다."
        )
    }


def save_results(merge_result: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    결과 저장 함수
    
    Args:
        merge_result: 병합 결과 딕셔너리
    """
    logger.info('결과 파일 저장 중...')
    
    # 출력 디렉토리 생성
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 병합된 데이터 저장
    with open(MERGED_OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(merge_result['merged'], f, ensure_ascii=False, indent=2)
    logger.info(f"병합된 데이터 저장 완료: {MERGED_OUTPUT_PATH} ({len(merge_result['merged'])}개 항목)")
    
    # 미매칭 낱알정보 저장
    with open(UNMATCHED_PILLS_PATH, 'w', encoding='utf-8') as f:
        json.dump(merge_result['unmatchedPills'], f, ensure_ascii=False, indent=2)
    logger.info(f"미매칭 낱알정보 저장 완료: {UNMATCHED_PILLS_PATH} ({len(merge_result['unmatchedPills'])}개 항목)")
    
    # 미매칭 허가정보 저장
    with open(UNMATCHED_APPROVALS_PATH, 'w', encoding='utf-8') as f:
        json.dump(merge_result['unmatchedApprovals'], f, ensure_ascii=False, indent=2)
    logger.info(f"미매칭 허가정보 저장 완료: {UNMATCHED_APPROVALS_PATH} ({len(merge_result['unmatchedApprovals'])}개 항목)")


def main() -> Dict[str, Any]:
    """
    메인 함수 - 데이터 로드, 병합 및 저장 실행
    
    Returns:
        실행 결과 및 분석 정보를 담은 딕셔너리
    """
    logger.info('의약품 데이터 병합 프로세스 시작...')
    logger.info(f'프로젝트 루트 디렉토리: {PROJECT_ROOT}')
    
    try:
        # 1. 데이터 로드
        logger.info('데이터 파일 로드 중...')
        approval_data_raw = load_json_file(APPROVAL_DATA_PATH)
        pill_data_raw = load_json_file(PILL_DATA_PATH)
        
        logger.info(f"로드된 허가정보 데이터: {len(approval_data_raw)}개 항목")
        logger.info(f"로드된 낱알정보 데이터: {len(pill_data_raw)}개 항목")
        
        # 2. 데이터 검증 및 전처리
        approval_data = validate_and_preprocess_data(approval_data_raw, '허가정보')
        pill_data = validate_and_preprocess_data(pill_data_raw, '낱알정보')
        
        # 3. 데이터 병합
        logger.info('ITEM_SEQ를 기준으로 데이터 병합 중...')
        merge_result = merge_drug_data(approval_data, pill_data)
        
        # 4. 분석 결과 출력
        analysis = analyze_results(merge_result)
        logger.info('=== 병합 결과 분석 ===')
        for key, value in analysis.items():
            logger.info(f"{key}: {value}")
        
        # 5. 결과 저장
        save_results(merge_result)
        
        logger.info('의약품 데이터 병합 프로세스 완료!')
        return {'success': True, 'analysis': analysis}
        
    except Exception as e:
        logger.error(f'오류 발생: {str(e)}', exc_info=True)
        return {'success': False, 'error': str(e)}


if __name__ == '__main__':
    main()