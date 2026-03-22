# -*- coding: utf-8 -*-
"""
[Public] Hospital Recruitment Data Validator
- 목적: 상급종합병원 크롤링 데이터의 링크 유효성 및 모집 기간 자동 검증
- 주요기능: 
    1. HTTP Status Check (Head -> Get Fallback)
    2. 모집 기간 텍스트 기반 진행/마감 자동 판정
    3. 상세 에러 사유(Timeout, SSL, 404 등) 분류 및 통계 추출
"""

import re
import pandas as pd
import time
import requests
import os
from datetime import date
from requests.exceptions import RequestException, Timeout, SSLError

# ==========================================
# 1. 설정 (Configuration)
# ==========================================
# 공개용 환경을 위해 파일명을 자동으로 탐색하도록 설정
target_files = [f for f in os.listdir('.') if f.endswith('.xlsx')]
if not target_files:
    # qa 폴더 안에 있을 경우 상위 폴더 탐색
    target_files = [f"../{f}" for f in os.listdir('..') if f.endswith('.xlsx')]

PATH = target_files[0] if target_files else "data_sample.xlsx"
SHEET = "통합전체"
COL_SITE, COL_TITLE, COL_PERIOD, COL_LINK = "병원", "제목", "모집기간", "링크"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"

# ==========================================
# 2. 검증 로직 (Validation Logic)
# ==========================================

def check_url(url, timeout=12):
    """링크 유효성 점검 및 실패 사유 반환"""
    if not url or pd.isna(url) or not str(url).startswith("http"):
        return ("skip", 0, url, "Invalid URL")
    
    try:
        headers = {"User-Agent": UA}
        # 1차 시도: Head 요청 (빠름)
        resp = requests.head(url, headers=headers, timeout=timeout, allow_redirects=True)
        
        # 400 이상일 경우 2차 시도: Get 요청
        if resp.status_code >= 400:
            resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        
        status_code = resp.status_code
        final_url = resp.url
        
        if status_code == 200:
            return ("ok", 200, final_url, "Success")
        elif status_code == 404:
            return ("bad", 404, final_url, "Not Found")
        else:
            return ("error", status_code, final_url, f"HTTP {status_code}")
            
    except (Timeout, requests.exceptions.ReadTimeout):
        return ("error", 0, url, "Timeout")
    except SSLError:
        return ("error", 0, url, "SSL Error")
    except RequestException as e:
        return ("error", 0, url, "Connection Error")
    except Exception as e:
        return ("error", 0, url, f"Unknown: {str(e)}")

def judge_period_status(period_str):
    """모집 기간 텍스트 분석을 통한 상태 판정"""
    if not period_str or pd.isna(period_str):
        return "확인불가"
    
    today = date.today()
    # 날짜 패턴(YYYY-MM-DD 등) 추출
    matches = re.findall(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", str(period_str))
    
    if not matches:
        content = str(period_str)
        if any(keyword in content for keyword in ["상시", "채용시", "공고시"]):
            return "진행중"
        return "확인불가"
        
    try:
        dates = [date(int(m[0]), int(m[1]), int(m[2])) for m in matches]
        last_date = sorted(dates)[-1]
        return "진행중" if last_date >= today else "마감"
    except:
        return "확인불가"

# ==========================================
# 3. 메인 실행 (Execution)
# ==========================================
if __name__ == "__main__":
    t0 = time.time()
    print(f"[*] 검증 프로세스 시작: {PATH}")
    
    if not os.path.exists(PATH) and PATH == "data_sample.xlsx":
        print("[!] 검증할 엑셀 파일이 존재하지 않습니다. 파일을 확인해주세요.")
    else:
        df = pd.read_excel(PATH, sheet_name=SHEET)
        total = len(df)
        df_clean = df.drop_duplicates(subset=[COL_LINK])
        dedup = len(df_clean)
        
        rows = []
        for i, row in df_clean.iterrows():
            url = row[COL_LINK]
            title = str(row[COL_TITLE])
            period = str(row[COL_PERIOD])
            hosp = row[COL_SITE]

            status, code, final_url, reason = check_url(url)
            
            rows.append({
                "병원": hosp,
                "제목": title,
                "상태": status,
                "코드": code,
                "이유": reason,
                "키워드매칭": "Y" if any(k in title for k in ["간호사", "간호직"]) else "N",
                "모집기간": period,
                "마감여부": judge_period_status(period)
            })
            if (i+1) % 10 == 0:
                print(f"[*] 진행 중... ({i+1}/{dedup})")

        # 결과 저장 및 요약 출력
        rep = pd.DataFrame(rows)
        rep.to_csv("qa_validation_report.csv", index=False, encoding='utf-8-sig')
        
        elapsed = round(time.time() - t0, 1)
        print("\n" + "="*30)
        print("      QA 검증 요약 보고서      ")
        print("="*30)
        print(f"- 총 건수: {total} (중복제거 후 {dedup})")
        print(f"- 정상 링크(200 OK): {(rep['상태'] == 'ok').sum()}")
        print(f"- 마감/에러 링크: {(rep['상태'] != 'ok').sum()}")
        print(f"- 진행중 공고: {(rep['마감여부'] == '진행중').sum()}")
        print(f"- 소요시간: {elapsed}초")
        print("="*30)
        print("[*] 상세 결과가 'qa_validation_report.csv'로 저장되었습니다.")
