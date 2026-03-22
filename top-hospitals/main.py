# -*- coding: utf-8 -*-
"""
===============================================================================
[Project] 42 Top-Tier Hospitals Nursing Job Scraper & QA Pipeline
- Description: 전국 42개 상급종합병원의 간호사 채용 공고를 자동 수집하는 크롤러
- Features:
  1. Selenium 기반의 동적 웹 크롤링 및 봇 탐지 우회
  2. Data Fingerprinting을 통한 수집 데이터 중복 방지 (seen_posts.csv)
  3. [QA] RaiT(Responsible AI & Test) 게이트웨이를 통한 데이터 품질 검증
===============================================================================
"""

import re
import time
import datetime
import traceback
import os
import hashlib
import pandas as pd
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException,
    StaleElementReferenceException
)
from webdriver_manager.chrome import ChromeDriverManager

# ==============================================================================
# [1] Global Settings & Utils (공통 설정 및 유틸리티)
# ==============================================================================

# --- Basic Settings ---
HEADLESS_MODE = True  # 화면 렌더링 생략 (CI/CD 환경 최적화)
PAGELOAD_TIMEOUT = 30
IMPLICIT_WAIT = 3
SEEN_CSV = os.path.join("top-hospitals", "seen_posts.csv")

# --- WebDriver Utility ---
def make_driver():
    """
    모든 크롤링 모듈에 공통 적용될 Chrome WebDriver를 생성합니다.
    보안이 까다로운 병원 사이트를 위해 봇 탐지 방지 및 인증서 무시 옵션을 포함합니다.
    """
    opts = webdriver.ChromeOptions()
    if HEADLESS_MODE:
        opts.add_argument("--headless=new")

    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    opts.add_argument("--ignore-certificate-errors")
    opts.add_argument("--allow-insecure-localhost")

    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    try:
        drv.set_page_load_timeout(PAGELOAD_TIMEOUT)
    except Exception:
        pass
    drv.implicitly_wait(IMPLICIT_WAIT)
    return drv

def wait_ready(drv, t=10):
    """DOM 로딩(document.readyState == 'complete') 완료 시점까지 대기합니다."""
    try:
        WebDriverWait(drv, t).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except Exception:
        pass

# --- Data Formatting Utilities ---
def safe_str(x):
    return (x or "").strip()

def get_text_safe(el):
    try:
        return el.text.strip()
    except Exception:
        return ""

def std_row(hospital, title, period, link):
    """수집된 데이터를 표준 딕셔너리 포맷으로 통일합니다."""
    return {
        "병원": hospital,
        "제목": safe_str(title),
        "모집기간": safe_str(period),
        "링크": safe_str(link),
    }

def df_std(rows):
    """표준 컬럼(Column) 구조를 가진 DataFrame을 반환합니다."""
    cols = ["병원", "제목", "모집기간", "링크"]
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows)
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df[cols].copy()

def safe_sheet(name):
    """엑셀 시트명 생성 시 발생하는 유효성 에러를 방지합니다."""
    return re.sub(r'[\\/*?:\[\]]', "_", (name or "Sheet")).strip()[:30]


# ==============================================================================
# [2] QA Validation & Data Management (데이터 품질 검증 및 저장)
# ==============================================================================

def verify_rait_compliance(row):
    """
    [QA Gateway] RaiT 관점의 데이터 품질 및 책임성 검증 로직
    - 크롤링된 Raw Data가 통합 DB에 저장되기 전 거치는 무결성 검사기입니다.
    """
    title = safe_str(row.get("제목"))
    period = safe_str(row.get("모집기간"))
    reasons = []
    
    # 1. Safety: 개인정보(휴대전화 번호 등) 노출 필터링
    if re.search(r"010-\d{4}-\d{4}", title):
        reasons.append("Safety(개인정보의심)")
        
    # 2. Reliability: 크롤링 누락/에러 필터링 (불완전한 제목 등)
    if not title or len(title) < 2 or title == "제목 없음":
        reasons.append("Reliability(제목누락)")
        
    # 3. Accuracy: 할루시네이션 및 연도 표기 오류 검증
    year_match = re.search(r"20\d{2}", period)
    if year_match:
        year = int(year_match.group())
        if year < 2024 or year > 2027:
            reasons.append("Accuracy(연도오류)")
            
    if not reasons:
        return pd.Series(["PASS", "Normal"])
    else:
        return pd.Series(["FAIL", " | ".join(reasons)])

def _fingerprint_row(row):
    """데이터 고유 식별자(Fingerprint) 생성 (해시화)"""
    def _norm(s):
        return (str(s) or "").strip().lower()

    url = _norm(row.get("링크", ""))
    if url and len(url) > 5:
        base = f"url::{url}"
    else:
        base = f"{_norm(row.get('병원'))}|{_norm(row.get('제목'))}|{_norm(row.get('모집기간'))}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def _load_seen():
    """기존 수집 이력(seen_posts.csv) 로드"""
    if os.path.exists(SEEN_CSV):
        try:
            return set(pd.read_csv(SEEN_CSV)["fingerprint"].astype(str).tolist())
        except Exception:
            return set()
    return set()

def _save_seen(new_fps):
    """새로운 정상 데이터를 수집 이력에 병합"""
    if not new_fps:
        return
    if os.path.exists(SEEN_CSV):
        old = pd.read_csv(SEEN_CSV)
        merged = pd.concat([old, pd.DataFrame({"fingerprint": new_fps})], ignore_index=True)
        merged.drop_duplicates(subset=["fingerprint"], inplace=True)
        merged.to_csv(SEEN_CSV, index=False)
    else:
        pd.DataFrame({"fingerprint": new_fps}).to_csv(SEEN_CSV, index=False)

def save_workbook_consolidated(per_hospital, out_path):
    """
    통합 엑셀 저장 및 QA 필터링 로직
    - 수집된 모든 병원의 데이터를 병합하고, RaiT 검증을 통해 불량 데이터를 격리합니다.
    """
    merged_parts = []
    
    for hosp, df in per_hospital.items():
        if df is not None and not df.empty:
            df2 = df.copy()
            if "병원" not in df2.columns or df2["병원"].eq("").all(): 
                df2["병원"] = hosp
            merged_parts.append(df2[["병원","제목","모집기간","링크"]])
    
    merged = pd.concat(merged_parts, ignore_index=True) if merged_parts else pd.DataFrame(columns=["병원","제목","모집기간","링크"])
    
    # QA Data Verification (RaiT)
    if not merged.empty:
        merged[["RaiT상태", "RaiT사유"]] = merged.apply(verify_rait_compliance, axis=1)
    else:
        merged["RaiT상태"] = []
        merged["RaiT사유"] = []
    
    # Deduplication Logic
    seen = _load_seen()
    merged["fingerprint"] = merged.apply(_fingerprint_row, axis=1)
    merged["상태"] = merged["fingerprint"].apply(lambda x: "기존" if x in seen else "신규")
    
    df_new = merged[merged["상태"]=="신규"].copy()
    
    # Only PASS data is saved to seen_posts to prevent garbage data injection
    df_new_pass = df_new[df_new["RaiT상태"] == "PASS"]
    if not df_new_pass.empty:
        _save_seen(df_new_pass["fingerprint"].tolist())

    # Export to Excel
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        merged.drop_duplicates(subset=["fingerprint"], inplace=True) 
        merged.to_excel(w, index=False, sheet_name="통합전체") 
        df_new.to_excel(w, index=False, sheet_name="미등록_신규")
        
        # QA Analysis Sheet
        df_fail = merged[merged["RaiT상태"] == "FAIL"]
        if not df_fail.empty:
            df_fail.to_excel(w, index=False, sheet_name="RaiT_검증실패(QA용)")
            
        if not merged.empty:
            merged.groupby("병원").size().reset_index(name="건수").sort_values("건수", ascending=False).to_excel(w, index=False, sheet_name="요약")

        for hosp, df in per_hospital.items():
            if df is not None and not df.empty:
                df.to_excel(w, index=False, sheet_name=safe_sheet(hosp))
                
    print(f"\n💾 통합 리포트 생성 완료: {out_path}")
    print(f"📊 총 수집: {len(merged)}건 | ✨ 신규 등록: {len(df_new)}건 | 🚨 QA 실패 차단: {len(merged[merged['RaiT상태']=='FAIL'])}건")


# ==============================================================================
# [3] Crawler Modules (각 병원별 수집 모듈)
# ==============================================================================

# --- Group 1 (기존 run_ 네이밍 유지) ---
def run_seoul_asan(drv):
    hospital = "서울아산병원"
    url = "https://recruit.amc.seoul.kr/recruit/career/list.do?codeFirst=T04005&codeTwo=T04005002"
    rows = []
    
    drv.get(url)
    wait_ready(drv)
    
    items = drv.find_elements(By.CSS_SELECTOR, "ul.dayListBox > li")
    for item in items:
        try:
            title = item.find_element(By.CSS_SELECTOR, "div.dayListTitle span").text.strip()
            if "간호사" not in title: 
                continue
            
            period = item.find_element(By.CSS_SELECTOR, "div.dayListTitle2 span").text.strip()
            onclick = item.find_element(By.CSS_SELECTOR, "div.dayListTitle a").get_attribute("onclick") or ""
            m2 = re.search(r"fnDetail\('(\d+)'", onclick)
            link = f"https://recruit.amc.seoul.kr/recruit/career/view.do?recruitNo={m2.group(1)}" if m2 else ""
            
            rows.append(std_row(hospital, title, period, link))
        except Exception: 
            continue
    return df_std(rows)

def run_cau_mc(drv):
    hospital = "중앙대병원"
    url = "https://ch.cauhs.or.kr/recruit/job/noticeList.do"
    rows = []
    
    drv.get(url)
    wait_ready(drv)

    trs = drv.find_elements(By.CSS_SELECTOR, "table.table_sty01 tbody tr")
    for tr in trs:
        try:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) < 6:
                continue

            status = tds[1].text.strip()
            if "원서접수중" not in status:
                continue

            a = tds[2].find_element(By.TAG_NAME, "a")
            title = a.text.strip()
            if "간호사" not in title:
                continue

            period = tds[3].text.strip()
            link = a.get_attribute("href")
            
            rows.append(std_row(hospital, title, period, link))
        except Exception:
            continue
    return df_std(rows)

def run_eumc(drv):
    hospital = "이화의료원"
    url = "https://eumc.applyin.co.kr/jobs/"
    rows = []
    
    drv.get(url)
    try:
        WebDriverWait(drv, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.recruit_list"))
        )
        jobs = drv.find_elements(By.CSS_SELECTOR, "div.recruit_list ul li")
        
        for job in jobs:
            try:
                title_el = job.find_element(By.CSS_SELECTOR, ".recruit_tit a.inner")
                title = title_el.text.strip()
                if "간호사" not in title: 
                    continue

                status_el = job.find_element(By.CSS_SELECTOR, ".recruit_badge .badge_ing")
                if "접수중" not in status_el.text: 
                    continue
                
                link = title_el.get_attribute("href")
                try:
                    period = job.find_element(By.CSS_SELECTOR, ".day_txt").text.strip()
                except Exception:
                    period = "수시채용(공고참조)"

                rows.append(std_row(hospital, title, period, link))
            except Exception:
                continue
    except Exception as e:
        pass
    return df_std(rows)

def run_gangneung_asan(drv):
    hospital = "강릉아산병원"
    url = "https://www.gnah.co.kr/kor/CMS/RecruitMgr/list.do?mCode=MN122"
    rows = []
    
    drv.get(url)
    wait_ready(drv)
    
    trs = drv.find_elements(By.CSS_SELECTOR, "table.board-list-table tbody tr")
    for tr in trs:
        try:
            title = tr.find_element(By.CSS_SELECTOR, "td.subject a").text.strip()
            status = tr.find_element(By.CSS_SELECTOR, "td.progress").text.strip()
            if "간호사" in title and "접수중" in status:
                link = tr.find_element(By.CSS_SELECTOR, "td.subject a").get_attribute("href")
                period = tr.find_element(By.CSS_SELECTOR, "td.period").text.strip()
                rows.append(std_row(hospital, title, period, urljoin(url, link)))
        except Exception: 
            continue
    return df_std(rows)

def run_inha(drv):
    hospital = "인하대병원"
    url = "https://www.inha.com/page/about/recruit/list"
    rows = []

    drv.get(url)
    wait_ready(drv)

    trs = drv.find_elements(By.CSS_SELECTOR, "table.table-type1.recruit tbody tr")
    for tr in trs:
        try:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) < 6:
                continue

            title = tds[2].text.strip()
            status = tds[4].text.strip()
            period = tds[3].text.strip()

            if "간호사" not in title or "모집중" not in status:
                continue

            a = tr.find_element(By.CSS_SELECTOR, "td.title a")
            data_id = a.get_attribute("data-id")
            link = f"https://www.inha.com/page/about/recruit/view/{data_id}"

            rows.append(std_row(hospital, title, period, link))
        except Exception:
            continue
    return df_std(rows)

def run_kbsmc(drv):
    hospital = "강북삼성병원"
    url = "https://recruit.kbsmc.co.kr/jsp/recruit/recruitList.jsp"
    rows = []

    drv.get(url)
    time.sleep(3) 

    boxes = drv.find_elements(By.CSS_SELECTOR, "ul.job-boxs a.job-box")
    for box in boxes:
        try:
            full_text = box.get_attribute("textContent").replace("\n", " ").strip()
            if "마감" in full_text or "간호사" not in full_text: 
                continue

            try:
                title_el = box.find_element(By.CSS_SELECTOR, "p.txt18[style*='height']")
                title = title_el.get_attribute("textContent").strip()
            except Exception:
                p_tags = box.find_elements(By.CSS_SELECTOR, "p.txt18")
                title = "제목 없음"
                for p in p_tags:
                    p_txt = p.get_attribute("textContent").strip()
                    if "간호사" in p_txt:
                        title = p_txt
                        break

            try:
                period = box.find_element(By.CSS_SELECTOR, "div.flex3 p.blue").get_attribute("textContent").strip()
            except Exception:
                period = "공고문 참조"

            link = box.get_attribute("href")
            rows.append(std_row(hospital, title, period, link))
        except Exception:
            continue
    return df_std(rows)

def run_khmc(drv):
    hospital = "경희의료원"
    url = "https://recruit.incruit.com/khmc/job/"
    rows = []
    
    drv.get(url)
    wait_ready(drv)
    
    items = drv.find_elements(By.CSS_SELECTOR, "div.list-item-box li")
    for it in items:
        try:
            title = it.find_element(By.CSS_SELECTOR, "span.title").text.strip()
            if "간호사" not in title: 
                continue
            if "마감" in it.find_element(By.CSS_SELECTOR, "span.state").text: 
                continue
            
            period = it.find_element(By.CSS_SELECTOR, "em.date").text.strip()
            link = it.find_element(By.CSS_SELECTOR, "a.btn").get_attribute("href")
            rows.append(std_row(hospital, title, period, link))
        except Exception: 
            continue
    return df_std(rows)

def run_kuh(drv):
    hospital = "건국대학교병원"
    url = "https://www.kuh.ac.kr/recruit/apply/noticeList.do"
    rows = []
    
    drv.get(url)
    wait_ready(drv)
    
    links = drv.find_elements(By.CSS_SELECTOR, "td.alignL a")
    for a in links:
        try:
            title = a.text.strip()
            if "간호사" not in title: 
                continue
            
            link = a.get_attribute("href")
            period_td = a.find_element(By.XPATH, "../following-sibling::td[2]")
            period = period_td.text.strip()
            
            rows.append(std_row(hospital, title, period, link))
        except Exception: 
            continue
    return df_std(rows)

def run_smc_changwon(drv):
    # 🕵️‍♂️ GitHub Actions 환경인지 체크 (os.getenv 활용)
    if os.getenv('GITHUB_ACTIONS') == 'true':
        print("⏭️ [SKIP] 삼성창원병원은 깃허브 보안 정책(해외 IP 차단)상 수집 불가 (로컬 전용)")
        # 텅 빈 데이터프레임을 돌려줘서 에러 없이 다음 병원으로 넘어가게 함
        return df_std([]) 
    
    hospital = "삼성창원병원"
    url = "https://smc.skku.edu/recruit/recruit/recruitInfo/list.do?mId=42&schPosition=C1N"
    rows = []
    
    # 여기서부터는 실제 크롤링 로직 (로컬에서만 실행됨)
    drv.get(url)
    wait_ready(drv)
    
    trs = drv.find_elements(By.CSS_SELECTOR, "table tbody tr")
    for tr in trs:
        try:
            if "지원하기" not in tr.find_element(By.CSS_SELECTOR, "td.state").text: 
                continue
            
            a = tr.find_element(By.CSS_SELECTOR, "td.title a")
            title = a.text.strip()
            onclick = a.get_attribute("onclick")
            idx = re.search(r"fn_goDtl\('(\d+)'", onclick).group(1)
            link = f"https://smc.skku.edu/recruit/recruit/recruitInfo/view.do?mId=42&idx={idx}"
            period = tr.find_element(By.CSS_SELECTOR, "td.date").text.strip()
            
            rows.append(std_row(hospital, title, period, link))
        except Exception: 
            continue
            
    return df_std(rows)

def run_yuhs(drv):
    hospital = "연세대학교의료원"
    url = "https://yuhs.recruiter.co.kr/app/jobnotice/list"
    rows = []
    
    drv.get(url)
    wait_ready(drv)
    
    lis = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in lis:
        try:
            title = li.find_element(By.CSS_SELECTOR, "h2.list-bbs-title a").text.strip()
            status = li.find_element(By.CSS_SELECTOR, "div.list-bbs-status").text
            if "간호사" in title and "접수중" in status:
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text
                link = li.find_element(By.CSS_SELECTOR, "h2.list-bbs-title a").get_attribute("href")
                rows.append(std_row(hospital, title, period, urljoin(url, link)))
        except Exception: 
            continue
    return df_std(rows)

# --- Group 2 (리팩토링: run_ 영문명 통일) ---
def run_donga(drv):
    name = "동아대병원"
    url = "https://www.damc.or.kr/05/03_2017.php"
    rows = []
    
    drv.get(url)
    wait_ready(drv)
    
    trs = drv.find_elements(By.CSS_SELECTOR, "table.list_normal_D tbody tr")
    for tr in trs:
        try:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) >= 5:
                a = tds[2].find_element(By.TAG_NAME, "a")
                title = safe_str(a.text)
                if "간호사" in title:
                    link = a.get_attribute("href")
                    period = safe_str(tds[3].text)
                    rows.append(std_row(name, title, period, link))
        except Exception: 
            continue
    return df_std(rows)

def run_samsung_seoul(drv):
    name = "삼성서울병원"
    url = "https://www.samsunghospital.com/home/recruit/recruitInfo/recruitNotice.do"
    rows = []
    
    drv.get(url)
    wait_ready(drv)
    
    trs = drv.find_elements(By.CSS_SELECTOR, "table.board-list tbody tr")
    for tr in trs:
        try:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) >= 7:
                if "간호사" in tds[2].text:
                    a = tds[3].find_element(By.TAG_NAME, "a")
                    rows.append(std_row(name, safe_str(a.text), safe_str(tds[4].text), a.get_attribute("href")))
        except Exception: 
            continue
    return df_std(rows)

def run_paik_busan(drv):
    name = "인제대학교부속백병원(부산)"
    url = "https://www.paik.ac.kr/busan/user/job/list.do?menuNo=900101"
    rows = []
    
    drv.get(url)
    wait_ready(drv)
    
    trs = drv.find_elements(By.CSS_SELECTOR, "table tbody tr")
    for tr in trs:
        try:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) < 2: 
                continue
            
            a = tds[1].find_element(By.TAG_NAME, "a")
            title = safe_str(a.text)
            if "간호사" in title:
                link_js = a.get_attribute("href") or ""
                link = link_js
                if "javascript:view('" in link_js:
                    notice_no = link_js.split("'")[1]
                    link = f"https://www.paik.ac.kr/busan/user/job/view.do?no={notice_no}"
                period = safe_str(tds[2].text)
                rows.append(std_row(name, title, period, link))
        except Exception: 
            continue
    return df_std(rows)

def run_gil(drv):
    name = "가천대길병원"
    base = "https://gilhospital.recruiter.co.kr"
    url = "https://gilhospital.recruiter.co.kr/app/jobnotice/list"
    rows = []
    
    drv.get(url)
    wait_ready(drv)
    
    items = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList li")
    for li in items:
        try:
            status = ""
            try: 
                status = li.find_element(By.CSS_SELECTOR, "span.list-bbs-status").text
            except Exception: 
                try: 
                    status = li.find_element(By.CSS_SELECTOR, "div.list-bbs-status span").text
                except Exception: 
                    pass
            
            if "접수중" not in status: 
                continue
            
            a = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a.text)
            if "간호사" in title:
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text
                rows.append(std_row(name, title, period, urljoin(base, a.get_attribute("href"))))
        except Exception: 
            continue
    return df_std(rows)

def run_ajou(drv):
    name = "아주대학교의료원"
    base = "https://ajoumc.recruiter.co.kr"
    url = "https://ajoumc.recruiter.co.kr/app/jobnotice/list"
    rows = []
    
    drv.get(url)
    wait_ready(drv)
    
    items = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList li")
    for li in items:
        try:
            status = li.find_element(By.CSS_SELECTOR, ".list-bbs-status .text-label").text
            if "접수중" not in status: 
                continue
            
            a = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a.get_attribute("textContent"))
            if "간호사" in title:
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").get_attribute("textContent")
                rows.append(std_row(name, title, period, urljoin(base, a.get_attribute("href"))))
        except Exception: 
            continue
    return df_std(rows)

def run_hanyang(drv):
    name = "한양대학교병원"
    base = "https://hyumc.recruiter.co.kr"
    url = f"{base}/career/home"
    rows = []

    drv.get(url)
    wait_ready(drv)

    cards = drv.find_elements(By.CSS_SELECTOR, "a[class*='RecruitList_list-item']")
    for card in cards:
        try:
            try:
                status_elem = card.find_element(By.CSS_SELECTOR, "span[class*='RecruitList_submission-status-tag']")
                status = status_elem.text.strip()
            except Exception:
                try:
                    status_elem = card.find_element(By.XPATH, ".//span[contains(.,'접수중')]")
                    status = status_elem.text.strip()
                except Exception:
                    continue

            if "접수중" not in status:
                continue

            title_elem = card.find_element(By.CSS_SELECTOR, "[class*='RecruitList_title']")
            title = title_elem.text.strip()
            if "간호" not in title: 
                continue

            try:
                date_elem = card.find_element(By.CSS_SELECTOR, "div[class*='RecruitList_date']")
                date = date_elem.text.strip()
            except Exception:
                date = ""

            href = card.get_attribute("href")
            rows.append(std_row(name, title, date, urljoin(base, href)))

        except Exception:
            continue

    return df_std(rows)

def run_dankook(drv):
    name = "단국대학교병원"
    URL = "https://www.dkuh.co.kr/board5/bbs/board?bo_table=01_03_05"
    rows = []
    
    drv.get(URL)
    wait_ready(drv)
    
    trs = drv.find_elements(By.CSS_SELECTOR, "form#fboardlist table tbody tr")
    for tr in trs:
        try:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 5: 
                continue
            
            status = tds[4].text
            if not any(k in status for k in ["진행","모집중","접수중"]): 
                continue
            
            a = tr.find_element(By.CSS_SELECTOR, "td.td_subject a")
            title = safe_str(a.get_attribute("innerText"))
            if "간호사" in title:
                rows.append(std_row(name, title, tds[3].text, a.get_attribute("href")))
        except Exception: 
            continue
    return df_std(rows)

def run_catholic_daegu(drv):
    name = "대구가톨릭대의료원"
    URL = "https://www.dcmc.co.kr/content/07community/01_05.asp"
    rows = []
    
    drv.get(URL)
    wait_ready(drv)
    
    trs = drv.find_elements(By.CSS_SELECTOR, "table tbody tr")
    for tr in trs:
        try:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 5: 
                continue
            if "모집완료" in tds[4].text: 
                continue
            
            a = tr.find_element(By.CSS_SELECTOR, "td.title a")
            title = safe_str(a.get_attribute("innerText"))
            if "간호사" in title:
                rows.append(std_row(name, title, tds[3].text, urljoin(URL, a.get_attribute("href"))))
        except Exception: 
            continue
    return df_std(rows)

def run_chosun(drv):
    name = "조선대병원"
    BASE = "https://hosp.chosun.ac.kr"
    URL = "https://hosp.chosun.ac.kr/bbs/?b_id=recruit&site=hospital&mn=211"
    rows = []
    
    drv.get(URL)
    wait_ready(drv)
    
    try:
        rows_el = drv.find_elements(By.XPATH,"//tbody//tr[td[contains(., '진행')]]") 
        for tr in rows_el:
            try:
                a = tr.find_element(By.XPATH, ".//td[contains(@class,'title')]//a")
                title = safe_str(a.get_attribute("innerText"))
                if "간호사" not in title: 
                    continue
                
                title_td = tr.find_element(By.XPATH, ".//td[contains(@class,'title')]")
                raw_text = title_td.get_attribute("innerText")
                period_raw = raw_text.replace(title, "").strip()
                period = re.sub(r'\s+', ' ', period_raw).strip()
                
                rows.append(std_row(name, title, period, urljoin(BASE, a.get_attribute("href"))))
            except Exception: 
                continue
    except Exception: 
        pass
    return df_std(rows)

def run_hallym(drv):
    name = "한림의료원"
    URL = "https://recruit.hallym.or.kr/index.jsp?inggbn=ing&movePage=1"
    rows = []
    
    drv.get(URL)
    wait_ready(drv)
    
    cards = drv.find_elements(By.CSS_SELECTOR, "div.main_rctbox a[href]")
    for card in cards:
        try:
            try: 
                title_el = card.find_element(By.CSS_SELECTOR, "ul.data_title li:last-child")
            except Exception: 
                title_el = card.find_element(By.CSS_SELECTOR, "ul.data_title li")
                
            title = safe_str(title_el.text)
            if "간호사" not in title: 
                continue
            
            status_text_el = card.find_element(By.CSS_SELECTOR, "ul.data_day")
            status_text = status_text_el.text.strip()
            
            if "마감" in status_text and "마감일" not in status_text: 
                continue
            
            deadline = status_text
            rows.append(std_row(name, title, deadline, urljoin(URL, card.get_attribute("href"))))
        
        except Exception: 
            continue
            
    return df_std(rows)

# --- Group 3 (리팩토링: run_ 영문명 통일) ---
def run_catholic_seoul(drv):
    name = "서울성모병원"
    HOME_URL = "https://recruit.cmcnu.or.kr/cmcseoul/index.do"
    rows = []
    
    drv.get(HOME_URL)
    try:
        WebDriverWait(drv, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.list-type01.recruit ul.items"))
        )
        cards = drv.find_elements(By.CSS_SELECTOR, "div.list-type01.recruit ul.items li")
        
        for card in cards:
            try:
                title_el = card.find_element(By.CSS_SELECTOR, ".tit strong")
                title = title_el.text.strip()
                
                if ("간호사" in title) or ("간호직" in title):
                    period = card.find_element(By.CSS_SELECTOR, ".info_wrap .data").text.strip()
                    a_tag = card.find_element(By.TAG_NAME, "a")
                    link = a_tag.get_attribute("href")
                    rows.append(std_row(name, title, period, link))
            except Exception:
                continue
    except Exception:
        pass
    return df_std(rows)

def run_kyungpook(drv):
    name = "경북대병원"
    URL = "https://www.knuh.kr/content/04information/02_01.asp#close"
    BASE = "https://www.knuh.kr"
    rows = []
    
    drv.get(URL)
    wait_ready(drv)
    
    trs = drv.find_elements(By.CSS_SELECTOR, "div#board table tbody tr")
    for tr in trs:
        try:
            if "진행" not in tr.text: 
                continue
            a = tr.find_element(By.CSS_SELECTOR, "td.title a")
            title = safe_str(a.get_attribute("innerText"))
            if "간호사" in title:
                period = tr.find_elements(By.CSS_SELECTOR, "td")[-1].text
                rows.append(std_row(name, title, period, urljoin(BASE, a.get_attribute("href"))))
        except Exception: 
            continue
    return df_std(rows)

def run_gyeongsang(drv):
    name = "경상대병원"
    URL = "https://www.gnuh.co.kr/gnuh/board/list.do?rbsIdx=109"
    rows = []
    
    drv.get(URL)
    wait_ready(drv)
    
    trs = drv.find_elements(By.CSS_SELECTOR, "table tbody tr")
    for tr in trs:
        try:
            a = tr.find_element(By.CSS_SELECTOR, "td.tt a")
            title = safe_str(a.get_attribute("innerText"))
            if "간호사" in title:
                posted = tr.find_element(By.CSS_SELECTOR, "td.date").text
                rows.append(std_row(name, title, posted, urljoin(drv.current_url, a.get_attribute("href"))))
        except Exception: 
            continue
    return df_std(rows)

def run_dongsan(drv):
    name = "동산의료원"
    URL = "https://dongsan.dsmc.or.kr:49870/content/03intro/04_01.php"
    rows = []
    
    drv.get(URL)
    wait_ready(drv)
    
    trs = drv.find_elements(By.CSS_SELECTOR, "div.mscroll table.table1.board tbody tr")
    for tr in trs:
        try:
            labels = tr.find_elements(By.CSS_SELECTOR, "span.label.regular, span.label.ing")
            if not labels:
                continue 
            
            a_tag = tr.find_element(By.CSS_SELECTOR, "td.title a, td.notice.title a")
            full_text = a_tag.get_attribute("innerText").strip()
            
            if not ("간호사" in full_text or "계약직" in full_text):
                continue
                
            title = full_text
            for lb in labels:
                lb_text = lb.get_attribute("innerText").strip()
                title = title.replace(lb_text, "").strip()

            period = ""
            try:
                period = tr.find_element(By.CSS_SELECTOR, "span.read").text.strip()
            except Exception:
                tds = tr.find_elements(By.TAG_NAME, "td")
                if len(tds) > 3:
                    period = tds[3].text.strip()
            
            rows.append(std_row(name, title, period, urljoin(URL, a_tag.get_attribute("href"))))
            
        except Exception:
            continue
    return df_std(rows)

def run_korea_univ(drv):
    name = "고려대의료원"
    HOME_URL = "https://kumc.recruiter.co.kr/career/home"
    rows = []

    drv.get(HOME_URL)
    try:
        WebDriverWait(drv, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[class*='RecruitList_list-item']"))
        )
        cards = drv.find_elements(By.CSS_SELECTOR, "a[class*='RecruitList_list-item']")

        for card in cards:
            try:
                status_el = card.find_element(By.CSS_SELECTOR, "span[class*='Tag_tag']")
                status = status_el.text.strip()
                
                if "접수중" not in status:
                    continue

                title_el = card.find_element(By.CSS_SELECTOR, "p[class*='RecruitList_title']")
                title = title_el.text.strip()

                if "간호사" not in title:
                    continue

                period_el = card.find_element(By.CSS_SELECTOR, "div[class*='RecruitList_date']")
                period = period_el.text.strip().replace("\n", " ~ ")
                link = card.get_attribute("href")

                rows.append(std_row(name, title, period, link))
            except Exception:
                continue
    except Exception:
        pass
    return df_std(rows)

def run_yeongnam(drv):
    name = "영남대학교병원"
    LIST_URL = "https://yumc.ac.kr:8443/bbs/List.do?bbsId=news5"
    BASE_URL = "https://yumc.ac.kr:8443"
    rows = []
    seen = set() 

    try:
        drv.get(LIST_URL)
        wait_ready(drv)
        time.sleep(2)

        try:
            table = drv.find_element(By.CSS_SELECTOR, "table.table_yumc_table")
        except Exception:
            table = drv.find_element(By.CSS_SELECTOR, "#content_body table")

        trs = table.find_elements(By.CSS_SELECTOR, "tbody > tr")
        if not trs:
            trs = table.find_elements(By.CSS_SELECTOR, "tr")

        for tr in trs:
            try:
                links = tr.find_elements(By.CSS_SELECTOR, 'td.t_left a[href*="/bbs/view.do"]')
                if not links:
                    continue

                a = links[0]
                title = safe_str(a.text)
                if "간호사" not in title:
                    continue

                metas = tr.find_elements(By.CSS_SELECTOR, "td.hidden-xs.hidden-sm")
                written_date = ""
                status_text = ""
                
                if len(metas) >= 2:
                    written_date = safe_str(metas[-2].text)
                    status_text = safe_str(metas[-1].text)
                elif len(metas) == 1:
                    status_text = safe_str(metas[-1].text)

                if "마감" in status_text:
                    continue

                key = (title, written_date)
                if key in seen:
                    continue
                seen.add(key)

                link = urljoin(BASE_URL, a.get_attribute("href"))
                rows.append(std_row(name, title, written_date, link))
            except Exception:
                continue
        return df_std(rows)

    except Exception:
        return df_std(rows)

def run_wonkwang(drv):
    name = "원광대학교병원"
    LIST_URL = "https://www.wkuh.org/recruit/jobs/recruit_notice.do?recruit_type=list&sh_rc_type=validity"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    items = drv.find_elements(By.CSS_SELECTOR, "div.recruit_list_bbs ul > li")
    for li in items:
        try:
            status = li.find_element(By.CSS_SELECTOR, ".list_bbs_status input.status_btn").get_attribute("value")
            if "지원가능" not in status: 
                continue
            
            a = li.find_element(By.CSS_SELECTOR, ".list_bbs_title p a")
            title = safe_str(a.text)
            if "간호사" not in title: 
                continue

            period = ""
            try:
                period = li.find_element(By.CSS_SELECTOR, ".list_bbs_title span:not(.dday)").text.strip()
            except Exception:
                pass
            
            rows.append(std_row(name, title, period, urljoin(drv.current_url, a.get_attribute("href"))))
        except Exception: 
            continue
    return df_std(rows)

def run_jeonbuk(drv):
    name = "전북대학교병원"
    LIST_URL = "https://jbuh.recruiter.co.kr/app/jobnotice/list"
    BASE_URL = "https://jbuh.recruiter.co.kr"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    items = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in items:
        try:
            status = li.find_element(By.CSS_SELECTOR, ".list-bbs-status .text-label").text
            if "접수중" not in status: 
                continue
                
            a = li.find_element(By.CSS_SELECTOR, ".list-bbs-title a")
            title = safe_str(a.text)
            if "간호사" in title:
                period = li.find_element(By.CSS_SELECTOR, ".list-bbs-date").text
                rows.append(std_row(name, title, period, urljoin(BASE_URL, a.get_attribute("href"))))
        except Exception: 
            continue
    return df_std(rows)

def run_yangsan_pusan(drv):
    name = "양산부산대학교병원"
    LIST_URL = "https://pnuyh.recruiter.co.kr/career/home"
    BASE_URL = "https://pnuyh.recruiter.co.kr"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    cards = drv.find_elements(By.CSS_SELECTOR, "a[href*='/career/jobs/']")
    for a in cards:
        try:
            status = a.find_element(By.CSS_SELECTOR, "span[class*='RecruitList_submission-status']").text
            if "접수중" not in status: 
                continue
                
            title = a.find_element(By.CSS_SELECTOR, "p[class*='RecruitList_title']").text
            if "간호사" in title:
                period = a.find_element(By.CSS_SELECTOR, "div[class*='RecruitList_date']").text
                rows.append(std_row(name, title, period, urljoin(BASE_URL, a.get_attribute("href"))))
        except Exception: 
            continue
    return df_std(rows)

def run_chilgok_kyungpook(drv):
    name = "칠곡경북대학교병원"
    LIST_URL = "https://knuh.recruiter.co.kr/app/jobnotice/list"
    BASE_URL = "https://knuh.recruiter.co.kr"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in cards:
        try:
            status = li.find_element(By.CSS_SELECTOR, ".list-bbs-status .text-label").text
            if "접수중" not in status: 
                continue
                
            a = li.find_element(By.CSS_SELECTOR, ".list-bbs-title a")
            title = safe_str(a.text)
            if "간호사" in title:
                period = li.find_element(By.CSS_SELECTOR, ".list-bbs-date").text
                rows.append(std_row(name, title, period, urljoin(BASE_URL, a.get_attribute("href"))))
        except Exception: 
            continue
    return df_std(rows)

# --- Group 4 (리팩토링: run_ 영문명 통일) ---
def run_sch_bucheon(drv):
    name = "순천향대부천병원"
    LIST_URL = "https://jobapplication.schmc.ac.kr/recruit/biz/job/recruiteList"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv) 
    
    try:
        cards = drv.find_elements(By.CSS_SELECTOR, "#tabs4 .list_box.clearfix li")
        for li in cards:
            try:
                btn = li.find_element(By.CSS_SELECTOR, ".list_button .btn01")
                if "신규지원" not in btn.text: 
                    continue
                
                title_el = li.find_element(By.CSS_SELECTOR, "p.l_title.tops a")
                title = safe_str(title_el.text).strip()
                
                if "간호사" in title:
                    period_el = li.find_element(By.CSS_SELECTOR, "span.d_right")
                    period = period_el.text.strip()
                    rows.append(std_row(name, title, period, LIST_URL))
            except Exception:
                continue
    except Exception:
        pass
    return df_std(rows)

def run_konyang(drv):
    name = "건양대학교의료원"
    LIST_URL = "https://www.kyuh.ac.kr/prog/recruitNotice/list.do?lyMcd=sub01_01"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    cards = drv.find_elements(By.CSS_SELECTOR, "div.program-skin_recruit ul > li")
    for li in cards:
        try:
            if "공고중" not in li.text: 
                continue
            title = li.find_element(By.CSS_SELECTOR, "strong.job").text
            if "간호사" in title:
                period = li.find_element(By.CSS_SELECTOR, "em.period").text
                rows.append(std_row(name, title, period, LIST_URL))
        except Exception: 
            continue
    return df_std(rows)

def run_pusan_univ(drv):
    name = "부산대학교병원"
    LIST_URL = "https://pnuh.recruiter.co.kr/app/jobnotice/list"
    BASE = "https://pnuh.recruiter.co.kr"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in cards:
        try:
            status_text = li.find_element(By.CSS_SELECTOR, ".list-bbs-status").text
            if "접수마감" in status_text:
                continue
            
            a = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a.text).strip()
            
            if any(keyword in title for keyword in ["간호사", "간호직"]):
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text.strip()
                link = urljoin(BASE, a.get_attribute("href"))
                rows.append(std_row(name, title, period, link))
        except Exception:
            continue
    return df_std(rows)

def run_seoul_univ(drv):
    name = "서울대학교병원"
    LIST_URL = "https://recruit.snuh.org/main.do"
    BASE = "https://recruit.snuh.org"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    cards = drv.find_elements(By.CSS_SELECTOR, "ul.posting > li")
    for li in cards:
        try:
            status_div = li.find_element(By.CSS_SELECTOR, "div[class^='status']")
            status_class = status_div.get_attribute("class")
            status_text = status_div.text.strip()
            
            if "status03" in status_class or "마감" in status_text:
                continue

            a_tag = li.find_element(By.CSS_SELECTOR, "a")
            title = safe_str(a_tag.text).strip()
            
            if any(keyword in title for keyword in ["간호직", "블라인드"]):
                try:
                    period = li.find_element(By.CSS_SELECTOR, "span").text.strip()
                except Exception:
                    period = "기간 정보 없음"
                
                link = urljoin(BASE, a_tag.get_attribute("href"))
                rows.append(std_row(name, title, period, link))
        except Exception:
            continue
    return df_std(rows)

def run_wonju_severance(drv):
    name = "원주연세의료원"
    LIST_URL = "https://ywmc.recruiter.co.kr/app/jobnotice/list"
    BASE = "https://ywmc.recruiter.co.kr"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in cards:
        try:
            if "접수중" not in li.find_element(By.CSS_SELECTOR, ".list-bbs-status").text: 
                continue
            
            a = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a.text)
            if "간호사" in title:
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text
                rows.append(std_row(name, title, period, urljoin(BASE, a.get_attribute("href"))))
        except Exception: 
            continue
    return df_std(rows)

def run_jeonnam(drv):
    name = "전남대학교병원"
    LIST_URL = "https://cnuh.recruiter.co.kr/app/jobnotice/list"
    BASE = "https://cnuh.recruiter.co.kr"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in cards:
        try:
            status_text = li.find_element(By.CSS_SELECTOR, ".list-bbs-status").text.strip()
            if "접수마감" in status_text:
                continue
            
            a_tag = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a_tag.text).strip()
            
            keywords = ["간호직", "지원직", "대체근로자"]
            if any(k in title for k in keywords):
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text.strip()
                link = urljoin(BASE, a_tag.get_attribute("href"))
                rows.append(std_row(name, title, period, link))
        except Exception:
            continue
    return df_std(rows)

def run_chungnam(drv):
    name = "충남대학교병원"
    LIST_URL = "https://cnuhinsa.recruiter.co.kr/career/apply"
    BASE = "https://cnuhinsa.recruiter.co.kr"
    rows = []

    drv.get(LIST_URL)
    wait_ready(drv)

    cards = drv.find_elements(By.CSS_SELECTOR, "[class*='RecruitList_list-item']")
    for a in cards:
        try:
            try:
                status_elem = a.find_element(By.CSS_SELECTOR, "span[class*='RecruitList_submission-status-tag']")
                status = status_elem.text.strip()
            except Exception:
                try:
                    status_elem = a.find_element(By.XPATH, ".//span[contains(.,'접수중')]")
                    status = status_elem.text.strip()
                except Exception:
                    continue

            if "접수중" not in status:
                continue

            title = a.find_element(By.CSS_SELECTOR, "[class*='RecruitList_title']").text.strip()
            if "간호" not in title:
                continue

            try:
                period = a.find_element(By.CSS_SELECTOR, "div[class*='RecruitList_date']").text.strip()
            except Exception:
                period = ""

            href = a.get_attribute("href")
            rows.append(std_row(name, title, period, urljoin(BASE, href)))

        except Exception:
            continue
    return df_std(rows)

def run_chungbuk(drv):
    name = "충북대학교병원"
    LIST_URL = "https://cbnuh.recruiter.co.kr/app/jobnotice/list"
    BASE = "https://cbnuh.recruiter.co.kr"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in cards:
        try:
            status_text = li.find_element(By.CSS_SELECTOR, ".list-bbs-status").text.strip()
            if "접수마감" in status_text:
                continue
            
            a_tag = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a_tag.text).strip()
            
            keywords = ["간호사", "기간제근무자"]
            if any(k in title for k in keywords):
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text.strip()
                link = urljoin(BASE, a_tag.get_attribute("href"))
                rows.append(std_row(name, title, period, link))
        except Exception:
            continue
    return df_std(rows)

def run_ulsan(drv):
    name = "울산대학교병원"
    HOME_URL = "https://recruit.uuh.ulsan.kr:8443/uuhrecruit/#!%EC%9E%85%EC%82%AC%EC%A7%80%EC%9B%90"
    rows = []
    
    drv.get(HOME_URL)
    try:
        WebDriverWait(drv, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr.v-table-row"))
        )
        cards = drv.find_elements(By.CSS_SELECTOR, "tr.v-table-row, tr.v-table-row-odd")
        
        for card in cards:
            try:
                text_all = card.text
                if "마감" in text_all:
                    continue
                
                try:
                    title_el = card.find_element(By.CSS_SELECTOR, ".v-button-caption")
                    title = title_el.text.strip()
                except Exception:
                    title = text_all.split('\n')[0]

                if "간호사" not in title:
                    continue
                
                period = ""
                lines = text_all.split('\n')
                for line in lines:
                    if "지원기간" in line:
                        period = line.replace("지원기간", "").replace(":", "").strip()
                        break
                
                link = HOME_URL
                rows.append(std_row(name, title, period, link))
            except Exception:
                continue
    except Exception:
        pass
    return df_std(rows)

def run_bundang_seoul(drv):
    name = "분당서울대학교병원(간호직)"
    LIST_URL = "https://snubh.recruiter.co.kr/app/jobnotice/list"
    BASE = "https://snubh.recruiter.co.kr"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in cards:
        try:
            if "접수중" not in li.find_element(By.CSS_SELECTOR, ".list-bbs-status").text: 
                continue
            
            a = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a.text)
            if "간호직" in title:
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text
                rows.append(std_row(name, title, period, urljoin(BASE, a.get_attribute("href"))))
        except Exception: 
            continue
    return df_std(rows)

def run_kosin(drv):
    name = "고신대복음병원"
    HOME_URL = "https://kosinmed.recruiter.co.kr/career/home"
    rows = []
    
    drv.get(HOME_URL)
    try:
        WebDriverWait(drv, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/career/jobs/']"))
        )
        cards = drv.find_elements(By.CSS_SELECTOR, "a[href*='/career/jobs/']")
        
        for card in cards:
            try:
                text_all = card.text
                if "마감" in text_all or "종료" in text_all:
                    continue

                try:
                    title = card.find_element(By.CSS_SELECTOR, "[class*='title']").text.strip()
                except Exception:
                    title = text_all.split('\n')[0]

                if "간호사" not in title:
                    continue

                try:
                    period = card.find_element(By.CSS_SELECTOR, "[class*='date']").text.strip()
                    period = period.replace("\n", " ~ ")
                except Exception:
                    period = "기간확인필요"

                link = card.get_attribute("href")
                rows.append(std_row(name, title, period, link))
            except Exception:
                continue
    except Exception:
        pass
    return df_std(rows)

def run_catholic_incheon(drv):
    name = "가톨릭대학교인천성모병원(간호직)"
    LIST_URL = "https://cmcism.recruiter.co.kr/app/jobnotice/list"
    BASE = "https://cmcism.recruiter.co.kr"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in cards:
        try:
            if "접수중" not in li.find_element(By.CSS_SELECTOR, ".list-bbs-status").text: 
                continue
            
            a = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a.text)
            if "간호직" in title:
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text
                rows.append(std_row(name, title, period, urljoin(BASE, a.get_attribute("href"))))
        except Exception: 
            continue
    return df_std(rows)


# ==============================================================================
# [4] Main Execution Engine (메인 실행부)
# ==============================================================================
if __name__ == "__main__":
    print("🚀 Automated QA Crawler Pipeline 시작 (42 Top-Tier Hospitals)")
    print(f"   - Headless Mode: {HEADLESS_MODE}")
    
    # WebDriver 초기화 (Single Instance 재사용)
    driver = make_driver()
    
    # 크롤링 모듈 리스트 (영문 네이밍 통일 완료)
    all_scrapers = [
        run_seoul_asan, run_cau_mc, run_eumc, run_gangneung_asan, 
        run_inha, run_kbsmc, run_khmc, run_kuh, run_smc_changwon, run_yuhs,
        run_donga, run_samsung_seoul, run_paik_busan, run_gil, run_ajou, 
        run_hanyang, run_dankook, run_catholic_daegu, run_chosun, run_hallym,
        run_catholic_seoul, run_kyungpook, run_gyeongsang, run_dongsan, 
        run_korea_univ, run_yeongnam, run_wonkwang, run_jeonbuk, run_yangsan_pusan, 
        run_chilgok_kyungpook, run_sch_bucheon, run_konyang, run_pusan_univ, 
        run_seoul_univ, run_wonju_severance, run_jeonnam, run_chungnam, 
        run_chungbuk, run_ulsan, run_bundang_seoul, run_kosin, run_catholic_incheon
    ]
    
    per_hospital = {}
    errors = []
    start_time = time.time()

    try:
        for i, scraper in enumerate(all_scrapers):
            func_name = scraper.__name__
            print(f"[{i+1}/{len(all_scrapers)}] ▶ {func_name} 수집 중...")
            
            try:
                # 공통 드라이버를 각 모듈에 의존성 주입(Dependency Injection) 형태로 전달
                df = scraper(driver)
                
                if df is not None and not df.empty:
                    hosp_name = df.iloc[0]['병원']
                    per_hospital[hosp_name] = df
                    print(f"   - OK: {len(df)}건 완료")
                else:
                    print("   - 공고 없음")
            except Exception as e:
                print(f"   ⚠️ {func_name} 에러: {e}")
                errors.append({"함수": func_name, "에러": str(e)})

    finally:
        # WebDriver 자원 반납
        driver.quit()
        end_time = time.time()
        print(f"\n🏁 파이프라인 실행 종료 (소요시간: {end_time - start_time:.1f}초)")

        # 결과 리포트 생성 및 병합
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        save_workbook_consolidated(per_hospital, out_path=f"간호사_통합_결과_{ts}.xlsx")
        
        if errors:
            # 📝 한글 깨짐 방지(utf-8-sig) 옵션 추가
            pd.DataFrame(errors).to_csv(f"error_log_{ts}.csv", index=False, encoding='utf-8-sig')
            print(f"⚠️ 에러 로그 저장 완료: error_log_{ts}.csv")