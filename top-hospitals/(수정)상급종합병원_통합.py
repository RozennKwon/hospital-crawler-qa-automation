# -*- coding: utf-8 -*-
# 260210 최신 버전 - 4차 고도화 : 누락된 병원 보완
import re, time, datetime, traceback, os, hashlib
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
# [1] 공통 설정 및 유틸리티 (Global Settings & Utils)
# ==============================================================================

# --- 기본 설정값 ---
HEADLESS_MODE = True   # 창을 보려면 False 로 변경
PAGELOAD_TIMEOUT = 30
IMPLICIT_WAIT = 3

# --- 드라이버/로딩 유틸 ---
def make_driver():
    """
    모든 병원에 적용될 공용 Chrome WebDriver 생성.
    이화의료원 등 보안이 까다로운 곳을 위해 '탐지 방지' 옵션 기본 탑재.
    """
    opts = webdriver.ChromeOptions()
    if HEADLESS_MODE:
        opts.add_argument("--headless=new")

    # 봇 탐지 방지 옵션 (공통 적용)
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    # **[핵심 추가 옵션] 8443 포트 및 인증서 오류 무시**
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
    """document.readyState == 'complete' 상태까지 대기 (best-effort)."""
    try:
        WebDriverWait(drv, t).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except Exception:
        pass


# --- 텍스트/문자열 유틸 ---
def safe_str(x):
    return (x or "").strip()


safe = safe_str  # 별칭


def get_text_safe(el):
    """웹 요소에서 텍스트를 안전하게 추출."""
    try:
        return el.text.strip()
    except Exception:
        return ""


# --- 행/데이터프레임 유틸 ---
def std_row(hospital, title, period, link):
    """크롤링 결과 표준 행 포맷."""
    return {
        "병원": hospital,
        "제목": safe_str(title),
        "모집기간": safe_str(period),
        "링크": safe_str(link),
    }


def df_std(rows):
    """표준 컬럼 순서로 DataFrame 생성."""
    cols = ["병원", "제목", "모집기간", "링크"]
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows)
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df[cols].copy()


def safe_sheet(name):
    """엑셀 시트명 유효문자/길이 보정."""
    return re.sub(r'[\\/*?:\[\]]', "_", (name or "Sheet")).strip()[:30]


# --- 기간/제목 추출 유틸 ---
def get_period_from_detail(driver, timeout=15):
    """
    상세 페이지에서 접수/모집/채용 기간을 다양한 방식으로 추출.
    - 테이블 th → td
    - 라벨/제목 다음 형제
    - 페이지 전체 텍스트 라인 스캔
    """
    wait = WebDriverWait(driver, timeout)

    # 1) 테이블 헤더 패턴: (th='접수기간' | '모집기간' | '채용기간') -> td
    for key in ["접수기간", "모집기간", "채용기간"]:
        try:
            el = wait.until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        f"//th[normalize-space()='{key}']/following-sibling::td[1]",
                    )
                )
            )
            txt = get_text_safe(el)
            if txt:
                return txt
        except Exception:
            pass

    # 2) 라벨/제목 다음 형제 노드
    for key in ["접수기간", "모집기간", "채용기간"]:
        try:
            el = driver.find_element(By.XPATH, f"//*[contains(normalize-space(),'{key}')]")
            try:
                sib = el.find_element(By.XPATH, "following::*[1]")
                txt = get_text_safe(sib)
                if txt:
                    return txt
            except Exception:
                pass
        except Exception:
            pass

    # 3) 페이지 전체 텍스트에서 라인 기반 추출 (정규식)
    try:
        body_text = driver.execute_script("return document.body.innerText")
        lines = [ln.strip() for ln in body_text.splitlines() if "기간" in ln]
        date_rgx = re.compile(
            r"\d{4}\.\d{1,2}\.\d{1,2}[^0-9]{0,5}\d{4}\.\d{1,2}\.\d{1,2}"
        )
        for ln in lines:
            if any(k in ln for k in ["접수", "모집", "채용"]):
                m = date_rgx.search(ln)
                if m:
                    return m.group(0)
        if lines:
            return lines[0]
    except Exception:
        pass

    return "기간정보_없음"

# --- 데이터 저장 및 중복 제거 로직 ---
# 수정 후 (폴더 경로를 명시해줌)
SEEN_CSV = os.path.join("top-hospitals", "seen_posts.csv")


def _fingerprint_row(row):
    """링크 또는 (병원+제목+기간) 기반 해시 fingerprint 생성."""
    def _norm(s):
        return (str(s) or "").strip().lower()

    url = _norm(row.get("링크", ""))
    if url and len(url) > 5:
        base = f"url::{url}"
    else:
        base = f"{_norm(row.get('병원'))}|{_norm(row.get('제목'))}|{_norm(row.get('모집기간'))}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _load_seen():
    """이미 수집된 fingerprint 집합 로드."""
    if os.path.exists(SEEN_CSV):
        try:
            return set(pd.read_csv(SEEN_CSV)["fingerprint"].astype(str).tolist())
        except Exception:
            return set()
    return set()


def _save_seen(new_fps):
    """새로운 fingerprint들을 seen 파일에 병합 저장."""
    if not new_fps:
        return
    if os.path.exists(SEEN_CSV):
        old = pd.read_csv(SEEN_CSV)
        merged = pd.concat(
            [old, pd.DataFrame({"fingerprint": new_fps})], ignore_index=True
        )
        merged.drop_duplicates(subset=["fingerprint"], inplace=True)
        merged.to_csv(SEEN_CSV, index=False)
    else:
        pd.DataFrame({"fingerprint": new_fps}).to_csv(SEEN_CSV, index=False)


def save_workbook_consolidated(per_hospital, out_path):
    """통합 엑셀 저장 함수 (데이터 유실 방지 로직 보강)"""
    merged_parts = []
    
    for hosp, df in per_hospital.items():
        if df is not None and not df.empty:
            df2 = df.copy()
            
            # **[수정 1] 병원명 필드가 비어있더라도 강제로 할당**
            # 병원 필드가 없는 경우/모든 값이 비어있는 경우, 현재 병원 이름으로 채움
            if "병원" not in df2.columns or df2["병원"].eq("").all(): 
                df2["병원"] = hosp
                
            merged_parts.append(df2[["병원","제목","모집기간","링크"]])
    
    merged = pd.concat(merged_parts, ignore_index=True) if merged_parts else pd.DataFrame(columns=["병원","제목","모집기간","링크"])
    
    # **[수정 2] 내부 중복 제거 로직 비활성화 (데이터 유실 방지를 위해)**
    # 이 로직을 비활성화하고, _key 생성 로직을 제거하여 유실 가능성을 없앱니다.
    
    # 신규/기존 판별 로직은 그대로 유지합니다.
    seen = _load_seen()
    merged["fingerprint"] = merged.apply(_fingerprint_row, axis=1)
    # merged["_key"] = merged["제목"].astype(str) + merged["링크"].astype(str) # 삭제
    merged["상태"] = merged["fingerprint"].apply(lambda x: "기존" if x in seen else "신규")
    
    df_new = merged[merged["상태"]=="신규"].copy()
    
    # 신규 건 seen 업데이트
    if not df_new.empty:
        _save_seen(df_new["fingerprint"].tolist())

    # 엑셀 쓰기 (나머지 로직은 유지)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        # **[수정 3] 중복 제거 없이 전체 데이터를 통합전체 시트에 씁니다.**
        merged.drop_duplicates(subset=["fingerprint"], inplace=True) # 중복 제거 로직을 fingerprint 기반으로 변경 (안전함)
        merged.to_excel(w, index=False, sheet_name="통합전체") 
        df_new.to_excel(w, index=False, sheet_name="미등록_신규")
        
        # ... (요약 시트 및 병원별 시트 로직은 유지)
        if not merged.empty:
            merged.groupby("병원").size().reset_index(name="건수").sort_values("건수", ascending=False).to_excel(w, index=False, sheet_name="요약")

        for hosp, df in per_hospital.items():
            if df is not None and not df.empty:
                df.to_excel(w, index=False, sheet_name=safe_sheet(hosp))
                
    print(f"\n💾 엑셀 저장 완료: {out_path}")
    print(f"📊 전체: {len(merged)}건 | ✨ 신규: {len(df_new)}건")

# ==============================================================================
# [2] 크롤링 함수 (Refactored: drv 인자 사용, 자체 driver 생성 제거)
# ==============================================================================

# --- Group 1 ---
def run_seoul_asan(drv):
    hospital = "서울아산병원"
    url = "https://recruit.amc.seoul.kr/recruit/career/list.do?codeFirst=T04005&codeTwo=T04005002"
    rows = []
    drv.get(url); wait_ready(drv)
    items = drv.find_elements(By.CSS_SELECTOR, "ul.dayListBox > li")
    for item in items:
        try:
            title = item.find_element(By.CSS_SELECTOR, "div.dayListTitle span").text.strip()
            if "간호사" not in title: continue
            period = item.find_element(By.CSS_SELECTOR, "div.dayListTitle2 span").text.strip()
            onclick = item.find_element(By.CSS_SELECTOR, "div.dayListTitle a").get_attribute("onclick") or ""
            m2 = re.search(r"fnDetail\('(\d+)'", onclick)
            link = f"https://recruit.amc.seoul.kr/recruit/career/view.do?recruitNo={m2.group(1)}" if m2 else ""
            rows.append(std_row(hospital, title, period, link))
        except: continue
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
            link = a.get_attribute("href")  # 이미 절대경로

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
        # [핵심 수정] div.recruit_list 가 뜰 때까지 기다림 (ul 아님!)
        WebDriverWait(drv, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.recruit_list"))
        )
        
        # [핵심 수정] div.recruit_list 바로 밑에 있는 ul, 그 밑에 있는 li들을 다 찾음
        jobs = drv.find_elements(By.CSS_SELECTOR, "div.recruit_list ul li")
        
        for job in jobs:
            try:
                # 1. 제목 (recruit_tit 클래스 안의 a 태그)
                title_el = job.find_element(By.CSS_SELECTOR, ".recruit_tit a.inner")
                title = title_el.text.strip()
                
                # 2. 필터링 (간호사 키워드 없으면 패스)
                if "간호사" not in title: continue

                # 3. 상태 확인 (recruit_badge 안의 badge_ing 텍스트)
                status_el = job.find_element(By.CSS_SELECTOR, ".recruit_badge .badge_ing")
                if "접수중" not in status_el.text: continue
                
                # 4. 링크 및 기간 추출
                link = title_el.get_attribute("href")
                
                # [예외처리] 수시 채용은 기간(.day_txt)이 없을 수도 있음
                try:
                    period = job.find_element(By.CSS_SELECTOR, ".day_txt").text.strip()
                except:
                    period = "수시채용(공고참조)"

                rows.append(std_row(hospital, title, period, link))
                
            except Exception:
                # li 중에 빈 li나 양식이 다른 게 섞여 있을 경우 건너뜀
                continue
                
    except Exception as e:
        print(f"이화의료원 에러: {e}")
        
    return df_std(rows)

def run_gangneung_asan(drv):
    hospital = "강릉아산병원"
    url = "https://www.gnah.co.kr/kor/CMS/RecruitMgr/list.do?mCode=MN122"
    rows = []
    drv.get(url); wait_ready(drv)
    trs = drv.find_elements(By.CSS_SELECTOR, "table.board-list-table tbody tr")
    for tr in trs:
        try:
            title = tr.find_element(By.CSS_SELECTOR, "td.subject a").text.strip()
            status = tr.find_element(By.CSS_SELECTOR, "td.progress").text.strip()
            if "간호사" in title and "접수중" in status:
                link = tr.find_element(By.CSS_SELECTOR, "td.subject a").get_attribute("href")
                period = tr.find_element(By.CSS_SELECTOR, "td.period").text.strip()
                rows.append(std_row(hospital, title, period, urljoin(url, link)))
        except: continue
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

            title = tds[2].text.strip()      # ✅ 제목
            status = tds[4].text.strip()     # 모집중
            period = tds[3].text.strip()

            if "간호사" not in title:
                continue
            if "모집중" not in status:
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
    time.sleep(3) # 데이터 로딩 대기

    # 친구님이 지적해주신대로 ul 클래스명을 'job-boxs'로 수정했습니다!
    # 그 아래에 있는 모든 'job-box' 클래스를 가진 a 태그들을 찾습니다.
    boxes = drv.find_elements(By.CSS_SELECTOR, "ul.job-boxs a.job-box")
    print(f"[DEBUG] 전체 job-box 수: {len(boxes)}")

    for box in boxes:
        try:
            # 텍스트를 가장 확실하게 긁어오는 textContent 사용
            full_text = box.get_attribute("textContent").replace("\n", " ").strip()

            # 필터링 로직 (마감된 공고는 제외)
            if "마감" in full_text: 
                continue
            
            # '간호' 키워드가 들어있는지 확인
            if "간호사" not in full_text: 
                continue

            # 제목 추출: 이미지에서 확인된 대로 p.txt18 중 style="height:20%" 속성을 가진 것
            try:
                title_el = box.find_element(By.CSS_SELECTOR, "p.txt18[style*='height']")
                title = title_el.get_attribute("textContent").strip()
            except:
                # 위 방법이 안될 경우를 대비한 백업 (p.txt18 중 '간호' 포함된 텍스트 찾기)
                p_tags = box.find_elements(By.CSS_SELECTOR, "p.txt18")
                title = "제목 없음"
                for p in p_tags:
                    p_txt = p.get_attribute("textContent").strip()
                    if "간호사" in p_txt:
                        title = p_txt
                        break

            # 기간 추출 (flex3 내부의 p.blue)
            period = ""
            try:
                period = box.find_element(By.CSS_SELECTOR, "div.flex3 p.blue").get_attribute("textContent").strip()
            except:
                period = "공고문 참조"

            link = box.get_attribute("href")
            
            rows.append(std_row(hospital, title, period, link))
            print(f"[DEBUG] 수집 성공: {title}")

        except Exception as e:
            continue

    return df_std(rows)


def run_khmc(drv):
    hospital = "경희의료원"
    url = "https://recruit.incruit.com/khmc/job/"
    rows = []
    drv.get(url); wait_ready(drv)
    items = drv.find_elements(By.CSS_SELECTOR, "div.list-item-box li")
    for it in items:
        try:
            title = it.find_element(By.CSS_SELECTOR, "span.title").text.strip()
            if "간호사" not in title: continue
            if "마감" in it.find_element(By.CSS_SELECTOR, "span.state").text: continue
            period = it.find_element(By.CSS_SELECTOR, "em.date").text.strip()
            link = it.find_element(By.CSS_SELECTOR, "a.btn").get_attribute("href")
            rows.append(std_row(hospital, title, period, link))
        except: continue
    return df_std(rows)

def run_kuh(drv):
    hospital = "건국대학교병원"
    url = "https://www.kuh.ac.kr/recruit/apply/noticeList.do"
    rows = []
    drv.get(url); wait_ready(drv)
    links = drv.find_elements(By.CSS_SELECTOR, "td.alignL a")
    for a in links:
        try:
            title = a.text.strip()
            if "간호사" not in title: continue
            
            link = a.get_attribute("href")
            
            # **수정된 기간 추출 로직 반영**
            period_td = a.find_element(By.XPATH, "../following-sibling::td[2]")
            period = period_td.text.strip()
            
            rows.append(std_row(hospital, title, period, link))
        except: continue
    return df_std(rows)

def run_smc_changwon(drv):
    hospital = "삼성창원병원"
    url = "https://smc.skku.edu/recruit/recruit/recruitInfo/list.do?mId=42&schPosition=C1N"
    rows = []
    drv.get(url); wait_ready(drv)
    trs = drv.find_elements(By.CSS_SELECTOR, "table tbody tr")
    for tr in trs:
        try:
            if "지원하기" not in tr.find_element(By.CSS_SELECTOR, "td.state").text: continue
            a = tr.find_element(By.CSS_SELECTOR, "td.title a")
            title = a.text.strip()
            onclick = a.get_attribute("onclick")
            idx = re.search(r"fn_goDtl\('(\d+)'", onclick).group(1)
            link = f"https://smc.skku.edu/recruit/recruit/recruitInfo/view.do?mId=42&idx={idx}"
            period = tr.find_element(By.CSS_SELECTOR, "td.date").text.strip()
            rows.append(std_row(hospital, title, period, link))
        except: continue
    return df_std(rows)

def run_yuhs(drv):
    hospital = "연세대학교의료원"
    url = "https://yuhs.recruiter.co.kr/app/jobnotice/list"
    rows = []
    drv.get(url); wait_ready(drv)
    lis = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in lis:
        try:
            title = li.find_element(By.CSS_SELECTOR, "h2.list-bbs-title a").text.strip()
            status = li.find_element(By.CSS_SELECTOR, "div.list-bbs-status").text
            if "간호사" in title and "접수중" in status:
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text
                link = li.find_element(By.CSS_SELECTOR, "h2.list-bbs-title a").get_attribute("href")
                rows.append(std_row(hospital, title, period, urljoin(url, link)))
        except: continue
    return df_std(rows)

# --- Group 2 (site01 ~ site10) ---
def site01(drv):
    name = "동아대병원"
    url = "https://www.damc.or.kr/05/03_2017.php"
    rows = []
    drv.get(url); wait_ready(drv)
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
        except: continue
    return df_std(rows)

def site02(drv):
    name = "삼성서울병원"
    url = "https://www.samsunghospital.com/home/recruit/recruitInfo/recruitNotice.do"
    rows = []
    drv.get(url); wait_ready(drv)
    trs = drv.find_elements(By.CSS_SELECTOR, "table.board-list tbody tr")
    for tr in trs:
        try:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) >= 7:
                if "간호사" in tds[2].text:
                    a = tds[3].find_element(By.TAG_NAME, "a")
                    rows.append(std_row(name, safe_str(a.text), safe_str(tds[4].text), a.get_attribute("href")))
        except: continue
    return df_std(rows)

def site03(drv):
    name = "인제대학교부속백병원(부산)"
    url = "https://www.paik.ac.kr/busan/user/job/list.do?menuNo=900101"
    rows = []
    drv.get(url); wait_ready(drv)
    trs = drv.find_elements(By.CSS_SELECTOR, "table tbody tr")
    for tr in trs:
        try:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) < 2: continue
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
        except: continue
    return df_std(rows)

def site04(drv):
    name = "가천대길병원"
    base = "https://gilhospital.recruiter.co.kr"
    url = "https://gilhospital.recruiter.co.kr/app/jobnotice/list"
    rows = []
    drv.get(url); wait_ready(drv)
    items = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList li")
    for li in items:
        try:
            status = ""
            try: status = li.find_element(By.CSS_SELECTOR, "span.list-bbs-status").text
            except: 
                try: status = li.find_element(By.CSS_SELECTOR, "div.list-bbs-status span").text
                except: pass
            if "접수중" not in status: continue
            
            a = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a.text)
            if "간호사" in title:
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text
                rows.append(std_row(name, title, period, urljoin(base, a.get_attribute("href"))))
        except: continue
    return df_std(rows)

def site05(drv):
    name = "아주대학교의료원"
    base = "https://ajoumc.recruiter.co.kr"; url = "https://ajoumc.recruiter.co.kr/app/jobnotice/list"
    rows = []
    drv.get(url); wait_ready(drv)
    items = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList li")
    for li in items:
        try:
            status = li.find_element(By.CSS_SELECTOR, ".list-bbs-status .text-label").text
            if "접수중" not in status: continue
            a = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a.get_attribute("textContent"))
            if "간호사" in title:
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").get_attribute("textContent")
                rows.append(std_row(name, title, period, urljoin(base, a.get_attribute("href"))))
        except: continue
    return df_std(rows)

def site06(drv):
    name = "한양대학교병원"
    base = "https://hyumc.recruiter.co.kr"
    url = f"{base}/career/home"
    rows = []

    drv.get(url)
    wait_ready(drv)

    cards = drv.find_elements(By.CSS_SELECTOR, "a[class*='RecruitList_list-item']")
    print(f"[한양대] 카드 수: {len(cards)}")

    for card in cards:
        try:
            # 1️⃣ 상태: class 난수 피해서 텍스트 기준으로 처리
            try:
                status_elem = card.find_element(By.CSS_SELECTOR, "span[class*='RecruitList_submission-status-tag']")
                status = status_elem.text.strip()
            except:
                try:
                    status_elem = card.find_element(By.XPATH, ".//span[contains(.,'접수중')]")
                    status = status_elem.text.strip()
                except:
                    continue

            if "접수중" not in status:
                continue

            # 2️⃣ 제목
            title_elem = card.find_element(By.CSS_SELECTOR, "[class*='RecruitList_title']")
            title = title_elem.text.strip()
            if "간호" not in title:  # '간호사', '간호직', '간호국' 등 다 포함
                continue

            # 3️⃣ 모집기간
            try:
                date_elem = card.find_element(By.CSS_SELECTOR, "div[class*='RecruitList_date']")
                date = date_elem.text.strip()
            except:
                date = ""

            # 4️⃣ 링크
            href = card.get_attribute("href")
            rows.append(std_row(name, title, date, urljoin(base, href)))

        except Exception as e:
            print("[한양대] 카드 처리 에러:", e)
            continue

    return df_std(rows)

def site07(drv):
    name = "단국대학교병원"
    URL = "https://www.dkuh.co.kr/board5/bbs/board?bo_table=01_03_05"
    rows = []
    drv.get(URL); wait_ready(drv)
    trs = drv.find_elements(By.CSS_SELECTOR, "form#fboardlist table tbody tr")
    for tr in trs:
        try:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 5: continue
            status = tds[4].text
            if not any(k in status for k in ["진행","모집중","접수중"]): continue
            a = tr.find_element(By.CSS_SELECTOR, "td.td_subject a")
            title = safe_str(a.get_attribute("innerText"))
            if "간호사" in title:
                rows.append(std_row(name, title, tds[3].text, a.get_attribute("href")))
        except: continue
    return df_std(rows)

def site08(drv):
    name = "대구가톨릭대의료원"
    URL = "https://www.dcmc.co.kr/content/07community/01_05.asp"
    rows = []
    drv.get(URL); wait_ready(drv)
    trs = drv.find_elements(By.CSS_SELECTOR, "table tbody tr")
    for tr in trs:
        try:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 5: continue
            if "모집완료" in tds[4].text: continue
            a = tr.find_element(By.CSS_SELECTOR, "td.title a")
            title = safe_str(a.get_attribute("innerText"))
            if "간호사" in title:
                rows.append(std_row(name, title, tds[3].text, urljoin(URL, a.get_attribute("href"))))
        except: continue
    return df_std(rows)

def site09(drv):
    name = "조선대병원"
    BASE = "https://hosp.chosun.ac.kr"; URL = "https://hosp.chosun.ac.kr/bbs/?b_id=recruit&site=hospital&mn=211"
    rows = []
    drv.get(URL); wait_ready(drv)
    try:
        # '진행'이라는 텍스트가 포함된 <tr>을 XPath로 찾음
        rows_el = drv.find_elements(By.XPATH,"//tbody//tr[td[contains(., '진행')]]") 
        for tr in rows_el:
            try:
                # 1. 제목 및 링크 추출
                a = tr.find_element(By.XPATH, ".//td[contains(@class,'title')]//a")
                title = safe_str(a.get_attribute("innerText"))
                if "간호사" not in title: continue
                
                # 2. 기간 정보 추출
                # 기간 텍스트가 a 태그의 부모인 td.title 안에 있으므로,
                # td.title 요소를 찾아서 그 전체 텍스트를 사용합니다.
                title_td = tr.find_element(By.XPATH, ".//td[contains(@class,'title')]")
                
                # 텍스트 전체를 가져와서 줄바꿈, 공백 등을 정리
                raw_text = title_td.get_attribute("innerText")
                
                # 제목 텍스트를 제거하여 기간 정보만 남김
                # (제목 텍스트는 간혹 td 텍스트에 섞여 들어오므로 제거하여 기간만 추출)
                period_raw = raw_text.replace(title, "").strip()
                
                # 줄바꿈 및 연속 공백 정리 (엑셀에 깔끔하게)
                import re
                period = re.sub(r'\s+', ' ', period_raw).strip()
                
                # 3. 데이터 저장
                rows.append(std_row(name, title, period, urljoin(BASE, a.get_attribute("href"))))
            except: continue
    except: pass
    return df_std(rows)

def site10(drv):
    name = "한림의료원"
    URL = "https://recruit.hallym.or.kr/index.jsp?inggbn=ing&movePage=1"
    rows = []
    drv.get(URL); wait_ready(drv)
    cards = drv.find_elements(By.CSS_SELECTOR, "div.main_rctbox a[href]")
    for card in cards:
        try:
            # 1. 제목(Title) 추출
            try: title_el = card.find_element(By.CSS_SELECTOR, "ul.data_title li:last-child")
            except: title_el = card.find_element(By.CSS_SELECTOR, "ul.data_title li")
            title = safe_str(title_el.text)
            
            # 간호사 필터링은 그대로 유지
            if "간호사" not in title: continue
            
            # 2. 상태 텍스트 (마감일/D-Day 정보) 추출
            # ul.data_day 안의 모든 텍스트를 가져와서 마감일을 포함하는 텍스트를 찾습니다.
            status_text_el = card.find_element(By.CSS_SELECTOR, "ul.data_day")
            status_text = status_text_el.text.strip()
            
            # 3. 마감된 공고 필터링 (기존 로직 유지)
            # D-DAY 또는 마감일이 명시된 텍스트가 아닌, '마감'만 있는 경우 제외
            if "마감" in status_text and "마감일" not in status_text: continue
            
            # 4. '마감일' 텍스트 추출 및 정제
            # 추출된 status_text에서 실제 마감일 날짜(YYYY.MM.DD) 또는 D-DAY 정보를 추출합니다.
            # 예시: "마감일 2025.12.31 D-29" -> "마감일 2025.12.31 D-29" 전체를 deadline으로 사용
            deadline = status_text

            # 5. 행(Row)에 마감일 정보 추가
            # std_row(name, title, deadline, link) 형태로 전달 (세 번째 인자가 마감일로 가정)
            rows.append(std_row(name, title, deadline, urljoin(URL, card.get_attribute("href"))))
        
        except Exception as e: 
            # 에러 발생 시 로그를 남겨 디버깅에 도움을 줄 수 있습니다.
            # print(f"Error processing card: {e}") 
            continue
            
    return df_std(rows)

# --- Group 3 (site11 ~ site20) ---
def site11(drv):
    name = "서울성모병원"
    HOME_URL = "https://recruit.cmcnu.or.kr/cmcseoul/index.do"
    rows = []
    
    drv.get(HOME_URL)
    
    try:
        # [핵심] 'on' 클래스는 변할 수 있으니 제외하고, 
        # 'list-type01'과 'recruit' 클래스가 있는 div를 찾습니다.
        WebDriverWait(drv, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.list-type01.recruit ul.items"))
        )
        
        # 목록 가져오기 (div.list-type01.recruit -> ul.items -> li)
        cards = drv.find_elements(By.CSS_SELECTOR, "div.list-type01.recruit ul.items li")
        
        for card in cards:
            try:
                # 1. 제목 (div.tit > strong)
                title_el = card.find_element(By.CSS_SELECTOR, ".tit strong")
                title = title_el.text.strip()
                
                # 2. 필터링 (간호사 OR 간호직)
                if ("간호사" in title) or ("간호직" in title):
                    
                    # 3. 날짜 (div.info_wrap > em.data)
                    period = card.find_element(By.CSS_SELECTOR, ".info_wrap .data").text.strip()
                    
                    # 4. 링크 (li 바로 밑 a태그)
                    a_tag = card.find_element(By.TAG_NAME, "a")
                    link = a_tag.get_attribute("href")
                    
                    rows.append(std_row(name, title, period, link))
            except Exception:
                continue
                
    except Exception as e:
        print(f"서울성모병원 에러: {e}")
        
    return df_std(rows)

def site12(drv):
    name = "경북대병원"
    URL = "https://www.knuh.kr/content/04information/02_01.asp#close"; BASE = "https://www.knuh.kr"
    rows = []
    drv.get(URL); wait_ready(drv)
    trs = drv.find_elements(By.CSS_SELECTOR, "div#board table tbody tr")
    for tr in trs:
        try:
            if "진행" not in tr.text: continue
            a = tr.find_element(By.CSS_SELECTOR, "td.title a")
            title = safe_str(a.get_attribute("innerText"))
            if "간호사" in title:
                period = tr.find_elements(By.CSS_SELECTOR, "td")[-1].text
                rows.append(std_row(name, title, period, urljoin(BASE, a.get_attribute("href"))))
        except: continue
    return df_std(rows)

def site13(drv):
    name = "경상대병원"
    URL = "https://www.gnuh.co.kr/gnuh/board/list.do?rbsIdx=109"
    rows = []
    drv.get(URL); wait_ready(drv)
    trs = drv.find_elements(By.CSS_SELECTOR, "table tbody tr")
    for tr in trs:
        try:
            a = tr.find_element(By.CSS_SELECTOR, "td.tt a")
            title = safe_str(a.get_attribute("innerText"))
            if "간호사" in title:
                # td.date는 작성일로 추정되지만, 마감일 정보가 목록에 없으므로 작성일을 대신 저장
                posted = tr.find_element(By.CSS_SELECTOR, "td.date").text
                
                # std_row의 period 인수에 posted(작성일) 저장
                rows.append(std_row(name, title, posted, urljoin(drv.current_url, a.get_attribute("href"))))
        except: continue
    return df_std(rows)

def site14(drv):
    name = "동산의료원"
    URL = "https://dongsan.dsmc.or.kr:49870/content/03intro/04_01.php"
    rows = []
    drv.get(URL)
    wait_ready(drv)
    
    # tr 목록 가져오기
    trs = drv.find_elements(By.CSS_SELECTOR, "div.mscroll table.table1.board tbody tr")
    
    for tr in trs:
        try:
            # [1. 라벨 체크] regular 혹은 ing 클래스가 있는지 확인
            # CSS 선택자의 콤마(,)는 OR 조건을 의미합니다.
            labels = tr.find_elements(By.CSS_SELECTOR, "span.label.regular, span.label.ing")
            if not labels:
                continue # 두 라벨 모두 없으면 접수 중이 아니라고 판단하고 스킵
            
            # [2. 제목 및 링크 추출]
            a_tag = tr.find_element(By.CSS_SELECTOR, "td.title a, td.notice.title a")
            full_text = a_tag.get_attribute("innerText").strip()
            
            # [3. 키워드 필터링]
            # "계약직"만 써도 "계약직원"까지 다 포함됩니다.
            if not ("간호사" in full_text or "계약직" in full_text):
                continue
                
            # 라벨 텍스트(상시모집 등)를 제목에서 제거하여 깨끗한 제목 만들기
            title = full_text
            for lb in labels:
                lb_text = lb.get_attribute("innerText").strip()
                title = title.replace(lb_text, "").strip()

            # [4. 등록일 추출]
            period = ""
            try:
                period = tr.find_element(By.CSS_SELECTOR, "span.read").text.strip()
            except:
                tds = tr.find_elements(By.TAG_NAME, "td")
                if len(tds) > 3:
                    period = tds[3].text.strip()
            
            rows.append(std_row(name, title, period, urljoin(URL, a_tag.get_attribute("href"))))
            
        except Exception as e:
            continue
            
    return df_std(rows)

def site15(drv):
    name = "고려대의료원"
    HOME_URL = "https://kumc.recruiter.co.kr/career/home"
    rows = []

    drv.get(HOME_URL)
    
    try:
        # [핵심 1] 리스트가 뜰 때까지 대기
        # 클래스명이 복잡하므로 앞부분 'RecruitList_list-item'이 포함된 a태그를 기다립니다.
        WebDriverWait(drv, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[class*='RecruitList_list-item']"))
        )
        
        # [핵심 2] 공고 아이템들 가져오기 (li > a 구조)
        # 클래스명에 'RecruitList_list-item'이 포함된 모든 a 태그 수집
        cards = drv.find_elements(By.CSS_SELECTOR, "a[class*='RecruitList_list-item']")

        for card in cards:
            try:
                # 1. 상태 확인 (Tag_tag... 클래스)
                # 스크린샷의 '접수중' 배지는 span 태그에 있습니다.
                status_el = card.find_element(By.CSS_SELECTOR, "span[class*='Tag_tag']")
                status = status_el.text.strip()
                
                # 접수중이 아니면 패스
                if "접수중" not in status:
                    continue

                # 2. 제목 확인 (RecruitList_title... 클래스)
                title_el = card.find_element(By.CSS_SELECTOR, "p[class*='RecruitList_title']")
                title = title_el.text.strip()

                # '간호사' 키워드 체크
                if "간호사" not in title:
                    continue

                # 3. 기간 추출 (RecruitList_date... 클래스)
                # 날짜가 두 줄(시작일, 종료일)로 되어 있으니 전체 텍스트를 가져와서 줄바꿈을 ~로 치환
                period_el = card.find_element(By.CSS_SELECTOR, "div[class*='RecruitList_date']")
                period = period_el.text.strip().replace("\n", " ~ ")

                # 4. 링크 추출 (a태그의 href)
                link = card.get_attribute("href")

                rows.append(std_row(name, title, period, link))

            except Exception:
                continue

    except Exception as e:
        print(f"고려대의료원 에러: {e}")

    return df_std(rows)

def site16(drv):
    name = "영남대학교병원"
    LIST_URL = "https://yumc.ac.kr:8443/bbs/List.do?bbsId=news5"
    BASE_URL = "https://yumc.ac.kr:8443"
    rows = []
    seen = set()  # 🔹 제목+작성일 기준 로컬 중복 체크

    try:
        drv.get(LIST_URL)
        wait_ready(drv)
        time.sleep(2)

        try:
            table = drv.find_element(By.CSS_SELECTOR, "table.table_yumc_table")
        except:
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

                # 상태 필터: 마감 제외
                if "마감" in status_text:
                    continue

                # 🔹 공고 고정 + 일반 중복 제거 (제목+작성일 동일하면 스킵)
                key = (title, written_date)
                if key in seen:
                    continue
                seen.add(key)

                link = urljoin(BASE_URL, a.get_attribute("href"))
                rows.append(std_row(name, title, written_date, link))
            except Exception:
                continue

        print(f"✅ 영남대학교병원 수집 완료: {len(rows)}건")
        return df_std(rows)

    except Exception as e:
        print(f"🔴 영남대학교병원 실행 오류: {e.__class__.__name__}")
        return df_std(rows)

def site17(drv):
    name = "원광대학교병원"
    LIST_URL = "https://www.wkuh.org/recruit/jobs/recruit_notice.do?recruit_type=list&sh_rc_type=validity"
    rows = []
    drv.get(LIST_URL); wait_ready(drv)
    items = drv.find_elements(By.CSS_SELECTOR, "div.recruit_list_bbs ul > li")
    for li in items:
        try:
            # 1. 상태 필터링 (기존 로직 유지)
            status = li.find_element(By.CSS_SELECTOR, ".list_bbs_status input.status_btn").get_attribute("value")
            if "지원가능" not in status: continue
            
            # 2. 제목/링크 추출
            a = li.find_element(By.CSS_SELECTOR, ".list_bbs_title p a")
            title = safe_str(a.text)
            if "간호사" not in title: continue

            # 3. **기간 추출 로직 추가 (수정 부분)**
            period = ""
            try:
                # .list_bbs_title 내에서 dday가 아닌 첫 번째 span을 찾아 기간으로 사용
                period = li.find_element(By.CSS_SELECTOR, ".list_bbs_title span:not(.dday)").text.strip()
            except:
                # 기간 정보가 없으면 빈 문자열("") 유지
                pass
            
            # 4. 데이터 저장
            rows.append(std_row(name, title, period, urljoin(drv.current_url, a.get_attribute("href"))))
        except: continue
    return df_std(rows)

def site18(drv):
    name = "전북대학교병원"
    LIST_URL = "https://jbuh.recruiter.co.kr/app/jobnotice/list"; BASE_URL = "https://jbuh.recruiter.co.kr"
    rows = []
    drv.get(LIST_URL); wait_ready(drv)
    items = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in items:
        try:
            status = li.find_element(By.CSS_SELECTOR, ".list-bbs-status .text-label").text
            if "접수중" not in status: continue
            a = li.find_element(By.CSS_SELECTOR, ".list-bbs-title a")
            title = safe_str(a.text)
            if "간호사" in title:
                period = li.find_element(By.CSS_SELECTOR, ".list-bbs-date").text
                rows.append(std_row(name, title, period, urljoin(BASE_URL, a.get_attribute("href"))))
        except: continue
    return df_std(rows)

def site19(drv):
    name = "양산부산대학교병원"
    LIST_URL = "https://pnuyh.recruiter.co.kr/career/home"; BASE_URL = "https://pnuyh.recruiter.co.kr"
    rows = []
    drv.get(LIST_URL); wait_ready(drv)
    cards = drv.find_elements(By.CSS_SELECTOR, "a[href*='/career/jobs/']")
    for a in cards:
        try:
            status = a.find_element(By.CSS_SELECTOR, "span[class*='RecruitList_submission-status']").text
            if "접수중" not in status: continue
            title = a.find_element(By.CSS_SELECTOR, "p[class*='RecruitList_title']").text
            if "간호사" in title:
                period = a.find_element(By.CSS_SELECTOR, "div[class*='RecruitList_date']").text
                rows.append(std_row(name, title, period, urljoin(BASE_URL, a.get_attribute("href"))))
        except: continue
    return df_std(rows)

def site20(drv):
    name = "칠곡경북대학교병원"
    LIST_URL = "https://knuh.recruiter.co.kr/app/jobnotice/list"; BASE_URL = "https://knuh.recruiter.co.kr"
    rows = []
    drv.get(LIST_URL); wait_ready(drv)
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in cards:
        try:
            status = li.find_element(By.CSS_SELECTOR, ".list-bbs-status .text-label").text
            if "접수중" not in status: continue
            a = li.find_element(By.CSS_SELECTOR, ".list-bbs-title a")
            title = safe_str(a.text)
            if "간호사" in title:
                period = li.find_element(By.CSS_SELECTOR, ".list-bbs-date").text
                rows.append(std_row(name, title, period, urljoin(BASE_URL, a.get_attribute("href"))))
        except: continue
    return df_std(rows)

# --- Group 4 (site21 ~ site32) ---
def site21(drv):
    name = "순천향대부천병원"
    # 부천병원 채용 목록 페이지
    LIST_URL = "https://jobapplication.schmc.ac.kr/recruit/biz/job/recruiteList"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv) # 별도로 정의된 대기 함수
    
    try:
        # 이미지 확인 결과 id가 tabs4인 영역 내부의 li 항목들을 가져옴
        cards = drv.find_elements(By.CSS_SELECTOR, "#tabs4 .list_box.clearfix li")
        
        for li in cards:
            try:
                # 1. 지원 가능 여부 확인 (신규지원 버튼 존재 확인)
                btn = li.find_element(By.CSS_SELECTOR, ".list_button .btn01")
                if "신규지원" not in btn.text: 
                    continue
                
                # 2. 제목 추출 (p.l_title.tops 내부의 a 태그 텍스트)
                title_el = li.find_element(By.CSS_SELECTOR, "p.l_title.tops a")
                title = safe_str(title_el.text).strip()
                
                # '간호사' 키워드 필터링 (필요 없으면 제거 가능)
                if "간호사" in title:
                    # 3. 기간 추출 (span.d_right)
                    period_el = li.find_element(By.CSS_SELECTOR, "span.d_right")
                    period = period_el.text.strip()
                    
                    # 4. 결과 저장
                    rows.append(std_row(name, title, period, LIST_URL))
                    
            except Exception as e:
                # 개별 카드 파싱 실패 시 건너뜀
                continue
    except Exception as e:
        print(f"Error logic: {e}")
        pass
        
    return df_std(rows)

def site22(drv):
    name = "건양대학교의료원"
    LIST_URL = "https://www.kyuh.ac.kr/prog/recruitNotice/list.do?lyMcd=sub01_01"
    rows = []
    drv.get(LIST_URL); wait_ready(drv)
    cards = drv.find_elements(By.CSS_SELECTOR, "div.program-skin_recruit ul > li")
    for li in cards:
        try:
            if "공고중" not in li.text: continue
            title = li.find_element(By.CSS_SELECTOR, "strong.job").text
            if "간호사" in title:
                period = li.find_element(By.CSS_SELECTOR, "em.period").text
                rows.append(std_row(name, title, period, LIST_URL))
        except: continue
    return df_std(rows)

def site23(drv):
    name = "부산대학교병원"
    LIST_URL = "https://pnuh.recruiter.co.kr/app/jobnotice/list"
    BASE = "https://pnuh.recruiter.co.kr"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    # 목록 1페이지의 모든 리스트 아이템 추출
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    
    for li in cards:
        try:
            # 1. 상태 텍스트 확인 (이미지상 '접수마감', 'D-324' 등이 표시되는 위치)
            status_text = li.find_element(By.CSS_SELECTOR, ".list-bbs-status").text
            
            # '접수마감'인 항목은 수집하지 않고 건너뜀
            if "접수마감" in status_text:
                continue
            
            # 2. 공고 제목 추출
            a = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a.text).strip()
            
            # 3. 키워드 필터링 (간호사 또는 간호직이 포함된 경우만)
            if any(keyword in title for keyword in ["간호사", "간호직"]):
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text.strip()
                link = urljoin(BASE, a.get_attribute("href"))
                
                rows.append(std_row(name, title, period, link))
                
        except Exception:
            # 개별 공고 처리 중 오류 발생 시 다음 항목으로 진행
            continue
            
    return df_std(rows)

def site24(drv):
    name = "서울대학교병원"
    LIST_URL = "https://recruit.snuh.org/main.do"
    BASE = "https://recruit.snuh.org"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    # 공고 목록 아이템 추출
    cards = drv.find_elements(By.CSS_SELECTOR, "ul.posting > li")
    
    for li in cards:
        try:
            # 1. 상태 필터링 (마감된 공고 제외)
            # 이미지 주석에 따르면 status02: 진행중, status03: 마감 입니다.
            status_div = li.find_element(By.CSS_SELECTOR, "div[class^='status']")
            status_class = status_div.get_attribute("class")
            status_text = status_div.text.strip()
            
            # '마감'이거나 클래스가 'status03'인 경우 건너뜀
            if "status03" in status_class or "마감" in status_text:
                continue

            # 2. 제목 추출 및 키워드 필터링 (간호직 OR 블라인드)
            a_tag = li.find_element(By.CSS_SELECTOR, "a")
            title = safe_str(a_tag.text).strip()
            
            # 요청하신 대로 '간호직' 혹은 '블라인드' 키워드 포함 여부 확인
            if any(keyword in title for keyword in ["간호직", "블라인드"]):
                
                # 3. 기간 추출 (목록에서 바로 가져오기)
                # 이미지상 <a> 태그 바로 아래 <span> 태그에 날짜가 있습니다.
                try:
                    period = li.find_element(By.CSS_SELECTOR, "span").text.strip()
                except:
                    period = "기간 정보 없음"
                
                # 4. 링크 생성
                link = urljoin(BASE, a_tag.get_attribute("href"))
                
                rows.append(std_row(name, title, period, link))
                
        except Exception:
            continue
            
    return df_std(rows)

def site25(drv):
    name = "원주연세의료원"
    LIST_URL = "https://ywmc.recruiter.co.kr/app/jobnotice/list"; BASE = "https://ywmc.recruiter.co.kr"
    rows = []
    drv.get(LIST_URL); wait_ready(drv)
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in cards:
        try:
            if "접수중" not in li.find_element(By.CSS_SELECTOR, ".list-bbs-status").text: continue
            a = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a.text)
            if "간호사" in title:
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text
                rows.append(std_row(name, title, period, urljoin(BASE, a.get_attribute("href"))))
        except: continue
    return df_std(rows)

def site26(drv):
    name = "전남대학교병원"
    LIST_URL = "https://cnuh.recruiter.co.kr/app/jobnotice/list"
    BASE = "https://cnuh.recruiter.co.kr"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    # 1. 공고 리스트 아이템 선택
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    
    for li in cards:
        try:
            # 2. 상태 확인 (이미지상 '접수마감'이 보임)
            status_text = li.find_element(By.CSS_SELECTOR, ".list-bbs-status").text.strip()
            
            # '접수마감'이 포함되어 있으면 건너뜁니다.
            if "접수마감" in status_text:
                continue
            
            # 3. 제목 추출 및 다중 키워드 필터링
            a_tag = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a_tag.text).strip()
            
            # 간호직, 지원직, 대체근로자 중 하나라도 포함되면 수집
            keywords = ["간호직", "지원직", "대체근로자"]
            if any(k in title for k in keywords):
                
                # 4. 기간 추출 및 링크 생성
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text.strip()
                link = urljoin(BASE, a_tag.get_attribute("href"))
                
                rows.append(std_row(name, title, period, link))
                
        except Exception:
            continue
            
    return df_std(rows)

def site27(drv):
    name = "충남대학교병원"
    LIST_URL = "https://cnuhinsa.recruiter.co.kr/career/apply"
    BASE = "https://cnuhinsa.recruiter.co.kr"
    rows = []

    drv.get(LIST_URL)
    wait_ready(drv)

    # 카드 수집
    cards = drv.find_elements(By.CSS_SELECTOR, "[class*='RecruitList_list-item']")
    print(f"[충남대] 카드 수: {len(cards)}")

    for a in cards:
        try:
            # 상태 (submission-status-tag 기준)
            try:
                status_elem = a.find_element(By.CSS_SELECTOR, "span[class*='RecruitList_submission-status-tag']")
                status = status_elem.text.strip()
            except:
                try:
                    status_elem = a.find_element(By.XPATH, ".//span[contains(.,'접수중')]")
                    status = status_elem.text.strip()
                except:
                    continue

            if "접수중" not in status:
                continue

            # 제목
            title = a.find_element(By.CSS_SELECTOR, "[class*='RecruitList_title']").text.strip()
            if "간호" not in title:
                continue

            # 모집기간
            try:
                period = a.find_element(By.CSS_SELECTOR, "div[class*='RecruitList_date']").text.strip()
            except:
                period = ""

            href = a.get_attribute("href")
            rows.append(std_row(name, title, period, urljoin(BASE, href)))

        except Exception as e:
            print("[충남대] 카드 처리 에러:", e)
            continue

    return df_std(rows)

def site28(drv):
    name = "충북대학교병원"
    LIST_URL = "https://cbnuh.recruiter.co.kr/app/jobnotice/list"
    BASE = "https://cbnuh.recruiter.co.kr"
    rows = []
    
    drv.get(LIST_URL)
    wait_ready(drv)
    
    # 1. 공고 리스트 아이템 추출
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    
    for li in cards:
        try:
            # 2. 상태 확인 (이미지상 '접수중' 주황색 버튼 영역)
            status_text = li.find_element(By.CSS_SELECTOR, ".list-bbs-status").text.strip()
            
            # '접수마감'이거나 '결과발표' 등 종료된 상태는 제외 (이미지상 '접수중' 위주로 체크)
            if "접수마감" in status_text:
                continue
            
            # 3. 제목 추출 및 다중 키워드 필터링 (간호사 OR 기간제근무자)
            a_tag = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a_tag.text).strip()
            
            # 요청하신 두 가지 키워드 체크
            keywords = ["간호사", "기간제근무자"]
            if any(k in title for k in keywords):
                
                # 4. 기간 추출 및 링크 생성
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text.strip()
                link = urljoin(BASE, a_tag.get_attribute("href"))
                
                rows.append(std_row(name, title, period, link))
                
        except Exception:
            continue
            
    return df_std(rows)

def site29(drv):
    name = "울산대학교병원"
    # URL 수정 (입사지원 페이지)
    HOME_URL = "https://recruit.uuh.ulsan.kr:8443/uuhrecruit/#!%EC%9E%85%EC%82%AC%EC%A7%80%EC%9B%90"
    rows = []
    
    drv.get(HOME_URL)
    
    try:
        # [핵심] 테이블의 행(tr)이 그려질 때까지 최대 20초 대기
        # Vaadin 사이트는 로딩이 느리므로 넉넉히 기다려야 합니다.
        WebDriverWait(drv, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr.v-table-row"))
        )
        
        # 행(tr) 가져오기: 홀수행(odd)과 짝수행(row) 모두 포함
        # table.v-table-table 안에 tr들이 있습니다.
        cards = drv.find_elements(By.CSS_SELECTOR, "tr.v-table-row, tr.v-table-row-odd")
        
        for card in cards:
            try:
                # 1. 텍스트 전체 확보 (가장 안전한 방법)
                text_all = card.text
                
                # 2. '마감' 필터링
                if "마감" in text_all:
                    continue
                
                # 3. 제목 추출 (span.v-button-caption)
                try:
                    title_el = card.find_element(By.CSS_SELECTOR, ".v-button-caption")
                    title = title_el.text.strip()
                except:
                    # 제목 요소를 못 찾으면 줄바꿈 기준 첫 줄 사용
                    title = text_all.split('\n')[0]

                # 4. 키워드 필터 (간호사)
                if "간호사" not in title:
                    continue
                
                # 5. 기간 추출
                # "지원기간 : 2026..." 형식의 텍스트를 찾습니다.
                period = ""
                lines = text_all.split('\n')
                for line in lines:
                    if "지원기간" in line:
                        period = line.replace("지원기간", "").replace(":", "").strip()
                        break
                
                # 링크는 리스트 페이지 고정 (상세 주소가 동적이므로)
                link = HOME_URL
                
                rows.append(std_row(name, title, period, link))
                
            except Exception:
                continue
                
    except Exception as e:
        print(f"울산대학교병원 에러: {e}")
        
    return df_std(rows)

def site30(drv):
    name = "분당서울대학교병원(간호직)"
    LIST_URL = "https://snubh.recruiter.co.kr/app/jobnotice/list"; BASE = "https://snubh.recruiter.co.kr"
    rows = []
    drv.get(LIST_URL); wait_ready(drv)
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in cards:
        try:
            if "접수중" not in li.find_element(By.CSS_SELECTOR, ".list-bbs-status").text: continue
            a = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a.text)
            if "간호직" in title:
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text
                rows.append(std_row(name, title, period, urljoin(BASE, a.get_attribute("href"))))
        except: continue
    return df_std(rows)

def site31(drv):
    name = "고신대복음병원"
    HOME_URL = "https://kosinmed.recruiter.co.kr/career/home"
    rows = []
    
    drv.get(HOME_URL)
    
    try:
        # [핵심 수정] 복잡한 클래스명 다 버리고, '채용 공고 링크'가 뜰 때까지 기다림
        # href 속성에 '/career/jobs/'가 포함된 a 태그를 찾음 (이건 절대 안 변함)
        WebDriverWait(drv, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/career/jobs/']"))
        )
        
        # 공고 카드들 싹 긁어오기
        cards = drv.find_elements(By.CSS_SELECTOR, "a[href*='/career/jobs/']")
        
        for card in cards:
            try:
                # 1. 텍스트 전체 가져오기 (제목, 날짜, 상태가 다 들어있음)
                text_all = card.text
                
                # 2. 상태 필터링 ('마감', '종료' 있으면 패스)
                if "마감" in text_all or "종료" in text_all:
                    continue

                # 3. 제목 추출
                # 카드 안에서 가장 굵은 글씨나 첫 줄이 제목일 확률 높음
                # 안전하게 클래스 이름에 'title' 들어간 놈 찾기 시도
                try:
                    title = card.find_element(By.CSS_SELECTOR, "[class*='title']").text.strip()
                except:
                    # 실패하면 그냥 텍스트 첫 줄 사용
                    title = text_all.split('\n')[0]

                # [디버깅용] 현재 읽고 있는 제목 출력 (나중에 주석 처리)
                # print(f"검색된 공고: {title}")

                # 4. 필터링: '간호사' 키워드
                if "간호사" not in title:
                    continue

                # 5. 기간 추출
                # 'date'라는 글자가 클래스에 포함된 태그 찾기
                try:
                    period = card.find_element(By.CSS_SELECTOR, "[class*='date']").text.strip()
                    # 줄바꿈이 있으면 물결로 연결
                    period = period.replace("\n", " ~ ")
                except:
                    period = "기간확인필요"

                # 6. 링크 추출
                link = card.get_attribute("href")
                
                rows.append(std_row(name, title, period, link))
                
            except Exception:
                continue
                
    except Exception as e:
        # 타임아웃이면 공고가 하나도 없거나 로딩 실패
        print(f"고신대복음병원 로딩 에러(또는 공고 없음): {e}")
        
    return df_std(rows)

def site32(drv):
    name = "가톨릭대학교인천성모병원(간호직)"
    LIST_URL = "https://cmcism.recruiter.co.kr/app/jobnotice/list"; BASE = "https://cmcism.recruiter.co.kr"
    rows = []
    drv.get(LIST_URL); wait_ready(drv)
    cards = drv.find_elements(By.CSS_SELECTOR, "#divJobnoticeList ul > li")
    for li in cards:
        try:
            if "접수중" not in li.find_element(By.CSS_SELECTOR, ".list-bbs-status").text: continue
            a = li.find_element(By.CSS_SELECTOR, "span.list-bbs-notice-name a")
            title = safe_str(a.text)
            if "간호직" in title:
                period = li.find_element(By.CSS_SELECTOR, "span.list-bbs-date").text
                rows.append(std_row(name, title, period, urljoin(BASE, a.get_attribute("href"))))
        except: continue
    return df_std(rows)


# ==============================================================================
# [3] 메인 실행부 (Execution Engine)
# ==============================================================================
if __name__ == "__main__":
    print("🚀 42개 병원 통합 크롤러 시작 (B학점 리팩토링 Ver.)")
    print(f"   - Headless Mode: {HEADLESS_MODE}")
    
    # 1. 드라이버 생성 (딱 1번)
    driver = make_driver()
    
    # 2. 실행할 함수 리스트 (42개 등록)
    #    주의: 함수 이름 뒤에 () 괄호가 없습니다. 함수 자체를 리스트에 넣습니다.
    all_scrapers = [
        run_seoul_asan, run_cau_mc, run_eumc, run_gangneung_asan, 
        run_inha, run_kbsmc, run_khmc, run_kuh, run_smc_changwon, run_yuhs,
        site01, site02, site03, site04, site05, site06, site07, site08, site09, site10,
        site11, site12, site13, site14, site15, site16, site17, site18, site19, site20,
        site21, site22, site23, site24, site25, site26, site27, site28, site29, site30, 
        site31, site32
    ]
    
    per_hospital = {}
    errors = []
    start_time = time.time()

    try:
        for i, scraper in enumerate(all_scrapers):
            func_name = scraper.__name__
            print(f"[{i+1}/{len(all_scrapers)}] ▶ {func_name} 실행 중...")
            
            try:
                # ★ 핵심: 밖에서 만든 driver를 각 함수에 빌려줌
                df = scraper(driver)
                
                if df is not None and not df.empty:
                    hosp_name = df.iloc[0]['병원']
                    per_hospital[hosp_name] = df
                    print(f"   - OK: {len(df)}건 수집 완료")
                else:
                    print("   - 공고 없음")
            except Exception as e:
                print(f"   ⚠️ {func_name} 에러: {e}")
                errors.append({"함수": func_name, "에러": str(e)})
                # 에러가 나도 다음 병원으로 계속 진행

    finally:
        # 3. 모든 작업 종료 후 드라이버 폐기
        driver.quit()
        end_time = time.time()
        print(f"\n🏁 크롤링 종료! (소요시간: {end_time - start_time:.1f}초)")

        # 4. 결과 저장
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        save_workbook_consolidated(per_hospital, out_path=f"간호사_통합_결과_{ts}.xlsx")
        
        if errors:
            pd.DataFrame(errors).to_csv(f"error_log_{ts}.csv", index=False)
            print(f"⚠️ 에러 로그 저장됨: error_log_{ts}.csv")