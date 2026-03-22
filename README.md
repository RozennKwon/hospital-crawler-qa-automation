# 🏥 Hospital-Recruit-QA-Bot 
> **국내 주요 병원 채용공고 QA 및 CI/CD 자동화 시스템**

![QA Pipeline Status](https://github.com/RozennKwon/hospital-crawler-qa-automation/actions/workflows/crawler.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python) ![Selenium](https://img.shields.io/badge/Selenium-4.x-green?logo=selenium) ![GitHub Actions](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-black?logo=githubactions) ![Status](https://img.shields.io/badge/Status-Automated-success)

국내 42개 주요 병원의 채용공고를 매일 아침 자동으로 수집하고, 데이터 무결성을 검증하는 **QA 관점의 자동화 파이프라인**입니다. 단순 크롤링을 넘어 데이터 신뢰성을 확보하기 위한 검증 게이트웨이를 포함하고 있습니다.

---

### 🚀 Key Features (핵심 기능)

1. **RaiT (Raw-data Analysis & Integrity Test) 검증 게이트웨이**
   - 수집된 로우 데이터의 결측치, 형식 오류를 실시간 검증합니다.
   - 데이터 무결성이 확보된 공고만 최종 결과물에 포함하여 데이터 신뢰도를 극대화했습니다.

2. **CI/CD 파이프라인 구축 (GitHub Actions)**
   - **Daily Automation:** 매일 오전 9시(KST) 가상 환경(Ubuntu)에서 자동 실행됩니다.
   - **Self-Managed Storage:** 크롤링 후 업데이트된 `seen_posts.csv`를 봇이 스스로 커밋/푸시하여 중복 수집을 영구적으로 차단합니다. (Zero-Touch Operation)

3. **중복 데이터 필터링 시스템**
   - 로컬 환경과 클라우드 환경이 동기화된 히스토리 DB(`seen_posts.csv`)를 통해 이미 확인한 공고는 제외하고 신규 공고만 선별하여 리포트합니다.

---

### 🛠 Tech Stack (기술 스택)

* **Language:** Python 3.10
* **Library:** Selenium, Pandas, Openpyxl, Webdriver-manager
* **Automation:** GitHub Actions (CI/CD)
* **Storage:** CSV (seen_posts.csv) & Excel (Daily Artifacts)

---

### 📂 Project Structure (폴더 구조)

```text
.github/workflows/  # CI/CD 파이프라인 설정 (crawler.yml)
top-hospitals/
  ├── qa/           # 병원별 크롤링 로직 및 RaiT 검증 모듈
  ├── main.py       # 메인 실행 엔진 (QA 게이트웨이 및 데이터 핸들링)
  └── seen_posts.csv # 중복 방지용 히스토리 데이터베이스 (Auto-updated)
```

---

### 🛠️ 문제 해결 및 최적화 기록 (Troubleshooting)
> **개발 과정에서 마주한 이슈를 QA 관점에서 분석하고 해결한 기록입니다.**

* **환경 간 경로 이식성 확보 (Path Optimization)**
    * **Issue:** 로컬 개발 환경과 GitHub Actions 가상 서버의 파일 경로 차이로 인한 실행 에러 발생.
    - **Solution:** `os.path`를 활용한 상대 경로 동적 생성 로직을 도입하여 환경에 무관한 안정적 실행 구조 구축.

* **데이터 정합성 및 중복 관리 자동화**
    - **Issue:** 반복 수집 시 발생하는 데이터 중복 문제 및 수동 관리의 한계.
    - **Solution:** `seen_posts.csv`를 활용한 히스토리 관리 시스템과 GitHub Actions의 자동 커밋 기능을 연동하여 **데이터 영속성** 확보.

* **보안 및 환경 격리 (Environment Hygiene)**
    - **Issue:** 불필요한 빌드 파일 및 민감한 결과물이 저장소에 포함되어 관리 복잡도 증가.
    - **Solution:** `.gitignore` 정교화를 통해 소스 코드와 데이터 산출물을 명확히 분리하고 관리 효율성 증대.

---

### 📊 QA Workflow (검증 프로세스)
1. **Trigger:** 매일 오전 9시(KST) GitHub Actions 스케줄러 자동 실행
2. **Scraping:** Selenium 기반 병원별 채용 페이지 동적 데이터 수집
3. **QA Validation:** **RaiT** 로직을 통한 데이터 무결성(Integrity) 검사
4. **Deduplication:** `seen_posts.csv` 대조를 통한 중복 공고 필터링
5. **Auto-Update:** 검증 완료된 신규 데이터를 레포지토리에 자동 커밋 & 푸시
6. **Reporting:** 최종 엑셀 결과물 및 에러 로그 아카이빙 (GitHub Artifacts)

---

### 💡 기대 효과
* **업무 효율성:** 수동 확인 대비 **약 95% 이상의 시간 절감** 효과
* **데이터 신뢰도:** RaiT 검증 및 중복 필터링을 통한 **클린 데이터(Clean Data)** 확보

---

---

### 🛠️ 문제 해결 및 최적화 기록 (Troubleshooting)
> **개발 과정에서 마주한 이슈를 QA 관점에서 분석하고 해결한 기록입니다.**

* **환경 간 경로 이식성 확보 (Path Optimization)**
    * **Issue:** 로컬 개발 환경과 GitHub Actions 가상 서버의 파일 경로 차이로 인한 실행 에러 발생.
    - **Solution:** `os.path`를 활용한 상대 경로 동적 생성 로직을 도입하여 환경에 무관한 안정적 실행 구조 구축.

* **데이터 정합성 및 중복 관리 자동화**
    - **Issue:** 반복 수집 시 발생하는 데이터 중복 문제 및 수동 관리의 한계.
    - **Solution:** `seen_posts.csv`를 활용한 히스토리 관리 시스템과 GitHub Actions의 자동 커밋 기능을 연동하여 **데이터 영속성** 확보.

* **보안 및 환경 격리 (Environment Hygiene)**
    - **Issue:** 불필요한 빌드 파일 및 민감한 결과물이 저장소에 포함되어 관리 복잡도 증가.
    - **Solution:** `.gitignore` 정교화를 통해 소스 코드와 데이터 산출물을 명확히 분리하고 관리 효율성 증대.

---

### 📊 QA Workflow (검증 프로세스)
1. **Trigger:** 매일 오전 9시(KST) GitHub Actions 스케줄러 자동 실행
2. **Scraping:** Selenium 기반 병원별 채용 페이지 동적 데이터 수집
3. **QA Validation:** **RaiT** 로직을 통한 데이터 무결성(Integrity) 검사
4. **Deduplication:** `seen_posts.csv` 대조를 통한 중복 공고 필터링
5. **Auto-Update:** 검증 완료된 신규 데이터를 레포지토리에 자동 커밋 & 푸시
6. **Reporting:** 최종 엑셀 결과물 및 에러 로그 아카이빙 (GitHub Artifacts)

---

### 💡 기대 효과
* **업무 효율성:** 수동 확인 대비 **약 95% 이상의 시간 절감** 효과
* **데이터 신뢰도:** RaiT 검증 및 중복 필터링을 통한 **클린 데이터(Clean Data)** 확보
