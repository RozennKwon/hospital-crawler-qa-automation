# 🏥 Hospital-Recruit-QA-Bot 
> **국내 주요 병원 채용공고 QA 및 CI/CD 자동화 시스템**

![QA Pipeline Status](https://github.com/RozennKwon/hospital-crawler-qa-automation/actions/workflows/crawler.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python) ![Selenium](https://img.shields.io/badge/Selenium-4.x-green?logo=selenium) ![GitHub Actions](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-black?logo=githubactions) ![Status](https://img.shields.io/badge/Status-Automated-success)

국내 42개 주요 병원의 채용공고를 매일 아침 자동으로 수집하고, 데이터 무결성을 검증하는 **QA 관점의 자동화 파이프라인**입니다. 단순 크롤링을 넘어 데이터 신뢰성을 확보하기 위한 검증 게이트웨이를 포함하고 있습니다.

---

### 🚀 Key Features (핵심 기능)

1. **RaiT (Responsible AI Testing) 검증 게이트웨이**
   - 수집된 로우 데이터의 결측치, 형식 오류를 **RaiT 프레임워크** 기반으로 실시간 검증합니다.
   - 검증을 통과한(PASS) 공고만 최종 결과물에 포함하고 `seen_posts`에 등록하여, 가비지 데이터 주입을 차단하고 데이터 신뢰도를 극대화했습니다.

2. **CI/CD 파이프라인 구축 (GitHub Actions)**
   - **Daily Automation:** 매일 오전 9시(KST) 가상 환경(Ubuntu)에서 자동 실행됩니다.
   - **Self-Managed Storage:** 크롤링 후 업데이트된 `seen_posts.csv`를 봇이 스스로 커밋/푸시하여 중복 수집을 영구적으로 차단합니다. (Zero-Touch Operation)

3. **중복 데이터 필터링 시스템**
   - 로컬 환경과 클라우드 환경이 동기화된 히스토리 DB(`seen_posts.csv`)를 통해 이미 확인한 공고는 제외하고 신규 공고만 선별하여 리포트합니다.

---

### 🧪 Test Cases (QA Logic Verification)
> **데이터 정합성을 확보하기 위해 설계된 핵심 테스트 시나리오입니다.**

| ID | 테스트 항목 | 기대 결과 | 상태 |
|:---:|:---|:---|:---:|
| **TC-01** | **RaiT 데이터 유효성 검증** | `verify_rait_compliance` 로직을 통해 결측치 및 형식 오류 데이터 자동 판별 | **Pass** |
| **TC-02** | **데이터 무결성 가드** | **PASS** 판정을 받은 신규 데이터만 중복 방지 목록(`seen_posts`)에 업데이트 | **Pass** |
| **TC-03** | **통합 리포트 분기 저장** | 전체/신규/RaiT_FAIL(QA용) 데이터를 개별 시트로 분리하여 엑셀 생성 | **Pass** |
| **TC-04** | **42개 병원 타겟 접속** | 보안 차단 사이트 제외, 모든 타겟 사이트 정상 접속 및 데이터 파싱 확인 | **Pass** |

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

### 🚀 Future Work (추후 작업 예정)

- [ ] **Pytest 프레임워크 도입**: 유닛 테스트 및 통합 테스트 자동화를 통한 코드 유지보수성 및 안정성 강화
- [ ] **다차원 데이터 검증 시스템**: 수집 데이터의 문맥적 오류 및 텍스트 정합성 판별 로직 고도화
- [ ] **알림 시스템 연동**: 크롤링 결과 및 RaiT 검증 실패(FAIL) 리포트 실시간 자동 송출 (Slack/Email)

---

### 💡 기대 효과
* **업무 효율성:** 수동 확인 대비 **약 95% 이상의 시간 절감** 효과
* **데이터 신뢰도:** RaiT 검증 및 중복 필터링을 통한 **클린 데이터(Clean Data)** 확보

---

## ⚠️ Known Issues & Limitations
* **삼성창원병원(SMC Changwon):** 해당 사이트는 강화된 보안 정책으로 인해 GitHub Actions(Ubuntu/해외 IP) 환경에서의 접근을 차단합니다. 
  * 해당 병원의 데이터 수집이 필요한 경우, **로컬 환경**에서 직접 실행하는 것을 권장합니다.
  * CI/CD 파이프라인에서는 안정성을 위해 해당 타겟을 자동으로 스킵(Skip)하도록 설정되어 있습니다.

---

## ⚖️ Disclaimer (면책 조항)
* 본 프로젝트는 개인의 기술적 역량 향상 및 포트폴리오 제작을 목적으로 하며, 수집된 정보의 상업적 이용을 엄격히 금합니다.
* 모든 채용 정보의 저작권은 해당 병원에 있으며, 원저작권자의 요청이 있을 경우 본 레포지토리는 즉시 비공개로 전환될 수 있습니다.
* 서버 부하를 최소화하기 위해 지연 시간(Wait/Sleep)을 적용하였으며, **RaiT(Responsible AI Testing)** 게이트웨이를 통해 개인정보를 포함한 유해 데이터의 수집을 원천 차단하고 있습니다.

---

## 📄 License
이 프로젝트는 [MIT License](LICENSE)를 따릅니다.
