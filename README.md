# Project MEDEASY – Crawling Drug Data

![프로젝트 로고](https://avatars.githubusercontent.com/u/197080815?s=200&v=4)

## 📖 개요

Project MEDEASY는 **의약품 데이터 수집 및 가공**을 위한 프로젝트입니다.  
공공데이터포털에서 제공하는 의약품 정보를 수집하여 정제된 형태로 가공하고, 이를 활용할 수 있도록 데이터베이스에 저장합니다.

본 프로젝트는 **식품의약품안전처(KFDA)**에서 제공하는 **공공데이터포털 API**를 활용하여 데이터를 수집합니다.

🔗 **사용하는 공공데이터 API**:

- [💊 의약품 제품 허가 상세정보 API](https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15095677)
- [💊 의약품 낱알 식별정보 API](https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15057639)

## 🚀 주요 기능

- 🔍 **의약품 데이터 수집**: 공공데이터포털에서 API를 통해 데이터를 가져옴
- 🛠 **XML 데이터 처리**: CDATA와 HTML 태그를 정리하여 구조화된 텍스트로 변환
- 💾 **데이터 저장**: 수집한 데이터를 가공하여 JSON 파일 또는 데이터베이스에 저장
