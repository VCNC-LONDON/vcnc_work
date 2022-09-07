select
  driver.*,
  driver_agency.name as agency_name,
  case
    when driver_agency.id = 'A00MJPLEX99999' then '파견' -- MJ플렉스
    when driver_agency.id = 'ATAXI000000000' then '택시' -- 개인택시
    when driver_agency.id = 'ATAXIKUMOH0000' then 'VIPVAN' -- 금오상운
    when driver_agency.id = 'ATAXIDAEHAN000' then 'VIPVAN' -- 대한상운
    when driver_agency.id = 'ATAXIDUKWANG10' then '택시' -- 덕왕기업
    when driver_agency.id = 'ATAXIDUKWANG00' then '택시' -- 덕왕운수
    when driver_agency.id = 'A00THXCAR09999' then '프리랜서' -- 땡큐카
    when driver_agency.id = 'A00ROADWIN9999' then '프리랜서' -- 로드윈
    when driver_agency.id = 'A00MOGUIDE9999' then '프리랜서' -- 모바일가이드
    when driver_agency.id = 'A00MOSILER9999' then '프리랜서' -- 모시러
    when driver_agency.id = 'A00BOBOSLINK99' then '프리랜서' -- 보보스링크
    when driver_agency.id = 'A4JTW1S88QSLJG' then 'TEST' -- 브이씨엔씨
    when driver_agency.id = 'A00STAFFS99999' then '프리랜서' -- 스탭스
    when driver_agency.id = 'A00CITEMP99999' then '프리랜서' -- 씨아이템프러리
    when driver_agency.id = 'A00ERULIM99999' then '프리랜서' -- 어울림
    when driver_agency.id = 'A3ZS8EEAA8B3WV' then '프리랜서' -- 에이스휴먼파워
    when driver_agency.id = 'A00MPLINE09999' then '파견' -- 엠플라인
    when driver_agency.id = 'A00WIZSEARCH99' then '파견' -- 위즈서치
    when driver_agency.id = 'ATAXIYOUCHANG0' then 'VIPVAN' -- 유창상운
    when driver_agency.id = 'ATAXIILHEUNG00' then 'VIPVAN' -- 일흥교통
    when driver_agency.id = 'A455ELYYXTPJ3J' then '프리랜서' -- 잡라이프
    when driver_agency.id = 'A00JNC99999999' then '프리랜서' -- 제이앤컴퍼니
    when driver_agency.id = 'ATAXITAEPYUNG0' then 'VIPVAN' -- 태평운수
    when driver_agency.id = 'A00TOS99999999' then '프리랜서' -- 티오에스
    when driver_agency.id = 'A00TOMNETW9999' then '파견' -- 티오엠네트웍
    when driver_agency.id = 'A00PUCIFIC9999' then '프리랜서' -- 퍼시픽 컨설팅
    when driver_agency.id = 'A00CVSJOB99999' then '파견' -- 편리한잡
    when driver_agency.id = 'A00PLUSTOP9999' then '프리랜서' -- 플러스탑
    when driver_agency.id = 'A00PANDK999999' then '프리랜서' -- 피엔케이
    when driver_agency.id = 'A00PPLING99999' then '프리랜서' -- 피플링
    when driver_agency.id = 'A00HERALD99999' then '프리랜서' -- 헤럴드
    when driver_agency.id = 'ATAXIHEUNGDUK0' then 'VIPVAN' -- 흥덕기업
    when driver_agency.id = 'A00INSIDEJOB99' then '프리랜서' -- 인사이드잡
    when driver_agency.id = 'A00SKYAIR99999' then 'VIPVAN' -- 스카이에어모빌리티
    when driver_agency.id = 'A00DAWOORIJOB9' then '프리랜서' -- 다우리잡
    when driver_agency.id = 'A00NINEWORK999' then '프리랜서' -- 나인워크
    when driver_agency.id = 'A00WITHGAIN999' then '프리랜서' -- 위드가인
    when driver_agency.id = 'A00HAEMIL99999' then '프리랜서' -- 해밀컨설팅
    when driver_agency.id = 'A00INCONNECT99' then '프리랜서' -- 인커넥트
    when driver_agency.id = 'A00GREENMAN999' then '프리랜서' -- 그린맨파워
    when driver_agency.id = 'A00JOEUNSYS999' then '프리랜서' -- 조은시스템
    when driver_agency.id = 'A00SEHINTER999' then '프리랜서' -- SEH인터내셔널
    when driver_agency.id = 'A00JOBPLUS9999' then '프리랜서' -- 잡플러스 # add by Andrew, 제주 베타테스트 때문에 추가됨
    when driver_agency.id = 'A00RIDEFLX9999' then '라이드플럭스' -- 라이드플럭스
    when driver_agency.driver_template_type = 'LITE' then '택시'
    when driver_agency.driver_template_type = 'PREMIUM' then '택시'
    when driver_agency.driver_template_type = 'PLUS' then '택시'
    when driver_agency.driver_template_type = 'NXT' then '택시'
  end as agency_type
from
  tada.driver
  left join tada.driver_agency on driver.agency_id = driver_agency.id
