## Airflow

- Google Composer 2 사용
- Seoul region(asia-northeast3) 에 위치
- Private environment 로 Airflow CLI 또는 Kubernetes API에 외부 네트워크에서 직접 접속할 수 없음
 
### Naming Guide
- ELT 가 목적인 경우 `Extract` prefix 를 사용
- 특수한 목적의 대시보드 등 특정 제품 제작이 목적인 경우 `Make` prefix 를 사용

	위 항목에 포함되지 않아 네이밍이 고민되는 경우 data chapter 에 공유하여 통일된 컨벤션을 유지할 수 있도록 합니다

### ELT 작업시 참고
- ELT 를 위한 query 는 스크립트 내부에 포함하지 않고 `airflow/sql` 폴더 안에 prefix를 제외한 파일명으로 저장합니다.
- dag file 내에서 아래와 같은 방법으로 호출합니다.
```
sql=utils.read_sql(
            os.path.join(os.environ["DAGS_FOLDER"], "sql/file_name.sql")
        )
```

### Backfill 작업시 참고
- 되도록 backfill이 필요한 상황을 만들지 않도록 합니다
- 짧은 기간은 airflow ui 에서 시각적으로 보면서 할 수 있도록 합니다.
- 만약 긴 기간의 경우 backfill cli 를 활용합니다.
  - backfill.sh example
  	- 반복해서 과거 데이터를 돌려야할 때 번거로움을 최소화하기 위해 스크립트로 만들어둠
  	- MAC에서 돌아가는 것 확인
	- sh backfill.sh <start_date> <end_date> <dag_id> <task_id> 로 입력
	
	```
	sh backfill.sh 2019-05-01 2019-05-03 transfer_tada_data_to_bigquery_hourly transfer_ride_data_1
	```

