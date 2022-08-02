from ast import If
from google.oauth2 import service_account
import pandas_gbq as gbq

class operator():
    def __init__(self) :
        self.credentials = service_account.Credentials.from_service_account_file(
            '/Users/london/DEv/VCNC/secret/kr-co-vcnc-tada-c34de97de161.json'
            )
        
    def read_bq(self, sql):
        raw = gbq.read_gbq(
                sql, 
                project_id = "kr-co-vcnc-tada",
                credentials=self.credentials
            )
        return raw
    
    def update_bq(self, target_data, target_table) :
        gbq.to_gbq(
            target_data,
            target_table,
            project_id="kr-co-vcnc-tada",
            credentials=self.credentials,
            if_exists="replace"
        )
        print(f"총 {len(target_data)}개의 데이터를 {target_table} 로 업데이트를 완료하였습니다.")