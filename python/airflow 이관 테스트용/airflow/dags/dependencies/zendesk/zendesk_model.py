class ZendeskModel:
    def __init__(self):
        pass

    def get_request_url(self, request_type: str, etl_start_timestamp: int) -> str:
        url = ""
        return url

    def parse_response(self, request_type: str, json_data: dict) -> tuple:
        next_request_url = ""
        json_results = ""
        return (next_request_url, json_results)

    def get_bigquery_info(self) -> dict:
        bigquery_info = []
        return bigquery_info
