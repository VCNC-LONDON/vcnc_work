import datetime
import json
import time

from google.cloud import bigquery

from dependencies.zendesk.zendesk_model import ZendeskModel


class ZendeskUser(ZendeskModel):
    BIGQUERY_PROJECT = "kr-co-vcnc-tada"
    BIGQUERY_DATASET = "tada_cx"
    BIGQUERY_TABLE = "zendesk_user"
    BIGQUERY_TIME_PARTITION_FIELD = "created_at"
    BIGQUERY_FIELDS = [
        "id",
        "external_id",
        "role",
        "role_type",
        "last_login_at",
        "created_at",
        "updated_at",
    ]

    def __init__(self):
        pass

    def get_request_url(self, request_type: str, etl_start_timestamp: int) -> str:
        if request_type == "search":
            UTC = datetime.timezone(datetime.timedelta(hours=0))
            KST = datetime.timezone(datetime.timedelta(hours=9))
            start_datetime = datetime.datetime.fromtimestamp(
                etl_start_timestamp, tz=KST
            )
            start_date = start_datetime.strftime("%Y-%m-%d")
            url = f"https://tadatadahelp.zendesk.com/api/v2/search.json?query=type:user%20updated_at>={start_date}"
        else:
            url = f"https://tadatadahelp.zendesk.com/api/v2/incremental/users.json?start_time={etl_start_timestamp}"
        return url

    def parse_response(self, request_type: str, json_data: dict) -> tuple:
        next_request_url = None
        if request_type == "search":
            users = json_data["results"]
            next_request_url = json_data["next_page"]
        else:
            users = json_data["users"]
            if not json_data["end_of_stream"]:
                next_request_url = json_data["next_page"]

        json_results = []
        for user in users:
            user_dict = {}
            user_dict["id"] = int(user["id"])
            user_dict["external_id"] = (
                str(user["external_id"]) if user["external_id"] else None
            )
            user_dict["role"] = str(user["role"]) if user["role"] else None
            user_dict["role_type"] = (
                str(user["role_type"]) if user["role_type"] else None
            )
            user_dict["last_login_at"] = (
                int(
                    datetime.datetime.strptime(
                        user["last_login_at"], "%Y-%m-%dT%H:%M:%SZ"
                    )
                    .replace(tzinfo=datetime.timezone.utc)
                    .timestamp()
                )
                if user["last_login_at"]
                else None
            )
            user_dict["created_at"] = int(
                datetime.datetime.strptime(user["created_at"], "%Y-%m-%dT%H:%M:%SZ")
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
            )
            user_dict["updated_at"] = int(
                datetime.datetime.strptime(user["updated_at"], "%Y-%m-%dT%H:%M:%SZ")
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
            )

            json_results.append(user_dict)
        return (next_request_url, json_results)

    def get_bigquery_info(self) -> dict:
        bigquery_info = {}
        bigquery_info["project"] = self.BIGQUERY_PROJECT
        bigquery_info["dataset"] = self.BIGQUERY_DATASET
        bigquery_info["table"] = self.BIGQUERY_TABLE
        bigquery_info["location"] = "US"
        bigquery_info["time_partition_field"] = self.BIGQUERY_TIME_PARTITION_FIELD
        bigquery_info["schema"] = [
            bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("external_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("role", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("role_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_login_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ]
        return bigquery_info

    def make_merge_query(
        self, source_dataset, source_table, etl_start_timestamp, etl_end_timestamp
    ) -> str:
        UTC = datetime.timezone(datetime.timedelta(hours=0))
        search_period_start_at = str(
            datetime.datetime.fromtimestamp(etl_start_timestamp, tz=UTC)
        )
        search_period_end_at = str(
            datetime.datetime.fromtimestamp(etl_end_timestamp, tz=UTC)
        )

        update_clause = ", ".join(
            [f"`{field}`=source.`{field}`" for field in self.BIGQUERY_FIELDS]
        )
        insert_fields = ", ".join([f"`{field}`" for field in self.BIGQUERY_FIELDS])

        target_source_condition = f"Merge {self.BIGQUERY_DATASET}.{self.BIGQUERY_TABLE} target USING {source_dataset}.{source_table} source"
        on_pk_condition = "target.id = source.id"
        # on_time_condition = f" AND target.{self.BIGQUERY_TIME_PARTITION_FIELD} >= '{search_period_start_at}' AND target.{self.BIGQUERY_TIME_PARTITION_FIELD} < '{search_period_end_at}'"
        on_time_condition = ""
        update_condition = f" WHEN MATCHED THEN UPDATE SET {update_clause}"
        insert_condition = (
            f" WHEN NOT MATCHED THEN INSERT ({insert_fields}) VALUES ({insert_fields})"
        )
        merge_query = (
            target_source_condition
            + " ON "
            + on_pk_condition
            + on_time_condition
            + update_condition
            + insert_condition
        )
        return merge_query
