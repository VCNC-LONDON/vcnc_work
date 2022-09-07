import datetime
import json
import time

from google.cloud import bigquery

from dependencies.zendesk.zendesk_model import ZendeskModel


class ZendeskTicketMetric(ZendeskModel):
    BIGQUERY_PROJECT = "kr-co-vcnc-tada"
    BIGQUERY_DATASET = "tada_cx"
    BIGQUERY_TABLE = "zendesk_ticket_metric"
    BIGQUERY_TIME_PARTITION_FIELD = "created_at"
    BIGQUERY_FIELDS = [
        "id",
        "ticket_id",
        "reopens",
        "replies",
        "group_stations",
        "assignee_stations",
        "assignee_updated_at",
        "requester_updated_at",
        "initially_assigned_at",
        "assigned_at",
        "solved_at",
        "latest_comment_added_at",
        "created_at",
        "updated_at",
        "reply_time_in_minutes",
        "first_resolution_time_in_minutes",
        "full_resolution_time_in_minutes",
        "agent_wait_time_in_minutes",
        "requester_wait_time_in_minutes",
        "on_hold_time_in_minutes",
    ]

    def __init__(self):
        pass

    def get_request_url(self, request_type: str, etl_start_timestamp: int) -> str:
        # API에 search, 시간 지원 없음
        # 전체 ticket metric 리턴
        url = f"https://tadatadahelp.zendesk.com/api/v2/ticket_metrics.json"
        return url

    def parse_response(self, request_type: str, json_data: dict) -> tuple:
        next_request_url = None
        ticket_metrics = json_data["ticket_metrics"]
        if ("next_page" in json_data) and json_data["next_page"]:
            next_request_url = json_data["next_page"]

        json_results = []
        for ticket_metric in ticket_metrics:
            ticket_metric_dict = {}
            ticket_metric_dict["id"] = int(ticket_metric["id"])
            ticket_metric_dict["ticket_id"] = int(ticket_metric["ticket_id"])
            ticket_metric_dict["reopens"] = int(ticket_metric["reopens"])
            ticket_metric_dict["replies"] = int(ticket_metric["replies"])
            ticket_metric_dict["group_stations"] = int(ticket_metric["group_stations"])
            ticket_metric_dict["assignee_stations"] = int(
                ticket_metric["assignee_stations"]
            )
            ticket_metric_dict["assignee_updated_at"] = (
                int(
                    datetime.datetime.strptime(
                        ticket_metric["assignee_updated_at"], "%Y-%m-%dT%H:%M:%SZ"
                    )
                    .replace(tzinfo=datetime.timezone.utc)
                    .timestamp()
                )
                if ticket_metric["assignee_updated_at"]
                else None
            )
            ticket_metric_dict["requester_updated_at"] = (
                int(
                    datetime.datetime.strptime(
                        ticket_metric["requester_updated_at"], "%Y-%m-%dT%H:%M:%SZ"
                    )
                    .replace(tzinfo=datetime.timezone.utc)
                    .timestamp()
                )
                if ticket_metric["requester_updated_at"]
                else None
            )
            ticket_metric_dict["initially_assigned_at"] = (
                int(
                    datetime.datetime.strptime(
                        ticket_metric["initially_assigned_at"], "%Y-%m-%dT%H:%M:%SZ"
                    )
                    .replace(tzinfo=datetime.timezone.utc)
                    .timestamp()
                )
                if ticket_metric["initially_assigned_at"]
                else None
            )
            ticket_metric_dict["assigned_at"] = (
                int(
                    datetime.datetime.strptime(
                        ticket_metric["assigned_at"], "%Y-%m-%dT%H:%M:%SZ"
                    )
                    .replace(tzinfo=datetime.timezone.utc)
                    .timestamp()
                )
                if ticket_metric["assigned_at"]
                else None
            )
            ticket_metric_dict["solved_at"] = (
                int(
                    datetime.datetime.strptime(
                        ticket_metric["solved_at"], "%Y-%m-%dT%H:%M:%SZ"
                    )
                    .replace(tzinfo=datetime.timezone.utc)
                    .timestamp()
                )
                if ticket_metric["solved_at"]
                else None
            )
            ticket_metric_dict["latest_comment_added_at"] = (
                int(
                    datetime.datetime.strptime(
                        ticket_metric["latest_comment_added_at"], "%Y-%m-%dT%H:%M:%SZ"
                    )
                    .replace(tzinfo=datetime.timezone.utc)
                    .timestamp()
                )
                if ticket_metric["latest_comment_added_at"]
                else None
            )
            ticket_metric_dict["created_at"] = int(
                datetime.datetime.strptime(
                    ticket_metric["created_at"], "%Y-%m-%dT%H:%M:%SZ"
                )
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
            )
            ticket_metric_dict["updated_at"] = int(
                datetime.datetime.strptime(
                    ticket_metric["updated_at"], "%Y-%m-%dT%H:%M:%SZ"
                )
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
            )
            ticket_metric_dict["reply_time_in_minutes"] = ticket_metric[
                "reply_time_in_minutes"
            ]
            ticket_metric_dict["first_resolution_time_in_minutes"] = ticket_metric[
                "first_resolution_time_in_minutes"
            ]
            ticket_metric_dict["full_resolution_time_in_minutes"] = ticket_metric[
                "full_resolution_time_in_minutes"
            ]
            ticket_metric_dict["agent_wait_time_in_minutes"] = ticket_metric[
                "agent_wait_time_in_minutes"
            ]
            ticket_metric_dict["requester_wait_time_in_minutes"] = ticket_metric[
                "requester_wait_time_in_minutes"
            ]
            ticket_metric_dict["on_hold_time_in_minutes"] = ticket_metric[
                "on_hold_time_in_minutes"
            ]

            json_results.append(ticket_metric_dict)
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
            bigquery.SchemaField("ticket_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("reopens", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("replies", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("group_stations", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("assignee_stations", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("assignee_updated_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("requester_updated_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("initially_assigned_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("assigned_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("solved_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField(
                "latest_comment_added_at", "TIMESTAMP", mode="NULLABLE"
            ),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField(
                "reply_time_in_minutes",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("calendar", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("business", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "first_resolution_time_in_minutes",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("calendar", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("business", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "full_resolution_time_in_minutes",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("calendar", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("business", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "agent_wait_time_in_minutes",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("calendar", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("business", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "requester_wait_time_in_minutes",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("calendar", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("business", "INTEGER", mode="NULLABLE"),
                ],
            ),
            bigquery.SchemaField(
                "on_hold_time_in_minutes",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("calendar", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("business", "INTEGER", mode="NULLABLE"),
                ],
            ),
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
