import datetime
import json
import time

from google.cloud import bigquery

from dependencies.zendesk.zendesk_model import ZendeskModel


class ZendeskTicket(ZendeskModel):
    BIGQUERY_PROJECT = "kr-co-vcnc-tada"
    BIGQUERY_DATASET = "tada_cx"
    BIGQUERY_TABLE = "zendesk_ticket"
    BIGQUERY_TIME_PARTITION_FIELD = "created_at"
    BIGQUERY_FIELDS = [
        "id",
        "assignee_id",
        "group_id",
        "requester_id",
        "brand_id",
        "submitter_id",
        "created_at",
        "updated_at",
        "generated_timestamp",
        "status",
        "description",
        "channel",
        "source_to",
        "source_from",
        "source_rel",
        "type",
        "priority",
        "ticket_form_id",
        "tags",
        "custom_fields",
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
            url = f"https://tadatadahelp.zendesk.com/api/v2/search.json?query=type:ticket%20updated_at>={start_date}"
        else:
            url = f"https://tadatadahelp.zendesk.com/api/v2/incremental/tickets.json?start_time={etl_start_timestamp}"
        return url

    def parse_response(self, request_type: str, json_data: dict) -> tuple:
        next_request_url = None
        if request_type == "search":
            tickets = json_data["results"]
            next_request_url = json_data["next_page"]
        else:
            tickets = json_data["tickets"]
            if not json_data["end_of_stream"]:
                next_request_url = json_data["next_page"]

        json_results = []
        for ticket in tickets:
            ticket_dict = {}
            ticket_dict["id"] = int(ticket["id"])
            ticket_dict["assignee_id"] = (
                int(ticket["assignee_id"]) if ticket["assignee_id"] else None
            )
            ticket_dict["group_id"] = (
                int(ticket["group_id"]) if ticket["group_id"] else None
            )
            ticket_dict["requester_id"] = (
                int(ticket["requester_id"]) if ticket["requester_id"] else None
            )
            ticket_dict["brand_id"] = (
                int(ticket["brand_id"]) if ticket["brand_id"] else None
            )
            ticket_dict["submitter_id"] = (
                int(ticket["submitter_id"]) if ticket["submitter_id"] else None
            )
            ticket_dict["created_at"] = int(
                datetime.datetime.strptime(ticket["created_at"], "%Y-%m-%dT%H:%M:%SZ")
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
            )
            ticket_dict["updated_at"] = int(
                datetime.datetime.strptime(ticket["updated_at"], "%Y-%m-%dT%H:%M:%SZ")
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
            )
            ticket_dict["generated_timestamp"] = None
            if request_type != "search":
                ticket_dict["generated_timestamp"] = (
                    int(ticket["generated_timestamp"])
                    if ticket["generated_timestamp"]
                    else None
                )
            ticket_dict["status"] = ticket["status"] if ticket["status"] else None
            ticket_dict["description"] = (
                ticket["description"] if ticket["description"] else None
            )

            ticket_dict["type"] = ticket["type"] if ticket["type"] else None
            ticket_dict["priority"] = ticket["priority"] if ticket["priority"] else None
            ticket_dict["ticket_form_id"] = (
                int(ticket["ticket_form_id"]) if ticket["ticket_form_id"] else None
            )

            ticket_dict["tags"] = ticket["tags"]

            for idx, c_field in enumerate(ticket["custom_fields"]):
                if type(c_field["value"]) == bool:
                    ticket["custom_fields"][idx]["value"] = (
                        "True" if c_field["value"] else "False"
                    )
                if type(c_field["value"]) == list:
                    values = ""
                    for contents in ticket["custom_fields"][idx]["value"]:
                        values += contents + "\n"
                    ticket["custom_fields"][idx]["value"] = values
            ticket_dict["custom_fields"] = ticket["custom_fields"]

            ticket_dict["channel"] = None
            ticket_dict["source_to"] = None
            ticket_dict["source_from"] = None
            ticket_dict["source_rel"] = None
            via = ticket["via"]
            if via["channel"]:
                ticket_dict["channel"] = via["channel"]
            if via["source"]:
                source = via["source"]
                if ("to" in source) and source["to"]:
                    ticket_dict["source_to"] = json.dumps(
                        source["to"], ensure_ascii=False
                    )
                if ("from" in source) and source["from"]:
                    ticket_dict["source_from"] = json.dumps(
                        source["from"], ensure_ascii=False
                    )
                if ("rel" in source) and source["rel"]:
                    ticket_dict["source_rel"] = json.dumps(
                        source["rel"], ensure_ascii=False
                    )

            json_results.append(ticket_dict)
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
            bigquery.SchemaField("assignee_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("group_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("requester_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("brand_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("submitter_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("generated_timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("channel", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source_to", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source_from", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source_rel", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("priority", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ticket_form_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
            bigquery.SchemaField(
                "custom_fields",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("value", "STRING", mode="NULLABLE"),
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
