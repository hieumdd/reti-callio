from typing import Any

from utils.utils import parse_unix_ts


def transform(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "_id": row.get("_id"),
            "gender": row.get("gender"),
            "facebookId": str(row.get("facebookId")),
            "zaloId": str(row.get("zaloId")),
            "zaloName": str(row.get("zaloName")),
            "createTime": parse_unix_ts(row.get("createTime")),
            "client": row.get("client"),
            "customer": {
                "_id": row["customer"].get("_id"),
                "flexChargeTime": parse_unix_ts(row["customer"].get("flexChargeTime")),
                "createTime": parse_unix_ts(row["customer"].get("createTime")),
                "updateTime": parse_unix_ts(row["customer"].get("updateTime")),
                "customFields": [
                    {
                        "key": custom_fields.get("key"),
                        "val": custom_fields.get("val"),
                        "_id": custom_fields.get("_id"),
                    }
                    for custom_fields in row["customer"]["customFields"]
                ]
                if row["customer"].get("customFields")
                else [],
                "client": row["customer"].get("client"),
                "desc": row["customer"].get("desc"),
                "name": row["customer"].get("name"),
                "__v": row["customer"].get("__v"),
                "mainContact": row["customer"].get("mainContact"),
                "id": row["customer"].get("id"),
            }
            if row.get("customer")
            else {},
            "phone": row.get("phone"),
            "name": row.get("name"),
            "__v": row.get("__v"),
        }
        for row in rows
    ]


schema = [
    {"name": "_id", "type": "STRING"},
    {"name": "gender", "type": "STRING"},
    {"name": "facebookId", "type": "STRING"},
    {"name": "zaloId", "type": "STRING"},
    {"name": "zaloName", "type": "STRING"},
    {"name": "createTime", "type": "TIMESTAMP"},
    {"name": "client", "type": "STRING"},
    {
        "name": "customer",
        "type": "RECORD",
        "fields": [
            {"name": "_id", "type": "STRING"},
            {"name": "flexChargeTime", "type": "TIMESTAMP"},
            {"name": "createTime", "type": "TIMESTAMP"},
            {"name": "updateTime", "type": "TIMESTAMP"},
            {
                "name": "customFields",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "key", "type": "STRING"},
                    {"name": "val", "type": "STRING", "mode": "REPEATED"},
                    {"name": "_id", "type": "STRING"},
                ],
            },
            {"name": "client", "type": "STRING"},
            {"name": "desc", "type": "STRING"},
            {"name": "name", "type": "STRING"},
            {"name": "__v", "type": "INTEGER"},
            {"name": "mainContact", "type": "STRING"},
            {"name": "id", "type": "STRING"},
        ],
    },
    {"name": "phone", "type": "STRING"},
    {"name": "name", "type": "STRING"},
    {"name": "__v", "type": "INTEGER"},
]
