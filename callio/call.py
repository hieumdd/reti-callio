from typing import Any


def transform(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "_id": row.get("_id"),
            "offNetwork": row.get("offNetwork"),
            "fromUser": row.get("fromUser"),
            "toUser": row.get("toUser"),
            "fromGroup": row.get("fromGroup"),
            "toGroup": row.get("toGroup"),
            "direction": row.get("direction"),
            "fromExt": row.get("fromExt"),
            "toExt": row.get("toExt"),
            "fromNumber": row.get("fromNumber"),
            "toNumber": row.get("toNumber"),
            "startTime": row.get("startTime"),
            "answerTime": row.get("answerTime"),
            "endTime": row.get("endTime"),
            "duration": row.get("duration"),
            "billDuration": row.get("billDuration"),
            "hangupCause": row.get("hangupCause"),
            "audioQuality": row.get("audioQuality"),
            "createTime": row.get("createTime"),
            "recordingDuration": row.get("recordingDuration"),
            "chargeTime": row.get("chargeTime"),
            "chargedFee": row.get("chargedFee"),
            "campaign": row.get("campaign"),
            "transcripts": [i for i in row["transcripts"]]
            if row.get("transcripts")
            else [],
            "fromContact": row.get("fromContact"),
            "toContact": row.get("toContact"),
        }
        for row in rows
    ]


schema = [
    {"name": "_id", "type": "STRING"},
    {"name": "offNetwork", "type": "BOOLEAN"},
    {"name": "fromUser", "type": "STRING"},
    {"name": "toUser", "type": "STRING"},
    {"name": "fromGroup", "type": "STRING"},
    {"name": "toGroup", "type": "STRING"},
    {"name": "direction", "type": "STRING"},
    {"name": "fromExt", "type": "STRING"},
    {"name": "toExt", "type": "STRING"},
    {"name": "fromNumber", "type": "STRING"},
    {"name": "toNumber", "type": "STRING"},
    {"name": "startTime", "type": "STRING"},
    {"name": "answerTime", "type": "STRING"},
    {"name": "endTime", "type": "STRING"},
    {"name": "duration", "type": "STRING"},
    {"name": "billDuration", "type": "STRING"},
    {"name": "hangupCause", "type": "STRING"},
    {"name": "audioQuality", "type": "INTEGER"},
    {"name": "createTime", "type": "INTEGER"},
    {"name": "recordingDuration", "type": "INTEGER"},
    {"name": "chargeTime", "type": "STRING"},
    {"name": "chargedFee", "type": "INTEGER"},
    {"name": "campaign", "type": "STRING"},
    {"name": "transcripts", "type": "STRING", "mode": "repeated"},
    {"name": "fromContact", "type": "STRING"},
    {"name": "toContact", "type": "STRING"},
]
