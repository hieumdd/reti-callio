from callio import callio_service
from tasks import cloud_tasks


def create_tasks_service(body: dict[str, str]) -> dict[str, int]:
    return {
        "tasks": cloud_tasks.create_tasks(
            [
                {
                    "table": table,
                    "start": body.get("start"),
                    "end": body.get("end"),
                }
                for table in callio_service.services.keys()
            ],
            lambda x: x["table"],
        )
    }
