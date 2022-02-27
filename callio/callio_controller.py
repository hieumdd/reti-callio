from callio import callio_service


def callio_controller(body: dict[str, str]):
    return callio_service.services[body.get("table", "")](
        body.get("start"),
        body.get("end"),
    )
