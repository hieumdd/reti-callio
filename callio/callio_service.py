from ctypes import util
from callio import callio_repo, call
from utils import utils


def pipeline_service(table, get, transform, load):
    def _svc(start, end):
        with callio_repo.get_session() as session:
            data = utils.compose(transform, get)(session, start, end)
        return data

    return _svc


call_inbound_service = pipeline_service(
    callio_repo.get_listing("call", {"direction": 1}),
    call.transform,

)
