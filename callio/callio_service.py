from ctypes import util
from callio import callio_repo
from utils import utils

def pipeline_service(get, transform):
    def _svc(start, end):
        with callio_repo.get_session() as session:
            data = utils.compose(
                transform,
                get
            )(session, start, end)
        return data
    return _svc
