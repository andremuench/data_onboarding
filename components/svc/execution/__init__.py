
from .celery.client import CeleryClient


def get_execution_client(ex_type):
    if ex_type == 'celery':
        return CeleryClient()