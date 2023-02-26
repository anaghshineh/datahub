import logging

GCS_PREFIX = "gs://"

logging.getLogger("py4j").setLevel(logging.ERROR)
logger: logging.Logger = logging.getLogger(__name__)


def is_gcs_uri(uri: str) -> bool:
    return uri.startswith(GCS_PREFIX)
