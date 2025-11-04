import datetime as dt
from datetime import datetime

import requests
from requests import Response

# Import settings
from src.config import settings

# Import logging
from src.logging import logger

def get_feed(url: str) -> Response:
    timeout: int = settings["http"]["get"][
        "timeout"
    ]
    
    logger.info("GET URL: %s with timeout %d", url, timeout)

    r: Response = requests.get(
        url=url, timeout=timeout
    )

    return r


def head_feed(url: str) -> Response:
    timeout: int = settings["http"]["head"][
        "timeout"
    ]

    logger.info("HEAD URL: %s with timeout %d", url, timeout)

    r: Response = requests.head(
        url=url, timeout=timeout
    )

    return r


def head(url: str, HttpHead: type[Base]):
    logger.info("HEAD: %s", url)

    at: datetime = datetime.now()
    resp: Response = get_feed(url)
    after: datetime = datetime.now()

    elapsed_delta: dt.timedelta = after - at
    elapsed: int = int(
        elapsed_delta.microseconds // 1000
    )

    # Http objects
    timeout: int = settings["http"]["head"][
        "timeout"
    ]

    last_modified: datetime = datetime.strptime(
        resp.headers["last-modified"],
        "%a, %d %b %Y %H:%M:%S %Z",
    )

    is_ok: bool = resp.ok
    reason: str = resp.reason
    status: int = resp.status_code

    http_head = HttpHead(
        timeout=timeout,
        url=url,
        at=at,
        status=status,
        elapsed=elapsed,
        last_modified=last_modified,
        is_ok=is_ok,
        reason=reason,
    )
    
    # Log created HttpHead object
    logger.info("Created HttpHead object: %s", http_head)

    return http_head


def get(url: str, HttpGet: type[Base]):
    logger.info("GET: %s", url)

    at: datetime = datetime.now()
    resp: Response = get_feed(url)
    after: datetime = datetime.now()

    elapsed_delta: dt.timedelta = after - at
    elapsed: int = int(
        elapsed_delta.microseconds // 1000
    )

    # Http objects
    timeout: int = settings["http"]["get"][
        "timeout"
    ]
    encoding: None | str = resp.encoding
    apparent_encoding: str = (
        resp.apparent_encoding
    )
    last_modified: datetime = datetime.strptime(
        resp.headers["last-modified"],
        "%a, %d %b %Y %H:%M:%S %Z",
    )
    transfer_encoding: str = resp.headers[
        "transfer-encoding"
    ]
    is_redirect: bool = resp.is_redirect
    is_ok: bool = resp.ok
    reason: str = resp.reason
    text: str = resp.text
    status: int = resp.status_code

    http_request = HttpGet(
        timeout=timeout,
        url=url,
        at=at,
        status=status,
        elapsed=elapsed,
        encoding=encoding,
        apparent_encoding=apparent_encoding,
        last_modified=last_modified,
        text=text,
        transfer_encoding=transfer_encoding,
        is_redirect=is_redirect,
        is_ok=is_ok,
        reason=reason,
    )
    
    # Log created HttpGet object
    logger.info("Created HttpGet object: %s", http_request)

    return http_request