import datetime as dt
import functools
from datetime import datetime
from typing import (
    Any,
    Optional,
    Type,
    TypeVar,
    Union,
)

import requests
from requests import Response
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import (
    InstrumentedAttribute,
)
from sqlalchemy.sql.elements import ColumnElement

# Import settings
from src.config import settings

# Import DbObject from src/db.py
from src.db import Base

# Import logging
from src.logging import logger

# Import file at src/db.py


T = TypeVar("T")
Col = Union[
    InstrumentedAttribute[Any], ColumnElement[Any]
]


@functools.cache
def get_feed(url: str) -> Response:
    timeout: int = settings["http"]["get"][
        "timeout"
    ]

    r: Response = requests.get(
        url=url, timeout=timeout
    )

    return r


def head_feed(url: str) -> Response:
    timeout: int = settings["http"]["head"][
        "timeout"
    ]

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

    return http_request


def list_rows(
    session: Session,
    table: Type[T],
):
    # accept single column or a sequence of columns
    return list(
        session.execute(select(table))
        .scalars()
        .all()
    )


def latest(
    session: Session,
    table: Type[T],
    where_col: Col,
    value: Any,
    order_col: Col,
) -> Optional[T]:
    stmt = (
        select(table)
        .where(where_col == value)
        .order_by(order_col.desc())
        .limit(1)
    )
    return session.execute(stmt).scalars().first()


def op(
    Links: type[Base],
    HttpGet: type[Base],
    HttpHead: type[Base],
    session: Session,
):
    links = list_rows(session, Links)
    for link in links:
        url: str = link.url
        logger.info(url)

        # Retrieve any requests for this url
        last_get = latest(
            session,
            HttpGet,
            HttpGet.url,
            url,
            HttpGet.at,
        )

        last_head = latest(
            session,
            HttpHead,
            HttpHead.url,
            url,
            HttpHead.at,
        )

        both = (last_get, last_head)

        if not (any(both)):
            logger.info(
                "No http request on record. Initiating first"
            )
            get_req = get(url, HttpGet)
            session.add(get_req)
            session.commit()
        elif (
            both[0] is not None
            and both[1] is None
        ):
            logger.info(
                "Most recent GET at: %s",
                last_get.at,
            )

            logger.info(
                "Initiating a HEAD request for modification date"
            )

            head_req = head(url, HttpHead)
            session.add(head_req)
            session.commit()

        elif (
            both[0] is None
            and both[1] is not None
        ):
            logger.info(
                "Most recent HEAD at: %s",
                last_head.at,
            )

            logger.info(
                "Initiating a GET request after HEAD"
            )

            get_req = get(url, HttpGet)
            session.add(get_req)
            session.commit()

        else:
            logger.info(
                "Both GET and HEAD requests exist"
            )

            if (
                last_head.last_modified
                > last_get.last_modified
            ):
                logger.info(
                    "Content modified since last GET at: %s",
                    last_get.last_modified,
                )

                logger.info(
                    "Initiating a new GET request"
                )

                get_req = get(url, HttpGet)
                session.add(get_req)
                session.commit()

            else:
                logger.info(
                    "Content not modified since last GET at: %s",
                    last_get.last_modified,
                )

                # If the content is not modified, check for a new HEAD request
                # But only if enough time has elapsed since the last HEAD
                # Reference http.head.wait setting
                wait_seconds: int = settings[
                    "http"
                ]["head"]["wait"]
                now: datetime = datetime.now()
                elapsed_since_head: dt.timedelta = (
                    now - last_head.at
                )
                if (
                    elapsed_since_head.total_seconds()
                    > wait_seconds
                ):
                    logger.info(
                        "Wait time exceeded since last HEAD. Initiating new HEAD request."
                    )
                    head_req = head(url, HttpHead)
                    session.add(head_req)
                    session.commit()

    # ----- Footnote -------
    session.close()
