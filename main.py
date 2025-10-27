import datetime
import functools
from typing import Any, Optional, Type, TypeVar, Union

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.elements import ColumnElement

# Import file at src/db.py
from src import db

# Import settings
from src.config import root_dir, settings

# Import DbObject from src/db.py
from src.db import Base, DbObject

# Import logging
from src.logging import logger

T = TypeVar("T")
Col = Union[InstrumentedAttribute[Any], ColumnElement[Any]]


@functools.cache
def get_feed(url: str) -> requests.Response:
    timeout: int = settings["http"]["timeout"]

    r: requests.Response = requests.get(
        url=url, timeout=timeout
    )

    return r


def make_req(url: str, HttpRequests: type[Base]):
    at: datetime.datetime = datetime.datetime.now()
    resp: requests.Response = get_feed(url)
    after: datetime.datetime = datetime.datetime.now()

    elapsed_delta: datetime.timedelta = after - at
    elapsed: int = int(elapsed_delta.microseconds // 1000)

    # Http objects
    timeout: int = settings["http"]["timeout"]
    encoding: str = resp.encoding
    apparent_encoding: str = resp.apparent_encoding
    last_modified: datetime.datetime = (
        datetime.datetime.strptime(
            resp.headers["last-modified"],
            "%a, %d %b %Y %H:%M:%S %Z",
        )
    )
    transfer_encoding: str = resp.headers[
        "transfer-encoding"
    ]
    is_redirect: bool = resp.is_redirect
    is_ok: bool = resp.ok
    reason: str = resp.reason
    text: str = resp.text
    status: int = resp.status_code

    http_request = HttpRequests(
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
    return session.execute(select(table)).scalars()


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


def pipeline(
    Links: type[Base],
    HttpRequests: type[Base],
    session: Session,
):
    links = list_rows(session, Links)
    for link in links:
        url: str = link.url
        logger.info(url)

        # Retrieve any requests for this url
        most_recent = latest(
            session,
            HttpRequests,
            HttpRequests.url,
            url,
            HttpRequests.at,
        )

        if most_recent:
            logger.info(
                "Most recent request at: %s", most_recent.at
            )

        req = make_req(url, HttpRequests)
        session.add(req)
        session.commit()

    # ----- Footnote -------
    session.close()


def main():
    obj: DbObject = db.init(root_dir)
    logger.info("Initialized database object: %s", obj)
    cls = obj.classes
    engine = obj.engine
    Links: type[Base] = cls["links"]
    HttpRequests: type[Base] = cls["http_request"]

    session = sessionmaker(engine, expire_on_commit=False)
    sess = session()
    pipeline(Links, HttpRequests, sess)


if __name__ == "__main__":
    main()
