import datetime as dt
from datetime import datetime
from typing import (
    Any,
    Optional,
    Type,
    TypeVar,
    Union,
)

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import (
    InstrumentedAttribute,
)
from sqlalchemy.sql.elements import ColumnElement

# Import settings
from src import http
from src.config import settings

# Import DbObject from src/db.py
from src.db import Base, HttpGet, HttpHead

# Import logging
from src.logging import logger

# Import file at src/db.py


T = TypeVar("T")
Col = Union[
    InstrumentedAttribute[Any], ColumnElement[Any]
]

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
    url: str,
    last_get: None | Base,
    last_head: None | Base,
) -> None | Base:
    logger.info(url)

    if last_get is None:
        logger.info(
            "Most recent HEAD at: %s",
            last_head,
        )

        logger.info(
            "Initiating a GET request after HEAD"
        )

        return http.get(url, HttpGet)

    if last_head is None:
        logger.info(
            "Most recent GET at: %s",
            last_get,
        )

        logger.info(
            "Initiating a HEAD request for modification date"
        )

        return http.head(url, HttpHead)

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

        return http.get(url, HttpGet)


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

        return http.head(url, HttpHead)
    
    logger.info(
        "Wait time not exceeded: Current time: %s, Last HEAD time: %s, Elapsed seconds: %s, Wait seconds: %s",
        now,
        last_head.at,
        elapsed_since_head.total_seconds(),
        wait_seconds,
    )
    
    return None
