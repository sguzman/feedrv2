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

