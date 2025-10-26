from __future__ import annotations

import os
import pathlib
import re
from dataclasses import dataclass
from typing import Any, Dict

from sqlalchemy import (
    CHAR,
    JSON,
    URL,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Engine,
    Float,
    Integer,
    LargeBinary,
    Numeric,
    SmallInteger,
    String,
    Text,
    Time,
    create_engine,
    event,
)
from sqlalchemy.orm import DeclarativeBase, mapped_column, sessionmaker

from src.config import config, read_toml, settings
from src.logging import logger

# ---------- Type mapping from TOML types to SQLAlchemy ----------
TYPE_MAP = {
    # Integers
    "int": Integer,
    "integer": Integer,
    "smallint": SmallInteger,
    "bigint": BigInteger,
    # Floating / fixed-precision numbers
    "real": Float,  # dialects may render REAL
    "float": Float,
    "double": Float,  # often DOUBLE PRECISION on supported dialects
    "decimal": Numeric,  # use Numeric(precision, scale) when specified
    # Booleans
    "bool": Boolean,
    "boolean": Boolean,
    # Text / strings
    "text": Text,
    "varchar": String,  # use String(length) when specified
    "char": CHAR,  # use CHAR(length) when specified
    # Date/time
    "date": Date,
    "time": Time,
    "datetime": DateTime,
    "timestamp": DateTime,
    # Binary / UUID
    "blob": LargeBinary,
    "uuid": String,  # prefer native UUID; else String(36) in your builder
    # Optional (not in your enum, keep if you plan to allow it)
    "str": String,
    "json": JSON,  # SQLite stores as TEXT; others may have native JSON
}


class Base(DeclarativeBase):
    pass


def camelize(name: str) -> str:
    # table or filename -> ClassName
    parts = re.split(r"[_\W]+", name.strip())
    return "".join(p[:1].upper() + p[1:] for p in parts if p)


def load_db_config(db_dir: pathlib.Path) -> Dict[str, Any]:
    # Extract last part of path as default name
    name = db_dir.name
    cfg_path = db_dir / f"{name}.db.toml"
    if not cfg_path.is_file():
        raise FileNotFoundError(f"Missing {name}.db.toml at {cfg_path}")
    doc = read_toml(cfg_path)
    # Normalize optional sections
    defaults = doc.setdefault("defaults", {})
    defaults.setdefault("columns", {})
    defaults.setdefault("indexes", {})
    defaults.setdefault("triggers", {})
    return doc


def discover_table_files(tables_dir: pathlib.Path) -> list[pathlib.Path]:
    if not tables_dir.is_dir():
        raise FileNotFoundError(f"Missing tables/ directory at {tables_dir}")
    return sorted(
        p for p in tables_dir.iterdir() if p.suffix == ".toml" and p.is_file()
    )


def parse_table_file(path: pathlib.Path) -> Dict[str, Any]:
    doc = read_toml(path)

    # Name of the table is everything before .table.toml
    table_name = path.name.split(".")[0]

    # If there is a top level field called table_name, override its name with that
    table_name = doc.get("table_name", table_name)

    # Top-level column tables (same as before)
    columns: Dict[str, Dict[str, Any]] = {
        k: v
        for k, v in doc.items()
        if k != "table" and isinstance(v, dict) and k not in ("indexes", "triggers")
    }

    return {
        "table": table_name,
        "columns": columns,
    }


def build_class_shells(table_defs: Dict[str, Dict[str, Any]]) -> Dict[str, type[Base]]:
    classes: Dict[str, type[Base]] = {}
    for table_name, table_def in table_defs.items():
        tbl = table_def["table"]
        cls_name = camelize(tbl)

        # Prepare attrs with tablename and eagerly attach all PK columns
        attrs: Dict[str, Any] = {"__tablename__": tbl}
        cols = table_def.get("columns", {})

        # Collect PKs to ensure mapper sees at least one at class creation time
        pk_items = [(name, spec) for name, spec in cols.items() if spec.get("primary")]
        if not pk_items:
            # Fail early with a clearer error than SQLAlchemy's mapper error
            raise ValueError(
                f"Table '{tbl}' has no primary key defined in its table TOML."
            )

        for col_name, cdef in pk_items:
            attrs[col_name] = make_mapped_column(col_name, cdef)

        # Create the mapped class now that it has PK metadata
        cls = type(cls_name, (Base,), attrs)
        classes[table_name] = cls

    return classes


def make_mapped_column(col_name: str, spec: Dict[str, Any]):
    col_type_name = spec.get("type")
    if col_type_name not in TYPE_MAP:
        raise ValueError(f"Unknown type '{col_type_name}' for column '{col_name}'")

    sa_type = TYPE_MAP[col_type_name]
    if col_type_name == "str":
        length = spec.get("length")
        if length is not None:
            sa_type = sa_type(int(length))

    primary_key = bool(spec.get("primary", False))
    # If the column is a primary key, nullable should be False
    nullable = False if primary_key else bool(spec.get("nullable", True))
    unique = bool(spec.get("unique", False))

    autoincrement_val = spec.get("autoincrement")
    if autoincrement_val is None:
        # Reasonable default: ints that are PK autoincrement
        autoincrement_val = bool(primary_key and col_type_name == "int")

    py_default = spec.get("default")  # may be None

    # Build kwargs only when values are NOT None, to avoid passing None
    kwargs: Dict[str, Any] = {
        "primary_key": primary_key,
        "nullable": nullable,
        "unique": unique,
        "autoincrement": autoincrement_val,
    }

    if py_default is not None:
        kwargs["default"] = py_default

    return mapped_column(sa_type, **kwargs)


def wire_columns(
    classes: Dict[str, type[Base]], table_defs: Dict[str, Dict[str, Any]]
) -> None:
    for table_name, cls in classes.items():
        columns = table_defs[table_name]["columns"]
        for col_name, cdef in columns.items():
            # PKs (and any pre-attached attrs) were already added in build_class_shells
            if hasattr(cls, col_name):
                continue
            setattr(cls, col_name, make_mapped_column(col_name, cdef))


def load_seed_data(db_dir: pathlib.Path) -> Dict[str, list[Dict[str, Any]]]:
    """
    Read a single *.data.toml that has top-level arrays of tables, e.g.:
      [[users]] ...
      [[posts]] ...
    Returns { "users": [ {...}, ... ], "posts": [ {...}, ... ] }.
    """
    if not db_dir.is_dir():
        raise FileNotFoundError(f"Database directory not found: {db_dir}")

    data_files = sorted(
        p
        for p in db_dir.iterdir()
        if p.suffix == ".toml" and p.name.endswith(".data.toml")
    )
    if not data_files:
        # no seeds is acceptable; return empty
        return {}
    if len(data_files) > 1:
        raise RuntimeError(
            f"Multiple *.data.toml files found in {db_dir}: {[p.name for p in data_files]}"
        )

    doc = read_toml(data_files[0])
    # keep only array-of-table entries
    out: Dict[str, list[Dict[str, Any]]] = {
        k: v for k, v in doc.items() if isinstance(v, list)
    }
    return out


def _enable_sqlite_foreign_keys(dbapi_conn, _connection_record):
    # For sqlite3, execute PRAGMA via a cursor on the raw DBAPI connection
    cur = dbapi_conn.cursor()
    try:
        cur.execute("PRAGMA foreign_keys=ON")
    finally:
        cur.close()


def main(root: str | os.PathLike = "."):
    cfg = config(root)
    db_dir = pathlib.Path(cfg["db"]["dir"])

    # 1) Config
    db_cfg = load_db_config(db_dir)

    # 1.1) Build URL
    db_cfg = db_cfg["database"]
    dialect: str = db_cfg["dialect"]
    path: str = cfg["data"]["path"]
    name: str = db_cfg["name"]
    db_file = (pathlib.Path(path) / name).resolve()

    # 1.2) If dev mode under [app], mode is on, remove database
    app_mode = settings.get("app", {}).get("mode", "dev")
    if app_mode == "dev" and db_file.is_file():
        logger.info(f"App in dev mode; removing existing database at {db_file}")
        db_file.unlink()

    url = URL.create(drivername=dialect, database=str(db_file))

    echo: bool = db_cfg["echo"]

    # 2) Read all table files
    tables_dir = db_dir / "tables"
    table_files = discover_table_files(tables_dir)
    parsed_tables_list = [parse_table_file(p) for p in table_files]
    table_defs = {m["table"]: m for m in parsed_tables_list}

    # 3) Build dynamic ORM classes
    classes = build_class_shells(table_defs)
    wire_columns(classes, table_defs)

    # 4) Create engine and tables
    engine = create_engine(url, echo=echo)
    if engine.url.drivername.startswith("sqlite"):
        event.listen(engine, "connect", _enable_sqlite_foreign_keys)
    Base.metadata.create_all(engine)

    # 5) Seed data (unchanged)
    seeds = load_seed_data(db_dir)
    session = sessionmaker(engine, expire_on_commit=False)
    with session() as sess:
        for table_name, rows in seeds.items():
            cls = classes.get(table_name)
            if cls is None:
                raise ValueError(f"Seed data refers to unknown table: {table_name}")
            sess.add_all([cls(**row) for row in rows])
        sess.commit()

    engine.dispose()


# A simple object to store and return classes, engine, session and base
@dataclass
class DbObject:
    classes: Dict[str, type[Base]]
    engine: Engine


def init(root: str | os.PathLike) -> DbObject:
    cfg = config(root)
    db_dir = pathlib.Path(cfg["db"]["dir"])

    # 1) Config
    db_cfg = load_db_config(db_dir)

    # 1.1) Build URL
    db_cfg = db_cfg["database"]
    dialect: str = db_cfg["dialect"]
    path: str = cfg["data"]["path"]
    name: str = db_cfg["name"]
    db_file = (pathlib.Path(path) / name).resolve()

    # 1.2) If dev mode under [app], mode is on, remove database
    app_mode = settings.get("app", {}).get("mode", "dev")
    if app_mode == "dev" and db_file.is_file():
        logger.info(f"App in dev mode; removing existing database at {db_file}")
        db_file.unlink()

    url = URL.create(drivername=dialect, database=str(db_file))

    echo: bool = db_cfg["echo"]

    # 2) Read all table files
    tables_dir = db_dir / "tables"
    table_files = discover_table_files(tables_dir)
    parsed_tables_list = [parse_table_file(p) for p in table_files]
    table_defs = {m["table"]: m for m in parsed_tables_list}

    # 3) Build dynamic ORM classes
    classes = build_class_shells(table_defs)
    wire_columns(classes, table_defs)

    # 4) Create engine and tables
    engine = create_engine(url, echo=echo)
    if engine.url.drivername.startswith("sqlite"):
        event.listen(engine, "connect", _enable_sqlite_foreign_keys)
    Base.metadata.create_all(engine)

    # 5) Seed data (unchanged)
    seeds = load_seed_data(db_dir)
    session = sessionmaker(engine, expire_on_commit=False)
    with session() as sess:
        for table_name, rows in seeds.items():
            cls = classes.get(table_name)
            if cls is None:
                raise ValueError(f"Seed data refers to unknown table: {table_name}")
            sess.add_all([cls(**row) for row in rows])
        sess.commit()

    # Return a DbObject
    return DbObject(classes=classes, engine=engine)


if __name__ == "__main__":
    from src.config import root_dir

    # Run as: python loader.py   (from inside mydb/)
    main(root_dir)
