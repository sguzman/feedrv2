from sqlalchemy.orm import sessionmaker

# Pipeline code
from src import db, pipeline

# Import settings
from src.config import root_dir, settings

# Import DbObject from src/db.py
from src.db import Base, DbObject

# Import logging
from src.logging import logger


def main():
    obj: DbObject = db.init(root_dir)
    logger.info(
        "Initialized database object: %s", obj
    )
    cls = obj.classes
    engine = obj.engine
    Links: type[Base] = cls["links"]
    HttpGet: type[Base] = cls["http_get"]
    HttpHead: type[Base] = cls["http_head"]

    session = sessionmaker(
        engine, expire_on_commit=False
    )
    sess = session()
    while True:
        # Sleep for 5 seconds
        import time

        logger.info(
            "Sleeping for %d seconds",
            settings["app"]["sleep"],
        )
        time.sleep(settings["app"]["sleep"])

        pipeline.op(
            Links, HttpGet, HttpHead, sess
        )


if __name__ == "__main__":
    main()
