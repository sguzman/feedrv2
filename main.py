from sqlalchemy.orm import sessionmaker

# Pipeline code
from src import db

# Import settings
from src.config import root_dir, settings

# Import DbObject from src/db.py
from src.db import Base, DbObject

# Import logging
from src.db_op import latest, list_rows
from src.logging import logger
from src.pipes import pipeline


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

        sess = session()
        # Retrieve any requests for this url
        last_get = latest(
            sess,
            HttpGet,
            HttpGet.url,
            url,
            HttpGet.at,
        )

        last_head = latest(
            sess,
            HttpHead,
            HttpHead.url,
            url,
            HttpHead.at,
        )

        links = list_rows(sess, Links)

        for link in links:
            url: str = link.url
            out = pipeline.op(
                url, last_get, last_head
            )
            sess.add(out, _warn=True)
            sess.commit()
            sess.close()


if __name__ == "__main__":
    main()
