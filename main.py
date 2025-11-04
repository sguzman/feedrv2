from sqlalchemy.orm import sessionmaker

# Pipeline code
from src import db

# Import settings
from src.pipes import http

from src.config import settings

# Import DbObject from src/db.py
from src.db import Links, HttpGet, HttpHead, engine, db_object

# Import logging
from src.db_op import latest, list_rows
from src.logging import logger


def main():
    logger.info(
        "Initialized database object: %s", db_object
    )

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
        
        sess = session()


        links = list_rows(sess, Links)

        for link in links:
            url: str = link.url
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
            out = http.op(
                url, last_get, last_head
            )
            
            if out is not None:
                sess.add(out)
                sess.commit()
                logger.info(
                    "Added new db object: %s", out
                )
            sess.close()

        time.sleep(settings["app"]["sleep"])


if __name__ == "__main__":
    main()
