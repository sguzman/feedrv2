import functools

import feedparser
import requests

# Import file at src/db.py
from src import db

# Import settings
from src.config import root_dir, settings

# Import DbObject from src/db.py
from src.db import DbObject

# Import logging
from src.logging import logger


@functools.cache
def get_feed(url: str):
    # Set up a http session to remember session
    logger.debug("get %s", url)
    timeout: int = settings["http"]["timeout"]
    retries: int = settings["http"]["retries"]

    r: requests.Response = requests.get(url=url, timeout=timeout)
    obj: feedparser.FeedParserDict = feedparser.parse(r.text)

    return (r, obj)


def main():
    obj: DbObject = db.init(root_dir)
    logger.info("Initialized database object: %s", obj)

    url = "https://www.govinfo.gov/rss/bills.xml"
    result = get_feed(url)
    logger.info(result)


if __name__ == "__main__":
    main()
