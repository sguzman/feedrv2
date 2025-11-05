import datetime
from datetime import datetime
from sys import version

import feedparser
from sqlalchemy.orm import sessionmaker

# Pipeline code
from src import db
from src.config import settings

# Import DbObject from src/db.py
from src.db import Feeds, HttpGet, HttpHead, Items, Links, db_object, engine

# Import logging
from src.db_op import latest, list_rows
from src.logging import logger

# Import settings
from src.pipes import http


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
            
            if out is None:
                logger.info(
                    "No new db object to add for url: %s", url
                )
                continue
            
            if isinstance(out, HttpGet):
                text = out.text
                out.text = None

            sess.add(out)
            sess.commit()
            logger.info(
                "Added new db object: %s", out
            )
            
            # If the output is a HttpHead, log and continue
            if isinstance(out, HttpHead):
                logger.info(
                    "Output is HttpHead, continuing to next link: %s", url
                )
                continue
            
            # If the output is a HttpGet, process further
            feed_obj = feedparser.parse(text)
            
            feed = Feeds(
                bozo=feed_obj.bozo,
                num_feeds=len(feed_obj.entries),
                encoding=feed_obj.encoding,
                version=feed_obj.version
            )
            
            sess.add(feed)
            sess.commit()
            
            # Log string for feed
            feed_log = f'''
Feed(id={feed.id}, bozo={feed.bozo}, num_feeds={feed.num_feeds}, encoding={feed.encoding}, version={feed.version})
            '''
            logger.info(
                "Added new feed: %s", feed_log
            )
            
            for entry in feed_obj.entries:
                # Log string for entry
                entry_log = f'''
Entry(id={entry.get("id", "N/A")}, title={entry.get("title", "N/A")}, link={entry.get("link", "N/A")}, published={entry.get("published", "N/A")})
                '''
                logger.info(
                    "New entry: %s", entry_log
                )
                item = Items(
                    item_id=entry["id"],
                    guidislink=entry["guidislink"],
                    title=entry["title"],
                    link=entry.get("link", None),
                    published=datetime.strptime(entry["published"], '%a, %d %b %Y %H:%M:%S %z'),
                )
                sess.add(item)
                sess.commit()
                logger.info(
                    "Added new item: %s", item
                )
                
            logger.info(
                "Finished processing item, sleeping for %d seconds", settings["app"]["item"]["sleep"]
            )
            time.sleep(settings["app"]["item"]["sleep"])
                
            
        sess.close()

        time.sleep(settings["app"]["sleep"])


if __name__ == "__main__":
    main()
