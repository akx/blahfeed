import asyncio
import hashlib
import os
import re
import time
from itertools import chain
from pathlib import Path
from typing import Optional, List

import aiohttp
import feedparser
from asgiref.sync import sync_to_async
from feedgen.feed import FeedGenerator

CACHE_ROOT = Path(os.environ.get("BLAHFEED_CACHE", "./cache"))
CACHE_ROOT.mkdir(parents=True, exist_ok=True)


def get_file_age(p) -> Optional[float]:
    # TODO: this could be async too, but we assume file systems are fast enough
    if not os.path.isfile(p):
        return None
    return time.time() - os.stat(p).st_mtime


async def parse_feed(
    sess: aiohttp.ClientSession, url: str, max_age: int = 3600
) -> feedparser.FeedParserDict:
    cache_path = CACHE_ROOT / re.sub("[^a-z0-9-]+", "-", url)
    cache_age = get_file_age(cache_path)
    if cache_age is None or cache_age > max_age:
        resp = await sess.get(url)
        resp.raise_for_status()
        content = await resp.content.read()
        with open(cache_path, "wb") as f:
            f.write(content)
    else:
        with open(cache_path, "rb") as f:
            content = f.read()

    async_parse = sync_to_async(
        lambda: feedparser.parse(content.decode()), thread_sensitive=False
    )
    return await async_parse()


async def generate_hybrid_feed(feed_urls: List[str], max_age: int = 3600) -> bytes:
    digest = hashlib.sha256("\n".join(sorted(feed_urls)).encode()).hexdigest()
    async with aiohttp.ClientSession() as sess:
        feeds = await asyncio.gather(
            *[parse_feed(sess, url, max_age=max_age) for url in feed_urls]
        )

    sorted_entries = sorted(
        chain(*(f["entries"] for f in feeds)),
        key=lambda item: item.get("updated_parsed") or item.get("published_parsed"),
        reverse=True,
    )
    fg = FeedGenerator()
    fg.title("Hybrid Feed")
    fg.id(f"feed-{digest}")
    for entry in sorted_entries:
        item = fg.add_item()
        item.title(entry["title"])
        item.guid(entry["guid"])  # TODO: ensure uniqueness across feeds?
        item.link(href=entry["link"])
        item.published(entry.get("published"))
        item.updated(entry.get("updated"))
        # TODO: add e.g. category, tags, image, ...?
    return fg.atom_str(pretty=True)


def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--max-age", type=int, default=3600)
    ap.add_argument("url", nargs="+")
    args = ap.parse_args()
    feed_urls = list(args.url)
    atom_str = asyncio.run(
        generate_hybrid_feed(feed_urls, max_age=args.max_age), debug=True
    )
    print(atom_str.decode())


if __name__ == "__main__":
    main()
