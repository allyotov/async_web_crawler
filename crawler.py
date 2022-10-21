import os
from typing import List
import asyncio

import logging
import aiohttp
import aiofiles

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

FILE = 'external_links.txt'
HTTP_PREFIXES = ('http', 'http://', 'http://www.', 'https://', 'https://www.')

async def parse_page(client: aiohttp.ClientSession, url: str) -> bytes:
    async with client.get(url) as response:
        if response.status == 200:
            text = await response.read()
            logger.debug(type(text))
            external_links = []
            for link in BeautifulSoup(text, 'html.parser').find_all('a'):
                if link.get('href'):
                    r = link.get('href')
                    if r.startswith('http'):
                        common = os.path.commonprefix([url, r])
                        if common in HTTP_PREFIXES:
                            logger.debug(r)
                            logger.debug(type(r))
                            await write_to_disk(content=r)
                            external_links.append(r)
                        else:
                            logger.debug('SAME SITE')
                            logger.debug(common)
                            logger.debug(r)
            return external_links

async def write_to_disk(content: str):
    async with aiofiles.open(FILE, mode='a') as f:
        logger.debug(type(content))
        await f.write('{}\n'.format(content))


async def parse_all_pages(urls: List[str]):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(15)) as client:
        tasks = [parse_page(client, url) for url in urls]
        return await asyncio.gather(*tasks)


def main():
    external_links = asyncio.run(parse_all_pages(urls=['https://habr.com/ru/post/337420/']))
    logger.debug(external_links)


if __name__ == '__main__':
    main()