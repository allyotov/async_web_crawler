import os
from typing import List
import asyncio
import itertools
import logging
import aiohttp
import aiofiles

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

FILE = 'external_links.txt'
HTTP_PREFIXES = ('http', 'http://', 'http://www.', 'https://', 'https://www.')


def get_basic_domain_name(addr):
    wo_http = None
    basic_dn = None
    logger.debug('get basic domain name of: %s' % addr)
    wo_http = addr.split('//')[1]
    logger.debug(wo_http)
    basic_dn = wo_http.split('/')[0]
    logger.debug('So, basic dn: %s' % basic_dn)
    return basic_dn

def compare_addresses(addr: str, current_domain_name: str) -> bool:
    logger.debug('Compare: %s <=> %s .' % (addr, current_domain_name))
    return get_basic_domain_name(addr) == current_domain_name


async def parse_page(client: aiohttp.ClientSession, url: str) -> bytes:
    try:
        basic_dn = get_basic_domain_name(url)
        async with client.get(url) as response:
            if response.status == 200:
                text = await response.read()
                external_links = []
                for link in BeautifulSoup(text, 'html.parser').find_all('a'):
                    if link.get('href'):
                        r = link.get('href')
                        if r.startswith('http://') or r.startswith('https://'):
                            logger.debug('Link address: %s;' % r)
                            if not compare_addresses(r, basic_dn):
                                logger.debug('writing to disc')
                                await write_to_disk(content=r)
                                external_links.append(r)
                            else:
                                logger.debug('Same resource: skip;')
                return external_links
    except aiohttp.client_exceptions.ClientConnectorError as e:
        logger.exception('%s : url - %s' % (e, url))
        return []
    except aiohttp.client_exceptions.ServerDisconnectedError as e:
        logger.exception('%s : url - %s' % (e, url))
        return []
    except asyncio.exceptions.TimeoutError as e:
        logger.exception('%s : url - %s' % (e, url))
        return []


async def write_to_disk(content: str):
    async with aiofiles.open(FILE, mode='a') as f:
        await f.write('{}\n'.format(content))


async def parse_all_pages(urls: List[str]):
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(15), trust_env=True) as client:
        tasks = [parse_page(client, url) for url in urls]
        return await asyncio.gather(*tasks)


def main(deep_degree: int, external_links: List[str]) -> None:
    while deep_degree and external_links:
        logger.debug('\n\n\n\n\nDeep: {};'.format(deep_degree))
        result_links = asyncio.run(parse_all_pages(urls=external_links))
        logger.info(result_links)
        external_links = list(itertools.chain.from_iterable(result_links))
        logger.debug(external_links)
        deep_degree -= 1
    logger.info('Successfully finished.')


if __name__ == '__main__':
    main(deep_degree=3, external_links=['https://habr.com/ru/post/337420/'])