import json
import logging


class AsyncioRequests:
    """
    Asyncio requests to urls
    """
    import aiohttp
    import asyncio
    from aiohttp import ClientSession


    def __init__(self, verify_ssl: bool = False, auth: tuple = (), 
                 timeout: tuple = (15, 45), max_retries: int = 3):
        self.metrics = {}
        self.retry = 0
        self.connector=self.aiohttp.TCPConnector(verify_ssl=verify_ssl)
        if auth:
            self.auth = self.aiohttp.BasicAuth(*auth)
        else:
            self.auth = None
        self.timeout = self.aiohttp.ClientTimeout(*timeout)
        self.max_retries = max_retries
        self.loop = self.asyncio.get_event_loop()
        
    
    async def __fetch_json(self, url: str, node: str, session: ClientSession) -> dict:
        """
        Get request wrapper to fetch json data from API
        """
        try:
            resp = await session.request(method='GET', url=url)
            resp.raise_for_status()
            json = await resp.json()
            return {"node": node, "metrics": json}
        except (TimeoutError):
            self.retry += 1
            if self.retry >= self.max_retries:
                return {"node": node, "metrics": {}}
            return await self.__fetch_json(url, node, session)
        except:
            logging.error(f"Cannot fetch data from {node} : url")
            return {"node": node, "metrics": {}}


    async def __requests(self, urls: list, nodes: list) -> list:
        async with self.ClientSession(connector=self.connector, 
                                      auth = self.auth, 
                                      timeout = self.timeout) as session:
            tasks = []
            for i, url in enumerate(urls):
                tasks.append(self.__fetch_json(url=url, node=nodes[i], session=session))
            return await self.asyncio.gather(*tasks)


    def bulk_fetch(self, urls: list, nodes: list) -> list:
        self.metrics =  self.loop.run_until_complete(self.__requests(urls, nodes))
        self.loop.close()
        return self.metrics