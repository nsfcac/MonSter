import json


class AsyncioRequests:
    """
    Asyncio requests to urls
    """
    import aiohttp
    import asyncio
    from aiohttp import ClientSession


    def __init__(self, auth: tuple = (), timeout: tuple = (15, 45), max_retries: int = 3):
        self.retry = 0
        self.loop = self.asyncio.get_event_loop()
        self.timeout = self.aiohttp.ClientTimeout(*timeout)
        self.max_retries = max_retries
        if auth:
            self.auth = self.aiohttp.BasicAuth(*auth)
        else:
            self.auth = None
    
    
    async def __fetch_json(self, url: str, host: str, session: ClientSession) -> dict:
        """
        Get request wrapper to fetch json data from API
        """
        try:
            resp = await session.request(method='GET', url=url)
            resp.raise_for_status()
            return await {host: resp.json()}
        except (TimeoutError):
            self.retry += 1
            if self.retry >= self.max_retries:
                return {}
            return await self.__fetch_json(url, host, session)
        except:
            return {}


    async def __requests(self, urls: list, hosts: list) -> list:
        async with self.ClientSession(auth = self.auth, timeout = self.timeout) as session:
            tasks = []
            for i, url in enumerate(urls):
                tasks.append(self.__fetch_json(url=url, host=hosts[i], session=session))
            return await self.asyncio.gather(*tasks)


    def bulk_fetch(self, urls: list, hosts: list) -> list:
        return self.loop.run_until_complete(self.__requests(urls, hosts))