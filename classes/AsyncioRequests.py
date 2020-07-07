import json


class AsyncioRequests:
    """
    Asyncio requests to urls
    """
    import aiohttp
    import asyncio
    from aiohttp import ClientSession


    def __init__(self, timeout: tuple = (15, 45), max_retries: int = 3):
        self.retry = 0
        self.loop = self.asyncio.get_event_loop()
        self.timeout = self.aiohttp.ClientTimeout(connect=timeout[0], total=timeout[1])
        self.max_retries = max_retries
    
    async def __fetch_json(self, url: str, session: ClientSession) -> dict:
        """
        Get request wrapper to fetch json data from API
        """
        try:
            resp = await session.request(method='GET', url=url)
            resp.raise_for_status()
            return await resp.json()
        except (TimeoutError):
            self.retry += 1
            if self.retry >= self.max_retries:
                raise TimeoutError()
            return await self.__fetch_json(url, session)


    async def __requests(self, urls: list) -> list:
        async with self.ClientSession(timeout = self.timeout) as session:
            tasks = []
            for url in urls:
                tasks.append(self.__fetch_json(url=url, session=session))
            return await self.asyncio.gather(*tasks)


    def bulk_fetch(self, urls: list) -> list:
        return self.loop.run_until_complete(self.__requests(urls))