import json


class AsyncioRequests:
    """
    Asyncio requests to urls
    """
    import aiohttp
    import asyncio
    from aiohttp import ClientSession
    loop = asyncio.get_event_loop()
    gather = asyncio.gather()

    def __init__(self):
        self.result = []

    
    async def __fetch_json(self, url: str, session: ClientSession) -> dict:
        """
        Get request wrapper to fetch json data from API
        """
        resp = await session.request(method='GET', url=url)
        resp.raise_for_status()
        json = await resp.json()
        return json


    async def __requests(self, urls: list) -> list:
        async with self.ClientSession() as session:
            tasks = []
            for url in urls:
                tasks.append(self.__fetch_json(url=url, session=session))
            return await self.gather(*tasks)


    def bulk_fetch(self, urls: list) -> list:
        self.result = self.loop.run_until_complete(self.__requests(urls))
        self.loop.close()
        return self.result