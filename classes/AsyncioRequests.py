import json


class AsyncioRequests:
    """
    Asyncio requests to urls
    """
    import aiohttp
    from asyncio import gather
    from aiohttp import ClientSession


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


    async def request(self, urls: list) -> list:
        async with self.ClientSession() as session:
            tasks = []
            for url in urls:
                tasks.append(self.__fetch_json(url=url, session=session))
            self.result = await self.gather(*tasks)
        return self.result