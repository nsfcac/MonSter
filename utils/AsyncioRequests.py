import logging
import time


class AsyncioRequests:
    import aiohttp
    import asyncio
    from aiohttp import ClientSession

    def __init__(self, verify_ssl: bool = False, auth: tuple = (),
                 timeout: tuple = (15, 45), max_retries: int = 3):
        self.metrics = {}
        self.timestamp = int(time.time() * 1000000000)
        self.retry = 0
        self.connector = self.aiohttp.TCPConnector(verify_ssl=verify_ssl)
        if auth:
            self.auth = self.aiohttp.BasicAuth(*auth)
        else:
            self.auth = None
        self.timeout = self.aiohttp.ClientTimeout(*timeout)
        self.max_retries = max_retries
        self.loop = self.asyncio.get_event_loop()

    async def __fetch_json(self,
                           url: str,
                           node: str,
                           session: ClientSession):
        """__fetch_json Fetch Url
        Get request wrapper to fetch json data from API
        Args:
            url (str): url of idrac
            node (str): ip address of the idrac
            session (ClientSession): Client Session
        Returns:
            dict: The return of url in json format
        """

        try:
            resp = await session.request(method='GET', url=url)
            resp.raise_for_status()
            json = await resp.json()
            return {"node": node,
                    "metrics": json,
                    "timestamp": self.timestamp}
        except (TimeoutError):
            self.retry += 1
            if self.retry >= self.max_retries:
                logging.error(f"Cannot fetch data from {node} : {url}")
                return {"node": node,
                        "metrics": {},
                        "timestamp": self.timestamp}
            return await self.__fetch_json(url, node, session)
        except Exception as err:
            logging.error(f"Cannot fetch data from {url} : {err}")
            return {"node": node,
                    "metrics": {},
                    "timestamp": self.timestamp}

    async def __requests(self, urls: list, nodes: list):
        async with self.ClientSession(connector=self.connector,
                                      auth=self.auth,
                                      timeout=self.timeout) as session:
            tasks = []
            for i, url in enumerate(urls):
                tasks.append(self.__fetch_json(url=url,
                                               node=nodes[i],
                                               session=session))
            return await self.asyncio.gather(*tasks)

    def bulk_fetch(self, urls: list, nodes: list):
        self.metrics = self.loop.run_until_complete(
            self.__requests(urls, nodes))
        self.loop.close()
        return self.metrics
