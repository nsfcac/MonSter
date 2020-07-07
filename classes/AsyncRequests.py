import json
import requests
import threading
import multiprocessing
from queue import Queue

from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth

thread_local = threading.local()


class AsyncRequests:
    """
    Async requests to urls
    """
    def __init__(self, ssl_verify: bool, max_retries: int, timeout: tuple, auth: tuple):
        self.result = []
        self.ssl_verify = False
        self.max_retries = 3
        self.timeout = (15, 45)
        self.auth = ()


    def requests(self, urls: list) -> list:
        # Partition urls
        cores = multiprocessing.cpu_count()

        urls_set = []
        urls_len = len(urls)

        urls_per_core = urls_len // cores
        surplus_urls = urls_len % cores

        increment = 1
        for i in range(cores):
            if(surplus_urls !=0 and i == (cores-1)):
                urls_set.append(urls[i * urls_per_core])
            else:
                urls_set.append(urls[i * urls_per_core : increment * urls_per_core])
                increment += 1
        
        # Each core takes care of a subset of urls
        with multiprocessing.Pool(processes=cores) as pool:
            responses = [pool.apply_async(self.__request_thread, args = (split_urls)) 
                         for split_urls in urls_set]
            self.result = [response.get() for response in responses]

        return self.result

    
    def __request_thread(self, split_urls: list) -> list:
        q = Queue(maxsize=0)
        metrics = [{} for url in split_urls]

        for i in range(len(split_urls)):
            q.put((i, split_urls[i]))

        for i in range(len(split_urls)):
            worker = threading.Thread(target=self.__get_metrics, args=(q, metrics))
            worker.setDaemon(True)
            worker.start()
        
        q.join()

        return metrics

    
    def __get_metrics(self, q: object, metrics: list) -> None:
        while not q.empty():
            work = q.get()
            index = work[0]
            url = work[1]

            adapter = HTTPAdapter(max_retries=self.max_retries)
            
            session = self.__get_session()
            session.mount(url, adapter)

            if self.auth:
                response = session.get(
                    url, verify = self.ssl_verify, timeout = self.timeout,
                    auth = HTTPBasicAuth(self.auth[0], self.auth[1])
                )
            else:
                response = session.get(
                    url, verify = self.ssl_verify, timeout = self.timeout
                )
            
            metric = response.json()

            metrics[index] = metric
            q.task_done()
        return True
    

    def __get_session(self):
        if not hasattr(thread_local, "session"):
            thread_local.session = requests.Session()
        return thread_local.session



            





    