import json
import zlib
from typing import Optional

from dateutil.parser import parse
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from monster import utils
from mbuilder import mb_utils
from mbuilder.metrics_builder import metrics_builder


config    = utils.parse_config()
partition = utils.get_partition(config)
front_urls = utils.get_front_urls(config)


app = FastAPI(
    root_path=f"/api/{partition}",
    openapi_url=f"/openapi.json"
)

available_metrics = list(mb_utils.get_metrics_map(config)['idrac'].keys()) + list(mb_utils.get_metrics_map(config)['slurm'].keys())

class Request(BaseModel):
    start      : Optional[str]  = "2025-06-15 12:00:00-06"
    end        : Optional[str]  = "2025-06-15 14:00:00-06"
    interval   : Optional[str]  = "5m"
    aggregation: Optional[str]  = "max"
    nodelist   : Optional[str]  = utils.get_nodelist_raw(config)[0]
    metrics    : Optional[list] = available_metrics
    compression: Optional[bool] = False


origins = front_urls

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post(f"/")
def main(request: Request):
    start_epoch = int(parse(request.start).timestamp())
    end_epoch   = int(parse(request.end).timestamp())

    # Check if the start time is earlier than the end time
    if start_epoch > end_epoch:
        return {"error": "Start time is later than end time"}

    # Check if the time range is within the 7 days
    if (end_epoch - start_epoch) > 604800:
        return {"error": "Time range is greater than 7 days"}

    data = metrics_builder(config,
                           request.start,
                           request.end,
                           request.interval,
                           request.aggregation,
                           request.nodelist,
                           request.metrics)
    if not request.compression:
        return data
    else:
        compressed_data = zlib.compress(json.dumps(data).encode('utf-8'))
        # Create a FastAPI Response with compressed data
        response = Response(content=compressed_data)
        # Set the Content-Encoding header to indicate compressions
        response.headers["Content-Encoding"] = "deflate"
        return response
