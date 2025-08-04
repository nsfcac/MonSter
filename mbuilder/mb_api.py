import json
import zlib
import hostlist
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

    # Check if the requested nodes are valid
    if request.nodelist:
        valid_nodes   = utils.get_nodelist(config)
        try:
            request_nodes = hostlist.expand_hostlist(request.nodelist)
        except Exception as e:  # Handle potential errors in expanding the hostlist
            return {"error": f"Failed to expand nodelist: {e}"}
        invalid_nodes = [node for node in request_nodes if node not in valid_nodes]
        if invalid_nodes:
           return {"error": f"Invalid nodes: {', '.join(invalid_nodes)}"}
    else:
        request.nodelist = utils.get_nodelist(config)
    
    # Check the interval format. Start with a number followed by 's', 'm', or 'h'. The number should be greater than 0.
    if not request.interval or not request.interval[:-1].isdigit() or int(request.interval[:-1]) <= 0 or request.interval[-1] not in ['s', 'm', 'h']:
        return {"error": "Invalid interval format. Use a positive number followed by 's', 'm', or 'h'."}

    # Check if the aggregation method is valid
    if request.aggregation not in ['max', 'min', 'avg', 'sum']:
        return {"error": "Invalid aggregation method. Use 'max', 'min', 'avg', or 'sum'."}


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
