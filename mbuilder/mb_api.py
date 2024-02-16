import zlib
import json

from fastapi import FastAPI, Response
from pydantic import BaseModel
from typing import Optional

from mbuilder.metrics_builder import metrics_builder


class Request(BaseModel):
    start      : Optional[str]  = "2024-02-14 12:00:00-06"
    end        : Optional[str]  = "2024-02-14 14:00:00-06"
    interval   : Optional[str]  = "5m"
    aggregation: Optional[str]  = "max"
    nodelist   : Optional[str]  = "10.101.1.[1-60]"
    metrics    : Optional[list] = ['SystemPower_iDRAC', 'NodeJobsCorrelation_Slurm', 'JobsInfo_Slurm']
    compression: Optional[bool] = False

app = FastAPI()

@app.post("/quanah")
def main(request: Request):
  data = metrics_builder(request.start, 
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
    