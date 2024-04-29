"""
MIT License

Copyright (c) 2024 Texas Tech University

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

"""
This file is part of MetricBuilder.

Author:
    Jie Li, jie.li@ttu.edu
"""

import os
import sys
import zlib
import json

from fastapi import FastAPI, Response
from pydantic import BaseModel
from typing import Optional
from dateutil.parser import parse
from fastapi.middleware.cors import CORSMiddleware

cur_dir     = os.path.dirname(__file__)
monster_dir = os.path.join(cur_dir, '../monster')
sys.path.append(monster_dir)

import utils
from mbuilder.metrics_builder import metrics_builder

partition = utils.get_partition()


class Request(BaseModel):
    start      : Optional[str]  = "2024-04-20 12:00:00-06"
    end        : Optional[str]  = "2024-04-20 14:00:00-06"
    interval   : Optional[str]  = "5m"
    aggregation: Optional[str]  = "max"
    nodelist   : Optional[str]  = utils.get_nodelist_raw()[0]
    metrics    : Optional[list] = ['SystemPower_iDRAC', 'NodeJobsCorrelation_Slurm', 'JobsInfo_Slurm', 'MemoryUsage_Slurm', 'MemoryUsed_Slurm']
    compression: Optional[bool] = False


app = FastAPI()

origins = [
    "http://localhost:5000",
    "http://127.0.0.1:5000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers={"*"},
)


@app.post(f"/{partition}")
def main(request: Request):
  start_epoch = int(parse(request.start).timestamp())
  end_epoch   = int(parse(request.end).timestamp())
  
  # Check if the start time is earlier than the end time
  if start_epoch > end_epoch:
    return {"error": "Start time is later than end time"}
  
   # Check if the time range is within the 7 days
  if (end_epoch - start_epoch) > 604800:
    return {"error": "Time range is greater than 7 days"}
  
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
    
