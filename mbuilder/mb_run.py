import uvicorn
from mbuilder.mb_api import app

from monster import utils

api_config = utils.parse_config().get("openapi", {})

uvicorn.run("mbuilder.mb_api:app", 
            host=api_config.get("host", "0.0.0.0"), 
            port=api_config.get("port", 5000), 
            ssl_keyfile=api_config.get("ssl_keyfile"),
            ssl_certfile=api_config.get("ssl_certfile"))
