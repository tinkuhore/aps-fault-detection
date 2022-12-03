import pymongo
import pandas as pd
import json
import os
from dataclasses import dataclass

@dataclass
class EnvironmentVariable():
    mongo_db_url:str = os.getenv("MONGO_DB_URL")

# Provide the mongodb localhost url to connect python to mongodb.
env_var = EnvironmentVariable()
client = pymongo.MongoClient(env_var.mongo_db_url)
