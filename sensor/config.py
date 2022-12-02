import pymongo
import pandas as pd
import json
import os

@dataclass


# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient()

DATABASE_NAME = "aps"
COLLECTION_NAME = "sensor"
DATA_FILE_PATH = "https://raw.githubusercontent.com/avnyadav/sensor-fault-detection/main/aps_failure_training_set1.csv"
