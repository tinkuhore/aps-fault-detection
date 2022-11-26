import pymongo
import pandas as pd
import json

# Provide the mongodb localhost url to connect python to mongodb.
client = pymongo.MongoClient("mongodb://localhost:27017/neurolabDB")

DATABASE_NAME = "aps"
COLLECTION_NAME = "sensor"
DATA_FILE_PATH = "https://raw.githubusercontent.com/avnyadav/sensor-fault-detection/main/aps_failure_training_set1.csv"

if __name__ == "__main__":
    df = pd.read_csv(DATA_FILE_PATH)
    print("Rows and Columns : ", df.shape)

    # comment df into json to dump these records into MongoDB
    df.reset_index(drop=True, inplace=True)

    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])

    # insert json records into MongoDB
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)