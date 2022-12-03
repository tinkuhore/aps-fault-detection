import pandas as pd
from sensor.config import mongo_client
from sensor.logger import logging
from sensor.exception import SensorException
import os, sys

def get_collection_as_dataframe(database_name:str, collection_name:str):
    """
    This function return collection as dataframe

    :param: database_name: database name in MongoDB
    :param: collection_name: collection name in MongoDB
    :return: pandas dataframe of any collection
    """
    try:
        logging.info(f"Loading data from Database {database_name} and Collection {collection_name}")
        df = pd.DataFrame(list(mongo_client[database_name][collection_name].find()))
        logging.info(f"Found columns : {df.columns}")
        if "_id" in df.columns:
            logging.info(f"Dropping column : _id")
            df = df.drop("_id", axis=1)
        logging.info(f"Rows and Columns in df : {df.shape}")
        return df
    except Exception as e:
        raise SensorException(e, sys)