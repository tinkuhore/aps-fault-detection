from sensor import utils
from sensor.entity import config_entity
from sensor.entity import artifact_entity
from sensor.exception import SensorException
from sensor.logger import logging
import os, sys
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

class DataIngestion:

    def __init__(self, data_ingestion_config:config_entity.DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise SensorException(e, sys)

    def initiate_data_ingestion(self)->artifact_entity.DataIngestionArtifact:
        try:
            logging.info(f"Exporting collection from MongoDB as Pandas DF")
            df:pd.DataFrame = utils.get_collection_as_dataframe(
                database_name = self.data_ingestion_config.database_name,
                collection_name= self.data_ingestion_config.collection_name)

            logging.info(f"Save data in feature store.")

            # replace na with Nan
            df.replace(to_replace="na", value=np.NAN, inplace=True)

            # Save data in feature store
            logging.info(f"Create feature store folder if not available.")
            feature_store_dir = os.path.dirname(self.data_ingestion_config.feature_store_file_path)
            os.makedirs(feature_store_dir, exists_ok=True)

            logging.info(f"Save df to feature store folder")
            df,to_csv(path_or_buf=self.data_ingestion_config.feature_store_file_path, index=False, header=True)

            # train_test split
            logging.info(f"Train_test split of the dataset")
            train_df, test_df = train_test_split(df, test_size=self.data_ingestion_config.test_size)

            logging.info(f"Create Dataset Dir if not exist")
            dataset_dir = os.path.dirname(self.data_ingestion_config.train_file_path)
            os.makedirs(dataset_dir, exist_ok=True)

            logging.info(f"save df to feature store")
            train_df.to_csv(path_or_buf=self.data_ingestion_config.train_file_path, index=False, header=True)
            test_df.to_csv(path_or_buf=self.data_ingestion_config.test_file_path, index=False, header=True)

            # prepare artifact
            data_ingestion_artifact = artifact_entity.DataIngestionArtifact(
                feature_store_file_path=self.data_ingestion_config.feature_store_file_path, 
                train_file_path=self.data_ingestion_config.train_file_path, 
                test_file_path=self.data_ingestion_config.test_file_path
                )
            logging.info(f"Data ingestion artifact : {data_ingestion_artifact}")
            return data_ingestion_artifact

        except Exception as e:
            raise SensorException(e, sys)