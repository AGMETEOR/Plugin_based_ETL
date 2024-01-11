import requests
import logging
import json
from typing import Any
import pandas as pd
from pandas import json_normalize, DataFrame
from sqlalchemy import create_engine, types
from core.plugin import PluginCore
from core.exceptions import StatusFileReadError, StatusFileWriteError, NoDataFoundException


class ETL():
    """
    Class that implements the ETL functions
    """
    def __init__(self, config: Any, etl_key: str, logger: logging.Logger):
        self.logger = logger
        self.key = etl_key
        self.config = config
        self.base_url = config["api"]
        self.engine = create_engine(config["destination"]["postgres"]["url"])
        self.indicator = config["indicator"]

    def construct_api_url(self, page_size, calculated_skip) -> str:
        return f"{self.base_url}/{self.indicator}?$count=true&$top={page_size}&$skip={calculated_skip}"

    def extract(self, page_size: int, calculated_skip: int) -> DataFrame:
        """
        Fetch data from GHO OData API
        """
        df = pd.DataFrame()
        try:
            headers = {'Content-type': 'application/json'}
            uri = self.construct_api_url(page_size, calculated_skip)
            request = requests.post(uri, headers=headers)
            data = json.loads(request.text)
            data_json = data['value']
            if data_json == []:
                raise NoDataFoundException("No data available in the 'value' field.")
            df = json_normalize(data_json)
        except requests.exceptions.RequestException:
            raise
        except NoDataFoundException:
            raise
        except Exception:
            raise
        return df

    def transform(self, data_frame: DataFrame) -> DataFrame:
        """
        Applies some transformations by removing some columns from the pandas data frame and renaming
        others inorder to prepare the data frame for storage in the postgres data base
        """
        columns = self.config["transform"]["columns"]
        transformed_df = data_frame[list(columns.keys())].rename(columns=columns)

        self.logger.info(f"DATA FRAME{transformed_df}")
        return transformed_df

    def load(self, df):
        """
        Save the data in a postgres database
        """
        df.to_sql(self.indicator,
                  con=self.engine,
                  index=False,
                  if_exists='append',
                  dtype={col: types.VARCHAR(255) for col in df.columns})

    def start(self):
        try:
            status_data = PluginCore.read_status_file(".psystem/status.yaml", self.logger)
            self.logger.info(f"Found status data: {status_data}")

            plugin_status_key = "GHOTopOSTGRES"
            page_size = 2
            page_num = 1
            etl_key = self.key

            if plugin_status_key in status_data['plugins']:
                if etl_key in status_data['plugins'][plugin_status_key]:
                    self.logger.info("Plugin status data available. Using it...")
                    page_num = status_data['plugins'][plugin_status_key][etl_key]['page_num']
                else:
                    status_data['plugins'][plugin_status_key][etl_key] = {'page_num': 1}

            else:
                status_data['plugins'] = {plugin_status_key: {etl_key: {'page_num': 1}}}
                PluginCore.write_status_file(".psystem/status.yaml", status_data)

            while True:
                skip = (page_num - 1) * page_size
                df = self.extract(page_size, skip)
                transformed_data = self.transform(df)
                self.load(transformed_data)
                page_num += 1
                status_data['plugins'][plugin_status_key][etl_key]['page_num'] = page_num
                PluginCore.write_status_file(".psystem/status.yaml", status_data)
        except StatusFileReadError as e:
            self.logger.error(f"Error reading status file: {e}")
        except StatusFileWriteError as e:
            self.logger.error(f"Error writing status file: {e}")
        except NoDataFoundException as e:
            self.logger.info(f"No data was found: {e}")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred during data extraction: {e}")
