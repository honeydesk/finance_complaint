

from pyspark.sql import DataFrame
from finance_complaint.config.spark_manager import spark_session
from finance_complaint.exception import FinanceException
from finance_complaint.logger import logging as  logger
import sys


def read_data() -> DataFrame:
        try:
            dataframe: DataFrame = spark_session.read.parquet("/config/workspace/artifact/data_ingestion/feature_store/finance_complaint"
            ).limit(10000)
            logger.info(f"Number of row: {dataframe.count()} and column: {len(dataframe.columns)}")
            #dataframe, _ = dataframe.randomSplit([0.001, 0.999])
            print(dataframe.head())
        except Exception as e:
            raise FinanceException(e, sys)


read_data()