import logging
import pyspark 
from pyspark.sql import SparkSession
from logging.handlers import RotatingFileHandler

class DataChanger:

	@staticmethod
	def renameColumns(df: pyspark.sql.dataframe.DataFrame, columns: dict) -> pyspark.sql.dataframe.DataFrame:
		'''
		Generic function for renaming columns in provided PySpark DataFrame. It returns DataFrame with changed names. 
		Original df remains without any changes (unless replaced by the result of this method). 

		:df: pyspark dataframe in which columns name should be renamed
		:columns: python dictionary in which key is the old name and value is the new name
		:output: pyspark dataframe which is copy of input dataframe but with changed columns name
		'''
		for key in columns:
			df=df.withColumnRenamed(key,columns[key])

		return df

	@staticmethod
	def filterCountry(df: pyspark.sql.dataframe.DataFrame, col_name: str, filter: list) -> pyspark.sql.dataframe.DataFrame:
		'''
		Generic function for filtering columns in provided PySpark DataFrame. It returns filtered original dataframe. 
		Original df remains without any changes (unless replaced by the result of this method). 

		:df: pyspark dataframe in which data should be filtered using *filter* parameter
		:col_name: name of column to which filter should be applied
		:filter: list of ** strings ** for which equality filtering will take place (string == value in column)
		:output: pyspark dataframe which is copy of input dataframe but limited only to rows with a data from filter list values
		'''

		df=df[df[col_name].isin(filter)]

		return df


	@staticmethod
	def getLogger() -> logging.LogRecord:
		'''
		Method creates a properly configured instance of logging from python standard library
		with a * RotatingFileHandler * and level set to * INFO *

		:output: logging with a proper settings

		'''
		logger = logging.getLogger("dataChangerLogger")
		logger.setLevel(logging.INFO)
		handler = RotatingFileHandler(filename="logs.log", maxBytes=1024, backupCount=5)
		logger.addHandler(handler)

		return logger

	@staticmethod
	def testFunc():
		print('DataChanger class is imported')
