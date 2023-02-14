#app.py dataset_one.csv dataset_two.csv -c Netherlands 'United Kingdom'
import argparse
import pyspark 
from pyspark.sql import SparkSession
import os
import shutil
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime


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


# ------------------------------------execution part

if __name__ == "__main__":

	#creating logging instance
	logger = DataChanger.getLogger()
	logger.info(str(datetime.now()) + ': starting app')
	
	#parse arguments of filepaths of used datasets and countries 
	parser = argparse.ArgumentParser(description='pyspark codac assignment')
	parser.add_argument('path1', nargs=1)
	parser.add_argument('path2', nargs=1)
	parser.add_argument('-c', nargs='*')

	parameters = vars(parser.parse_args())

	logger.info(str(datetime.now()) + ': parsing parameters')

	filepath1 = parameters['path1'][0]
	filepath2 = parameters['path2'][0]
	country_list = parameters['c']

	logger.info(str(datetime.now()) + ': running pyspark')

	#1 starting PySpark
	spark=SparkSession.builder.appName('mk').getOrCreate()

	logger.info(str(datetime.now()) + ': loading data')

	#2 loading dataframes
	df1=spark.read.csv(filepath1, header=True, inferSchema=True)
	df2=spark.read.csv(filepath2, header=True, inferSchema=True)

	logger.info(str(datetime.now()) + ': filtering data')

	#3 filtering through countries
	df1=DataChanger.filterCountry(df1, 'country', country_list)

	#4 dropping unused column 
	df1=df1.drop('first_name','last_name','country')
	df2=df2.drop('cc_n')

	logger.info(str(datetime.now()) + ': renaming column')

	#5 renaming columns 
	df1_dict = {'id' : 'client_identifier'}

	df2_dict= {
		'id' : 'client_identifier',
		'btc_a' : 'bitcoin_address',
		'cc_t' : 'credit_card_type'
	}

	df1=DataChanger.renameColumns(df1, df1_dict)
	df2=DataChanger.renameColumns(df2, df2_dict)

	logger.info(str(datetime.now()) + ': merging data')

	#6 merging 
	df = df1.join(df2, 'client_identifier', 'left')

	logger.info(str(datetime.now()) + ': saving output')

	#7 saving output

	#first check if specifing directory exists and if not create it 
	directory='../client_data'

	if(os.path.exists(directory)):
		shutil.rmtree(directory) #deleting existing

	df.write.csv(directory)

	logger.info(str(datetime.now()) + ': Success! All tasks are completed')
