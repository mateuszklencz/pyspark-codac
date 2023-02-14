#python3 app.py ../data/dataset_one.csv ../data/dataset_two.csv -c France
import os
import shutil
import logging
import argparse
import pyspark 
from pyspark.sql import SparkSession
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Tuple
from datachanger import DataChanger

class OutputBuilder():

	'''
	self.logger: logging.LogRecord
	self.dataset1: str
	self.dataset2: str
	self.countries: list
	self.spark: pyspark.sql.SparkSession
	self.df1: pyspark.sql.DataFrame
	self.df2: pyspark.sql.DataFrame
	self.df_merged: pyspark.sql.DataFrame
	'''
	def __init__(self):
		self.logger: logging.LogRecord
		self.dataset1: str
		self.dataset2: str
		self.countries: list
		self.spark: pyspark.sql.SparkSession
		self.df1: pyspark.sql.DataFrame
		self.df2: pyspark.sql.DataFrame
		self.df_merged: pyspark.sql.DataFrame


	def getLogger(self):
		'''
		Method creates a properly configured instance of logging from python standard library
		with a * RotatingFileHandler * and level set to * INFO *

		:output: logging with a proper settings

		'''
		logger = logging.getLogger("dataChangerLogger")
		logger.setLevel(logging.INFO)
		handler = RotatingFileHandler(filename="logs.log", maxBytes=1024, backupCount=5)
		logger.addHandler(handler)

		self.logger = logger

		self.logReq('log started')

		return self


	def parseArgs(self):

		self.logReq('parsing parameters')

		#parse arguments of filepaths of used datasets and countries 
		parser = argparse.ArgumentParser(description='pyspark codac assignment')
		parser.add_argument('path1', nargs=1, default='../data/dataset_one.csv',
			help='system path to first dataset', type=str)
		parser.add_argument('path2', nargs=1, default='../data/dataset_two.csv',
			help='system path to second dataset', type=str)
		parser.add_argument('-c', nargs='*', default=["United Kingdom", "Netherlands"],
			help='countries for which extract data separeted by space', required=False)

		parameters = vars(parser.parse_args())

		filepath1 = parameters['path1'][0]
		filepath2 = parameters['path2'][0]
		country_list = parameters['c']

		self.dataset1 = filepath1
		self.dataset2 = filepath2
		self.countries = country_list

		print(self.dataset1)
		print(self.dataset2)
		print(self.countries)

		return self


	def settingSparkSession(self):
		self.logReq('starting spark')

		self.spark=SparkSession.builder.appName('mk').getOrCreate()

		return self


	def loadingDataFrames(self):
		self.logReq('loading dataframes')

		self.df1=self.spark.read.csv(self.dataset1, header=True, inferSchema=True)
		self.df2=self.spark.read.csv(self.dataset2, header=True, inferSchema=True)

		return self

	def droppingUnusedColumns(self, list_of_columns_for_df1: list, list_of_columns_for_df2: list):
		self.logReq('deleting unused columns')

		for c in list_of_columns_for_df1:
			self.df1=self.df1.drop(c)

		for c in list_of_columns_for_df2:
			self.df2=self.df2.drop(c)

		return self

	def renameColumns(self, dict_for_df1: dict, dict_for_df2: dict):
		self.logReq('renaming columns')

		self.df1=DataChanger.renameColumns(self.df1, dict_for_df1)
		self.df2=DataChanger.renameColumns(self.df2, dict_for_df2)

		return self

	def mergeDataFrames(self, column_id: str):
		self.logReq('merging dataframes')

		self.mergedDf=self.df1.join(self.df2, column_id, 'left')

		return self

	def filterByCountry(self):
		self.logReq('filtering by country')

		self.df1=DataChanger.filterCountry(self.df1, 'country', self.countries)

		return self


	def saveMergedOutput(self, directory='../client_data'):
		self.logReq('saving output')


		if(os.path.exists(directory)):
			shutil.rmtree(directory) #deleting existing

		self.mergedDf.write.csv(directory)

	def logReq(self, msg: str):
		if self.logger != None:
			self.logger.info(str(datetime.now()) + ": " + msg)


# ------------------------------------execution part

if __name__ == "__main__":

	output = OutputBuilder().getLogger().parseArgs()\
		.settingSparkSession().loadingDataFrames()\
		.filterByCountry().droppingUnusedColumns(['first_name','last_name','country'],['cc_n'])\
		.renameColumns({'id' : 'client_identifier'}, {
		'id' : 'client_identifier',
		'btc_a' : 'bitcoin_address',
		'cc_t' : 'credit_card_type'
		}).mergeDataFrames('client_identifier')


	output.saveMergedOutput()