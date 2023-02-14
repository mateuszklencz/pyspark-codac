from app import DataChanger
import pytest
from chispa import assert_df_equality
import pyspark 
from pyspark.sql import SparkSession

def test_renameColumns():
	'''
	function to test static method * renameColumns * from DataChanger class. 

	It uses sample data to check wheter direct pyspark methods to **rename columns** have the same output as
	method from custom class. 

	It uses chispa testing and *assert_df_equality()* 
	'''

	data = [
		('sample11', 'sample12', 'sample13'),
		('sample21', 'sample22', 'sample23'),
		('sample31', 'sample32', 'sample33')
	]

	df = (spark.createDataFrame(data, ['col1', 'col2', 'col3']))

	expected_df = df.withColumnRenamed('col1', 'column1')
	expected_df = expected_df.withColumnRenamed('col3', 'column3')

	actual_df = DataChanger.renameColumns(df, {
		'col1' : 'column1',
		'col3' : 'column3'
		})

	assert_df_equality(actual_df, expected_df)


def test_filterCountry():

	'''
	function to test static method *filterCountry()* from DataChanger class. 

	It uses sample data to check wheter direct pyspark methods to **filter strings (isin)** have the same output as
	method from custom class. 

	It uses chispa testing and *assert_df_equality()* 
	'''

	data = [
		('Germany', 'Berlin', 'Europe'),
		('Poland', 'Warsaw', 'Europe'),
		('Germany', 'Berlin', 'Europe'),
		('Poland', 'Warsaw', 'Europe'),
		('Austria', 'Vienna', 'Europe'),
		('Poland', 'Warsaw', 'Europe')
	]

	df = (spark.createDataFrame(data, ['country', 'city', 'continent']))

	expected_df = df.filter((df['city'].like('Berlin')) | (df['city'].like('Vienna'))) 

	actual_df = DataChanger.filterCountry(df, 'city', ['Berlin', 'Vienna'])

	assert_df_equality(actual_df, expected_df)



#spark session used for testing
spark = SparkSession.builder \
      .master("local") \
      .appName("chispa") \
      .getOrCreate()
