import sys
import pytest
import pyspark 
from pyspark.sql import SparkSession
from chispa import assert_df_equality
from code.datachanger import DataChanger


class TestGroup:

	@pytest.fixture
	def spark_configuration(self) -> SparkSession:
		'''
		Fixture function which provide spark session for others tests
		'''
		spark = SparkSession.builder \
      .master("local") \
      .appName("chispa") \
      .getOrCreate()

		return spark

	def test_renameColumns(self, spark_configuration):
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

		df = (spark_configuration.createDataFrame(data, ['col1', 'col2', 'col3']))

		expected_df = df.withColumnRenamed('col1', 'column1')
		expected_df = expected_df.withColumnRenamed('col3', 'column3')

		actual_df = DataChanger.renameColumns(df, {
			'col1' : 'column1',
			'col3' : 'column3'
			})

		assert_df_equality(actual_df, expected_df)


	def test_filterCountry(self, spark_configuration):

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

		df = (spark_configuration.createDataFrame(data, ['country', 'city', 'continent']))

		expected_df = df.filter((df['city'].like('Berlin')) | (df['city'].like('Vienna'))) 

		actual_df = DataChanger.filterCountry(df, 'city', ['Berlin', 'Vienna'])

		assert_df_equality(actual_df, expected_df)