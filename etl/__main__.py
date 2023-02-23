import os
import shutil
import logging
import argparse
import pyspark 
from typing import Tuple
from pyspark.sql import SparkSession
from logging.handlers import RotatingFileHandler
from datetime import datetime
from datachanger import DataChanger


class OutputBuilder():
    '''
    Class is a simplified interpretation of Builder Design Pattern.
    Its description can be found on: 
    - https://python-patterns.guide/gang-of-four/builder/
    - https://stackoverflow.com/questions/11977279/builder-pattern-equivalent-in-python
    - E. Gamma, R. Helm, R. Johnson, J. Vlissides, Design Patterns 2009, s. 97-107
    It is simplified because only one class is used as a builder, product and director. 

    Main idea is about creating methods which take *self* (if necessary, other parameters)
    and as output it also returns self, but with changed data. 

    This allows the preferred instance (product) to be built according to need. 
    For example, creating an instance that uploads only datasets and a filter list without 
    creating a logger and running pyspark is possible in the following form:

    > instance = OutputBuilder().loadDataSets('filepath1','filepath2').setCountryList(country_list)

    It is also possible to build an instance that only creates a logger without any further calls:

    > instance = OutputBuilder().getLogger()

    Only the methods needed to perform this task are defined. The class could be extended and generalised. 

    In constructor this class only indicate which attributes are needed to perform all nedeed tasks.
    Subsequent methods assign values to these attributes and create final output. 
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
        Method creates a properly configured instance of 
        logging from python standard library
        with a *RotatingFileHandler* and level set to *INFO*

        :output: logging with a proper settings
        '''
        logger = logging.getLogger("dataChangerLogger")
        logger.setLevel(logging.INFO)
        handler = RotatingFileHandler(filename="logs.log", maxBytes=1024, backupCount=5)
        logger.addHandler(handler)

        self.logger = logger

        self.logReq('log started')

        return self

    def loadDataSets(self, file_path1: str, file_path2: str):
        '''
        Method takes paths to the first and second data files on which the transformation is to be made

        :file_path1: string file path to dataset with personal data
        :file_path2: string file path to dataset with financial data
        '''
        self.logReq('loading datasets filepaths')

        self.dataset1 = file_path1
        self.dataset2 = file_path2

        return self

    def setCountryList(self, country_list: list):
        '''
        Method takes list of countries in string format by which data should be filtered 

        :country_list: list of strings which indicate on which country output should be based
        '''    	
        self.countries = country_list

        return self

    def setSparkSession(self):
        '''
        Sets a new Spark Session. 

        Neccessary if we would like to perform pyspark-related tasks in next steps
        '''
        self.logReq('starting spark')

        self.spark = SparkSession.builder.appName('mk').getOrCreate()

        return self

    def loadDataFrames(self):
        '''
        Loads data from self.dataset1 and self.dataset2 to pyspark, changing them
        into pyspark DatFrame (from .csv)
        '''
        self.logReq('loading dataframes')

        self.df1 = self.spark.read.csv(self.dataset1, header=True, inferSchema=True)
        self.df2 = self.spark.read.csv(self.dataset2, header=True, inferSchema=True)

        return self

    def dropUnusedColumns(self, list_of_columns_for_df1: list, list_of_columns_for_df2: list):
        '''
        Drops unused and confidential data (columns) from sell.df1 and self.df2

        :list_of_columns_for_df1: list of strings of names of columns in self.df1 which should be dropped
        :list_of_columns_for_df2: list of strings of names of columns in self.df2 which should be dropped
        '''
        self.logReq('deleting unused columns')

        for c in list_of_columns_for_df1:
            self.df1 = self.df1.drop(c)

        for c in list_of_columns_for_df2:
            self.df2 = self.df2.drop(c)

        return self

    def renameColumns(self, dict_for_df1: dict, dict_for_df2: dict):
        '''
        Use DataChanger class from datachanger and renames columns in self.df1 and self.df2

        :dict_for_df1: dictionary in format "old value":"new value" for changing names of columns of self.df1
        :dict_for_df2: dictionary in format "old value":"new value" for changing names of columns of self.df2
        '''
        self.logReq('renaming columns')

        self.df1 = DataChanger.renameColumns(self.df1, dict_for_df1)
        self.df2 = DataChanger.renameColumns(self.df2, dict_for_df2)

        return self

    def mergeDataFrames(self, column_id: str):
        '''
        Merges a self.df1 and self.df2 as left join, based on column with name provided as param

        :columnd_id: column name as string on which data will be joined
        '''
        self.logReq('merging dataframes')

        self.mergedDf = self.df1.join(self.df2, column_id, 'left')

        return self

    def filterByCountry(self):
        '''
        Use DataChanger class from datachanger module and filter first dataframe 
        using a self.countries attribute which should be a list of countries 
        '''
        self.logReq('filtering by country')

        self.df1 = DataChanger.filterCountry(self.df1, 'country', self.countries)

        return self

    def saveMergedOutput(self, directory='../client_data'):
        '''
        Saves to a csv file an attribute *self.merged_df*

        :directory: directory in which output data should be saved
        '''
        self.logReq('saving output')

        if (os.path.exists(directory)):
            shutil.rmtree(directory)  #deleting existing directory

        self.mergedDf.write.csv(directory, header=True)

    def logReq(self, msg: str):
        '''
        Check if instance has a logger and if so 
        it takes a message in the form of a string 
        which it then saves as a log with the addition of the current time

        :msg: a string message which is printed and saved as a log
        '''
        if self.logger is not None:
            self.logger.info(str(datetime.now()) + ": " + msg)


# ------------------------------------execution part
if __name__ == "__main__":

    def parseArgs() -> Tuple[str, str, list]:
        
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

        print(filepath1)
        print(filepath2)
        print(country_list)	

        return filepath1, filepath2, country_list

    inputs = parseArgs()

    output = (OutputBuilder().getLogger().loadDataSets(inputs[0], inputs[1])
        .setCountryList(inputs[2]).setSparkSession().loadDataFrames()
        .filterByCountry().dropUnusedColumns(['first_name', 'last_name', 'country'], ['cc_n'])
        .renameColumns(
            {'id': 'client_identifier'}, 
            {
            'id': 'client_identifier',
            'btc_a': 'bitcoin_address',
            'cc_t': 'credit_card_type'
            }).mergeDataFrames('client_identifier')
    )

    output.saveMergedOutput()