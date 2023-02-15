from pyspark.sql.dataframe import DataFrame


class DataChanger:

    @staticmethod
    def renameColumns(df: DataFrame, columns: dict) -> DataFrame:
        '''
        Generic function for renaming columns in provided PySpark DataFrame. 
        It returns DataFrame with changed names. 
        Original df remains without any changes 
        (unless replaced by the result of this method). 
 
        :df: pyspark dataframe in which columns name should be renamed
        :columns: dictionary in which key is the old name and value is new name
        :output: pyspark dataframe with changed columns name
        '''
        for key in columns:
            df = df.withColumnRenamed(key, columns[key])

        return df

    @staticmethod
    def filterCountry(df: DataFrame, col_name: str, filter: list) -> DataFrame:
        '''
        Generic function for filtering columns in provided PySpark DataFrame. 
        It returns filtered original dataframe. 
        Original df remains without any changes (unless replaced by the result of this method). 

        :df: pyspark dataframe in which data should be filtered using *filter* parameter
        :col_name: name of column to which filter should be applied
        :filter: list of **strings** for which equality filtering will take place (string == value in column)
        :output: pyspark dataframe which is copy of input dataframe but limited only to rows with a data from filter list values
        '''
        df=df[df[col_name].isin(filter)]

        return df