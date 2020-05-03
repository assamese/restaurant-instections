import pandas as pd
import datetime
from csv_reader import CSV_Reader
from data_sanitizer import DataSanitizer
from config_framework import ConfigFramework
from dim_table_readers import DimTableReaders
from table_updaters import TableUpdaters

class PipelineDriver:
    @staticmethod
    def main():
        print("Starting Pipeline .......")
        print("Read the Inspection file: Pipeline-Task #1")
        dfFromCSV = CSV_Reader.readCSV()
        dfFromCSV = dfFromCSV.iloc[0:5]  # just 5 rows for now FOR TESTING ======

        print("Sanitize raw Inspection data: Pipeline-Task #2")
        DataSanitizer.sanitizeCSVCols(dfFromCSV)

        print("Read all dimension Tables into DataFrames: Pipeline-Task #3")
        postgres_str = ConfigFramework.getPostgresStr()
        dfDimDate = DimTableReaders.readDimDateTable(postgres_str)  # Pipeline-Task #3.1
        dfDimRestaurant = DimTableReaders.readDimRestaurantTable(postgres_str)  # Pipeline-Task #3.2
        dfDimCuisine = DimTableReaders.readDimCuisineTable(postgres_str)  # Pipeline-Task #3.3

        print("For each CSV-row, insert/update dimTables and create DF to insert into factTable: Pipeline-Task #4")
        df_fact_table_new_rows = pd.DataFrame(
            columns=['inspectionkey', 'restaurantkey', 'datekey', 'cuisinekey', 'score', 'grade', 'created_on'],
            dtype=object)
        for index, csv_row in dfFromCSV.iterrows():
            # print("csv_row: %s" % (csv_row,))
            datekey = TableUpdaters.updateDateDimension(csv_row, dfDimDate, postgres_str)
            restaurantkey = TableUpdaters.updateRestaurantDimension(csv_row, dfDimRestaurant, postgres_str)
            cuisinekey = TableUpdaters.updateCuisineDimension(csv_row, dfDimCuisine, postgres_str)
            #print("restaurantkey: %s" % (restaurantkey))
            df_fact_table_new_rows = df_fact_table_new_rows.append({'inspectionkey': None
                                                                       , 'restaurantkey': restaurantkey
                                                                       , 'datekey': datekey
                                                                       , 'cuisinekey': cuisinekey
                                                                       , 'score': csv_row['SCORE']
                                                                       , 'grade': csv_row['GRADE']
                                                                       , 'created_on': datetime.datetime.now()
                                                                    }
                                                                   , ignore_index=True
                                                                   )
        print("Sanitize factTable DF: Pipeline-Task #5")
        DataSanitizer.sanitizeFactCols(df_fact_table_new_rows)
        print(df_fact_table_new_rows.head())

        print("Append factTable DF to factTable: Pipeline-Task #6")
        TableUpdaters.appendToFactTable(df_fact_table_new_rows, postgres_str)
        print("Pipeline Finished")

if __name__ == "__main__": PipelineDriver.main()
