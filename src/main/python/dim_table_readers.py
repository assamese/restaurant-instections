import sqlalchemy
from sqlalchemy import create_engine, select, MetaData, Table, and_, func, case
import pandas as pd

class DimTableReaders:
    def readDimRestaurantTable(postgres_str):
        try:
            # Create the postgres connection
            cnx = create_engine(postgres_str)
            metadata = MetaData(bind=None)

            # load entire contents of the table into df
            table_dim_restaurant = Table('dimRestaurant', metadata, autoload=True, autoload_with=cnx)
            dfDimRestaurant = pd.read_sql_query('''select * from public."dimRestaurant";''', cnx)
            # print(dfDimRestaurant.head())
            return dfDimRestaurant
        finally:
            # Close the DB Connection
            cnx.dispose()

    def readDimDateTable(postgres_str):
        try:
            # Create the postgres connection
            cnx = create_engine(postgres_str)
            metadata = MetaData(bind=None)

            # load entire contents of the table into df
            table_dim_date = Table('dimDate', metadata, autoload=True, autoload_with=cnx)
            dfDimDate = pd.read_sql_query('''select * from public."dimDate";''', cnx)
            # print(dfDimDate.head())
            return dfDimDate
        finally:
            # Close the DB Connection
            cnx.dispose()

    def readDimCuisineTable(postgres_str):
        try:
            # Create the postgres connection
            cnx = create_engine(postgres_str)
            metadata = MetaData(bind=None)

            # load entire contents of the table into df
            table_dim_cuisine = Table('dimCuisine', metadata, autoload=True, autoload_with=cnx)
            dfDimCuisine = pd.read_sql_query('''select * from public."dimCuisine";''', cnx)
            # print(dfDimCuisine.head())
            return dfDimCuisine
        finally:
            # Close the DB Connection
            cnx.dispose()

