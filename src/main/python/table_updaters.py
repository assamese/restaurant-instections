import pandas as pd
import numpy as np
import datetime
import sqlalchemy
from sqlalchemy import create_engine, select, MetaData, Table, and_, func, case

class TableUpdaters:
    @staticmethod
    def appendToFactTable(df_fact_inspections, postgres_str):
        try:
            # Create the postgres connection
            cnx = create_engine(postgres_str)
            metadata = MetaData(bind=None)
            table_fact_inspections = Table('factInspections', metadata, autoload=True, autoload_with=cnx)
            # df.to_sql('factInspections', con=cnx, if_exists='append', index=False)
            for index, df_fact_inspection_row in df_fact_inspections.iterrows():
                stmt = table_fact_inspections.insert().values(restaurantkey=df_fact_inspection_row['restaurantkey']
                                                              , datekey=df_fact_inspection_row['datekey']
                                                              , cuisinekey=df_fact_inspection_row['cuisinekey']
                                                              , score=df_fact_inspection_row['score']
                                                              , grade=df_fact_inspection_row['grade']
                                                              , created_on=datetime.datetime.now()
                                                              )
                cnx.execute(stmt)
        finally:
            # Close the DB Connection
            cnx.dispose()

    @staticmethod
    def updateCuisineDimension(csv_row, dfDimCuisine, postgres_str):
        cnx = None
        try:
            # test if csv_row.cuisine is missing in the dfDimCuisine table
            # insert into table if missing
            cuisinekey = np.nan
            for index, dim_cuisine_row in dfDimCuisine.iterrows():
                if csv_row['CUISINE DESCRIPTION'] == dim_cuisine_row['description']:
                    cuisinekey = dim_cuisine_row['cuisinekey']
                    break
            # end-loop
            if pd.isnull(cuisinekey):  # not in DB, so insert
                # print("not in DB, so insert")
                # Create the postgres connection
                cnx = create_engine(postgres_str)
                metadata = MetaData(bind=None)
                table_dim_cuisine = Table('dimCuisine', metadata, autoload=True, autoload_with=cnx)
                stmt = table_dim_cuisine.insert().values(description=csv_row['CUISINE DESCRIPTION']
                                                         , created_on=datetime.datetime.now()
                                                         )
                cnx.execute(stmt)
                # find the key we just inserted
                stmt = select([table_dim_cuisine]).where(
                    table_dim_cuisine.columns.description == csv_row['CUISINE DESCRIPTION'])
                result = cnx.execute(stmt).fetchall()
                cuisinekey = result[0][0]
            return cuisinekey
        finally:
            # Close the DB Connection
            if (cnx != None):
                cnx.dispose()

    @staticmethod
    def updateDateDimension(csv_row, dfDimDate, postgres_str):
        # print("csv_row INSPECTION DATE: %s" % (csv_row['INSPECTION DATE']))
        year = int(csv_row['INSPECTION DATE'].strftime("%Y"))
        month = csv_row['INSPECTION DATE'].strftime("%B")
        day = int(csv_row['INSPECTION DATE'].strftime("%d"))

        cnx = None
        try:
            # test if csv_row.cuisine is missing in the dfDimCuisine table
            # insert into table if missing
            datekey = np.nan
            for index, dim_date_row in dfDimDate.iterrows():
                if year == dim_date_row['year'] and month == dim_date_row['month'] and day == dim_date_row['day']:
                    datekey = dim_date_row['datekey']
                    break
            # end-loop
            if pd.isnull(datekey):  # not in DB, so insert
                # print("not in DB, so insert")
                # Create the postgres connection
                cnx = create_engine(postgres_str)
                metadata = MetaData(bind=None)
                table_dim_date = Table('dimDate', metadata, autoload=True, autoload_with=cnx)
                stmt = table_dim_date.insert().values(year=year
                                                      , month=month
                                                      , day=day
                                                      , created_on=datetime.datetime.now()
                                                      )
                cnx.execute(stmt)
                # find the key we just inserted
                stmt = select([table_dim_date]).where(
                    table_dim_date.columns.year == year and table_dim_date.columns.month == month and table_dim_date.columns.day == day)
                result = cnx.execute(stmt).fetchall()
                datekey = result[0][0]
            return datekey
        finally:
            # Close the DB Connection
            if (cnx != None):
                cnx.dispose()

    @staticmethod
    def updateRestaurantDimension(csv_row, dfDimRestaurant, postgres_str):
        cnx = None
        try:
            # test if csv_row.restaurant is missing in the dfDimRestaurant table
            # insert into table if missing
            restaurantkey = np.nan
            for index, dim_restaurant_row in dfDimRestaurant.iterrows():
                # print("dim_restaurant_row camis: %s" % (dim_restaurant_row['camis']))
                # print("csv_row CAMIS: %s" % (csv_row['CAMIS']))
                if csv_row['CAMIS'] == dim_restaurant_row['camis']:  # CAMIS
                    restaurantkey = dim_restaurant_row['restaurantkey']  # RestaurantKey
                    break
            # end-loop
            if pd.isnull(restaurantkey):  # not in DB, so insert
                # print("not in DB, so insert")
                # Create the postgres connection
                cnx = create_engine(postgres_str)
                metadata = MetaData(bind=None)
                table_dim_restaurant = Table('dimRestaurant', metadata, autoload=True, autoload_with=cnx)
                stmt = table_dim_restaurant.insert().values(camis=csv_row['CAMIS']
                                                            , dba=csv_row['DBA']
                                                            , building=csv_row['BUILDING']
                                                            , street=csv_row['STREET']
                                                            , zip=csv_row['ZIPCODE']
                                                            , created_on=datetime.datetime.now()
                                                            )
                cnx.execute(stmt)
                # find the key we just inserted
                stmt = select([table_dim_restaurant]).where(table_dim_restaurant.columns.camis == csv_row['CAMIS'])
                result = cnx.execute(stmt).fetchall()
                restaurantkey = result[0][0]
            return restaurantkey
        finally:
            # Close the DB Connection
            if (cnx != None):
                cnx.dispose()
