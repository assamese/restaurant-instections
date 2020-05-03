import psycopg2
from config_framework import ConfigFramework

class SchemaCreator:
    @staticmethod
    def create_tables():
        """ create tables in the PostgreSQL database"""
        commands = (
            """
            CREATE TABLE IF NOT EXISTS public."factInspections" (
             	inspectionkey serial PRIMARY KEY,
                restaurantkey bigint NOT NULL,
                datekey bigint NOT NULL,
                cuisinekey bigint,
                score int,
                grade char,
                created_on TIMESTAMP NOT NULL
            )
            """
            ,
            """
            CREATE TABLE IF NOT EXISTS public."dimDate" (
                datekey serial PRIMARY KEY,
                year int NOT NULL,
                month VARCHAR (10) NOT NULL,
                day int NOT NULL,
                created_on TIMESTAMP NOT NULL
            )
            """
            ,
            """
            CREATE TABLE IF NOT EXISTS public."dimRestaurant" (
                RestaurantKey serial PRIMARY KEY,
                CAMIS VARCHAR (50) NOT NULL,
                DBA VARCHAR (255) NOT NULL,
                BUILDING VARCHAR (255) NOT NULL,
                STREET VARCHAR (255) NOT NULL,
                ZIP VARCHAR (10) NOT NULL,
                created_on TIMESTAMP NOT NULL
            )
            """
            ,
            """
            CREATE TABLE IF NOT EXISTS public."dimCuisine" (
                cuisinekey serial PRIMARY KEY,
                description VARCHAR (255) NOT NULL,
                created_on TIMESTAMP NOT NULL
            )
            """
        )

        print("Creating DB Tables: Pipeline-Task #0")
        print("Establishing connection to DB ....")
        conn = None
        try:
            conn = psycopg2.connect(ConfigFramework.getPsycopg2Str())
            #conn = psycopg2.connect("dbname='ljalsmbf' user='ljalsmbf' host='raja.db.elephantsql.com' password='S9Azyxk9xVsYn53mITCB64EhG8G-PmZQ'")
            cur = conn.cursor()
            # create table one by one
            print("Creating table one by one ....")
            for command in commands:
                cur.execute(command)
            # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            conn.commit()
            print("Finished creating DB Tables successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


if __name__ == "__main__": SchemaCreator.create_tables()
