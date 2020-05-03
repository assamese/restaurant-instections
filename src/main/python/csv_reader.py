import pandas as pd
from smart_open import smart_open
from config_framework import ConfigFramework

class CSV_Reader:
    @staticmethod
    def readCSV():
        aws_key = 'AKIAJXZFEJVRUKRQQUQQ'
        aws_secret = ConfigFramework.getAWS_Secret()

        bucket_name = 'restaurant-data-sanjay'
        object_key = '20200501/DOHMH_New_York_City_Restaurant_Inspection_Results.csv'

        ################### sample CSV data ##################################
        #       CAMIS                      DBA      BORO  ...        BIN           BBL   NTA
        # 0  50004498  PEACHWAVE FROZEN YOGURT     Bronx  ...  2094636.0  2.030730e+09  BX06
        # 1  40378796  KNIGHTS OF BARON DEKALB  Brooklyn  ...  3248030.0  3.088150e+09  BK17
        # 2  41139432    THE HAAB MEXICAN CAFE    Queens  ...  4053065.0  4.022910e+09  QN31
        # 3  50086161           ROSALIA'S CAFE  Brooklyn  ...  3030710.0  3.012200e+09  BK61
        # 4  50006045               DREAM CAFE    Queens  ...  4010282.0  4.006540e+09  QN70
        ##############################################################################

        path = 's3://{}:{}@{}/{}'.format(aws_key, aws_secret, bucket_name, object_key)

        dfFromCSV = pd.read_csv(smart_open(path))

        print(dfFromCSV.head())
        return dfFromCSV
