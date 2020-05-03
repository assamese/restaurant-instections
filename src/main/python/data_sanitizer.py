import pandas as pd

class DataSanitizer:
    # sanitize FactTable columns
    def sanitizeFactCols(df_fact_cols):
        # df_restaurant_inspections.rename(columns={'CAMIS':'InspectionID', 'GRADE':'Grade'}, inplace=True)
        # replace NaN grades with 'N'
        df_fact_cols.grade = df_fact_cols.grade.fillna('N')
        # replace NaN scores with 0
        df_fact_cols.score = df_fact_cols.score.fillna(0)

    # sanitize FactTable columns
    def sanitizeCSVCols(df_csv_cols):
        # convert type of ZIPCODE, CAMIS from int to str
        df_csv_cols['ZIPCODE'] = df_csv_cols['ZIPCODE'].astype(str)  # .apply(str)
        df_csv_cols['CAMIS'] = df_csv_cols['CAMIS'].astype(str)  # .apply(str)
        df_csv_cols['INSPECTION DATE'] = pd.to_datetime(df_csv_cols['INSPECTION DATE'], infer_datetime_format=True)
