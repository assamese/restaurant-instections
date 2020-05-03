# Restaurant Inspection Data Pipeline

Ingest data from https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j

stage data to S3

sanitize/transform

Save data into Fact/Dimension tables in Postgres

## Configuration

A remote PostgresDB hosted at elephantsql.com is used.

The password for PostgresDB and S3 needs to be plugged into this python file:
src/main/python/config_framework.py (Line 7 and Line 9)

## Usage
```bash
cd src/main/scripts
./run_driver.sh
```

## Airflow
The Airflow script is unfinished and under construction

## License
[MIT](https://choosealicense.com/licenses/mit/)
