#!/bin/sh
pip install numpy
pip install pandas
pip install smart_open
pip install psycopg2-binary
pip install SQLAlchemy

cd ../python
python schema_creator.py
python pipeline_driver.py

