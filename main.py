import os
import pandas as import pd
import dask.dataframe as dd
from sqlalchemy import create_engine
####
from secret import conn_str

# I don't know the property for this. Maybe you can also make it insert into a specific schema?
engine = create_engine(conn_str())

def get_list_of_files_in_directory(dirName):
    return os.listdir(dirName)


def get_full_path_to_file(dirName, fileName)
    return os.path.join(dirName, fileName)


def read_csv_to_ddf(fullFilePath):
    return dd.read_csv(fullFilePath)


def ddf_insert_to_postgres(ddf, tableName, engine):
    ddf.map_partition(lambda pdf: pdf.to_sql(tableName, engine, index=False, if_exists='append'))


def run(dirName):
    for fileName in get_list_of_files_in_directory(dirName):
        if ".csv" in fileName:
            fullpath = get_full_path_to_file(dirName, fileName)
            ddf = read_csv_to_ddf(fullpath)
            ddf_insert_to_postgres(ddf, fileName, engine)

# - enter directory here - #
run()