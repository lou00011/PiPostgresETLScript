import pandas as pd
from sqlalchemy import create_engine

# from secret import conn_str
# engine = create_engine(conn_str)

schema_name = "external"
table_name = "covid_canada"
url = "https://docs.google.com/spreadsheets/d/1D6okqtBS3S2NRC7GFVHzaZ67DuTw7LX49-fqSLwJyeo/export?format=csv"


def read_csv_from_url(url):
    return pd.read_csv(url, header=3)


def to_postgres(pdf, name, engine):
    pdf.to_sql(name, engine, index=False, if_exists='replace')


def get_full_table_name(schema_name, table_name):
    return schema_name + "." + table_name


def test_print_dataframe(pandas_data_frame):
    print(pandas_data_frame.to_string(max_rows=5))

def run():
    pdf = read_csv_from_url(url)
    full_table_name = get_full_table_name(schema_name, table_name)
    test_print_dataframe(pdf)


run()