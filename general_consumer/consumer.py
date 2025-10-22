import requests
import os
from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Integer, Float,
    Boolean, JSON, inspect
)

try:
    with open('.env') as f:
        for line in f:
            key, value = line.strip().split('=')
            os.environ[key] = value
except FileNotFoundError:
    print("Error: Could not find .env file")
    sys.exit(1)


psql_url = os.environ.get('POSTGRES_URL')
psql_user = os.environ.get('POSTGRES_USER')
psql_password = os.environ.get('POSTGRES_PASSWORD')
psql_db = os.environ.get('POSTGRES_DB')
DATABASE_URL = f"postgresql://{psql_user}:{psql_password}@{psql_url}/{psql_db}"



# Call the Odoo URL defined in the env variable ODOO_URL
odoo_url = os.environ.get('ODOO_URL')
model = os.environ.get('MODEL')

response = requests.get(f"{odoo_url}/api/model_details", params={'model': model})

if response.status_code == 200:
    data = response.json()
    table = data.get('name').replace('.', '_')
    create_table_if_not_exists(table)
    
else:
    print(f"Error: {response.status_code} - {response.text}")





def create_table_if_not_exists(table_name, fields):
    engine = create_engine(DATABASE_URL)
    metadata = MetaData()

    SQLA_TYPE_MAP = {
        "char": String,
        "text": String,
        "html": String,
        "selection": String,
        "binary": String,
        "many2one": String,
        "one2many": String,
        "many2many": String,
        "boolean": Boolean,
        "integer": Integer,
        "float": Float,
        "monetary": Float,
        "date": Date,
        "datetime": DateTime,
    }

    table = Table(
        table_name, 
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('model', String),
        Column('operation', String),
        Column('timestamp', String),
        Column('by', String),
        Column('odoo_internal_id', Integer)
    )