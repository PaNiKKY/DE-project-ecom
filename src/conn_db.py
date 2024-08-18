from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.schema import CreateSchema
from .constants import DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME

def create_data_warehouse():
    engine = create_engine(f"postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}")

    if not database_exists(engine.url):
        create_database(engine.url)
        print(f"database {DATABASE_NAME} created")
    else:
        print(f"database {DATABASE_NAME} already exists")
    
    if not engine.dialect.has_schema(engine, "warehouses"):
        engine.execute(CreateSchema("warehouses"))
        print(f"schema warehouses created")
    else:
        print(f"schema warehouses already exists")
    
    return engine.connect()
