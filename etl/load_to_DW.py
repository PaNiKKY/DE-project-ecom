import pandas as pd
from .transform_DW import tables_constraint

def load_to_postgres(df: pd.DataFrame, engine, table_name: str):
    df.to_sql(table_name, engine, if_exists="append", index=False, schema="warehouses")
    print(f"Table {table_name} loaded to data warehouse successfully")

    if not engine.dialect.has_schema(engine, "warehouses"):
        tables_constraint(engine)
    else:
        print(f"Table {table_name} constraint was created")


