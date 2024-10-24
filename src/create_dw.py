from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.schema import CreateSchema
from sqlalchemy import text


def create_data_warehouse(engine):
    if not database_exists(engine.url):
        create_database(engine.url)
        print(f"data warehouse created")
    else:
        print(f"data warehouse already exists")

def create_schema(engine):
    if not engine.dialect.has_schema(engine, "warehouses"):
        engine.execute(CreateSchema("warehouses"))
        print(f"schema warehouses created")
    else:
        print(f"schema warehouses already exists")

def create_tables(engine):
    create_data_warehouse(engine)
    create_schema(engine)

    with engine as conn:
        conn.execute(
            text('''
                    ALTER TABLE warehouses.dim_customers ADD CONSTRAINT pk_customer_id PRIMARY KEY (customer_id);
                    ALTER TABLE warehouses.dim_products ADD CONSTRAINT pk_product_id PRIMARY KEY (product_id);
                    ALTER TABLE warehouses.dim_sellers ADD CONSTRAINT pk_store_id PRIMARY KEY (seller_id);
                    ALTER TABLE warehouses.fact_orders_payments ADD CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES warehouses.dim_customers (customer_id);
                    ALTER TABLE warehouses.fact_orders_items ADD CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES warehouses.dim_customers (customer_id);
                    ALTER TABLE warehouses.fact_orders_items ADD CONSTRAINT fk_seller_id FOREIGN KEY (seller_id) REFERENCES warehouses.dim_sellers (seller_id);
                    ALTER TABLE warehouses.fact_orders_items ADD CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES warehouses.dim_products (product_id);
                '''
            )
        )   
    print("table constraint created")
