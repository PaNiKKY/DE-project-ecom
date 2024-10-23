from sqlalchemy import text

def tables_constraint(engine):
    with engine as conn:
        conn.execute(text('''
                            ALTER TABLE warehouses.customers ADD CONSTRAINT pk_customer_id PRIMARY KEY (customer_id);
                            ALTER TABLE warehouses.products ADD CONSTRAINT pk_product_id PRIMARY KEY (product_id);
                            ALTER TABLE warehouses.sellers ADD CONSTRAINT pk_store_id PRIMARY KEY (seller_id);
                            ALTER TABLE warehouses.orders_payments ADD CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES warehouses.customers (customer_id);
                            ALTER TABLE warehouses.orders_items ADD CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES warehouses.customers (customer_id);
                            ALTER TABLE warehouses.orders_items ADD CONSTRAINT fk_seller_id FOREIGN KEY (seller_id) REFERENCES warehouses.sellers (seller_id);
                            ALTER TABLE warehouses.orders_items ADD CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES warehouses.products (product_id);
                          '''))
        
    print("table constraint created")