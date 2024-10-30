import pandas as pd

def customer_clean_df(customer_df):
    # customer_df = pd.read_csv(f"/opt/airflow/data/{customer}.csv")
    customer_df["order_id"] = customer_df["customer_id"].astype(str)
    customer_df["customer_unique_id"] = customer_df["customer_unique_id"].astype(str)
    customer_df["customer_zip_code_prefix"] = customer_df["customer_zip_code_prefix"].astype(str)
    customer_df["customer_city"] = customer_df["customer_city"].astype(str)
    customer_df["customer_state"] = customer_df["customer_state"].astype(str)
    customer_df = customer_df.drop_duplicates()
    return customer_df

def geolocation_clean_df(geolocation_df):
    # geolocation_df = pd.read_csv(f"/opt/airflow/data/{geolocation}.csv")
    geolocation_df["geolocation_zip_code_prefix"] = geolocation_df["geolocation_zip_code_prefix"].astype(str)
    geolocation_df["geolocation_lat"] = geolocation_df["geolocation_lat"].astype(float)
    geolocation_df["geolocation_lng"] = geolocation_df["geolocation_lng"].astype(float)
    geo_uniq = geolocation_df.groupby(by=["geolocation_zip_code_prefix"], as_index=False)[["geolocation_lat","geolocation_lng"]].mean()
    return geo_uniq

def order_items_clean_df(order_items_df):
    # order_items_df = pd.read_csv(f"/opt/airflow/data/{order_items}.csv")
    order_items_df["order_id"] = order_items_df["order_id"].astype(str)
    order_items_df["order_item_id"] = order_items_df["order_item_id"].astype(str)
    order_items_df["product_id"] = order_items_df["product_id"].astype(str)
    order_items_df["seller_id"] = order_items_df["seller_id"].astype(str)
    order_items_df["shipping_limit_date"] = pd.to_datetime(order_items_df["shipping_limit_date"], errors="coerce")
    order_items_df["price"] = order_items_df["price"].astype(float)
    order_items_df["freight_value"] = order_items_df["freight_value"].astype(float)
    order_items_df = order_items_df.drop_duplicates()
    order_items_df = order_items_df.dropna()
    return order_items_df

def order_clean_df(order_df):
    # order_df = pd.read_csv(f"/opt/airflow/data/{order}.csv")
    order_df["order_id"] = order_df["order_id"].astype(str)
    order_df["customer_id"] = order_df["customer_id"].astype(str)
    order_df["order_status"] = order_df["order_status"].astype(str)
    order_df["order_purchase_timestamp"] = pd.to_datetime(order_df["order_purchase_timestamp"], errors="coerce")
    order_df["order_approved_at"] = pd.to_datetime(order_df["order_approved_at"], errors="coerce")
    order_df["order_delivered_carrier_date"] = pd.to_datetime(order_df["order_delivered_carrier_date"], errors="coerce")
    order_df["order_delivered_customer_date"] = pd.to_datetime(order_df["order_delivered_customer_date"], errors="coerce")
    order_df["order_estimated_delivery_date"] = pd.to_datetime(order_df["order_estimated_delivery_date"], errors="coerce")
    order_df = order_df.drop_duplicates()
    order_df = order_df.dropna()
    return order_df

def product_clean_df(product_df):
    # product_df = pd.read_csv(f"/opt/airflow/data/{product}.csv")
    product_df["product_id"] = product_df["product_id"].astype(str)
    product_df["product category"] = product_df["product category"].astype(str)
    product_df["product_name_length"] = product_df["product_name_length"].astype(float)
    product_df["product_description_length"] = product_df["product_description_length"].astype(float)
    product_df["product_photos_qty"] = product_df["product_photos_qty"].astype(float)
    product_df["product_weight_g"] = product_df["product_weight_g"].astype(float)
    product_df["product_length_cm"] = product_df["product_length_cm"].astype(float)
    product_df["product_height_cm"] = product_df["product_height_cm"].astype(float)
    product_df["product_width_cm"] = product_df["product_width_cm"].astype(float)
    product_df = product_df.drop_duplicates()
    return product_df

def seller_clean_df(seller_df):
    # seller_df = pd.read_csv(f"/opt/airflow/data/{seller}.csv")
    seller_df["seller_id"] = seller_df["seller_id"].astype(str)
    seller_df["seller_zip_code_prefix"] = seller_df["seller_zip_code_prefix"].astype(str)
    seller_df["seller_city"] = seller_df["seller_city"].astype(str)
    seller_df["seller_state"] = seller_df["seller_state"].astype(str)
    seller_df = seller_df.drop_duplicates()
    return seller_df


def clean_df(df_name, df):
    if df_name == "customers.csv":
        df = customer_clean_df(df)
    elif df_name == "geolocation.csv":
        df = geolocation_clean_df(df)    
    elif df_name == "order_items.csv":
        df = order_items_clean_df(df)
    elif df_name == "orders.csv":
        df = order_clean_df(df)
    elif df_name == "products.csv":
        df = product_clean_df(df)
    elif df_name == "sellers.csv":
        df = seller_clean_df(df)

    return df