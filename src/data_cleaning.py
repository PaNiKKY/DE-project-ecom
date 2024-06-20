import pandas as pd

def customer_clean_df():
    customer_df = pd.read_csv("/opt/airflow/data/customers.csv")
    customer_df["order_id"] = customer_df["customer_id"].astype(str)
    customer_df["customer_unique_id"] = customer_df["customer_unique_id"].astype(str)
    customer_df["customer_zip_code_prefix"] = customer_df["customer_zip_code_prefix"].astype(str)
    customer_df["customer_city"] = customer_df["customer_city"].astype(str)
    customer_df["customer_state"] = customer_df["customer_state"].astype(str)
    customer_df = customer_df.drop_duplicates()
    return customer_df

def geolocation_clean_df():
    geolocation_df = pd.read_csv("/opt/airflow/data/geolocation.csv")
    geolocation_df["geolocation_zip_code_prefix"] = geolocation_df["geolocation_zip_code_prefix"].astype(str)
    geolocation_df["geolocation_lat"] = geolocation_df["geolocation_lat"].astype(float)
    geolocation_df["geolocation_lng"] = geolocation_df["geolocation_lng"].astype(float)
    geo_uniq = geolocation_df.groupby(by=["geolocation_zip_code_prefix"], as_index=False)[["geolocation_lat","geolocation_lng"]].mean()
    return geo_uniq
