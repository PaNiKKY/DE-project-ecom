import pandas as pd

def orders_payments_df(order_df: pd.DataFrame, payment_df: pd.DataFrame) -> pd.DataFrame:
    merge_df = order_df.merge(payment_df, on="order_id", how="outer")
    return merge_df

def order_items_order_df(order_df: pd.DataFrame, order_items_df: pd.DataFrame) -> pd.DataFrame:
    orders_items_customers_df =  order_items_df.merge(order_df[["order_id", "customer_id"]], on="order_id", how="inner")
    return orders_items_customers_df

def customers_geolocation_df(customer_df: pd.DataFrame, geolocation_df: pd.DataFrame) -> pd.DataFrame:
    customers_geolocation_df = customer_df.merge(geolocation_df, left_on="customer_zip_code_prefix", right_on="geolocation_zip_code_prefix", how="left").drop(columns=["geolocation_zip_code_prefix"])
    return customers_geolocation_df

def sellers_geolocation_df(seller_df: pd.DataFrame, geolocation_df: pd.DataFrame) -> pd.DataFrame:
    sellers_geolocation_df = seller_df.merge(geolocation_df, left_on="seller_zip_code_prefix", right_on="geolocation_zip_code_prefix", how="left").drop(columns=["geolocation_zip_code_prefix"])
    return sellers_geolocation_df




