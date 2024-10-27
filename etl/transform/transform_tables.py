import pandas as pd

# def orders_payments_df(order_df: pd.DataFrame, payment_df: pd.DataFrame) -> pd.DataFrame:
#     merge_df = order_df.merge(payment_df, on="order_id", how="outer")
#     return merge_df

def order_items_order_df(order_df: pd.DataFrame, order_items_df: pd.DataFrame) -> pd.DataFrame:
    orders_items_customers_df =  order_items_df.merge(order_df, on="order_id", how="inner")
    return orders_items_customers_df

def customers_geolocation_df(customer_df: pd.DataFrame, geolocation_df: pd.DataFrame) -> pd.DataFrame:
    customers_geolocation_df = customer_df.merge(geolocation_df, left_on="customer_zip_code_prefix", right_on="geolocation_zip_code_prefix", how="left").drop(columns=["geolocation_zip_code_prefix", "order_id", "customer_id"])
    return customers_geolocation_df

def sellers_geolocation_df(seller_df: pd.DataFrame, geolocation_df: pd.DataFrame) -> pd.DataFrame:
    sellers_geolocation_df = seller_df.merge(geolocation_df, left_on="seller_zip_code_prefix", right_on="geolocation_zip_code_prefix", how="left").drop(columns=["geolocation_zip_code_prefix"])
    return sellers_geolocation_df

def transform_tables(df_dict: dict):
    orders_items_transformed = order_items_order_df(df_dict["orders"], df_dict["order_items"])
    # orders_payments_transformed = orders_payments_df(df_dict["orders"], df_dict["payments"])
    customers_geolocation_transformed = customers_geolocation_df(df_dict["customers"], df_dict["geolocation"])
    sellers_geolocation_transformed = sellers_geolocation_df(df_dict["sellers"], df_dict["geolocation"])

    return [
            ["fact_orders_items",orders_items_transformed], 
            ["dim_customers",customers_geolocation_transformed], 
            ["dim_sellers",sellers_geolocation_transformed],
            ["dim_products",df_dict["products"]]
        ]


