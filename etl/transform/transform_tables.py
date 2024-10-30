import pandas as pd

# def orders_payments_df(order_df: pd.DataFrame, payment_df: pd.DataFrame) -> pd.DataFrame:
#     merge_df = order_df.merge(payment_df, on="order_id", how="outer")
#     return merge_df

def order_items_order_df(order_df: pd.DataFrame, order_items_df: pd.DataFrame) -> pd.DataFrame:
    order_items_columns = [
                            'order_id', 'order_item_id', 'product_id', 'seller_id','shipping_limit_date', 
                            'price', 'freight_value', 'customer_id','order_status', 'order_purchase_timestamp_date',
                            'order_purchase_timestamp_time', 'order_approved_at_date', 'order_approved_at_time',
                            'order_delivered_carrier_date', 'order_delivered_customer_date','order_estimated_delivery_date'
                           ]
    orders_items_customers_df =  order_items_df.merge(order_df, on="order_id", how="inner")
    orders_items_customers_df["order_purchase_timestamp"] = pd.to_datetime(orders_items_customers_df["order_purchase_timestamp"], errors="coerce")
    orders_items_customers_df["order_approved_at"] = pd.to_datetime(orders_items_customers_df["order_approved_at"], errors="coerce")
    orders_items_customers_df["order_purchase_timestamp_date"] = orders_items_customers_df["order_purchase_timestamp"].dt.date
    orders_items_customers_df["order_purchase_timestamp_time"] = orders_items_customers_df["order_purchase_timestamp"].dt.time
    orders_items_customers_df = orders_items_customers_df.drop(columns=["order_purchase_timestamp"])
    orders_items_customers_df["order_approved_at_date"] = orders_items_customers_df["order_approved_at"].dt.date
    orders_items_customers_df["order_approved_at_time"] = orders_items_customers_df["order_approved_at"].dt.time
    orders_items_customers_df = orders_items_customers_df.drop(columns=["order_approved_at"])
    orders_items_customers_df = orders_items_customers_df[order_items_columns]
    return orders_items_customers_df

def customers_geolocation_df(customer_df: pd.DataFrame, geolocation_df: pd.DataFrame) -> pd.DataFrame:
    customers_geolocation_df = customer_df.merge(geolocation_df, left_on="customer_zip_code_prefix", right_on="geolocation_zip_code_prefix", how="left").drop(columns=["geolocation_zip_code_prefix", "order_id"])
    return customers_geolocation_df

def sellers_geolocation_df(seller_df: pd.DataFrame, geolocation_df: pd.DataFrame) -> pd.DataFrame:
    sellers_geolocation_df = seller_df.merge(geolocation_df, left_on="seller_zip_code_prefix", right_on="geolocation_zip_code_prefix", how="left").drop(columns=["geolocation_zip_code_prefix"])
    return sellers_geolocation_df

def transform_tables(df_dict: dict):
    orders_items_transformed = order_items_order_df(df_dict["orders"], df_dict["order_items"])
    customers_geolocation_transformed = customers_geolocation_df(df_dict["customers"], df_dict["geolocation"])
    sellers_geolocation_transformed = sellers_geolocation_df(df_dict["sellers"], df_dict["geolocation"])

    # orders_items_transformed = orders_items_transformed[orders_items_transformed["customer_id"].isin(customers_geolocation_transformed["customer_id"])]
    # orders_items_transformed = orders_items_transformed[orders_items_transformed["seller_id"].isin(sellers_geolocation_transformed["seller_id"])]

    return {
            "fact_orders_items":["fact_orders_items",orders_items_transformed], 
            "dim_customers":["dim_customers",customers_geolocation_transformed], 
            "dim_sellers":["dim_sellers",sellers_geolocation_transformed],
            "dim_products":["dim_products",df_dict["products"]]
        }


