from src.constants import AWS_S3_BUCKET, OUT_PATH
from etl.load_to_s3 import load_to_S3, connect_to_s3, save_df_to_s3
from etl.read_file_s3 import read_file_from_S3
from etl.transform_tables import orders_payments_df, order_items_df, customers_geolocation_df, sellers_geolocation_df
from etl.data_cleaning import customer_clean_df, seller_clean_df, order_clean_df, payment_clean_df, order_items_clean_df, geolocation_clean_df, product_clean_df
import os
import glob
def load_to_S3_pipeline(MONTH_YEAR: str):
    s3 = connect_to_s3()
    file_paths = glob.glob(f"{OUT_PATH}/*.csv")
    if s3 is not None:
        for file_path in file_paths:
            file_name = f"{MONTH_YEAR}_"+ os.path.basename(file_path)
            load_to_S3(s3, file_path, AWS_S3_BUCKET, file_name)

# clean and transform order_payments table
def transform_pipeline(MONTH_YEAR: str):
    s3 = connect_to_s3()
    order_df = read_file_from_S3(s3, f"{AWS_S3_BUCKET}/raw/{MONTH_YEAR}_orders.csv")
    payment_df = read_file_from_S3(s3, f"{AWS_S3_BUCKET}/raw/{MONTH_YEAR}_payments.csv")
    order_df = order_clean_df(order_df)
    payment_df = payment_clean_df(payment_df)
    merge_df = orders_payments_df(order_df, payment_df)
    save_df_to_s3(merge_df, s3, AWS_S3_BUCKET, f"{MONTH_YEAR}_orders_payments.csv")
    print("orders_payments table loaded successfully")

    order_it_df = read_file_from_S3(s3, f"{AWS_S3_BUCKET}/raw/{MONTH_YEAR}_order_items.csv")
    order_df = read_file_from_S3(s3, f"{AWS_S3_BUCKET}/raw/{MONTH_YEAR}_orders.csv")
    order_it_df = order_items_clean_df(order_it_df)
    order_df = order_clean_df(order_df)
    orders_items_customers_df = order_items_df(order_df, order_it_df)
    save_df_to_s3(orders_items_customers_df, s3, AWS_S3_BUCKET, f"{MONTH_YEAR}_orders_items_customers.csv")
    print("orders_items_customers table loaded successfully")
    
    customer_df = read_file_from_S3(s3, f"{AWS_S3_BUCKET}/raw/{MONTH_YEAR}_customers.csv")
    geolocation_df = read_file_from_S3(s3, f"{AWS_S3_BUCKET}/raw/{MONTH_YEAR}_geolocation.csv")
    customer_df = customer_clean_df(customer_df)
    geolocation_df = geolocation_clean_df(geolocation_df)
    customers_geo_df = customers_geolocation_df(customer_df, geolocation_df)
    save_df_to_s3(customers_geo_df, s3, AWS_S3_BUCKET, f"{MONTH_YEAR}_customers_geolocation.csv")
    print("customers_geolocation table loaded successfully")

    seller_df = read_file_from_S3(s3, f"{AWS_S3_BUCKET}/raw/{MONTH_YEAR}_sellers.csv")
    geolocation_df = read_file_from_S3(s3, f"{AWS_S3_BUCKET}/raw/{MONTH_YEAR}_geolocation.csv")
    seller_df = seller_clean_df(seller_df)
    geolocation_df = geolocation_clean_df(geolocation_df)
    sellers_geo_df = sellers_geolocation_df(seller_df, geolocation_df)
    save_df_to_s3(sellers_geo_df, s3, AWS_S3_BUCKET, f"{MONTH_YEAR}_sellers_geolocation.csv")
    print("sellers_geolocation table loaded successfully")

    product_df = read_file_from_S3(s3, f"{AWS_S3_BUCKET}/raw/{MONTH_YEAR}_products.csv")
    product_df = product_clean_df(product_df)
    save_df_to_s3(product_df, s3, AWS_S3_BUCKET, f"{MONTH_YEAR}_products.csv")
    print("products table loaded successfully")
