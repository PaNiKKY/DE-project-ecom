import pandas as pd
import os
import glob
from src.data_cleaning import customer_clean_df, geolocation_clean_df, order_items_clean_df, order_clean_df, payment_clean_df, product_clean_df


def clean_df():
    customer_df = customer_clean_df()
    geolocation_df = geolocation_clean_df()
    order_items_df = order_items_clean_df()
    order_df = order_clean_df()
    payment_df = payment_clean_df()
    product_df = product_clean_df()
    print("dataframe cleaned")
    return customer_df, geolocation_df, order_items_df, order_df, payment_df, product_df
