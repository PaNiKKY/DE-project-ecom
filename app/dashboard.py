import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.constants import DATABASE_NAME
from src.connections import connect_to_duckdb

conn = connect_to_duckdb(DATABASE_NAME)

st.title("Analytics Dashboard")

st.sidebar.header("Filters")

df = conn.execute('''
                SELECT p.product_category,
                COUNT(*) AS category_count, 
                FROM fact_orders_items f
                JOIN dim_products p ON f.product_id = p.product_id
                WHERE p.product_category IS NOT NULL
                GROUP BY p.product_category
            '''
        ).df()

fig = px.bar(df, x="product_category", y="category_count")
fig.update_layout(title="Categories Count")
st.plotly_chart(fig, use_container_width=True)

