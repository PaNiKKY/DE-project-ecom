CREATE TABLE dim_customers (
    customer_id VARCHAR(50) NOT NULL PRIMARY KEY,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix VARCHAR(50) NOT NULL,
    customer_city VARCHAR(50) NOT NULL,
    customer_state VARCHAR(50) NOT NULL,
    geolocation_lat FLOAT NOT NULL,
    geolocation_lng FLOAT NOT NULL
);


CREATE TABLE dim_sellers (
    seller_id VARCHAR(50) NOT NULL PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(50) NOT NULL,
    seller_city VARCHAR(50) NOT NULL,
    seller_state VARCHAR(50) NOT NULL,
    geolocation_lat FLOAT NOT NULL,
    geolocation_lng FLOAT NOT NULL
);


CREATE TABLE dim_products (
    product_id VARCHAR(50) NOT NULL PRIMARY KEY,
    product_category VARCHAR(50) NOT NULL,
    product_name_length INTEGER NOT NULL,   
    product_description_length INTEGER NOT NULL,
    product_photos_qty INTEGER NOT NULL,
    product_weight_g INTEGER NOT NULL,
    product_length_cm INTEGER NOT NULL,
    product_height_cm INTEGER NOT NULL,
    product_width_cm INTEGER NOT NULL
);

CREATE TABLE fact_orders_items (
    order_id VARCHAR(50) NOT NULL,
    order_item_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    shipping_limit_date DATE NOT NULL,
    price FLOAT NOT NULL,
    freight_value FLOAT NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    order_purchase_timestamp_date DATE NOT NULL,
    order_purchase_timestamp_time TIME NOT NULL,
    order_approved_at_date DATE NOT NULL,
    order_approved_at_time TIME NOT NULL,
    order_delivered_carrier_date DATE NOT NULL,
    order_delivered_customer_date DATE NOT NULL,
    order_estimated_delivery_date DATE NOT NULL,

    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (order_purchase_timestamp_date) REFERENCES dim_date(date_key),
    FOREIGN KEY (order_approved_at_date) REFERENCES dim_date(date_key),
    FOREIGN KEY (order_delivered_carrier_date) REFERENCES dim_date(date_key),
    FOREIGN KEY (order_delivered_customer_date) REFERENCES dim_date(date_key),
    FOREIGN KEY (order_estimated_delivery_date) REFERENCES dim_date(date_key),
    FOREIGN KEY (order_purchase_timestamp_time) REFERENCES dim_time(time_key),
    FOREIGN KEY (order_approved_at_time) REFERENCES dim_time(time_key),
);
