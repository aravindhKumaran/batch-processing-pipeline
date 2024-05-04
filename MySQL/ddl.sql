CREATE DATABASE IF NOT EXISTS retail_database;

USE retail_database;

CREATE TABLE IF NOT EXISTS store (
    store_key INT,
    store_num VARCHAR(30),
    store_desc VARCHAR(150),
    addr VARCHAR(500),
    city VARCHAR(50),
    region VARCHAR(100),
    cntry_cd VARCHAR(30),
    cntry_nm VARCHAR(150),
    postal_zip_cd VARCHAR(10),
    prov_state_desc VARCHAR(30),
    prov_state_cd VARCHAR(30),
    store_type_cd VARCHAR(30),
    store_type_desc VARCHAR(150),
    frnchs_flg BOOLEAN,
    store_size DECIMAL(19,3),
    market_key INT,
    market_name VARCHAR(150),
    submarket_key INT,
    submarket_name VARCHAR(150),
    latitude DECIMAL(19, 6),
    longitude DECIMAL(19, 6)
);

CREATE TABLE IF NOT EXISTS sales (
    trans_id INT,
    prod_key INT,
    store_key INT,
    trans_dt DATE,
    trans_time INT,
    sales_qty DECIMAL(38,2),
    sales_price DECIMAL(38,2),
    sales_amt DECIMAL(38,2),
    discount DECIMAL(38,2),
    sales_cost DECIMAL(38,2),
    sales_mgrn DECIMAL(38,2),
    ship_cost DECIMAL(38,2)
);

CREATE TABLE IF NOT EXISTS calendar (
    cal_dt DATE NOT NULL,
    cal_type_desc VARCHAR(20),
    day_of_wk_num VARCHAR(30),
    day_of_wk_desc VARCHAR(30),
    yr_num INT,
    wk_num INT,
    yr_wk_num INT,
    mnth_num INT,
    yr_mnth_num INT,
    qtr_num INT,
    yr_qtr_num INT
);

CREATE TABLE IF NOT EXISTS product (
    prod_key INT,
    prod_name VARCHAR(255),
    vol DECIMAL(38,2),
    wgt DECIMAL(38,2),
    brand_name VARCHAR(255),
    status_code INT,
    status_code_name VARCHAR(255),
    category_key INT,
    category_name VARCHAR(255),
    subcategory_key INT,
    subcategory_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS inventory (
    cal_dt DATE,
    store_key INT,
    prod_key INT,
    inventory_on_hand_qty DECIMAL(38,2),
    inventory_on_order_qty DECIMAL(38,2),
    out_of_stock_flg INT,
    waste_qty DECIMAL(38,2),
    promotion_flg BOOLEAN,
    next_delivery_dt DATE
);