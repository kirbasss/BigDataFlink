CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_pet (
    pet_key TEXT PRIMARY KEY,
    customer_key TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT,
    pet_category TEXT
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_key TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key TEXT PRIMARY KEY,
    product_name TEXT,
    product_category TEXT,
    price NUMERIC(12, 2),
    inventory_quantity INTEGER,
    weight NUMERIC(12, 2),
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(4, 2),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key TEXT PRIMARY KEY,
    store_name TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_key TEXT PRIMARY KEY,
    supplier_name TEXT,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE,
    day_of_month INTEGER,
    month_number INTEGER,
    year_number INTEGER,
    quarter_number INTEGER
);

CREATE TABLE IF NOT EXISTS fact_sales (
    event_key TEXT PRIMARY KEY,
    source_file TEXT,
    source_row_number INTEGER,
    customer_key TEXT,
    pet_key TEXT,
    seller_key TEXT,
    product_key TEXT,
    store_key TEXT,
    supplier_key TEXT,
    date_key INTEGER,
    sale_customer_id TEXT,
    sale_seller_id TEXT,
    sale_product_id TEXT,
    sale_quantity INTEGER,
    sale_total_price NUMERIC(12, 2),
    unit_price NUMERIC(12, 2)
);

