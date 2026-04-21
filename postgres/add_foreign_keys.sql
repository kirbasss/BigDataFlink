ALTER TABLE fact_sales
    ADD CONSTRAINT fk_fact_sales_customer
    FOREIGN KEY (customer_key) REFERENCES dim_customer (customer_key);

ALTER TABLE fact_sales
    ADD CONSTRAINT fk_fact_sales_pet
    FOREIGN KEY (pet_key) REFERENCES dim_pet (pet_key);

ALTER TABLE fact_sales
    ADD CONSTRAINT fk_fact_sales_seller
    FOREIGN KEY (seller_key) REFERENCES dim_seller (seller_key);

ALTER TABLE fact_sales
    ADD CONSTRAINT fk_fact_sales_product
    FOREIGN KEY (product_key) REFERENCES dim_product (product_key);

ALTER TABLE fact_sales
    ADD CONSTRAINT fk_fact_sales_store
    FOREIGN KEY (store_key) REFERENCES dim_store (store_key);

ALTER TABLE fact_sales
    ADD CONSTRAINT fk_fact_sales_supplier
    FOREIGN KEY (supplier_key) REFERENCES dim_supplier (supplier_key);

ALTER TABLE fact_sales
    ADD CONSTRAINT fk_fact_sales_date
    FOREIGN KEY (date_key) REFERENCES dim_date (date_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_key ON fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_pet_key ON fact_sales (pet_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_seller_key ON fact_sales (seller_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_key ON fact_sales (product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_store_key ON fact_sales (store_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_supplier_key ON fact_sales (supplier_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_key ON fact_sales (date_key);
