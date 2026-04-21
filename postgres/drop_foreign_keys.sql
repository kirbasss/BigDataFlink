ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS fk_fact_sales_date;
ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS fk_fact_sales_supplier;
ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS fk_fact_sales_store;
ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS fk_fact_sales_product;
ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS fk_fact_sales_seller;
ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS fk_fact_sales_pet;
ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS fk_fact_sales_customer;
ALTER TABLE dim_pet DROP CONSTRAINT IF EXISTS fk_dim_pet_customer;
