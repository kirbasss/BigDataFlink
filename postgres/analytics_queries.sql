-- Базовые проверки загрузки

SELECT COUNT(*) AS fact_rows
FROM fact_sales;

SELECT COUNT(*) AS customers
FROM dim_customer;

SELECT COUNT(*) AS products
FROM dim_product;

SELECT COUNT(*) AS stores
FROM dim_store;


-- Проверка целостности после добавления внешних ключей

SELECT
    tc.table_name,
    tc.constraint_name,
    kcu.column_name,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = 'public'
ORDER BY tc.table_name, tc.constraint_name;


-- Пример витринного запроса по продажам
-- В запросе участвуют факт и связанные измерения звезды

SELECT
    fs.event_key,
    dd.full_date,
    dc.first_name || ' ' || dc.last_name AS customer_name,
    dpet.pet_name,
    dp.product_name,
    dseller.first_name || ' ' || dseller.last_name AS seller_name,
    dst.store_name,
    fs.sale_quantity,
    fs.sale_total_price
FROM fact_sales fs
JOIN dim_customer dc ON dc.customer_key = fs.customer_key
JOIN dim_pet dpet ON dpet.pet_key = fs.pet_key
JOIN dim_product dp ON dp.product_key = fs.product_key
JOIN dim_seller dseller ON dseller.seller_key = fs.seller_key
JOIN dim_store dst ON dst.store_key = fs.store_key
JOIN dim_date dd ON dd.date_key = fs.date_key
LIMIT 20;


-- Топ-10 самых продаваемых товаров

SELECT
    dp.product_name,
    SUM(fs.sale_quantity) AS total_quantity,
    SUM(fs.sale_total_price) AS total_revenue
FROM fact_sales fs
JOIN dim_product dp ON dp.product_key = fs.product_key
GROUP BY dp.product_name
ORDER BY total_revenue DESC
LIMIT 10;


-- Продажи по странам покупателей

SELECT
    dc.country,
    COUNT(DISTINCT fs.customer_key) AS unique_customers,
    COUNT(*) AS sales_count,
    SUM(fs.sale_total_price) AS total_revenue
FROM fact_sales fs
JOIN dim_customer dc ON dc.customer_key = fs.customer_key
GROUP BY dc.country
ORDER BY total_revenue DESC;


-- Продажи по месяцам

SELECT
    dd.year_number,
    dd.month_number,
    COUNT(*) AS sales_count,
    SUM(fs.sale_total_price) AS total_revenue
FROM fact_sales fs
JOIN dim_date dd ON dd.date_key = fs.date_key
GROUP BY dd.year_number, dd.month_number
ORDER BY dd.year_number, dd.month_number;


-- Средний чек по магазинам

SELECT
    ds.store_name,
    ds.city,
    ROUND(AVG(fs.sale_total_price), 2) AS avg_receipt,
    SUM(fs.sale_total_price) AS total_revenue
FROM fact_sales fs
JOIN dim_store ds ON ds.store_key = fs.store_key
GROUP BY ds.store_name, ds.city
ORDER BY total_revenue DESC
LIMIT 15;


-- Поставщики с наибольшей выручкой по связанным товарам

SELECT
    ds.supplier_name,
    COUNT(*) AS sales_count,
    SUM(fs.sale_total_price) AS total_revenue
FROM fact_sales fs
JOIN dim_supplier ds ON ds.supplier_key = fs.supplier_key
GROUP BY ds.supplier_name
ORDER BY total_revenue DESC
LIMIT 10;


-- Популярные категории товаров по типу питомца

SELECT
    dpet.pet_type,
    dp.product_category,
    SUM(fs.sale_quantity) AS total_quantity,
    SUM(fs.sale_total_price) AS total_revenue
FROM fact_sales fs
JOIN dim_pet dpet ON dpet.pet_key = fs.pet_key
JOIN dim_product dp ON dp.product_key = fs.product_key
GROUP BY dpet.pet_type, dp.product_category
ORDER BY total_revenue DESC
LIMIT 20;
