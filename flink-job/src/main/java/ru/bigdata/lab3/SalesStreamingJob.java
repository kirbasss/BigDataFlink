package ru.bigdata.lab3;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class SalesStreamingJob {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() { };
    private static final DateTimeFormatter SOURCE_DATE_FORMAT = DateTimeFormatter.ofPattern("M/d/yyyy");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5_000L);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
                .setTopics(env("KAFKA_TOPIC", "petshop_sales"))
                .setGroupId("lab3-flink-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<RawSaleEvent> events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(SalesStreamingJob::parseEvent)
                .returns(TypeInformation.of(RawSaleEvent.class))
                .filter(Objects::nonNull)
                .name("parse-json");

        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(env("JDBC_URL", "jdbc:postgresql://postgres:5432/lab3"))
                .withDriverName("org.postgresql.Driver")
                .withUsername(env("JDBC_USER", "flink"))
                .withPassword(env("JDBC_PASSWORD", "flink"))
                .build();

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(200)
                .withBatchIntervalMs(1_000)
                .withMaxRetries(3)
                .build();

        DataStream<CustomerDimension> customerStream = events
                .map((MapFunction<RawSaleEvent, CustomerDimension>) CustomerDimension::fromEvent)
                .returns(TypeInformation.of(CustomerDimension.class))
                .name("map-customer");

        DataStream<PetDimension> petStream = events
                .map((MapFunction<RawSaleEvent, PetDimension>) PetDimension::fromEvent)
                .returns(TypeInformation.of(PetDimension.class))
                .name("map-pet");

        DataStream<SellerDimension> sellerStream = events
                .map((MapFunction<RawSaleEvent, SellerDimension>) SellerDimension::fromEvent)
                .returns(TypeInformation.of(SellerDimension.class))
                .name("map-seller");

        DataStream<ProductDimension> productStream = events
                .map((MapFunction<RawSaleEvent, ProductDimension>) ProductDimension::fromEvent)
                .returns(TypeInformation.of(ProductDimension.class))
                .name("map-product");

        DataStream<StoreDimension> storeStream = events
                .map((MapFunction<RawSaleEvent, StoreDimension>) StoreDimension::fromEvent)
                .returns(TypeInformation.of(StoreDimension.class))
                .name("map-store");

        DataStream<SupplierDimension> supplierStream = events
                .map((MapFunction<RawSaleEvent, SupplierDimension>) SupplierDimension::fromEvent)
                .returns(TypeInformation.of(SupplierDimension.class))
                .name("map-supplier");

        DataStream<DateDimension> dateStream = events
                .flatMap(new FlatMapFunction<RawSaleEvent, DateDimension>() {
                    @Override
                    public void flatMap(RawSaleEvent event, Collector<DateDimension> out) {
                        DateDimension dateDimension = DateDimension.fromEvent(event);
                        if (dateDimension != null) {
                            out.collect(dateDimension);
                        }
                    }
                })
                .returns(TypeInformation.of(DateDimension.class))
                .name("map-date");

        DataStream<FactSale> factStream = events
                .map((MapFunction<RawSaleEvent, FactSale>) FactSale::fromEvent)
                .returns(TypeInformation.of(FactSale.class))
                .name("map-fact");

        customerStream
                .addSink(JdbcSink.sink(customerSql(), SalesStreamingJob::bindCustomer, executionOptions, jdbcOptions))
                .name("sink-dim-customer");

        petStream
                .addSink(JdbcSink.sink(petSql(), SalesStreamingJob::bindPet, executionOptions, jdbcOptions))
                .name("sink-dim-pet");

        sellerStream
                .addSink(JdbcSink.sink(sellerSql(), SalesStreamingJob::bindSeller, executionOptions, jdbcOptions))
                .name("sink-dim-seller");

        productStream
                .addSink(JdbcSink.sink(productSql(), SalesStreamingJob::bindProduct, executionOptions, jdbcOptions))
                .name("sink-dim-product");

        storeStream
                .addSink(JdbcSink.sink(storeSql(), SalesStreamingJob::bindStore, executionOptions, jdbcOptions))
                .name("sink-dim-store");

        supplierStream
                .addSink(JdbcSink.sink(supplierSql(), SalesStreamingJob::bindSupplier, executionOptions, jdbcOptions))
                .name("sink-dim-supplier");

        dateStream
                .addSink(JdbcSink.sink(dateSql(), SalesStreamingJob::bindDate, executionOptions, jdbcOptions))
                .name("sink-dim-date");

        factStream
                .addSink(JdbcSink.sink(factSql(), SalesStreamingJob::bindFact, executionOptions, jdbcOptions))
                .name("sink-fact-sales");

        env.execute("Lab 3 Streaming ETL");
    }

    private static RawSaleEvent parseEvent(String json) throws Exception {
        Map<String, Object> payload = OBJECT_MAPPER.readValue(json, MAP_TYPE);
        return new RawSaleEvent(payload);
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static String customerSql() {
        return """
                INSERT INTO dim_customer (customer_key, first_name, last_name, age, email, country, postal_code)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (customer_key) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    age = EXCLUDED.age,
                    email = EXCLUDED.email,
                    country = EXCLUDED.country,
                    postal_code = EXCLUDED.postal_code
                """;
    }

    private static String petSql() {
        return """
                INSERT INTO dim_pet (pet_key, customer_key, pet_type, pet_name, pet_breed, pet_category)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (pet_key) DO UPDATE SET
                    customer_key = EXCLUDED.customer_key,
                    pet_type = EXCLUDED.pet_type,
                    pet_name = EXCLUDED.pet_name,
                    pet_breed = EXCLUDED.pet_breed,
                    pet_category = EXCLUDED.pet_category
                """;
    }

    private static String sellerSql() {
        return """
                INSERT INTO dim_seller (seller_key, first_name, last_name, email, country, postal_code)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (seller_key) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    country = EXCLUDED.country,
                    postal_code = EXCLUDED.postal_code
                """;
    }

    private static String productSql() {
        return """
                INSERT INTO dim_product (
                    product_key, product_name, product_category, price, inventory_quantity, weight,
                    color, size, brand, material, description, rating, reviews, release_date, expiry_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (product_key) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    product_category = EXCLUDED.product_category,
                    price = EXCLUDED.price,
                    inventory_quantity = EXCLUDED.inventory_quantity,
                    weight = EXCLUDED.weight,
                    color = EXCLUDED.color,
                    size = EXCLUDED.size,
                    brand = EXCLUDED.brand,
                    material = EXCLUDED.material,
                    description = EXCLUDED.description,
                    rating = EXCLUDED.rating,
                    reviews = EXCLUDED.reviews,
                    release_date = EXCLUDED.release_date,
                    expiry_date = EXCLUDED.expiry_date
                """;
    }

    private static String storeSql() {
        return """
                INSERT INTO dim_store (store_key, store_name, location, city, state, country, phone, email)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (store_key) DO UPDATE SET
                    store_name = EXCLUDED.store_name,
                    location = EXCLUDED.location,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    country = EXCLUDED.country,
                    phone = EXCLUDED.phone,
                    email = EXCLUDED.email
                """;
    }

    private static String supplierSql() {
        return """
                INSERT INTO dim_supplier (supplier_key, supplier_name, contact, email, phone, address, city, country)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (supplier_key) DO UPDATE SET
                    supplier_name = EXCLUDED.supplier_name,
                    contact = EXCLUDED.contact,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    address = EXCLUDED.address,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country
                """;
    }

    private static String dateSql() {
        return """
                INSERT INTO dim_date (date_key, full_date, day_of_month, month_number, year_number, quarter_number)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (date_key) DO UPDATE SET
                    full_date = EXCLUDED.full_date,
                    day_of_month = EXCLUDED.day_of_month,
                    month_number = EXCLUDED.month_number,
                    year_number = EXCLUDED.year_number,
                    quarter_number = EXCLUDED.quarter_number
                """;
    }

    private static String factSql() {
        return """
                INSERT INTO fact_sales (
                    event_key, source_file, source_row_number, customer_key, pet_key, seller_key,
                    product_key, store_key, supplier_key, date_key, sale_customer_id, sale_seller_id,
                    sale_product_id, sale_quantity, sale_total_price, unit_price
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (event_key) DO UPDATE SET
                    source_file = EXCLUDED.source_file,
                    source_row_number = EXCLUDED.source_row_number,
                    customer_key = EXCLUDED.customer_key,
                    pet_key = EXCLUDED.pet_key,
                    seller_key = EXCLUDED.seller_key,
                    product_key = EXCLUDED.product_key,
                    store_key = EXCLUDED.store_key,
                    supplier_key = EXCLUDED.supplier_key,
                    date_key = EXCLUDED.date_key,
                    sale_customer_id = EXCLUDED.sale_customer_id,
                    sale_seller_id = EXCLUDED.sale_seller_id,
                    sale_product_id = EXCLUDED.sale_product_id,
                    sale_quantity = EXCLUDED.sale_quantity,
                    sale_total_price = EXCLUDED.sale_total_price,
                    unit_price = EXCLUDED.unit_price
                """;
    }

    private static void bindCustomer(PreparedStatement statement, CustomerDimension customer) throws SQLException {
        statement.setString(1, customer.customerKey);
        statement.setString(2, customer.firstName);
        statement.setString(3, customer.lastName);
        setInteger(statement, 4, customer.age);
        statement.setString(5, customer.email);
        statement.setString(6, customer.country);
        statement.setString(7, customer.postalCode);
    }

    private static void bindPet(PreparedStatement statement, PetDimension pet) throws SQLException {
        statement.setString(1, pet.petKey);
        statement.setString(2, pet.customerKey);
        statement.setString(3, pet.petType);
        statement.setString(4, pet.petName);
        statement.setString(5, pet.petBreed);
        statement.setString(6, pet.petCategory);
    }

    private static void bindSeller(PreparedStatement statement, SellerDimension seller) throws SQLException {
        statement.setString(1, seller.sellerKey);
        statement.setString(2, seller.firstName);
        statement.setString(3, seller.lastName);
        statement.setString(4, seller.email);
        statement.setString(5, seller.country);
        statement.setString(6, seller.postalCode);
    }

    private static void bindProduct(PreparedStatement statement, ProductDimension product) throws SQLException {
        statement.setString(1, product.productKey);
        statement.setString(2, product.productName);
        statement.setString(3, product.productCategory);
        setDecimal(statement, 4, product.price);
        setInteger(statement, 5, product.inventoryQuantity);
        setDecimal(statement, 6, product.weight);
        statement.setString(7, product.color);
        statement.setString(8, product.size);
        statement.setString(9, product.brand);
        statement.setString(10, product.material);
        statement.setString(11, product.description);
        setDecimal(statement, 12, product.rating);
        setInteger(statement, 13, product.reviews);
        setDate(statement, 14, product.releaseDate);
        setDate(statement, 15, product.expiryDate);
    }

    private static void bindStore(PreparedStatement statement, StoreDimension store) throws SQLException {
        statement.setString(1, store.storeKey);
        statement.setString(2, store.storeName);
        statement.setString(3, store.location);
        statement.setString(4, store.city);
        statement.setString(5, store.state);
        statement.setString(6, store.country);
        statement.setString(7, store.phone);
        statement.setString(8, store.email);
    }

    private static void bindSupplier(PreparedStatement statement, SupplierDimension supplier) throws SQLException {
        statement.setString(1, supplier.supplierKey);
        statement.setString(2, supplier.supplierName);
        statement.setString(3, supplier.contact);
        statement.setString(4, supplier.email);
        statement.setString(5, supplier.phone);
        statement.setString(6, supplier.address);
        statement.setString(7, supplier.city);
        statement.setString(8, supplier.country);
    }

    private static void bindDate(PreparedStatement statement, DateDimension dateDimension) throws SQLException {
        statement.setInt(1, dateDimension.dateKey);
        setDate(statement, 2, dateDimension.fullDate);
        statement.setInt(3, dateDimension.dayOfMonth);
        statement.setInt(4, dateDimension.monthNumber);
        statement.setInt(5, dateDimension.yearNumber);
        statement.setInt(6, dateDimension.quarterNumber);
    }

    private static void bindFact(PreparedStatement statement, FactSale sale) throws SQLException {
        statement.setString(1, sale.eventKey);
        statement.setString(2, sale.sourceFile);
        setInteger(statement, 3, sale.sourceRowNumber);
        statement.setString(4, sale.customerKey);
        statement.setString(5, sale.petKey);
        statement.setString(6, sale.sellerKey);
        statement.setString(7, sale.productKey);
        statement.setString(8, sale.storeKey);
        statement.setString(9, sale.supplierKey);
        setInteger(statement, 10, sale.dateKey);
        statement.setString(11, sale.saleCustomerId);
        statement.setString(12, sale.saleSellerId);
        statement.setString(13, sale.saleProductId);
        setInteger(statement, 14, sale.saleQuantity);
        setDecimal(statement, 15, sale.saleTotalPrice);
        setDecimal(statement, 16, sale.unitPrice);
    }

    private static void setInteger(PreparedStatement statement, int index, Integer value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.INTEGER);
        } else {
            statement.setInt(index, value);
        }
    }

    private static void setDecimal(PreparedStatement statement, int index, BigDecimal value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.NUMERIC);
        } else {
            statement.setBigDecimal(index, value);
        }
    }

    private static void setDate(PreparedStatement statement, int index, LocalDate value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.DATE);
        } else {
            statement.setDate(index, Date.valueOf(value));
        }
    }

    private static Integer parseInteger(String value) {
        try {
            return value == null || value.isBlank() ? null : Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static BigDecimal parseDecimal(String value) {
        try {
            return value == null || value.isBlank() ? null : new BigDecimal(value);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private static LocalDate parseDate(String value) {
        try {
            return value == null || value.isBlank() ? null : LocalDate.parse(value, SOURCE_DATE_FORMAT);
        } catch (Exception ex) {
            return null;
        }
    }

    private static String hashKey(String prefix, String... values) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String material = String.join("|", Arrays.stream(values).map(value -> value == null ? "" : value).toList());
            byte[] hash = digest.digest(material.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(prefix).append('_');
            for (int i = 0; i < 12; i++) {
                builder.append(String.format("%02x", hash[i]));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is not available", ex);
        }
    }

    private static String read(Map<String, Object> payload, String key) {
        Object value = payload.get(key);
        if (value == null) {
            return null;
        }
        String normalized = value.toString().trim();
        return normalized.isEmpty() ? null : normalized;
    }

    private static final class RawSaleEvent implements java.io.Serializable {
        private final Map<String, Object> payload;

        private RawSaleEvent(Map<String, Object> payload) {
            this.payload = payload;
        }

        private String get(String key) {
            return read(payload, key);
        }
    }

    private static final class CustomerDimension implements java.io.Serializable {
        private final String customerKey;
        private final String firstName;
        private final String lastName;
        private final Integer age;
        private final String email;
        private final String country;
        private final String postalCode;

        private CustomerDimension(String customerKey, String firstName, String lastName, Integer age,
                                  String email, String country, String postalCode) {
            this.customerKey = customerKey;
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
            this.email = email;
            this.country = country;
            this.postalCode = postalCode;
        }

        private static CustomerDimension fromEvent(RawSaleEvent event) {
            return new CustomerDimension(
                    hashKey("customer", event.get("customer_email"), event.get("customer_first_name"),
                            event.get("customer_last_name"), event.get("customer_country")),
                    event.get("customer_first_name"),
                    event.get("customer_last_name"),
                    parseInteger(event.get("customer_age")),
                    event.get("customer_email"),
                    event.get("customer_country"),
                    event.get("customer_postal_code")
            );
        }
    }

    private static final class PetDimension implements java.io.Serializable {
        private final String petKey;
        private final String customerKey;
        private final String petType;
        private final String petName;
        private final String petBreed;
        private final String petCategory;

        private PetDimension(String petKey, String customerKey, String petType, String petName,
                             String petBreed, String petCategory) {
            this.petKey = petKey;
            this.customerKey = customerKey;
            this.petType = petType;
            this.petName = petName;
            this.petBreed = petBreed;
            this.petCategory = petCategory;
        }

        private static PetDimension fromEvent(RawSaleEvent event) {
            String customerKey = hashKey("customer", event.get("customer_email"), event.get("customer_first_name"),
                    event.get("customer_last_name"), event.get("customer_country"));
            return new PetDimension(
                    hashKey("pet", event.get("customer_email"), event.get("customer_pet_name"),
                            event.get("customer_pet_type"), event.get("customer_pet_breed")),
                    customerKey,
                    event.get("customer_pet_type"),
                    event.get("customer_pet_name"),
                    event.get("customer_pet_breed"),
                    event.get("pet_category")
            );
        }
    }

    private static final class SellerDimension implements java.io.Serializable {
        private final String sellerKey;
        private final String firstName;
        private final String lastName;
        private final String email;
        private final String country;
        private final String postalCode;

        private SellerDimension(String sellerKey, String firstName, String lastName, String email,
                                String country, String postalCode) {
            this.sellerKey = sellerKey;
            this.firstName = firstName;
            this.lastName = lastName;
            this.email = email;
            this.country = country;
            this.postalCode = postalCode;
        }

        private static SellerDimension fromEvent(RawSaleEvent event) {
            return new SellerDimension(
                    hashKey("seller", event.get("seller_email"), event.get("seller_first_name"),
                            event.get("seller_last_name"), event.get("seller_country")),
                    event.get("seller_first_name"),
                    event.get("seller_last_name"),
                    event.get("seller_email"),
                    event.get("seller_country"),
                    event.get("seller_postal_code")
            );
        }
    }

    private static final class ProductDimension implements java.io.Serializable {
        private final String productKey;
        private final String productName;
        private final String productCategory;
        private final BigDecimal price;
        private final Integer inventoryQuantity;
        private final BigDecimal weight;
        private final String color;
        private final String size;
        private final String brand;
        private final String material;
        private final String description;
        private final BigDecimal rating;
        private final Integer reviews;
        private final LocalDate releaseDate;
        private final LocalDate expiryDate;

        private ProductDimension(String productKey, String productName, String productCategory, BigDecimal price,
                                 Integer inventoryQuantity, BigDecimal weight, String color, String size,
                                 String brand, String material, String description, BigDecimal rating,
                                 Integer reviews, LocalDate releaseDate, LocalDate expiryDate) {
            this.productKey = productKey;
            this.productName = productName;
            this.productCategory = productCategory;
            this.price = price;
            this.inventoryQuantity = inventoryQuantity;
            this.weight = weight;
            this.color = color;
            this.size = size;
            this.brand = brand;
            this.material = material;
            this.description = description;
            this.rating = rating;
            this.reviews = reviews;
            this.releaseDate = releaseDate;
            this.expiryDate = expiryDate;
        }

        private static ProductDimension fromEvent(RawSaleEvent event) {
            return new ProductDimension(
                    hashKey("product", event.get("product_name"), event.get("product_category"), event.get("product_brand"),
                            event.get("product_size"), event.get("product_color"), event.get("product_material")),
                    event.get("product_name"),
                    event.get("product_category"),
                    parseDecimal(event.get("product_price")),
                    parseInteger(event.get("product_quantity")),
                    parseDecimal(event.get("product_weight")),
                    event.get("product_color"),
                    event.get("product_size"),
                    event.get("product_brand"),
                    event.get("product_material"),
                    event.get("product_description"),
                    parseDecimal(event.get("product_rating")),
                    parseInteger(event.get("product_reviews")),
                    parseDate(event.get("product_release_date")),
                    parseDate(event.get("product_expiry_date"))
            );
        }
    }

    private static final class StoreDimension implements java.io.Serializable {
        private final String storeKey;
        private final String storeName;
        private final String location;
        private final String city;
        private final String state;
        private final String country;
        private final String phone;
        private final String email;

        private StoreDimension(String storeKey, String storeName, String location, String city, String state,
                               String country, String phone, String email) {
            this.storeKey = storeKey;
            this.storeName = storeName;
            this.location = location;
            this.city = city;
            this.state = state;
            this.country = country;
            this.phone = phone;
            this.email = email;
        }

        private static StoreDimension fromEvent(RawSaleEvent event) {
            return new StoreDimension(
                    hashKey("store", event.get("store_email"), event.get("store_name"), event.get("store_city"),
                            event.get("store_country"), event.get("store_phone")),
                    event.get("store_name"),
                    event.get("store_location"),
                    event.get("store_city"),
                    event.get("store_state"),
                    event.get("store_country"),
                    event.get("store_phone"),
                    event.get("store_email")
            );
        }
    }

    private static final class SupplierDimension implements java.io.Serializable {
        private final String supplierKey;
        private final String supplierName;
        private final String contact;
        private final String email;
        private final String phone;
        private final String address;
        private final String city;
        private final String country;

        private SupplierDimension(String supplierKey, String supplierName, String contact, String email,
                                  String phone, String address, String city, String country) {
            this.supplierKey = supplierKey;
            this.supplierName = supplierName;
            this.contact = contact;
            this.email = email;
            this.phone = phone;
            this.address = address;
            this.city = city;
            this.country = country;
        }

        private static SupplierDimension fromEvent(RawSaleEvent event) {
            return new SupplierDimension(
                    hashKey("supplier", event.get("supplier_email"), event.get("supplier_name"), event.get("supplier_phone")),
                    event.get("supplier_name"),
                    event.get("supplier_contact"),
                    event.get("supplier_email"),
                    event.get("supplier_phone"),
                    event.get("supplier_address"),
                    event.get("supplier_city"),
                    event.get("supplier_country")
            );
        }
    }

    private static final class DateDimension implements java.io.Serializable {
        private final int dateKey;
        private final LocalDate fullDate;
        private final int dayOfMonth;
        private final int monthNumber;
        private final int yearNumber;
        private final int quarterNumber;

        private DateDimension(int dateKey, LocalDate fullDate, int dayOfMonth, int monthNumber,
                              int yearNumber, int quarterNumber) {
            this.dateKey = dateKey;
            this.fullDate = fullDate;
            this.dayOfMonth = dayOfMonth;
            this.monthNumber = monthNumber;
            this.yearNumber = yearNumber;
            this.quarterNumber = quarterNumber;
        }

        private static DateDimension fromEvent(RawSaleEvent event) {
            LocalDate date = parseDate(event.get("sale_date"));
            if (date == null) {
                return null;
            }
            int dateKey = date.getYear() * 10000 + date.getMonthValue() * 100 + date.getDayOfMonth();
            return new DateDimension(
                    dateKey,
                    date,
                    date.getDayOfMonth(),
                    date.getMonthValue(),
                    date.getYear(),
                    ((date.getMonthValue() - 1) / 3) + 1
            );
        }
    }

    private static final class FactSale implements java.io.Serializable {
        private final String eventKey;
        private final String sourceFile;
        private final Integer sourceRowNumber;
        private final String customerKey;
        private final String petKey;
        private final String sellerKey;
        private final String productKey;
        private final String storeKey;
        private final String supplierKey;
        private final Integer dateKey;
        private final String saleCustomerId;
        private final String saleSellerId;
        private final String saleProductId;
        private final Integer saleQuantity;
        private final BigDecimal saleTotalPrice;
        private final BigDecimal unitPrice;

        private FactSale(String eventKey, String sourceFile, Integer sourceRowNumber, String customerKey, String petKey,
                         String sellerKey, String productKey, String storeKey, String supplierKey, Integer dateKey,
                         String saleCustomerId, String saleSellerId, String saleProductId, Integer saleQuantity,
                         BigDecimal saleTotalPrice, BigDecimal unitPrice) {
            this.eventKey = eventKey;
            this.sourceFile = sourceFile;
            this.sourceRowNumber = sourceRowNumber;
            this.customerKey = customerKey;
            this.petKey = petKey;
            this.sellerKey = sellerKey;
            this.productKey = productKey;
            this.storeKey = storeKey;
            this.supplierKey = supplierKey;
            this.dateKey = dateKey;
            this.saleCustomerId = saleCustomerId;
            this.saleSellerId = saleSellerId;
            this.saleProductId = saleProductId;
            this.saleQuantity = saleQuantity;
            this.saleTotalPrice = saleTotalPrice;
            this.unitPrice = unitPrice;
        }

        private static FactSale fromEvent(RawSaleEvent event) {
            String sourceFile = event.get("source_file");
            Integer sourceRowNumber = parseInteger(event.get("source_row_number"));
            LocalDate saleDate = parseDate(event.get("sale_date"));
            Integer dateKey = saleDate == null ? null : saleDate.getYear() * 10000 + saleDate.getMonthValue() * 100 + saleDate.getDayOfMonth();
            return new FactSale(
                    hashKey("sale", sourceFile, event.get("id"), String.valueOf(sourceRowNumber)),
                    sourceFile,
                    sourceRowNumber,
                    hashKey("customer", event.get("customer_email"), event.get("customer_first_name"),
                            event.get("customer_last_name"), event.get("customer_country")),
                    hashKey("pet", event.get("customer_email"), event.get("customer_pet_name"),
                            event.get("customer_pet_type"), event.get("customer_pet_breed")),
                    hashKey("seller", event.get("seller_email"), event.get("seller_first_name"),
                            event.get("seller_last_name"), event.get("seller_country")),
                    hashKey("product", event.get("product_name"), event.get("product_category"), event.get("product_brand"),
                            event.get("product_size"), event.get("product_color"), event.get("product_material")),
                    hashKey("store", event.get("store_email"), event.get("store_name"), event.get("store_city"),
                            event.get("store_country"), event.get("store_phone")),
                    hashKey("supplier", event.get("supplier_email"), event.get("supplier_name"), event.get("supplier_phone")),
                    dateKey,
                    event.get("sale_customer_id"),
                    event.get("sale_seller_id"),
                    event.get("sale_product_id"),
                    parseInteger(event.get("sale_quantity")),
                    parseDecimal(event.get("sale_total_price")),
                    parseDecimal(event.get("product_price"))
            );
        }
    }
}
