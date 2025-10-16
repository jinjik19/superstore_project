CREATE DATABASE IF NOT EXISTS superstore_db;

USE superstore_db;

CREATE TABLE IF NOT EXISTS stg_orders
(
    row_id          Int32,
    order_id        String,
    order_date      Date,
    ship_date       Date,
    ship_mode       String,
    customer_id     String,
    customer_name   String,
    segment         String,
    country         String,
    city            String,
    state           String,
    postal_code     Nullable(Int32),
    region          String,
    product_id      String,
    category        String,
    sub_category    String,
    product_name    String,
    sales           Float64,
    quantity        Int32,
    discount        Float32,
    profit          Float64,
    load_date       DateTime,
    source_name     String,
    batch_id        String
)
ENGINE = ReplacingMergeTree(load_date)
ORDER BY (row_id, load_date);

CREATE TABLE IF NOT EXISTS stg_people
(
    person          String,
    region          String,
    load_date       DateTime,
    source_name     String,
    batch_id        String
)
ENGINE = ReplacingMergeTree(load_date)
ORDER BY (person, region, load_date);

CREATE TABLE IF NOT EXISTS stg_returns
(
    returned     String,
    order_id     String,
    load_date    DateTime,
    source_name  String,
    batch_id     String
)
ENGINE = ReplacingMergeTree(load_date)
ORDER BY (order_id, load_date);
