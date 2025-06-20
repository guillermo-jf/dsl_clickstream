CREATE TABLE IF NOT EXISTS `dsl_project.web_visits` (
  session_id STRING,
  user_id STRING,
  device_type STRING,
  geolocation STRING,
  user_agent STRING,
  visit_start_time TIMESTAMP,
  visit_end_time TIMESTAMP,
  events ARRAY<STRUCT<
    event_type STRING,
    event_timestamp TIMESTAMP,
    -- PageView
    page_view STRUCT<
      page_url STRING,
      referrer_url STRING
    >,
    -- AddItemToCart
    add_cart STRUCT<
        product_id STRING,
        product_name STRING,
        category STRING,
        price FLOAT64,
        quantity INT64
    >,
    --PurchaseDetails
    purchase STRUCT<
        order_id STRING,
        amount FLOAT64,
        currency STRING,
        items ARRAY<STRUCT<
            product_id STRING,
            product_name STRING,
            category STRING,
            price FLOAT64,
            quantity INT64
        >>
    >
  >>
)
PARTITION BY DATE(visit_start_time)
CLUSTER BY device_type, user_id;