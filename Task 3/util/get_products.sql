-- Get a distinct list of products with their attributes from the add_cart record
SELECT
  DISTINCT add_cart.product_id,
  add_cart.product_name,
  add_cart.category,
  add_cart.price
FROM
  `datasolutions.visits`, UNNEST(events) as events
WHERE
  event_type = 'add_cart'
ORDER BY
  add_cart.product_id;