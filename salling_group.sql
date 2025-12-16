# Lav tabellen med butikkerne:
CREATE TABLE stores (
  store_id   VARCHAR(64) PRIMARY KEY,
  store_name       VARCHAR(255),
  zip        VARCHAR(10),
  first_seen DATETIME DEFAULT CURRENT_TIMESTAMP
);

# Lav tabellen med produkterne:
CREATE TABLE discount_products (
  offer_id         INT AUTO_INCREMENT PRIMARY KEY,

  store_id         VARCHAR(64),

  -- Produkt-info
  ean              VARCHAR(32),
  description      VARCHAR(255),

  -- Tilbuds-info
  new_price        DECIMAL(10,2),
  original_price   DECIMAL(10,2),
  discount_percent DECIMAL(10,2),
  stock            DECIMAL(10,2),
  stock_unit       VARCHAR(20),

  run_timestamp    DATETIME,

  FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

# Lav tabellen hvor butikker og produkter er koblet sammen (slet tabellen først og lav den igen):
DROP TABLE IF EXISTS store_products;

CREATE TABLE store_products AS
SELECT
    d.offer_id,
    d.store_id,
    s.store_name,
    d.ean,
    d.description,
    d.new_price,
    d.original_price,
    d.discount_percent,
    d.stock,
    d.stock_unit,
    d.run_timestamp
FROM discount_products d
LEFT JOIN stores s
    ON d.store_id = s.store_id;
    
# Tæl hvor man produkter der er hentet ned, ved hver kørsel:
SELECT run_timestamp, COUNT(*) 
FROM discount_products
GROUP BY run_timestamp;

# Vis tabellen med produkterne, sorteret efter de nyeste timestamps:
SELECT *
FROM discount_products
ORDER BY run_timestamp DESC;

# Vis tabellen med både produkter og butikker, sorteret efter de nyeste timestamps:
SELECT *
FROM sallingdb.store_products
ORDER BY run_timestamp DESC;

