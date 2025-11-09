CREATE DATABASE COFFEE_INDIA;
USE COFFEE_INDIA;

CREATE TABLE cities (
  city_id INT PRIMARY KEY,
  city_name VARCHAR(15),
  state VARCHAR(15),
  population BIGINT,
  tier TINYINT
);

CREATE TABLE stores (
  store_id INT PRIMARY KEY,
  store_name VARCHAR(50),
  city_id INT,
  opening_date DATE,
  store_type VARCHAR(50),
  FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

CREATE TABLE products (
  product_id INT PRIMARY KEY,
  product_name VARCHAR(50),
  category VARCHAR(30),
  size_ml INT,
  unit_price DECIMAL(8,2)
);

CREATE TABLE customers (
  customer_id INT PRIMARY KEY,
  gender ENUM('M','F','Other') NOT NULL,
  age INT NOT NULL,
  city_id INT NULL,
  FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

CREATE TABLE sales (
  sale_id BIGINT PRIMARY KEY,
  sale_date DATE,
  store_id INT,
  product_id INT,
  customer_id INT NULL,
  qty INT,
  unit_price DECIMAL(8,2),
  total_amount DECIMAL(10,2),
  payment_type VARCHAR(50),
  FOREIGN KEY (store_id) REFERENCES stores(store_id),
  FOREIGN KEY (product_id) REFERENCES products(product_id),
  FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales.csv'
INTO TABLE sales
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(sale_id, sale_date, store_id, product_id, customer_id, qty, unit_price, total_amount, payment_type);

SHOW ENGINE INNODB STATUS;
SELECT customer_id FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM orders);
SET FOREIGN_KEY_CHECKS = 0;

select * from sales;

SELECT COUNT(*) AS null_total_amount FROM sales WHERE total_amount IS NULL;

SELECT sale_id, COUNT(*) FROM sales GROUP BY sale_id HAVING COUNT(*) > 1;

SELECT s.store_id FROM sales s
LEFT JOIN stores st ON s.store_id = st.store_id
WHERE st.store_id IS NULL
LIMIT 10;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cities.csv'
INTO TABLE cities
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(city_id, city_name, state, population, tier);

-- Basic aggregation — total sales by city

SELECT c.city_name, SUM(s.total_amount) AS total_sales, SUM(s.qty) AS total_qty
FROM sales s
JOIN stores st ON s.store_id = st.store_id
JOIN cities c ON st.city_id = c.city_id
GROUP BY c.city_name
ORDER BY total_sales DESC
LIMIT 20;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/stores.csv'
INTO TABLE stores
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(store_id, store_name, city_id, opening_date, store_type);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/products.csv'
INTO TABLE products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id, product_name, category, size_ml, unit_price);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customers.csv'
INTO TABLE customers
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(customer_id, @gender, age, city_id)
SET gender = TRIM(@gender);

-- Monthly time series — sales by month

SELECT DATE_FORMAT(sale_date, '%Y-%m') AS month, 
       SUM(total_amount) AS monthly_sales
FROM sales
GROUP BY month
ORDER BY month;

-- Top products by revenue

SELECT p.product_name, p.category, SUM(s.total_amount) AS revenue, SUM(s.qty) AS qty_sold
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 20;

-- Average ticket size by city

SELECT c.city_name,
       AVG(s.total_amount) AS avg_ticket,
       COUNT(DISTINCT s.sale_id) AS transactions
FROM sales s
JOIN stores st ON s.store_id = st.store_id
JOIN cities c ON st.city_id = c.city_id
GROUP BY c.city_name
ORDER BY avg_ticket DESC;

-- Window function — city market share (top 5)

SELECT city_name, total_sales,
       ROUND(total_sales / SUM(total_sales) OVER () * 100, 2) AS market_share_pct
FROM (
  SELECT c.city_name, SUM(s.total_amount) AS total_sales
  FROM sales s
  JOIN stores st ON s.store_id = st.store_id
  JOIN cities c ON st.city_id = c.city_id
  GROUP BY c.city_name
)
ORDER BY total_sales DESC
LIMIT 10;

CREATE VIEW city_monthly_sales AS
SELECT c.city_id, c.city_name, DATE_FORMAT(s.sale_date, '%Y-%m') AS month, SUM(s.total_amount) AS monthly_sales
FROM sales s
JOIN stores st ON s.store_id = st.store_id
JOIN cities c ON st.city_id = c.city_id
GROUP BY c.city_id, month;

SELECT * FROM sales;