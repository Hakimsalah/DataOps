CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;

CREATE TABLE IF NOT EXISTS products_metadata (
  id INT AUTO_INCREMENT PRIMARY KEY,
  product_name VARCHAR(512),
  brand VARCHAR(255),
  category VARCHAR(255),
  origin_country VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS price_trends (
  id INT AUTO_INCREMENT PRIMARY KEY,
  category VARCHAR(255),
  avg_price DOUBLE,
  day DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Exemples metadata
INSERT INTO products_metadata (product_name, brand, category, origin_country) VALUES
('iPhone 9', 'Apple', 'smartphones', 'USA'),
('WHITE HANGING HEART T-LIGHT HOLDER', 'HomeDecor', 'home', 'UK');
