CREATE TABLE customers (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    address TEXT
);

CREATE TABLE products (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50)
);

CREATE TABLE orders (
    id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    region VARCHAR(50),
    sale_date DATE,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount DECIMAL(4,2),
    shipping_cost DECIMAL(10,2),
    payment_method VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE INDEX idx_orders_sale_date ON orders(sale_date);
CREATE INDEX idx_orders_region ON orders(region);
