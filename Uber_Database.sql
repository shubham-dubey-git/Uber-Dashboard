CREATE DATABASE uber_database;

USE uber_database;


-- Customers Table
CREATE TABLE customers (
    customer_key INT PRIMARY KEY AUTO_INCREMENT,
    customer_id VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Vehicles Table
CREATE TABLE vehicles (
    vehicle_key INT PRIMARY KEY AUTO_INCREMENT,
    vehicle_type VARCHAR(100) UNIQUE NOT NULL
);

-- Locations Table (Simplified - no cities table needed)
CREATE TABLE locations (
    location_key INT PRIMARY KEY AUTO_INCREMENT,
    location_name VARCHAR(255) UNIQUE NOT NULL
);

-- Payment Methods Table
CREATE TABLE payment_methods (
    payment_method_key INT PRIMARY KEY AUTO_INCREMENT,
    payment_method VARCHAR(100) UNIQUE NOT NULL
);


CREATE TABLE fact_bookings (
    fact_booking_key INT PRIMARY KEY AUTO_INCREMENT,
    booking_id VARCHAR(100) NOT NULL UNIQUE,
    booking_status VARCHAR(100),
    booking_datetime DATETIME NOT NULL,
    
    -- Foreign Keys to dimension tables
    customer_key INT NOT NULL,
    vehicle_key INT NOT NULL,
    pickup_location_key INT NOT NULL,
    drop_location_key INT NOT NULL,
    payment_method_key INT NOT NULL,
    
    -- Metrics
    booking_value DECIMAL(12,2),
    ride_distance DECIMAL(12,2),
    driver_ratings DECIMAL(3,2),
    customer_rating DECIMAL(3,2),
    
    -- Flags
    is_cancelled BOOLEAN DEFAULT FALSE,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Foreign Key Constraints (for EER diagram relationships)
    CONSTRAINT fk_fact_customer
        FOREIGN KEY (customer_key) REFERENCES customers(customer_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
        
    CONSTRAINT fk_fact_vehicle
        FOREIGN KEY (vehicle_key) REFERENCES vehicles(vehicle_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
        
    CONSTRAINT fk_fact_pickup_location
        FOREIGN KEY (pickup_location_key) REFERENCES locations(location_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
        
    CONSTRAINT fk_fact_drop_location
        FOREIGN KEY (drop_location_key) REFERENCES locations(location_key)
        ON DELETE RESTRICT ON UPDATE CASCADE,
        
    CONSTRAINT fk_fact_payment_method
        FOREIGN KEY (payment_method_key) REFERENCES payment_methods(payment_method_key)
        ON DELETE RESTRICT ON UPDATE CASCADE
);


-- Verify data loaded from Python
SELECT COUNT(*) AS total_records_from_python FROM bookings;

-- Check for duplicates in staging table
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT booking_id) AS unique_bookings,
    COUNT(*) - COUNT(DISTINCT booking_id) AS duplicate_rows
FROM bookings;

-- Insert unique customers from staging table
INSERT IGNORE INTO customers (customer_id)
SELECT DISTINCT customer_id
FROM bookings
WHERE customer_id IS NOT NULL AND customer_id <> '';

-- Insert unique vehicle types
INSERT IGNORE INTO vehicles (vehicle_type)
SELECT DISTINCT vehicle_type
FROM bookings
WHERE vehicle_type IS NOT NULL AND vehicle_type <> '';

-- Insert unique locations (both pickup and drop)
INSERT IGNORE INTO locations (location_name)
SELECT DISTINCT pickup_location
FROM bookings
WHERE pickup_location IS NOT NULL AND pickup_location <> ''
UNION
SELECT DISTINCT drop_location
FROM bookings
WHERE drop_location IS NOT NULL AND drop_location <> '';

-- Insert unique payment methods
INSERT IGNORE INTO payment_methods (payment_method)
SELECT DISTINCT payment_method
FROM bookings
WHERE payment_method IS NOT NULL AND payment_method <> '';


-- Insert data from staging table (bookings) into fact table
INSERT IGNORE INTO fact_bookings (
    booking_id,
    booking_status,
    booking_datetime,
    customer_key,
    vehicle_key,
    pickup_location_key,
    drop_location_key,
    payment_method_key,
    booking_value,
    ride_distance,
    driver_ratings,
    customer_rating,
    is_cancelled
)
SELECT
    b.booking_id,
    b.booking_status,
    b.booking_datetime,
    c.customer_key,
    v.vehicle_key,
    pl.location_key,
    dl.location_key,
    pm.payment_method_key,
    b.booking_value,
    b.ride_distance,
    b.driver_ratings,
    b.customer_rating,
    COALESCE(b.is_cancelled, FALSE)
FROM bookings b
LEFT JOIN customers c ON c.customer_id = b.customer_id
LEFT JOIN vehicles v ON v.vehicle_type = b.vehicle_type
LEFT JOIN locations pl ON pl.location_name = b.pickup_location
LEFT JOIN locations dl ON dl.location_name = b.drop_location
LEFT JOIN payment_methods pm ON pm.payment_method = b.payment_method
WHERE b.booking_id IS NOT NULL;


-- Check row counts (staging vs fact table)
SELECT 
    'Staging Table' AS source,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT booking_id) AS unique_bookings
FROM bookings
UNION ALL
SELECT 
    'Fact Table' AS source,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT booking_id) AS unique_bookings
FROM fact_bookings;

-- Check for missing foreign keys (data quality issues)
SELECT
    SUM(customer_key IS NULL) AS missing_customers,
    SUM(vehicle_key IS NULL) AS missing_vehicles,
    SUM(pickup_location_key IS NULL) AS missing_pickup,
    SUM(drop_location_key IS NULL) AS missing_drop,
    SUM(payment_method_key IS NULL) AS missing_payment
FROM fact_bookings;

-- Sample records that failed to load (if any)
SELECT 
    b.booking_id,
    b.customer_id,
    b.vehicle_type,
    b.pickup_location,
    b.drop_location,
    'Record not in fact table' AS issue
FROM bookings b
LEFT JOIN fact_bookings f ON f.booking_id = b.booking_id
WHERE f.booking_id IS NULL
LIMIT 10;

-- Dimension table row counts
SELECT 'Customers' AS dimension, COUNT(*) AS count FROM customers
UNION ALL
SELECT 'Vehicles', COUNT(*) FROM vehicles
UNION ALL
SELECT 'Locations', COUNT(*) FROM locations
UNION ALL
SELECT 'Payment Methods', COUNT(*) FROM payment_methods;

-- 1. Overall business metrics
SELECT 
    COUNT(*) AS total_rides,
    ROUND(SUM(booking_value), 2) AS total_revenue,
    ROUND(AVG(booking_value), 2) AS avg_fare,
    ROUND(AVG(ride_distance), 2) AS avg_distance_km
FROM fact_bookings;


-- 2. Cancellation rate
SELECT 
    SUM(is_cancelled) AS cancelled_rides,
    COUNT(*) AS total_rides,
    ROUND(SUM(is_cancelled) / COUNT(*) * 100, 2) AS cancellation_rate_pct
FROM fact_bookings;


-- 3. Top 10 pickup locations
SELECT 
    l.location_name,
    COUNT(*) AS total_rides
FROM fact_bookings f
JOIN locations l ON l.location_key = f.pickup_location_key
GROUP BY l.location_name
ORDER BY total_rides DESC
LIMIT 10;


-- 4. Revenue by vehicle type
SELECT 
    v.vehicle_type,
    COUNT(*) AS total_rides,
    ROUND(SUM(f.booking_value), 2) AS total_revenue,
    ROUND(AVG(f.booking_value), 2) AS avg_fare
FROM fact_bookings f
JOIN vehicles v ON v.vehicle_key = f.vehicle_key
GROUP BY v.vehicle_type
ORDER BY total_revenue DESC;


-- 5. Top 10 customers by spending
SELECT 
    c.customer_id,
    COUNT(*) AS total_rides,
    ROUND(SUM(f.booking_value), 2) AS total_spent
FROM fact_bookings f
JOIN customers c ON c.customer_key = f.customer_key
GROUP BY c.customer_id
ORDER BY total_spent DESC
LIMIT 10;

-- 6. Daily bookings trend
SELECT 
    DATE(booking_datetime) AS booking_date,
    COUNT(*) AS total_rides,
    ROUND(SUM(booking_value), 2) AS daily_revenue
FROM fact_bookings
GROUP BY DATE(booking_datetime)
ORDER BY booking_date;


-- 7. Hourly booking pattern
SELECT 
    HOUR(booking_datetime) AS booking_hour,
    COUNT(*) AS total_rides,
    ROUND(AVG(booking_value), 2) AS avg_fare
FROM fact_bookings
GROUP BY HOUR(booking_datetime)
ORDER BY booking_hour;

-- 8. Payment method analysis
SELECT 
    pm.payment_method,
    COUNT(*) AS total_rides,
    ROUND(SUM(f.booking_value), 2) AS total_revenue
FROM fact_bookings f
JOIN payment_methods pm ON pm.payment_method_key = f.payment_method_key
GROUP BY pm.payment_method
ORDER BY total_revenue DESC;
