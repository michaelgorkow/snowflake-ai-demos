-- ============================================================================
-- Cortex Analyst Demo: Sales Data Generation Script
-- Database: SVA_DEMO
-- ============================================================================

-- Create and use schema
CREATE OR REPLACE SCHEMA AI_DEMOS.SVA_DEMO;
USE SCHEMA AI_DEMOS.SVA_DEMO;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- DIM_DATE (with descriptions)
CREATE OR REPLACE TABLE DIM_DATE (
    DATE_KEY INTEGER PRIMARY KEY COMMENT 'Unique identifier for the date dimension',
    FULL_DATE DATE COMMENT 'The actual calendar date',
    MONTH VARCHAR(20) COMMENT 'Name of the month',
    QUARTER VARCHAR(10) COMMENT 'Quarter of the year (Q1-Q4)',
    YEAR INTEGER COMMENT 'Calendar year',
    FISCAL_PERIOD VARCHAR(20) COMMENT 'Fiscal period identifier',
    IS_HOLIDAY BOOLEAN COMMENT 'Flag indicating if the date is a holiday'
) COMMENT = 'Date dimension table containing calendar and fiscal period attributes';

-- DIM_PRODUCT (no descriptions per requirements)
CREATE OR REPLACE TABLE DIM_PRODUCT (
    PRODUCT_KEY INTEGER PRIMARY KEY,
    PRODUCT_NAME VARCHAR(100),
    CATEGORY VARCHAR(50),
    SUB_CATEGORY VARCHAR(50),
    BRAND VARCHAR(50),
    SUPPLIER_PRICE DECIMAL(10,2)
);

-- DIM_CUSTOMER (with descriptions)
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    CUSTOMER_KEY INTEGER PRIMARY KEY COMMENT 'Unique identifier for the customer',
    CUSTOMER_NAME VARCHAR(100) COMMENT 'Full name of the customer',
    GENDER VARCHAR(10) COMMENT 'Customer gender (Male/Female/Other)',
    EMAIL VARCHAR(150) COMMENT 'Customer email address',
    LOYALTY_SEGMENT VARCHAR(20) COMMENT 'Customer loyalty tier (Bronze/Silver/Gold/Premium)'
) COMMENT = 'Customer dimension table containing customer demographics and loyalty information';

-- DIM_STORE (with descriptions)
CREATE OR REPLACE TABLE DIM_STORE (
    STORE_KEY INTEGER PRIMARY KEY COMMENT 'Unique identifier for the store',
    STORE_NAME VARCHAR(100) COMMENT 'Name of the retail store',
    CITY VARCHAR(50) COMMENT 'City where the store is located',
    STATE VARCHAR(50) COMMENT 'State where the store is located',
    REGION VARCHAR(20) COMMENT 'Geographic region (Northeast/Southeast/Midwest/Southwest/West)',
    STORE_TYPE VARCHAR(30) COMMENT 'Type of store (Flagship/Standard/Outlet/Express)'
) COMMENT = 'Store dimension table containing retail location attributes';

-- DIM_EMPLOYEE (with descriptions)
CREATE OR REPLACE TABLE DIM_EMPLOYEE (
    EMPLOYEE_KEY INTEGER PRIMARY KEY COMMENT 'Unique identifier for the employee',
    EMPLOYEE_NAME VARCHAR(100) COMMENT 'Full name of the employee',
    JOB_TITLE VARCHAR(50) COMMENT 'Employee job title',
    HIRE_DATE DATE COMMENT 'Date the employee was hired',
    MANAGER_ID INTEGER COMMENT 'Employee key of the employees manager'
) COMMENT = 'Employee dimension table containing sales representative information';

-- FACT_SALES (with descriptions)
CREATE OR REPLACE TABLE FACT_SALES (
    SALES_ID INTEGER PRIMARY KEY COMMENT 'Unique identifier for the sale transaction',
    DATE_KEY INTEGER COMMENT 'Foreign key linking to DIM_DATE',
    PRODUCT_KEY INTEGER COMMENT 'Foreign key linking to DIM_PRODUCT',
    CUSTOMER_KEY INTEGER COMMENT 'Foreign key linking to DIM_CUSTOMER',
    STORE_KEY INTEGER COMMENT 'Foreign key linking to DIM_STORE',
    EMPLOYEE_KEY INTEGER COMMENT 'Foreign key linking to DIM_EMPLOYEE',
    QUANTITY INTEGER COMMENT 'Number of units sold in the transaction',
    UNIT_PRICE DECIMAL(10,2) COMMENT 'Price per unit at the time of sale',
    DISCOUNT_AMOUNT DECIMAL(10,2) COMMENT 'Total discount amount applied to the transaction',
    NET_SALES DECIMAL(12,2) COMMENT 'Net sales amount calculated as (QUANTITY * UNIT_PRICE) - DISCOUNT_AMOUNT',
    FOREIGN KEY (DATE_KEY) REFERENCES DIM_DATE(DATE_KEY),
    FOREIGN KEY (PRODUCT_KEY) REFERENCES DIM_PRODUCT(PRODUCT_KEY),
    FOREIGN KEY (CUSTOMER_KEY) REFERENCES DIM_CUSTOMER(CUSTOMER_KEY),
    FOREIGN KEY (STORE_KEY) REFERENCES DIM_STORE(STORE_KEY),
    FOREIGN KEY (EMPLOYEE_KEY) REFERENCES DIM_EMPLOYEE(EMPLOYEE_KEY)
) COMMENT = 'Fact table containing sales transactions with measures for quantity, pricing, discounts, and net sales';

-- ============================================================================
-- POPULATE DIMENSION TABLES
-- ============================================================================

-- Populate DIM_DATE (2023-2025)
INSERT INTO DIM_DATE
SELECT
    ROW_NUMBER() OVER (ORDER BY date_val) AS DATE_KEY,
    date_val AS FULL_DATE,
    MONTHNAME(date_val) AS MONTH,
    'Q' || QUARTER(date_val) AS QUARTER,
    YEAR(date_val) AS YEAR,
    'FP' || LPAD(MONTH(date_val)::VARCHAR, 2, '0') || '-' || YEAR(date_val) AS FISCAL_PERIOD,
    CASE 
        WHEN (MONTH(date_val) = 1 AND DAY(date_val) = 1) THEN TRUE
        WHEN (MONTH(date_val) = 7 AND DAY(date_val) = 4) THEN TRUE
        WHEN (MONTH(date_val) = 11 AND DAY(date_val) BETWEEN 22 AND 28 AND DAYOFWEEK(date_val) = 4) THEN TRUE
        WHEN (MONTH(date_val) = 12 AND DAY(date_val) = 25) THEN TRUE
        WHEN (MONTH(date_val) = 11 AND DAY(date_val) BETWEEN 23 AND 29 AND DAYOFWEEK(date_val) = 5) THEN TRUE
        WHEN (MONTH(date_val) = 12 AND DAY(date_val) BETWEEN 24 AND 26) THEN TRUE
        ELSE FALSE
    END AS IS_HOLIDAY
FROM (
    SELECT DATEADD(day, seq4(), '2023-01-01'::DATE) AS date_val
    FROM TABLE(GENERATOR(ROWCOUNT => 1096))
) dates
WHERE date_val <= '2025-12-31';

-- Populate DIM_PRODUCT
INSERT INTO DIM_PRODUCT VALUES
(1, 'Ultra HD Smart TV 55"', 'Electronics', 'Televisions', 'TechVision', 450.00),
(2, 'Ultra HD Smart TV 65"', 'Electronics', 'Televisions', 'TechVision', 650.00),
(3, 'Wireless Noise-Canceling Headphones', 'Electronics', 'Audio', 'SoundMax', 120.00),
(4, 'Bluetooth Speaker Pro', 'Electronics', 'Audio', 'SoundMax', 45.00),
(5, 'Gaming Laptop 15"', 'Electronics', 'Computers', 'ByteForce', 800.00),
(6, 'Business Ultrabook 14"', 'Electronics', 'Computers', 'ByteForce', 650.00),
(7, 'Smartphone Pro Max', 'Electronics', 'Mobile', 'TechVision', 550.00),
(8, 'Smartphone Lite', 'Electronics', 'Mobile', 'TechVision', 280.00),
(9, 'Ergonomic Office Chair', 'Furniture', 'Chairs', 'ComfortPlus', 180.00),
(10, 'Executive Leather Chair', 'Furniture', 'Chairs', 'ComfortPlus', 320.00),
(11, 'Standing Desk Electric', 'Furniture', 'Desks', 'WorkSpace', 400.00),
(12, 'Corner Computer Desk', 'Furniture', 'Desks', 'WorkSpace', 220.00),
(13, 'Modular Bookshelf', 'Furniture', 'Storage', 'HomeStyle', 150.00),
(14, 'Filing Cabinet 4-Drawer', 'Furniture', 'Storage', 'WorkSpace', 180.00),
(15, 'Running Shoes Elite', 'Apparel', 'Footwear', 'AthleticPro', 85.00),
(16, 'Training Sneakers', 'Apparel', 'Footwear', 'AthleticPro', 55.00),
(17, 'Winter Jacket Premium', 'Apparel', 'Outerwear', 'NorthStyle', 120.00),
(18, 'Rain Jacket Lightweight', 'Apparel', 'Outerwear', 'NorthStyle', 65.00),
(19, 'Yoga Mat Premium', 'Sports', 'Fitness', 'FitGear', 25.00),
(20, 'Adjustable Dumbbell Set', 'Sports', 'Fitness', 'FitGear', 200.00),
(21, 'Mountain Bike Pro', 'Sports', 'Cycling', 'TrailMaster', 450.00),
(22, 'City Commuter Bike', 'Sports', 'Cycling', 'TrailMaster', 280.00),
(23, 'Tennis Racket Pro', 'Sports', 'Racquet Sports', 'GameSet', 95.00),
(24, 'Badminton Set Complete', 'Sports', 'Racquet Sports', 'GameSet', 45.00),
(25, 'Coffee Maker Deluxe', 'Home & Kitchen', 'Appliances', 'BrewMaster', 85.00),
(26, 'Espresso Machine Pro', 'Home & Kitchen', 'Appliances', 'BrewMaster', 350.00),
(27, 'Air Fryer XL', 'Home & Kitchen', 'Appliances', 'CookSmart', 95.00),
(28, 'Blender High-Speed', 'Home & Kitchen', 'Appliances', 'CookSmart', 120.00),
(29, 'Cookware Set 10-Piece', 'Home & Kitchen', 'Cookware', 'ChefPro', 180.00),
(30, 'Cast Iron Skillet Set', 'Home & Kitchen', 'Cookware', 'ChefPro', 75.00),
-- New sub-categories introduced in 2024
(31, 'Smart Watch Series X', 'Electronics', 'Wearables', 'TechVision', 180.00),
(32, 'Fitness Tracker Band', 'Electronics', 'Wearables', 'TechVision', 60.00),
(33, 'Wireless Earbuds Pro', 'Electronics', 'Audio', 'SoundMax', 80.00),
(34, 'Gaming Console Next', 'Electronics', 'Gaming', 'ByteForce', 380.00),
(35, 'VR Headset Immersive', 'Electronics', 'Gaming', 'ByteForce', 420.00);

-- Populate DIM_CUSTOMER
INSERT INTO DIM_CUSTOMER
SELECT
    seq4() + 1 AS CUSTOMER_KEY,
    CASE MOD(seq4(), 20)
        WHEN 0 THEN 'James Smith' WHEN 1 THEN 'Maria Garcia' WHEN 2 THEN 'Robert Johnson'
        WHEN 3 THEN 'Linda Williams' WHEN 4 THEN 'Michael Brown' WHEN 5 THEN 'Patricia Jones'
        WHEN 6 THEN 'David Miller' WHEN 7 THEN 'Jennifer Davis' WHEN 8 THEN 'William Wilson'
        WHEN 9 THEN 'Elizabeth Moore' WHEN 10 THEN 'Richard Taylor' WHEN 11 THEN 'Susan Anderson'
        WHEN 12 THEN 'Joseph Thomas' WHEN 13 THEN 'Margaret Jackson' WHEN 14 THEN 'Charles White'
        WHEN 15 THEN 'Sarah Harris' WHEN 16 THEN 'Christopher Martin' WHEN 17 THEN 'Karen Thompson'
        WHEN 18 THEN 'Daniel Garcia' WHEN 19 THEN 'Nancy Robinson'
    END || ' ' || (seq4() + 1)::VARCHAR AS CUSTOMER_NAME,
    CASE MOD(seq4(), 3) WHEN 0 THEN 'Male' WHEN 1 THEN 'Female' ELSE 'Other' END AS GENDER,
    LOWER(REPLACE(
        CASE MOD(seq4(), 20)
            WHEN 0 THEN 'james.smith' WHEN 1 THEN 'maria.garcia' WHEN 2 THEN 'robert.johnson'
            WHEN 3 THEN 'linda.williams' WHEN 4 THEN 'michael.brown' WHEN 5 THEN 'patricia.jones'
            WHEN 6 THEN 'david.miller' WHEN 7 THEN 'jennifer.davis' WHEN 8 THEN 'william.wilson'
            WHEN 9 THEN 'elizabeth.moore' WHEN 10 THEN 'richard.taylor' WHEN 11 THEN 'susan.anderson'
            WHEN 12 THEN 'joseph.thomas' WHEN 13 THEN 'margaret.jackson' WHEN 14 THEN 'charles.white'
            WHEN 15 THEN 'sarah.harris' WHEN 16 THEN 'chris.martin' WHEN 17 THEN 'karen.thompson'
            WHEN 18 THEN 'daniel.garcia' WHEN 19 THEN 'nancy.robinson'
        END, ' ', '.')) || (seq4() + 1)::VARCHAR || '@email.com' AS EMAIL,
    CASE 
        WHEN MOD(seq4(), 10) < 4 THEN 'Bronze'
        WHEN MOD(seq4(), 10) < 7 THEN 'Silver'
        WHEN MOD(seq4(), 10) < 9 THEN 'Gold'
        ELSE 'Premium'
    END AS LOYALTY_SEGMENT
FROM TABLE(GENERATOR(ROWCOUNT => 500));

-- Populate DIM_STORE
INSERT INTO DIM_STORE VALUES
(1, 'Manhattan Flagship', 'New York', 'New York', 'Northeast', 'Flagship'),
(2, 'Brooklyn Heights', 'Brooklyn', 'New York', 'Northeast', 'Standard'),
(3, 'Boston Commons', 'Boston', 'Massachusetts', 'Northeast', 'Standard'),
(4, 'Philadelphia Center', 'Philadelphia', 'Pennsylvania', 'Northeast', 'Standard'),
(5, 'Newark Outlet', 'Newark', 'New Jersey', 'Northeast', 'Outlet'),
(6, 'Miami Beach', 'Miami', 'Florida', 'Southeast', 'Flagship'),
(7, 'Orlando Mall', 'Orlando', 'Florida', 'Southeast', 'Standard'),
(8, 'Atlanta Peachtree', 'Atlanta', 'Georgia', 'Southeast', 'Standard'),
(9, 'Charlotte Square', 'Charlotte', 'North Carolina', 'Southeast', 'Standard'),
(10, 'Tampa Bay Express', 'Tampa', 'Florida', 'Southeast', 'Express'),
(11, 'Chicago Loop', 'Chicago', 'Illinois', 'Midwest', 'Flagship'),
(12, 'Detroit Metro', 'Detroit', 'Michigan', 'Midwest', 'Standard'),
(13, 'Minneapolis Mall', 'Minneapolis', 'Minnesota', 'Midwest', 'Standard'),
(14, 'Cleveland Center', 'Cleveland', 'Ohio', 'Midwest', 'Outlet'),
(15, 'Indianapolis Express', 'Indianapolis', 'Indiana', 'Midwest', 'Express'),
(16, 'Dallas Galleria', 'Dallas', 'Texas', 'Southwest', 'Flagship'),
(17, 'Houston Heights', 'Houston', 'Texas', 'Southwest', 'Standard'),
(18, 'Phoenix Sun', 'Phoenix', 'Arizona', 'Southwest', 'Standard'),
(19, 'Denver Mile High', 'Denver', 'Colorado', 'Southwest', 'Standard'),
(20, 'Austin South', 'Austin', 'Texas', 'Southwest', 'Express'),
(21, 'Los Angeles Beverly', 'Los Angeles', 'California', 'West', 'Flagship'),
(22, 'San Francisco Union', 'San Francisco', 'California', 'West', 'Standard'),
(23, 'Seattle Pike', 'Seattle', 'Washington', 'West', 'Standard'),
(24, 'Portland Pearl', 'Portland', 'Oregon', 'West', 'Standard'),
(25, 'San Diego Outlet', 'San Diego', 'California', 'West', 'Outlet');

-- Populate DIM_EMPLOYEE
INSERT INTO DIM_EMPLOYEE VALUES
(1, 'Alexandra Thompson', 'Regional Sales Director', '2018-03-15', NULL),
(2, 'Marcus Chen', 'Senior Sales Manager', '2019-06-01', 1),
(3, 'Samantha Williams', 'Senior Sales Manager', '2019-08-20', 1),
(4, 'Derek Johnson', 'Sales Manager', '2020-01-10', 2),
(5, 'Rachel Martinez', 'Sales Manager', '2020-04-15', 2),
(6, 'Kevin O''Brien', 'Sales Manager', '2020-07-22', 3),
(7, 'Michelle Lee', 'Sales Manager', '2020-11-05', 3),
(8, 'Brandon Taylor', 'Senior Sales Rep', '2021-02-14', 4),
(9, 'Jessica Adams', 'Senior Sales Rep', '2021-05-18', 4),
(10, 'Tyler Rodriguez', 'Senior Sales Rep', '2021-08-09', 5),
(11, 'Amanda Clark', 'Senior Sales Rep', '2021-10-25', 5),
(12, 'Justin Wright', 'Sales Representative', '2022-01-03', 6),
(13, 'Megan Scott', 'Sales Representative', '2022-03-21', 6),
(14, 'Ryan Mitchell', 'Sales Representative', '2022-06-14', 7),
(15, 'Lauren Hall', 'Sales Representative', '2022-09-08', 7),
(16, 'Andrew Young', 'Sales Representative', '2023-01-16', 4),
(17, 'Stephanie King', 'Sales Representative', '2023-04-03', 5),
(18, 'Nathan Green', 'Junior Sales Rep', '2023-07-10', 8),
(19, 'Olivia Baker', 'Junior Sales Rep', '2023-10-02', 9),
(20, 'Ethan Nelson', 'Junior Sales Rep', '2024-01-08', 10),
(21, 'Sophia Carter', 'Junior Sales Rep', '2024-03-18', 11),
(22, 'Daniel Perez', 'Sales Trainee', '2024-06-01', 12),
(23, 'Emma Roberts', 'Sales Trainee', '2024-08-15', 13),
(24, 'Matthew Turner', 'Sales Trainee', '2024-10-01', 14),
(25, 'Isabella Phillips', 'Sales Trainee', '2025-01-06', 15);

-- ============================================================================
-- POPULATE FACT_SALES
-- ============================================================================

INSERT INTO FACT_SALES
WITH date_range AS (
    SELECT DATE_KEY, FULL_DATE, IS_HOLIDAY
    FROM DIM_DATE
),
product_prices AS (
    SELECT PRODUCT_KEY, SUPPLIER_PRICE,
           CASE 
               WHEN PRODUCT_KEY <= 30 THEN '2023-01-01'::DATE
               ELSE '2024-06-01'::DATE
           END AS AVAILABLE_FROM
    FROM DIM_PRODUCT
),
base_sales AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY RANDOM()) AS SALES_ID,
        d.DATE_KEY,
        p.PRODUCT_KEY,
        UNIFORM(1, 500, RANDOM()) AS CUSTOMER_KEY,
        UNIFORM(1, 25, RANDOM()) AS STORE_KEY,
        CASE 
            WHEN UNIFORM(1, 100, RANDOM()) <= 60 THEN UNIFORM(8, 25, RANDOM())
            WHEN UNIFORM(1, 100, RANDOM()) <= 85 THEN UNIFORM(4, 7, RANDOM())
            ELSE UNIFORM(1, 3, RANDOM())
        END AS EMPLOYEE_KEY,
        CASE 
            WHEN d.IS_HOLIDAY THEN UNIFORM(1, 8, RANDOM())
            WHEN DAYOFWEEK(d.FULL_DATE) IN (0, 6) THEN UNIFORM(1, 5, RANDOM())
            ELSE UNIFORM(1, 3, RANDOM())
        END AS QUANTITY,
        ROUND(p.SUPPLIER_PRICE * UNIFORM(120, 180, RANDOM()) / 100, 2) AS UNIT_PRICE,
        CASE 
            WHEN d.IS_HOLIDAY THEN ROUND(p.SUPPLIER_PRICE * UNIFORM(10, 30, RANDOM()) / 100, 2)
            WHEN UNIFORM(1, 100, RANDOM()) <= 30 THEN ROUND(p.SUPPLIER_PRICE * UNIFORM(5, 15, RANDOM()) / 100, 2)
            ELSE 0
        END AS DISCOUNT_AMOUNT
    FROM date_range d
    CROSS JOIN product_prices p
    WHERE d.FULL_DATE >= p.AVAILABLE_FROM
      AND UNIFORM(1, 100, RANDOM()) <= 
          CASE 
              WHEN d.IS_HOLIDAY THEN 25
              WHEN DAYOFWEEK(d.FULL_DATE) IN (0, 6) THEN 15
              ELSE 8
          END
)
SELECT
    SALES_ID,
    DATE_KEY,
    PRODUCT_KEY,
    CUSTOMER_KEY,
    STORE_KEY,
    EMPLOYEE_KEY,
    QUANTITY,
    UNIT_PRICE,
    DISCOUNT_AMOUNT,
    ROUND((QUANTITY * UNIT_PRICE) - DISCOUNT_AMOUNT, 2) AS NET_SALES
FROM base_sales;

SHOW TABLES IN DATABASE;