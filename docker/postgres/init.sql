-- Create source tables (simulate banking database)

CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    date_of_birth DATE,
    nationality VARCHAR(10),
    risk_score INTEGER,
    kyc_status VARCHAR(20),
    onboarding_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    customer_segment VARCHAR(50),
    country_residence VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) REFERENCES customers(customer_id),
    account_type VARCHAR(50),
    currency VARCHAR(10),
    balance DECIMAL(18, 2),
    status VARCHAR(20),
    opened_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50) REFERENCES accounts(account_id),
    transaction_type VARCHAR(20),
    amount DECIMAL(18, 2),
    currency VARCHAR(10),
    merchant_name VARCHAR(255),
    merchant_category VARCHAR(50),
    transaction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    country VARCHAR(10),
    is_international BOOLEAN,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_accounts_customer ON accounts(customer_id);
CREATE INDEX idx_transactions_account ON transactions(account_id);
CREATE INDEX idx_transactions_timestamp ON transactions(transaction_timestamp);

-- Enable logical replication (for Debezium CDC - optional)
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;