-- Banking CDC Streaming - PostgreSQL Source Database
-- Configured for Debezium CDC with logical replication

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create tables
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(50),
    date_of_birth DATE,
    nationality VARCHAR(10),
    risk_score INTEGER CHECK (risk_score BETWEEN 1 AND 100),
    kyc_status VARCHAR(20) CHECK (kyc_status IN ('VERIFIED', 'PENDING', 'REJECTED')),
    onboarding_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    customer_segment VARCHAR(50) CHECK (customer_segment IN ('RETAIL', 'CORPORATE', 'PRIVATE_BANKING')),
    country_residence VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    account_type VARCHAR(50) CHECK (account_type IN ('SAVINGS', 'CHECKING', 'CREDIT_CARD', 'LOAN')),
    currency VARCHAR(10) CHECK (currency IN ('AED', 'USD', 'EUR', 'GBP', 'SAR')),
    balance DECIMAL(18, 2) DEFAULT 0.00,
    status VARCHAR(20) CHECK (status IN ('ACTIVE', 'INACTIVE', 'FROZEN', 'CLOSED')) DEFAULT 'ACTIVE',
    opened_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    transaction_type VARCHAR(20) CHECK (transaction_type IN ('DEBIT', 'CREDIT')),
    amount DECIMAL(18, 2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(10),
    merchant_name VARCHAR(255),
    merchant_category VARCHAR(50),
    transaction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    country VARCHAR(10),
    is_international BOOLEAN DEFAULT FALSE,
    status VARCHAR(20) CHECK (status IN ('COMPLETED', 'PENDING', 'FAILED', 'REVERSED')) DEFAULT 'COMPLETED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_risk_score ON customers(risk_score);
CREATE INDEX idx_accounts_customer ON accounts(customer_id);
CREATE INDEX idx_accounts_status ON accounts(status);
CREATE INDEX idx_transactions_account ON transactions(account_id);
CREATE INDEX idx_transactions_timestamp ON transactions(transaction_timestamp);
CREATE INDEX idx_transactions_amount ON transactions(amount);

-- Enable logical replication (CRITICAL for Debezium)
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

-- Create updated_at trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_accounts_updated_at
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Grant privileges
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO banking_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO banking_user;

-- Create publication for Debezium (CRITICAL)
CREATE PUBLICATION dbz_publication FOR TABLE customers, accounts, transactions;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ Banking CDC database initialized!';
    RAISE NOTICE '✅ Logical replication enabled';
    RAISE NOTICE '✅ Publication created: dbz_publication';
END $$;