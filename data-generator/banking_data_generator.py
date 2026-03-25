#!/usr/bin/env python3
"""
Banking Data Generator - Writes to PostgreSQL (triggers Debezium CDC)

This generates realistic banking data by inserting into PostgreSQL.
Debezium automatically captures these changes and publishes to Kafka.

Usage:
    python banking_data_generator.py --duration 10 --rate 20
"""

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import random
import time
import sys
import signal
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Tuple
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker
fake = Faker()
Faker.seed(12345)


class BankingDataGenerator:
    """Generates banking data by inserting into PostgreSQL"""
    
    def __init__(self, db_config: dict):
        """Initialize database connection"""
        self.db_config = db_config
        self.conn = None
        self.cur = None
        self.running = True
        
        # In-memory tracking
        self.customer_ids: List[str] = []
        self.account_ids: List[str] = []
        
        # Statistics
        self.stats = {
            'customers_created': 0,
            'accounts_created': 0,
            'transactions_created': 0,
            'customer_updates': 0,
            'account_updates': 0
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self._connect()
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown gracefully"""
        logger.info("\n🛑 Shutdown signal received. Stopping generator...")
        self.running = False
    
    def _connect(self):
        """Connect to PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cur = self.conn.cursor()
            logger.info(f"✅ Connected to PostgreSQL: {self.db_config['host']}:{self.db_config['port']}")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            sys.exit(1)
    
    def _load_existing_data(self):
        """Load existing customer and account IDs"""
        try:
            # Load customer IDs
            self.cur.execute("SELECT customer_id FROM customers")
            self.customer_ids = [row[0] for row in self.cur.fetchall()]
            
            # Load account IDs
            self.cur.execute("SELECT account_id FROM accounts")
            self.account_ids = [row[0] for row in self.cur.fetchall()]
            
            logger.info(f"📊 Loaded {len(self.customer_ids)} existing customers, {len(self.account_ids)} accounts")
        except Exception as e:
            logger.warning(f"⚠️  Could not load existing data: {e}")
    
    def bootstrap_customers(self, count: int = 100):
        """Create initial customers"""
        logger.info(f"🌱 Creating {count} initial customers...")
        
        customers = []
        for i in range(count):
            customer_id = f"CUST{fake.unique.random_int(min=1000000000, max=9999999999)}"
            customer = (
                customer_id,
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.phone_number()[:20],
                fake.date_of_birth(minimum_age=18, maximum_age=80),
                random.choice(['AE', 'SA', 'IN', 'PK', 'EG', 'US', 'GB']),
                random.randint(1, 100),
                random.choice(['VERIFIED', 'PENDING', 'REJECTED']),
                datetime.utcnow(),
                random.choice(['RETAIL', 'CORPORATE', 'PRIVATE_BANKING']),
                'AE'
            )
            customers.append(customer)
            self.customer_ids.append(customer_id)
            
            if (i + 1) % 25 == 0:
                logger.info(f"   Progress: {i + 1}/{count} customers")
        
        # Bulk insert
        try:
            execute_values(
                self.cur,
                """
                INSERT INTO customers (
                    customer_id, first_name, last_name, email, phone,
                    date_of_birth, nationality, risk_score, kyc_status,
                    onboarding_date, customer_segment, country_residence
                ) VALUES %s
                """,
                customers
            )
            self.conn.commit()
            self.stats['customers_created'] += count
            logger.info(f"✅ Created {count} customers")
        except Exception as e:
            logger.error(f"❌ Error creating customers: {e}")
            self.conn.rollback()
    
    def bootstrap_accounts(self, count: int = 200):
        """Create initial accounts"""
        if not self.customer_ids:
            logger.warning("⚠️  No customers available. Create customers first.")
            return
        
        logger.info(f"🌱 Creating {count} initial accounts...")
        
        accounts = []
        for i in range(count):
            account_id = f"ACC{fake.unique.random_int(min=1000000000, max=9999999999)}"
            account = (
                account_id,
                random.choice(self.customer_ids),
                random.choice(['SAVINGS', 'CHECKING', 'CREDIT_CARD', 'LOAN']),
                random.choice(['AED', 'USD', 'EUR', 'GBP']),
                round(random.uniform(1000, 500000), 2),
                'ACTIVE',
                datetime.utcnow(),
                datetime.utcnow()
            )
            accounts.append(account)
            self.account_ids.append(account_id)
            
            if (i + 1) % 50 == 0:
                logger.info(f"   Progress: {i + 1}/{count} accounts")
        
        # Bulk insert
        try:
            execute_values(
                self.cur,
                """
                INSERT INTO accounts (
                    account_id, customer_id, account_type, currency,
                    balance, status, opened_date, last_activity_date
                ) VALUES %s
                """,
                accounts
            )
            self.conn.commit()
            self.stats['accounts_created'] += count
            logger.info(f"✅ Created {count} accounts")
        except Exception as e:
            logger.error(f"❌ Error creating accounts: {e}")
            self.conn.rollback()
    
    def generate_transaction(self, high_risk: bool = False):
        """Generate a single transaction"""
        if not self.account_ids:
            return
        
        account_id = random.choice(self.account_ids)
        transaction_id = f"TXN{fake.unique.random_int(min=100000000000, max=999999999999)}"
        
        if high_risk:
            # High-risk transaction for AML testing
            transaction = (
                transaction_id,
                account_id,
                'DEBIT',
                random.choice([55000, 75000, 100000, 150000]),
                'AED',
                random.choice(['Crypto Exchange', 'Offshore Casino', 'Unknown Merchant']),
                random.choice(['CRYPTO', 'GAMBLING']),
                datetime.utcnow(),
                random.choice(['US', 'CN', 'RU', 'VE']),
                True,
                'COMPLETED'
            )
        else:
            # Normal transaction
            transaction = (
                transaction_id,
                account_id,
                random.choice(['DEBIT', 'CREDIT']),
                round(random.uniform(10, 5000), 2),
                random.choice(['AED', 'USD', 'EUR', 'GBP']),
                fake.company(),
                random.choice(['GROCERIES', 'RESTAURANTS', 'FUEL', 'SHOPPING',
                             'ELECTRONICS', 'TRAVEL', 'UTILITIES', 'HEALTHCARE']),
                datetime.utcnow(),
                random.choice(['AE', 'US', 'GB', 'SA', 'IN']),
                random.choice([True, False]),
                'COMPLETED'
            )
        
        try:
            self.cur.execute(
                """
                INSERT INTO transactions (
                    transaction_id, account_id, transaction_type, amount, currency,
                    merchant_name, merchant_category, transaction_timestamp,
                    country, is_international, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                transaction
            )
            self.conn.commit()
            self.stats['transactions_created'] += 1
        except Exception as e:
            logger.error(f"❌ Error creating transaction: {e}")
            self.conn.rollback()
    
    def update_customer_risk_score(self):
        """Update a customer's risk score (triggers UPDATE CDC event)"""
        if not self.customer_ids:
            return
        
        customer_id = random.choice(self.customer_ids)
        new_risk_score = random.randint(1, 100)
        
        try:
            self.cur.execute(
                """
                UPDATE customers 
                SET risk_score = %s, kyc_status = %s, updated_at = %s
                WHERE customer_id = %s
                """,
                (new_risk_score, random.choice(['VERIFIED', 'PENDING']), datetime.utcnow(), customer_id)
            )
            self.conn.commit()
            self.stats['customer_updates'] += 1
        except Exception as e:
            logger.error(f"❌ Error updating customer: {e}")
            self.conn.rollback()
    
    def update_account_balance(self):
        """Update an account balance (triggers UPDATE CDC event)"""
        if not self.account_ids:
            return
        
        account_id = random.choice(self.account_ids)
        
        try:
            # Get current balance
            self.cur.execute("SELECT balance FROM accounts WHERE account_id = %s", (account_id,))
            result = self.cur.fetchone()
            if not result:
                return
            
            current_balance = result[0]
            change = Decimal(str(round(random.uniform(-10000, 10000), 2)))
            new_balance = max(Decimal('0'), current_balance + change)
            
            self.cur.execute(
                """
                UPDATE accounts 
                SET balance = %s, last_activity_date = %s, updated_at = %s
                WHERE account_id = %s
                """,
                (round(new_balance, 2), datetime.utcnow(), datetime.utcnow(), account_id)
            )
            self.conn.commit()
            self.stats['account_updates'] += 1
        except Exception as e:
            logger.error(f"❌ Error updating account: {e}")
            self.conn.rollback()
    
    def run(self, duration_minutes: int = 60, events_per_minute: int = 20):
        """Run data generation"""
        logger.info("=" * 70)
        logger.info("🚀 Starting Banking Data Generator")
        logger.info("=" * 70)
        logger.info(f"⏱️  Duration: {duration_minutes} minutes")
        logger.info(f"📊 Rate: {events_per_minute} events/minute")
        logger.info(f"🎯 Database: {self.db_config['dbname']}@{self.db_config['host']}")
        logger.info("=" * 70 + "\n")
        
        # Load existing data
        self._load_existing_data()
        
        # Bootstrap if needed
        if len(self.customer_ids) < 50:
            self.bootstrap_customers(100)
            self.bootstrap_accounts(200)
        else:
            logger.info(f"📊 Using existing data: {len(self.customer_ids)} customers, {len(self.account_ids)} accounts")
        
        logger.info("\n📡 Starting event stream...\n")
        
        start_time = datetime.utcnow()
        end_time = start_time + timedelta(minutes=duration_minutes)
        event_count = 0
        
        while datetime.utcnow() < end_time and self.running:
            
            # Generate mix of events
            event_type = random.choices(
                ['transaction', 'high_risk_txn', 'customer_update', 'account_update'],
                weights=[65, 10, 15, 10]
            )[0]
            
            try:
                if event_type == 'transaction':
                    self.generate_transaction(high_risk=False)
                elif event_type == 'high_risk_txn':
                    self.generate_transaction(high_risk=True)
                elif event_type == 'customer_update':
                    self.update_customer_risk_score()
                else:  # account_update
                    self.update_account_balance()
                
                event_count += 1
                
                if event_count % 10 == 0:
                    logger.info(
                        f"📤 [{event_count}] Events: "
                        f"TXN={self.stats['transactions_created']} "
                        f"CUST_UPD={self.stats['customer_updates']} "
                        f"ACC_UPD={self.stats['account_updates']}"
                    )
                
            except Exception as e:
                logger.error(f"❌ Error generating {event_type}: {e}")
            
            # Sleep to maintain rate
            time.sleep(60 / events_per_minute)
        
        # Shutdown
        logger.info("\n" + "=" * 70)
        logger.info("🏁 Data Generation Complete")
        logger.info("=" * 70)
        logger.info(f"📊 Total Events: {event_count}")
        logger.info(f"📈 Statistics:")
        logger.info(f"   • Customers created: {self.stats['customers_created']}")
        logger.info(f"   • Accounts created: {self.stats['accounts_created']}")
        logger.info(f"   • Transactions: {self.stats['transactions_created']}")
        logger.info(f"   • Customer updates: {self.stats['customer_updates']}")
        logger.info(f"   • Account updates: {self.stats['account_updates']}")
        logger.info("=" * 70)
        
        self.cur.close()
        self.conn.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Banking Data Generator (triggers Debezium CDC)'
    )
    parser.add_argument('--duration', type=int, default=60, help='Duration in minutes')
    parser.add_argument('--rate', type=int, default=20, help='Events per minute')
    parser.add_argument('--host', type=str, default=os.getenv('POSTGRES_HOST', 'localhost'))
    parser.add_argument('--port', type=int, default=int(os.getenv('POSTGRES_PORT_EXTERNAL', 5433)))
    parser.add_argument('--db', type=str, default=os.getenv('POSTGRES_DB', 'banking_core'))
    parser.add_argument('--user', type=str, default=os.getenv('POSTGRES_USER', 'banking_user'))
    parser.add_argument('--password', type=str, default=os.getenv('POSTGRES_PASSWORD', 'banking_pass'))
    
    args = parser.parse_args()
    
    db_config = {
        'host': args.host,
        'port': args.port,
        'dbname': args.db,
        'user': args.user,
        'password': args.password
    }
    
    try:
        generator = BankingDataGenerator(db_config)
        generator.run(duration_minutes=args.duration, events_per_minute=args.rate)
    except Exception as e:
        logger.error(f"❌ Generator failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()