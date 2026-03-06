#!/usr/bin/env python3
"""
CDC (Change Data Capture) Simulator for Banking Lakehouse
Simulates real-time database changes and publishes to Kafka

Usage:
    python cdc_simulator.py --duration 10 --rate 20
"""

import json
import random
import time
import sys
import signal
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from kafka import KafkaProducer
from kafka.errors import KafkaError
from faker import Faker
import uuid
import logging
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Initialize Faker
fake = Faker()
Faker.seed(12345)

class CDCEventSimulator:
    """Simulates CDC events from banking databases"""
    
    def __init__(self, 
                 kafka_bootstrap_servers: str = 'kafka:9093',
                 bootstrap_customers: int = 100,
                 bootstrap_accounts: int = 200):
        """
        Initialize CDC Simulator
        
        Args:
            kafka_bootstrap_servers: Kafka broker address
            bootstrap_customers: Initial number of customers to create
            bootstrap_accounts: Initial number of accounts to create
        """
        self.bootstrap_servers = kafka_bootstrap_servers
        self.bootstrap_customers_count = bootstrap_customers
        self.bootstrap_accounts_count = bootstrap_accounts
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1,
                compression_type='gzip'
            )
            logger.info(f"✅ Connected to Kafka: {kafka_bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"❌ Failed to connect to Kafka: {e}")
            sys.exit(1)
        
        # In-memory state (simulates database)
        self.customers: Dict[str, dict] = {}
        self.accounts: Dict[str, dict] = {}
        self.event_count = 0
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("✅ CDC Simulator initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info("\n🛑 Shutdown signal received. Stopping simulator...")
        self.running = False
    
    def _create_cdc_event(self, 
                         operation: str, 
                         source_table: str,
                         before: Optional[dict] = None, 
                         after: Optional[dict] = None) -> dict:
        """
        Create a CDC event in Debezium format
        
        Args:
            operation: 'c' (create), 'u' (update), 'r' (read), 'd' (delete)
            source_table: Table name
            before: Record state before change
            after: Record state after change
        
        Returns:
            CDC event dictionary
        """
        current_ts = int(datetime.utcnow().timestamp() * 1000)
        
        return {
            "schema": {
                "type": "struct",
                "fields": [],
                "optional": False,
                "name": f"banking_core.public.{source_table}.Envelope"
            },
            "payload": {
                "op": operation,
                "source": {
                    "version": "1.0.0",
                    "connector": "postgresql",
                    "name": "banking_core",
                    "ts_ms": current_ts,
                    "db": "banking_core",
                    "schema": "public",
                    "table": source_table
                },
                "ts_ms": current_ts,
                "before": before,
                "after": after
            }
        }
    
    def generate_customer_insert(self) -> Tuple[str, str, dict]:
        """Generate new customer INSERT event"""
        customer_id = f"CUST{str(uuid.uuid4().int)[:10]}"
        
        customer_data = {
            "customer_id": customer_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number()[:20],
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "nationality": random.choice(['AE', 'SA', 'IN', 'PK', 'EG', 'US', 'GB']),
            "risk_score": random.randint(1, 100),
            "kyc_status": random.choice(['VERIFIED', 'PENDING', 'REJECTED']),
            "onboarding_date": datetime.now(timezone.utc),
            "customer_segment": random.choice(['RETAIL', 'CORPORATE', 'PRIVATE_BANKING']),
            "country_residence": 'AE',
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        
        # Store in state
        self.customers[customer_id] = customer_data.copy()
        
        # Create CDC event
        event = self._create_cdc_event('c', 'customers', before=None, after=customer_data)
        
        return 'banking.customers', customer_id, event
    
    def generate_customer_update(self) -> Optional[Tuple[str, str, dict]]:
        """Generate customer UPDATE event"""
        if not self.customers:
            return None
        
        customer_id = random.choice(list(self.customers.keys()))
        before_state = self.customers[customer_id].copy()
        
        # Simulate risk score update
        after_state = before_state.copy()
        after_state['risk_score'] = random.randint(1, 100)
        after_state['kyc_status'] = random.choice(['VERIFIED', 'PENDING'])
        after_state['updated_at'] = datetime.now(timezone.utc)
        
        # Update state
        self.customers[customer_id] = after_state
        
        event = self._create_cdc_event('u', 'customers', before=before_state, after=after_state)
        
        return 'banking.customers', customer_id, event
    
    def generate_account_insert(self) -> Optional[Tuple[str, str, dict]]:
        """Generate new account INSERT event"""
        if not self.customers:
            return None
        
        account_id = f"ACC{str(uuid.uuid4().int)[:10]}"
        customer_id = random.choice(list(self.customers.keys()))
        
        account_data = {
            "account_id": account_id,
            "customer_id": customer_id,
            "account_type": random.choice(['SAVINGS', 'CHECKING', 'CREDIT_CARD', 'LOAN']),
            "currency": random.choice(['AED', 'USD', 'EUR', 'GBP']),
            "balance": float(round(random.uniform(1000, 500000), 2)),
            "status": "ACTIVE",
            "opened_date": datetime.now(timezone.utc),
            "last_activity_date": datetime.now(timezone.utc),
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        
        # Store in state
        self.accounts[account_id] = account_data.copy()
        
        event = self._create_cdc_event('c', 'accounts', before=None, after=account_data)
        
        return 'banking.accounts', account_id, event
    
    def generate_account_update(self) -> Optional[Tuple[str, str, dict]]:
        """Generate account UPDATE event (balance change)"""
        if not self.accounts:
            return None
        
        account_id = random.choice(list(self.accounts.keys()))
        before_state = self.accounts[account_id].copy()
        
        # Simulate balance update
        after_state = before_state.copy()
        change = random.uniform(-10000, 10000)
        after_state['balance'] = float(round(max(0, before_state['balance'] + change), 2))
        after_state['last_activity_date'] = datetime.now(timezone.utc)
        after_state['updated_at'] = datetime.now(timezone.utc)
        
        # Update state
        self.accounts[account_id] = after_state
        
        event = self._create_cdc_event('u', 'accounts', before=before_state, after=after_state)
        
        return 'banking.accounts', account_id, event
    
    def generate_transaction_insert(self) -> Optional[Tuple[str, str, dict]]:
        """Generate new transaction INSERT event"""
        if not self.accounts:
            return None
        
        account_id = random.choice(list(self.accounts.keys()))
        account = self.accounts[account_id]
        transaction_id = f"TXN{str(uuid.uuid4().int)[:12]}"
        
        transaction_data = {
            "transaction_id": transaction_id,
            "account_id": account_id,
            "transaction_type": random.choice(['DEBIT', 'CREDIT']),
            "amount": float(round(random.uniform(10, 50000), 2)),
            "currency": account['currency'],
            "merchant_name": fake.company(),
            "merchant_category": random.choice([
                'GROCERIES', 'RESTAURANTS', 'FUEL', 'SHOPPING',
                'ELECTRONICS', 'TRAVEL', 'UTILITIES', 'HEALTHCARE'
            ]),
            "transaction_timestamp": datetime.now(timezone.utc),
            "country": random.choice(['AE', 'US', 'GB', 'SA', 'IN']),
            "is_international": random.choice([True, False]),
            "status": "COMPLETED",
            "created_at": datetime.now(timezone.utc)
        }
        
        event = self._create_cdc_event('c', 'transactions', before=None, after=transaction_data)
        
        return 'banking.transactions', transaction_id, event
    
    def generate_high_risk_transaction(self) -> Optional[Tuple[str, str, dict]]:
        """Generate suspicious transaction for AML testing"""
        if not self.accounts:
            return None
        
        account_id = random.choice(list(self.accounts.keys()))
        transaction_id = f"TXN{str(uuid.uuid4().int)[:12]}"
        
        # High-risk patterns
        transaction_data = {
            "transaction_id": transaction_id,
            "account_id": account_id,
            "transaction_type": "DEBIT",
            "amount": float(random.choice([55000, 75000, 100000, 150000])),
            "currency": "AED",
            "merchant_name": random.choice(["Crypto Exchange", "Offshore Casino", "Unknown Merchant"]),
            "merchant_category": random.choice(['CRYPTO', 'GAMBLING']),
            "transaction_timestamp": datetime.now(timezone.utc),
            "country": random.choice(['US', 'CN', 'RU', 'VE']),  # High-risk countries
            "is_international": True,
            "status": "COMPLETED",
            "created_at": datetime.now(timezone.utc)
        }
        
        event = self._create_cdc_event('c', 'transactions', before=None, after=transaction_data)
        
        return 'banking.transactions', transaction_id, event
    
    def publish_event(self, topic: str, key: str, event: dict) -> bool:
        """Publish CDC event to Kafka topic"""
        try:
            future = self.producer.send(topic, key=key, value=event)
            record_metadata = future.get(timeout=10)
            
            self.event_count += 1
            
            if self.event_count % 10 == 0:  # Log every 10th event
                logger.info(
                    f"📤 [{self.event_count}] {topic.split('.')[-1][:4].upper()} | "
                    f"Op: {event['payload']['op']} | "
                    f"Partition: {record_metadata.partition} | "
                    f"Offset: {record_metadata.offset}"
                )
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error publishing to {topic}: {e}")
            return False
    
    def bootstrap_data(self):
        """Create initial customers and accounts"""
        logger.info("🌱 Bootstrapping initial data...")
        
        # Create customers
        for i in range(self.bootstrap_customers_count):
            topic, key, event = self.generate_customer_insert()
            self.publish_event(topic, key, event)
            
            if (i + 1) % 25 == 0:
                logger.info(f"   Created {i + 1}/{self.bootstrap_customers_count} customers")
        
        # Create accounts
        for i in range(self.bootstrap_accounts_count):
            result = self.generate_account_insert()
            if result:
                topic, key, event = result
                self.publish_event(topic, key, event)
                
                if (i + 1) % 50 == 0:
                    logger.info(f"   Created {i + 1}/{self.bootstrap_accounts_count} accounts")
        
        logger.info(
            f"✅ Bootstrap complete: {len(self.customers)} customers, "
            f"{len(self.accounts)} accounts\n"
        )
    
    def run_simulation(self, duration_minutes: int = 60, events_per_minute: int = 20):
        """
        Run CDC simulation
        
        Args:
            duration_minutes: How long to run simulation
            events_per_minute: Rate of event generation
        """
        logger.info("=" * 70)
        logger.info("🚀 Starting CDC Simulation")
        logger.info("=" * 70)
        logger.info(f"⏱️  Duration: {duration_minutes} minutes")
        logger.info(f"📊 Rate: {events_per_minute} events/minute")
        logger.info(f"📈 Expected total events: ~{duration_minutes * events_per_minute}")
        logger.info(f"🎯 Kafka brokers: {self.bootstrap_servers}")
        logger.info("=" * 70 + "\n")
        
        # Bootstrap initial data
        self.bootstrap_data()
        
        # Main simulation loop
        start_time = datetime.utcnow()
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        logger.info("📡 Starting event stream...\n")
        
        while datetime.utcnow() < end_time and self.running:
            
            # Generate mix of events with weighted probabilities
            event_type = random.choices(
                ['transaction', 'high_risk_txn', 'customer_update', 'account_update', 'account_insert'],
                weights=[60, 10, 10, 15, 5]
            )[0]
            
            result = None
            
            if event_type == 'transaction':
                result = self.generate_transaction_insert()
            elif event_type == 'high_risk_txn':
                result = self.generate_high_risk_transaction()
            elif event_type == 'customer_update':
                result = self.generate_customer_update()
            elif event_type == 'account_update':
                result = self.generate_account_update()
            else:  # account_insert
                result = self.generate_account_insert()
            
            if result:
                topic, key, event = result
                self.publish_event(topic, key, event)
            
            # Sleep to maintain rate
            time.sleep(60 / events_per_minute)
        
        # Shutdown
        logger.info("\n" + "=" * 70)
        logger.info("🏁 Simulation Complete")
        logger.info("=" * 70)
        logger.info(f"📊 Total events published: {self.event_count}")
        logger.info(f"👥 Final state: {len(self.customers)} customers, {len(self.accounts)} accounts")
        logger.info(f"⏱️  Duration: {(datetime.utcnow() - start_time).total_seconds():.1f} seconds")
        logger.info("=" * 70)
        
        self.producer.flush()
        self.producer.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='CDC Event Simulator for Banking Lakehouse',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--duration', 
        type=int, 
        default=60,
        help='Simulation duration in minutes (default: 60)'
    )
    parser.add_argument(
        '--rate', 
        type=int, 
        default=20,
        help='Events per minute (default: 20)'
    )
    parser.add_argument(
        '--kafka', 
        type=str, 
        default=os.getenv('KAFKA_BOOTSTRAP_SERVERS_EXTERNAL', 'localhost:9092'),
        help='Kafka bootstrap servers (default: from .env or localhost:9092)'
    )
    parser.add_argument(
        '--bootstrap-customers',
        type=int,
        default=int(os.getenv('CDC_BOOTSTRAP_CUSTOMERS', 100)),
        help='Number of initial customers to create'
    )
    parser.add_argument(
        '--bootstrap-accounts',
        type=int,
        default=int(os.getenv('CDC_BOOTSTRAP_ACCOUNTS', 200)),
        help='Number of initial accounts to create'
    )
    
    args = parser.parse_args()
    
    # Load environment variables from .env file
    load_dotenv()
    
    try:
        simulator = CDCEventSimulator(
            kafka_bootstrap_servers=args.kafka,
            bootstrap_customers=args.bootstrap_customers,
            bootstrap_accounts=args.bootstrap_accounts
        )
        simulator.run_simulation(
            duration_minutes=args.duration,
            events_per_minute=args.rate
        )
    except Exception as e:
        logger.error(f"❌ Simulation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()