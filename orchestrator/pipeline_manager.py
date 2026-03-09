#!/usr/bin/env python3
"""
Streaming Pipeline Orchestrator

Manages the entire CDC streaming pipeline lifecycle.

Usage:
    python pipeline_manager.py start --all
    python pipeline_manager.py status
    python pipeline_manager.py stop --all
"""

import click
import docker
import requests
import time
import json
from typing import Dict, List, Optional
from rich.console import Console
from rich.table import Table
from rich import print as rprint
import logging
from jobs_definition import JOBS, DEBEZIUM_CONNECTOR, SPARK_SUBMIT_TEMPLATE

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

console = Console()


class PipelineOrchestrator:
    """Manages streaming pipeline lifecycle"""
    
    def __init__(self, spark_master: str = "spark://spark-master:7077"):
        self.spark_master = spark_master
        self.docker_client = docker.from_env()
        self.running_jobs: Dict[str, dict] = {}
    
    def deploy_debezium_connector(self) -> bool:
        """Deploy Debezium PostgreSQL connector"""
        console.print("\n[bold cyan]📡 Deploying Debezium Connector...[/bold cyan]")
        
        connector_name = DEBEZIUM_CONNECTOR['name']
        connect_url = "http://localhost:8083"
        
        # Load connector config
        try:
            with open('./config/debezium-connector.json', 'r') as f:
                connector_config = json.load(f)
        except FileNotFoundError:
            logger.error("❌ Connector config not found: ../config/debezium-connector.json")
            return False
        
        # Check if connector exists
        try:
            response = requests.get(f"{connect_url}/connectors/{connector_name}")
            if response.status_code == 200:
                console.print(f"[yellow]⚠️  Connector '{connector_name}' already exists[/yellow]")
                return True
        except requests.exceptions.RequestException:
            logger.error("❌ Cannot reach Kafka Connect")
            return False
        
        # Create connector
        try:
            response = requests.post(
                f"{connect_url}/connectors",
                json=connector_config,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code in [200, 201]:
                console.print(f"[green]✅ Connector '{connector_name}' deployed successfully[/green]")
                return True
            else:
                logger.error(f"❌ Failed to create connector: {response.text}")
                return False
        except Exception as e:
            logger.error(f"❌ Error deploying connector: {e}")
            return False
    
    def check_debezium_health(self) -> str:
        """Check Debezium connector health"""
        try:
            response = requests.get(
                "http://localhost:8083/connectors/banking-postgres-connector/status",
                timeout=5
            )
            if response.status_code == 200:
                status = response.json()
                connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
                task_state = status.get('tasks', [{}])[0].get('state', 'UNKNOWN')
                
                if connector_state == 'RUNNING' and task_state == 'RUNNING':
                    return "✅ RUNNING"
                else:
                    return f"⚠️ {connector_state}/{task_state}"
            else:
                return "❌ NOT FOUND"
        except Exception as e:
            return f"❌ ERROR: {str(e)[:30]}"
    
    def start_spark_job(self, job_id: str) -> bool:
        """Start a Spark streaming job"""
        if job_id not in JOBS:
            logger.error(f"❌ Unknown job: {job_id}")
            return False
        
        job = JOBS[job_id]
        console.print(f"\n[bold cyan]🚀 Starting job: {job['name']}[/bold cyan]")
        console.print(f"   Description: {job['description']}")
        
        # Check dependencies
        for dep in job.get('depends_on', []):
            if dep not in self.running_jobs:
                console.print(f"[yellow]⚠️  Dependency not running: {dep}[/yellow]")
                console.print(f"   Starting dependency first...")
                if not self.start_spark_job(dep):
                    return False
                time.sleep(5)
        
        # Build spark-submit command
        cmd = SPARK_SUBMIT_TEMPLATE.format(
            spark_master=self.spark_master,
            job_name=job['name'],
            script_path=job['script']
        )
        
        # Execute in Spark master container
        try:
            container = self.docker_client.containers.get("cdc-spark-master")

            # Collapse the multi-line template into a single command string
            # and run it via bash -c so the shell handles path expansion
            single_line_cmd = " ".join(
                line.strip().rstrip("\\").strip()
                for line in cmd.strip().splitlines()
                if line.strip() and line.strip() != "\\"
            )

            # Run in detached mode
            exec_result = container.exec_run(
                ["bash", "-c", single_line_cmd],
                detach=True
            )

            self.running_jobs[job_id] = {
                'name': job['name'],
                'exec_id': exec_result.output if hasattr(exec_result, 'output') else None,
                'started_at': time.time()
            }

            console.print(f"[green]✅ Job '{job['name']}' submitted to Spark[/green]")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to start job: {e}")
            return False
    
    def check_kafka_topics(self) -> Dict[str, int]:
        """Check Kafka topic message counts"""
        topics = {}
        try:
            container = self.docker_client.containers.get("cdc-kafka")
            
            for topic in ['cdc.public.transactions', 'cdc.public.customers', 'cdc.public.accounts']:
                cmd = f"""
                kafka-run-class kafka.tools.GetOffsetShell \
                    --broker-list localhost:9092 \
                    --topic {topic} | tail -1
                """
                result = container.exec_run(f"bash -c '{cmd}'")
                
                if result.exit_code == 0:
                    output = result.output.decode('utf-8').strip()
                    if ':' in output:
                        offset = int(output.split(':')[-1])
                        topics[topic] = offset
                    else:
                        topics[topic] = 0
                else:
                    topics[topic] = 0
        except Exception as e:
            logger.warning(f"Could not check Kafka topics: {e}")
        
        return topics
    
    def get_pipeline_status(self) -> Dict:
        """Get status of entire pipeline"""
        status = {
            'debezium': self.check_debezium_health(),
            'kafka_topics': self.check_kafka_topics(),
            'spark_jobs': {}
        }
        
        # Check Spark jobs (simplified - just check if submitted)
        for job_id, job in JOBS.items():
            if job_id in self.running_jobs:
                status['spark_jobs'][job_id] = "✅ RUNNING"
            else:
                status['spark_jobs'][job_id] = "⏸️  NOT STARTED"
        
        return status
    
    def start_all(self):
        """Start entire pipeline in dependency order"""
        console.print("\n[bold green]" + "=" * 70 + "[/bold green]")
        console.print("[bold green]🚀 Starting Complete Streaming Pipeline[/bold green]")
        console.print("[bold green]" + "=" * 70 + "[/bold green]\n")
        
        # 1. Deploy Debezium
        if not self.deploy_debezium_connector():
            console.print("[red]❌ Failed to deploy Debezium. Aborting.[/red]")
            return False
        
        console.print("[cyan]⏳ Waiting for Debezium to start capturing...[/cyan]")
        time.sleep(10)
        
        # 2. Start Bronze layer
        if not self.start_spark_job("bronze"):
            console.print("[red]❌ Failed to start Bronze layer. Aborting.[/red]")
            return False
        
        console.print("[cyan]⏳ Waiting for Bronze to initialize...[/cyan]")
        time.sleep(10)
        
        # 3. Start Silver layer (parallel)
        console.print("\n[bold cyan]📊 Starting Silver Layer...[/bold cyan]")
        self.start_spark_job("silver_transactions")
        time.sleep(2)
        self.start_spark_job("silver_customers")
        
        console.print("[cyan]⏳ Waiting for Silver to initialize...[/cyan]")
        time.sleep(10)
        
        # 4. Start Gold layer
        self.start_spark_job("gold_aml")
        
        console.print("\n[bold green]" + "=" * 70 + "[/bold green]")
        console.print("[bold green]✅ Pipeline Started Successfully![/bold green]")
        console.print("[bold green]" + "=" * 70 + "[/bold green]\n")
        
        # Show status
        self.display_status()
        
        return True
    
    def display_status(self):
        """Display pipeline status in a nice table"""
        status = self.get_pipeline_status()
        
        # Create table
        table = Table(title="🔍 Pipeline Status", show_header=True, header_style="bold magenta")
        table.add_column("Component", style="cyan", width=30)
        table.add_column("Status", style="green", width=20)
        table.add_column("Details", width=30)
        
        # Debezium
        table.add_row("Debezium Connector", status['debezium'], "PostgreSQL CDC")
        
        # Kafka Topics
        for topic, count in status['kafka_topics'].items():
            topic_name = topic.replace('cdc.public.', '')
            table.add_row(f"Kafka Topic: {topic_name}", f"{count} messages", "")
        
        # Spark Jobs
        for job_id, job_status in status['spark_jobs'].items():
            job_name = JOBS[job_id]['name']
            table.add_row(f"Spark Job: {job_name}", job_status, "")
        
        console.print(table)
        
        console.print("\n[bold cyan]📊 Monitoring URLs:[/bold cyan]")
        console.print("  • Kafka UI:        http://localhost:8080")
        console.print("  • Spark Master:    http://localhost:8082")
        console.print("  • Kafka Connect:   http://localhost:8083")
        console.print("  • MinIO Console:   http://localhost:9001")
    
    def stop_all(self):
        """Stop all jobs (gracefully kill Spark jobs)"""
        console.print("\n[bold red]🛑 Stopping Pipeline...[/bold red]\n")
        
        # Note: Proper implementation would track PIDs and kill gracefully
        # For now, user can use Spark UI or docker stop
        console.print("[yellow]ℹ️  To stop Spark jobs:[/yellow]")
        console.print("  1. Visit Spark UI: http://localhost:8082")
        console.print("  2. Or: docker exec cdc-spark-master pkill -f spark-submit")
        console.print("  3. Or: docker-compose down")


@click.group()
def cli():
    """Streaming Pipeline Manager"""
    pass


@cli.command()
@click.option('--all', 'start_all', is_flag=True, help='Start entire pipeline')
@click.option('--debezium', is_flag=True, help='Deploy Debezium connector only')
@click.option('--bronze', is_flag=True, help='Start Bronze layer only')
@click.option('--silver', is_flag=True, help='Start Silver layer only')
@click.option('--gold', is_flag=True, help='Start Gold layer only')
def start(start_all, debezium, bronze, silver, gold):
    """Start pipeline components"""
    orchestrator = PipelineOrchestrator()
    
    if start_all:
        orchestrator.start_all()
    elif debezium:
        orchestrator.deploy_debezium_connector()
    elif bronze:
        orchestrator.start_spark_job("bronze")
    elif silver:
        orchestrator.start_spark_job("silver_transactions")
        orchestrator.start_spark_job("silver_customers")
    elif gold:
        orchestrator.start_spark_job("gold_aml")
    else:
        console.print("[yellow]⚠️  Please specify what to start (use --all or specific component)[/yellow]")


@cli.command()
def status():
    """Check pipeline health"""
    orchestrator = PipelineOrchestrator()
    orchestrator.display_status()


@cli.command()
def stop():
    """Stop all pipeline components"""
    orchestrator = PipelineOrchestrator()
    orchestrator.stop_all()


if __name__ == "__main__":
    cli()