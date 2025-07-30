#!/usr/bin/env python3
"""
Ultra Fast DocumentDB Elastic Cluster Data Insertion
Removed all statistics and monitoring to maximize insertion speed
Based on fast_parallel_insert_optimized.py with statistics removed
"""

import asyncio
import motor.motor_asyncio
import time
import random
import string
import logging
import argparse
import sys
from multiprocessing import Process, Value
import signal
from datetime import datetime
import uuid

# Minimal logging configuration - only errors
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ultra_fast_insert.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class UltraFastDocumentDBInserter:
    def __init__(self, num_processes=32, connections_per_process=4, batch_size=8000, 
                 target_records=13_000_000_000, target_duration=7200):
        # DocumentDB connection configuration
        self.connection_string = "mongodb://<username>:<passwd>@<cluster-name>-<account-name>.us-east-1.docdb-elastic.amazonaws.com:27017/?tls=true&authMechanism=SCRAM-SHA-1&retryWrites=false"
        self.database_name = "test"
        self.collection_name = "<your collection name>"
        
        # Aggressive performance parameters
        self.num_processes = num_processes
        self.connections_per_process = connections_per_process
        self.batch_size = batch_size
        self.target_records = target_records
        self.target_duration = target_duration
        
        # Optimized connection settings
        self.max_concurrent_batches = 2
        self.connection_pool_size = connections_per_process * 3
        
        # Reduced timeouts for speed
        self.socket_timeout = 120000  # 120 seconds
        self.server_selection_timeout = 30000  # 30 seconds
        self.connect_timeout = 60000  # 60 seconds
        
        # Retry configuration - minimal for speed
        self.max_retries = 3
        self.base_retry_delay = 5
        self.max_retry_delay = 30
        
        # Minimal tracking - only what's absolutely necessary
        self.running = Value('b', True)
        self.start_time = None
        
        # Sample data templates
        self.sample_data_templates = [
            "duf_3d_feas_map_1", "duf_3d_feas_map_2", "duf_3d_feas_map_3",
            "duf_3d_feas_map_4", "duf_3d_feas_map_5", "duf_3d_feas_map_6"
        ]

    def print_configuration(self):
        """Print minimal configuration"""
        total_connections = self.num_processes * self.connections_per_process
        
        print("=" * 50)
        print("ULTRA FAST DOCUMENTDB INSERTER")
        print("=" * 50)
        print(f"Processes: {self.num_processes}")
        print(f"Connections per process: {self.connections_per_process}")
        print(f"Total connections: {total_connections}")
        print(f"Batch size: {self.batch_size:,}")
        print(f"Target records: {self.target_records:,}")
        print("=" * 50)

    def generate_oneid(self) -> str:
        """Generate oneid that distributes evenly across shards"""
        return str(uuid.uuid4()).replace('-', '')[:32]

    def generate_feature_data(self, size_kb: float = 1.4) -> str:
        """Generate feature data of specified size"""
        target_size = int(size_kb * 1024)
        
        templates = [
            "215595213818809135779111209478113818813230129562760946132185661343",
            "0.49",
            "com.rushpro.clonemaster1,com.seenax.HideAndSeek2,jp.co.goodroid.hyper.miningssrm",
            "com.lemongame.freecell.solitaire2,com.wordgame.cross.android.en2se.maginieractive.w"
        ]
        
        data_parts = []
        current_size = 0
        
        while current_size < target_size:
            template = random.choice(templates)
            extended_data = template + ''.join(random.choices(string.ascii_lowercase + string.digits, k=50))
            data_parts.append(extended_data)
            current_size += len(extended_data)
        
        return ','.join(data_parts)[:target_size]

    def create_document(self):
        """Create a single document"""
        oneid = self.generate_oneid()
        feature_key = random.choice(self.sample_data_templates)
        feature_data = self.generate_feature_data()
        
        return {
            "oneid": oneid,
            "duf_3d_feas_map": {
                feature_key: feature_data
            },
            "created_at": datetime.utcnow(),
            "batch_id": str(uuid.uuid4())[:8]
        }

    def calculate_retry_delay(self, attempt: int) -> int:
        """Calculate minimal retry delay"""
        delay = min(self.base_retry_delay * (2 ** attempt), self.max_retry_delay)
        return int(delay * random.uniform(0.8, 1.2))

    async def insert_batch_with_retry(self, client, documents):
        """Insert batch with minimal retry logic"""
        for attempt in range(self.max_retries):
            try:
                db = client[self.database_name]
                collection = db[self.collection_name]
                
                # Use unordered insert for maximum speed
                await collection.insert_many(documents, ordered=False)
                return True
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.calculate_retry_delay(attempt)
                    await asyncio.sleep(wait_time)
                else:
                    # Log only final failures
                    logger.error(f"Final failure: {type(e).__name__}")
                    return False
        
        return False

    async def connection_worker(self, client, batches_per_connection: int):
        """Individual connection worker - maximum speed focus"""
        try:
            batch_count = 0
            consecutive_failures = 0
            max_consecutive_failures = 3  # Reduced for speed
            
            while self.running.value and batch_count < batches_per_connection:
                if not self.running.value:
                    break
                
                # Quick circuit breaker
                if consecutive_failures >= max_consecutive_failures:
                    await asyncio.sleep(10)  # Short pause
                    consecutive_failures = 0
                
                documents = [self.create_document() for _ in range(self.batch_size)]
                
                success = await self.insert_batch_with_retry(client, documents)
                
                if success:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                
                batch_count += 1
                
                # Minimal delay - only for failed batches
                if consecutive_failures > 0:
                    await asyncio.sleep(1)
            
        except Exception as e:
            logger.error(f"Worker error: {type(e).__name__}")

    async def process_worker_async(self, process_id: int, batches_per_process: int):
        """Asynchronous process worker - speed optimized"""
        try:
            # Create connections with speed-optimized settings
            clients = []
            for i in range(self.connections_per_process):
                client = motor.motor_asyncio.AsyncIOMotorClient(
                    self.connection_string,
                    # Speed-optimized connection pool
                    maxPoolSize=self.connection_pool_size,
                    minPoolSize=2,
                    maxIdleTimeMS=300000,  # 5 minutes
                    
                    # Reduced timeouts for speed
                    serverSelectionTimeoutMS=self.server_selection_timeout,
                    socketTimeoutMS=self.socket_timeout,
                    connectTimeoutMS=self.connect_timeout,
                    
                    # Speed settings
                    heartbeatFrequencyMS=30000,  # 30 seconds
                    retryReads=True,
                    retryWrites=False,
                    
                    # Minimal app name
                    appname=f"ultra_fast_p{process_id}_c{i}"
                )
                clients.append(client)
            
            # Calculate batches per connection
            batches_per_connection = (batches_per_process + len(clients) - 1) // len(clients)
            
            # Start connection workers
            connection_tasks = []
            for client in clients:
                task = self.connection_worker(client, batches_per_connection)
                connection_tasks.append(task)
            
            # Wait for all connection workers to complete
            await asyncio.gather(*connection_tasks, return_exceptions=True)
            
            # Close connections
            for client in clients:
                client.close()
                
        except Exception as e:
            logger.error(f"Process {process_id} error: {type(e).__name__}")

    def process_worker_sync(self, process_id: int, batches_per_process: int):
        """Synchronous wrapper for process worker"""
        try:
            asyncio.run(self.process_worker_async(process_id, batches_per_process))
        except Exception as e:
            logger.error(f"Process {process_id} sync error: {type(e).__name__}")

    def simple_monitor(self):
        """Minimal monitoring - only time-based stopping"""
        while self.running.value:
            time.sleep(60)  # Check every minute
            
            elapsed_time = time.time() - self.start_time
            
            # Check if time limit reached
            if elapsed_time >= self.target_duration:
                print(f"Time limit reached ({elapsed_time:.0f}s), stopping...")
                self.running.value = False
                break

    async def test_connection(self):
        """Quick connection test"""
        try:
            client = motor.motor_asyncio.AsyncIOMotorClient(
                self.connection_string,
                serverSelectionTimeoutMS=15000
            )
            db = client[self.database_name]
            
            # Quick ping test
            await db.command("ping")
            
            # Quick insert test
            collection = db[self.collection_name]
            test_doc = {
                "oneid": str(uuid.uuid4()).replace('-', '')[:32],
                "test": True, 
                "timestamp": datetime.utcnow()
            }
            result = await collection.insert_one(test_doc)
            await collection.delete_one({"_id": result.inserted_id})
            
            client.close()
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {type(e).__name__}")
            return False

    def run(self):
        """Main execution method - speed focused"""
        self.print_configuration()
        
        # Quick connection test
        if not asyncio.run(self.test_connection()):
            print("Connection test failed, exiting...")
            return
        
        self.start_time = time.time()
        print(f"Starting ultra-fast insertion at {datetime.now()}")
        
        # Calculate work distribution
        total_batches = (self.target_records + self.batch_size - 1) // self.batch_size
        batches_per_process = (total_batches + self.num_processes - 1) // self.num_processes
        
        print(f"Total batches: {total_batches:,}")
        print(f"Batches per process: {batches_per_process:,}")
        
        # Start processes
        processes = []
        for i in range(self.num_processes):
            p = Process(target=self.process_worker_sync, args=(i, batches_per_process))
            p.start()
            processes.append(p)
        
        # Start minimal monitor
        monitor_process = Process(target=self.simple_monitor)
        monitor_process.start()
        
        # Signal handler
        def signal_handler(signum, frame):
            print("Received interrupt signal, stopping...")
            self.running.value = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            # Wait for all processes
            for p in processes:
                p.join()
            
            # Stop monitor
            self.running.value = False
            monitor_process.join(timeout=5)
            if monitor_process.is_alive():
                monitor_process.terminate()
            
            # Minimal final output
            total_time = time.time() - self.start_time
            print("=" * 50)
            print("INSERTION COMPLETED")
            print(f"Total time: {total_time:.2f} seconds")
            print(f"Completed at: {datetime.now()}")
            print("=" * 50)
            
        except KeyboardInterrupt:
            print("Interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error: {type(e).__name__}")
        finally:
            # Cleanup
            for p in processes:
                if p.is_alive():
                    p.terminate()
                    p.join(timeout=3)

def main():
    parser = argparse.ArgumentParser(description='Ultra Fast DocumentDB Data Insertion')
    parser.add_argument('--processes', type=int, default=32, help='Number of processes (default: 32)')
    parser.add_argument('--connections', type=int, default=4, help='Connections per process (default: 4)')
    parser.add_argument('--batch-size', type=int, default=8000, help='Batch size (default: 8000)')
    parser.add_argument('--target-records', type=int, default=13_000_000_000, help='Target records (default: 13B)')
    parser.add_argument('--duration', type=int, default=7200, help='Max duration in seconds (default: 7200)')
    
    args = parser.parse_args()
    
    inserter = UltraFastDocumentDBInserter(
        num_processes=args.processes,
        connections_per_process=args.connections,
        batch_size=args.batch_size,
        target_records=args.target_records,
        target_duration=args.duration
    )
    
    inserter.run()

if __name__ == "__main__":
    main()
