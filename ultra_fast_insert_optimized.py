#!/usr/bin/env python3
"""
Ultra Fast DocumentDB Elastic Cluster Data Insertion - Optimized Connection Pool
Fixed connection pool management for better performance and resource utilization
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

# Minimal logging configuration
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ultra_fast_insert_optimized.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OptimizedDocumentDBInserter:
    def __init__(self, num_processes=32, connections_per_process=4, batch_size=8000, 
                 target_records=13_000_000_000, target_duration=7200):
        # DocumentDB connection configuration
        self.connection_string = "mongodb://masteruser:Welcome1@mobvistar-new3-028183925784.us-east-1.docdb-elastic.amazonaws.com:27017/?tls=true&authMechanism=SCRAM-SHA-1&retryWrites=false"
        self.database_name = "db"
        self.collection_name = "mobvistar"
        
        # Optimized performance parameters
        self.num_processes = num_processes
        self.connections_per_process = connections_per_process
        self.batch_size = batch_size
        self.target_records = target_records
        self.target_duration = target_duration
        
        # 🔥 Optimized connection pool configuration
        # Each process uses only one client, managing concurrency through connection pool
        self.max_pool_size = connections_per_process  # Actual connection count
        self.min_pool_size = max(1, connections_per_process // 2)  # Minimum connections
        
        # Optimized timeout configuration
        self.socket_timeout = 60000  # 60 seconds (reduced)
        self.server_selection_timeout = 15000  # 15 seconds (reduced)
        self.connect_timeout = 30000  # 30 seconds (reduced)
        
        # Connection health check
        self.max_idle_time = 180000  # 3 minutes
        self.heartbeat_frequency = 60000  # 1 minute
        
        # Retry configuration
        self.max_retries = 2  # Reduced retry count
        self.base_retry_delay = 3
        self.max_retry_delay = 15
        
        # Control variables
        self.running = Value('b', True)
        self.start_time = None
        
        # Sample data templates
        self.sample_data_templates = [
            "duf_3d_feas_map_1", "duf_3d_feas_map_2", "duf_3d_feas_map_3",
            "duf_3d_feas_map_4", "duf_3d_feas_map_5", "duf_3d_feas_map_6"
        ]

    def print_configuration(self):
        """Print optimized configuration"""
        total_connections = self.num_processes * self.max_pool_size
        
        print("=" * 60)
        print("OPTIMIZED ULTRA FAST DOCUMENTDB INSERTER")
        print("=" * 60)
        print(f"Processes: {self.num_processes}")
        print(f"Max connections per process: {self.max_pool_size}")
        print(f"Min connections per process: {self.min_pool_size}")
        print(f"Total max connections: {total_connections}")
        print(f"Batch size: {self.batch_size:,}")
        print(f"Target records: {self.target_records:,}")
        print(f"Socket timeout: {self.socket_timeout/1000:.0f}s")
        print(f"Server selection timeout: {self.server_selection_timeout/1000:.0f}s")
        print("=" * 60)

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
        """Calculate optimized retry delay"""
        delay = min(self.base_retry_delay * (2 ** attempt), self.max_retry_delay)
        return int(delay * random.uniform(0.8, 1.2))

    async def insert_batch_with_retry(self, collection, documents):
        """Insert batch with optimized retry logic"""
        for attempt in range(self.max_retries):
            try:
                # Use unordered insert for maximum speed
                await collection.insert_many(documents, ordered=False)
                return True
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.calculate_retry_delay(attempt)
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Final failure after {self.max_retries} attempts: {type(e).__name__}")
                    return False
        
        return False

    async def batch_worker(self, collection, batches_to_process: int, worker_id: int):
        """Individual batch worker using shared connection pool"""
        try:
            batch_count = 0
            consecutive_failures = 0
            max_consecutive_failures = 3
            
            while self.running.value and batch_count < batches_to_process:
                if not self.running.value:
                    break
                
                # Circuit breaker for consecutive failures
                if consecutive_failures >= max_consecutive_failures:
                    await asyncio.sleep(5)  # Short pause
                    consecutive_failures = 0
                
                # Generate documents
                documents = [self.create_document() for _ in range(self.batch_size)]
                
                # Insert with retry
                success = await self.insert_batch_with_retry(collection, documents)
                
                if success:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                
                batch_count += 1
                
                # Small delay only for failed batches
                if consecutive_failures > 0:
                    await asyncio.sleep(0.5)
            
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {type(e).__name__}")

    async def process_worker_async(self, process_id: int, batches_per_process: int):
        """Optimized asynchronous process worker"""
        client = None
        try:
            # 🔥 Create single optimized client using connection pool for concurrency management
            client = motor.motor_asyncio.AsyncIOMotorClient(
                self.connection_string,
                # Optimized connection pool configuration
                maxPoolSize=self.max_pool_size,  # Actual maximum connections
                minPoolSize=self.min_pool_size,  # Minimum connections
                maxIdleTimeMS=self.max_idle_time,  # Connection idle time
                
                # Optimized timeout configuration
                serverSelectionTimeoutMS=self.server_selection_timeout,
                socketTimeoutMS=self.socket_timeout,
                connectTimeoutMS=self.connect_timeout,
                
                # Connection health check
                heartbeatFrequencyMS=self.heartbeat_frequency,
                
                # Performance optimization
                retryReads=True,
                retryWrites=False,
                
                # Application identifier
                appname=f"optimized_ultra_fast_p{process_id}"
            )
            
            # Get database and collection
            db = client[self.database_name]
            collection = db[self.collection_name]
            
            # 🔥 Use async tasks for concurrent processing instead of multiple clients
            # Calculate batches per worker
            batches_per_worker = max(1, batches_per_process // self.connections_per_process)
            remaining_batches = batches_per_process % self.connections_per_process
            
            # Create concurrent tasks
            tasks = []
            for worker_id in range(self.connections_per_process):
                worker_batches = batches_per_worker
                if worker_id < remaining_batches:
                    worker_batches += 1
                
                if worker_batches > 0:
                    task = self.batch_worker(collection, worker_batches, worker_id)
                    tasks.append(task)
            
            # Wait for all tasks to complete
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                
        except Exception as e:
            logger.error(f"Process {process_id} error: {type(e).__name__}")
        finally:
            # Ensure client is properly closed
            if client:
                client.close()

    def process_worker_sync(self, process_id: int, batches_per_process: int):
        """Synchronous wrapper for process worker"""
        try:
            asyncio.run(self.process_worker_async(process_id, batches_per_process))
        except Exception as e:
            logger.error(f"Process {process_id} sync error: {type(e).__name__}")

    def simple_monitor(self):
        """Minimal monitoring"""
        while self.running.value:
            time.sleep(60)  # Check every minute
            
            elapsed_time = time.time() - self.start_time
            
            # Check if time limit reached
            if elapsed_time >= self.target_duration:
                print(f"Time limit reached ({elapsed_time:.0f}s), stopping...")
                self.running.value = False
                break

    async def test_connection(self):
        """Optimized connection test"""
        client = None
        try:
            client = motor.motor_asyncio.AsyncIOMotorClient(
                self.connection_string,
                serverSelectionTimeoutMS=10000,  # 10 second test timeout
                maxPoolSize=2  # Only need few connections for testing
            )
            db = client[self.database_name]
            
            # Quick ping test
            await db.command("ping")
            
            # Quick insert/delete test
            collection = db[self.collection_name]
            test_doc = {
                "oneid": str(uuid.uuid4()).replace('-', '')[:32],
                "test": True, 
                "timestamp": datetime.utcnow()
            }
            result = await collection.insert_one(test_doc)
            await collection.delete_one({"_id": result.inserted_id})
            
            return True
            
        except Exception as e:
            logger.error(f"Connection test failed: {type(e).__name__}: {str(e)}")
            return False
        finally:
            if client:
                client.close()

    def run(self):
        """Main execution method"""
        self.print_configuration()
        
        # Connection test
        print("Testing connection...")
        if not asyncio.run(self.test_connection()):
            print("❌ Connection test failed, exiting...")
            return
        print("✅ Connection test successful")
        
        self.start_time = time.time()
        print(f"🚀 Starting optimized ultra-fast insertion at {datetime.now()}")
        
        # Calculate work distribution
        total_batches = (self.target_records + self.batch_size - 1) // self.batch_size
        batches_per_process = (total_batches + self.num_processes - 1) // self.num_processes
        
        print(f"📊 Total batches: {total_batches:,}")
        print(f"📊 Batches per process: {batches_per_process:,}")
        print(f"📊 Estimated total connections: {self.num_processes * self.max_pool_size}")
        
        # Start processes
        processes = []
        for i in range(self.num_processes):
            p = Process(target=self.process_worker_sync, args=(i, batches_per_process))
            p.start()
            processes.append(p)
        
        # Start monitor
        monitor_process = Process(target=self.simple_monitor)
        monitor_process.start()
        
        # Signal handler
        def signal_handler(signum, frame):
            print("⚠️  Received interrupt signal, stopping...")
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
            
            # Final output
            total_time = time.time() - self.start_time
            print("=" * 60)
            print("✅ INSERTION COMPLETED")
            print(f"⏱️  Total time: {total_time:.2f} seconds")
            print(f"🏁 Completed at: {datetime.now()}")
            print("=" * 60)
            
        except KeyboardInterrupt:
            print("⚠️  Interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error: {type(e).__name__}: {str(e)}")
        finally:
            # Cleanup
            for p in processes:
                if p.is_alive():
                    p.terminate()
                    p.join(timeout=3)

def main():
    parser = argparse.ArgumentParser(description='Optimized Ultra Fast DocumentDB Data Insertion')
    parser.add_argument('--processes', type=int, default=32, help='Number of processes (default: 32)')
    parser.add_argument('--connections', type=int, default=4, help='Max connections per process (default: 4)')
    parser.add_argument('--batch-size', type=int, default=8000, help='Batch size (default: 8000)')
    parser.add_argument('--target-records', type=int, default=13_000_000_000, help='Target records (default: 13B)')
    parser.add_argument('--duration', type=int, default=7200, help='Max duration in seconds (default: 7200)')
    
    args = parser.parse_args()
    
    # Validate parameter reasonableness
    total_connections = args.processes * args.connections
    if total_connections > 500:
        print(f"⚠️  Warning: Total connections ({total_connections}) may be too high for DocumentDB")
        print("Consider reducing --processes or --connections")
    
    inserter = OptimizedDocumentDBInserter(
        num_processes=args.processes,
        connections_per_process=args.connections,
        batch_size=args.batch_size,
        target_records=args.target_records,
        target_duration=args.duration
    )
    
    inserter.run()

if __name__ == "__main__":
    main()
