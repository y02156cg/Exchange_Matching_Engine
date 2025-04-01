#!/usr/bin/env python3
import socket
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
import numpy as np
import subprocess
import os
import statistics
import sys

class ExchangeClient:
    def __init__(self, host='localhost', port=12345, timeout=10):
        self.host = host
        self.port = port
        self.timeout = timeout
        
    def send_request(self, xml_data):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)  # Set socket timeout
        
        try:
            s.connect((self.host, self.port))
            
            data_bytes = xml_data.encode()
            s.sendall(f"{len(data_bytes)}\n".encode())
            s.sendall(data_bytes)
            
            # Get response length
            len_line = b''
            while b'\n' not in len_line:
                chunk = s.recv(1)
                if not chunk:
                    return None
                len_line += chunk
                
            response_len = int(len_line.decode().strip())
            
            # Read response
            response = b''
            while len(response) < response_len:
                chunk = s.recv(min(4096, response_len - len(response)))
                if not chunk:
                    break
                response += chunk
                
            return response.decode()
        except Exception as e:
            print(f"Error in send_request: {e}")
            return None
        finally:
            s.close()
            
    def create_account(self, account_id, balance):
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<create>
<account id="{account_id}" balance="{balance}"/>
</create>"""
        return self.send_request(xml)
        
    def create_symbol(self, symbol, account_positions):
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<create>
<symbol sym="{symbol}">
"""
        for account_id, amount in account_positions:
            xml += f"""<account id="{account_id}">{amount}</account>
"""
        xml += """</symbol>
</create>"""
        return self.send_request(xml)
        
    def place_order(self, account_id, symbol, amount, limit):
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<transactions id="{account_id}">
<order sym="{symbol}" amount="{amount}" limit="{limit}"/>
</transactions>"""
        return self.send_request(xml)

class ScalabilityTester:
    def __init__(self):
        self.client = ExchangeClient(timeout=30)  # Increase timeout
        print("ScalabilityTester initialized")

    def setup_data(self):
        print("Setting up test data...")
        # Setup 10 accounts
        for i in range(1, 11):
            try:
                response = self.client.create_account(f"account{i}", "1000000.00")
                if response and "<error" in response:
                    print(f"Error creating account{i}: {response}")
                else:
                    print(f"Created account{i}")
            except Exception as e:
                print(f"Exception creating account{i}: {e}")

        # Setup symbols
        symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "META"]
        for symbol in symbols:
            try:
                positions = [(f"account{i}", "10000") for i in range(1, 11)]
                response = self.client.create_symbol(symbol, positions)
                if response and "<error" in response:
                    print(f"Error creating symbol {symbol}: {response}")
                else:
                    print(f"Created symbol {symbol}")
            except Exception as e:
                print(f"Exception creating symbol {symbol}: {e}")
                
        print("Setup complete")

    def run_load_test(self, num_threads, num_requests_per_thread):
        """Run load test with multiple threads"""
        print(f"Running load test with {num_threads} threads, {num_requests_per_thread} requests per thread")

        def worker(thread_id):
            """Worker function for each thread"""
            client = ExchangeClient(timeout=30)  # Create new client for each thread
            account_id = f"account{thread_id % 10 + 1}"
            symbol = ["AAPL", "MSFT", "GOOG", "AMZN", "META"][thread_id % 5]
            
            results = []
            start_time = time.time()
            
            for i in range(num_requests_per_thread):
                try:
                    # Alternate buy/sell
                    if i % 2 == 0:
                        amount = "5"
                        limit = "100.00"
                    else:
                        amount = "-5"
                        limit = "95.00"
                    
                    # Log progress occasionally
                    if i % 10 == 0:
                        print(f"Thread {thread_id}: Request {i}/{num_requests_per_thread}")
                    
                    response = client.place_order(account_id, symbol, amount, limit)
                    if not response:
                        print(f"Thread {thread_id}: No response for request {i}")
                    elif "<error" in response:
                        print(f"Thread {thread_id}: Error in request {i}: {response}")
                    
                    # Add small delay between requests to avoid overwhelming server
                    time.sleep(0.01)
                    
                except Exception as e:
                    print(f"Thread {thread_id}: Exception in request {i}: {e}")
            
            end_time = time.time()
            duration = end_time - start_time
            print(f"Thread {thread_id} completed in {duration:.2f}s")
            return duration
        
        # Execute workers with thread pool
        durations = []
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit all tasks
            future_to_thread = {executor.submit(worker, i): i for i in range(num_threads)}
            
            # Collect results with timeout
            for future in future_to_thread:
                try:
                    duration = future.result(timeout=300)  # 5-minute timeout
                    durations.append(duration)
                except Exception as e:
                    print(f"Worker failed: {e}")
        
        # Calculate metrics
        if not durations:
            print("No successful workers")
            return {
                'total_requests': 0,
                'total_time': 0,
                'requests_per_second': 0,
                'avg_duration': 0,
                'max_duration': 0,
                'min_duration': 0
            }
            
        total_requests = len(durations) * num_requests_per_thread
        max_time = max(durations)
        throughput = total_requests / max_time
        
        result = {
            'total_requests': total_requests,
            'total_time': max_time,
            'requests_per_second': throughput,
            'avg_duration': statistics.mean(durations),
            'max_duration': max(durations),
            'min_duration': min(durations)
        }
        
        print(f"Test completed: {total_requests} requests in {max_time:.2f}s ({throughput:.2f} req/s)")
        return result

    def run_scalability_tests(self):
        cores_to_test = [1]  # Start with just one core for debugging
        num_threads = 5      # Reduced thread count
        requests_per_thread = 10  # Reduced request count
        results = []

        for cores in cores_to_test:
            print(f"Testing with {cores} CPU cores...")
            try:
                self.setup_data()
                result = self.run_load_test(num_threads, requests_per_thread)
                
                result['cores'] = cores
                result['std_throughput'] = 0  # No std dev with single run
                results.append(result)
                
                print(f"Results for {cores} cores: {result['requests_per_second']:.2f} req/s")
            except Exception as e:
                print(f"Test failed: {e}")
                import traceback
                traceback.print_exc()

        return results

    def generate_plots(self, results):
        if not results:
            print("No results to plot")
            return None
            
        # Create writeup directory
        os.makedirs('writeup', exist_ok=True)
        
        # Write results to file
        with open('writeup/results.txt', 'w') as f:
            for r in results:
                f.write(f"Cores: {r['cores']}\n")
                f.write(f"Requests: {r['total_requests']}\n")
                f.write(f"Time: {r['total_time']:.2f}s\n")
                f.write(f"Throughput: {r['requests_per_second']:.2f} req/s\n")
                f.write(f"Avg Duration: {r['avg_duration']:.2f}s\n")
                f.write(f"Min Duration: {r['min_duration']:.2f}s\n")
                f.write(f"Max Duration: {r['max_duration']:.2f}s\n")
                f.write("\n")
        
        print("Results saved to writeup/results.txt")
        return {
            'cores': [r['cores'] for r in results],
            'throughput': [r['requests_per_second'] for r in results]
        }

if __name__ == "__main__":
    print("Running scalability test...")
    tester = ScalabilityTester()
    
    if len(sys.argv) > 1 and sys.argv[1] == "--setup-only":
        # Just set up the data without running the test
        tester.setup_data()
        print("Setup complete")
    else:
        # Run full test
        results = tester.run_scalability_tests()
        if results:
            tester.generate_plots(results)
            print("Test completed successfully!")
        else:
            print("Test failed to produce results")