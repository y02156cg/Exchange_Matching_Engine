import socket
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from exchangeclient import ExchangeClient
import matplotlib.pyplot as plt
import numpy as np
import subprocess
import os
import signal
import statistics

class ScalabilityTester:
    def __init__(self, server_script='server.py'):
        self.sever_script = server_script
        self.server_process = None
        self.client = ExchangeClient()

    def setup_data(self):
        # Setup 10 accounts each has $1000000
        for i in range(1, 11):
            self.client.create_account(f"account{i}", "1000000.00")

        symbols = ["AAPL", "MSFT", "GOOG", "AMZN", "META"]
        # each symbol has 10000 amounts and stored in the account
        for symbol in symbols:
            positions = [(f"account{i}", "10000") for i in range(1, 11)]
            self.client.create_symbol(symbol, positions)

    def run_load_test(self, num_threads, num_requests_per_thread):
        """Run num_threads threads and each thread has num_requests_per_thread requests"""

        def worker(thread_id):
            """Worker function for load testing"""
            client = ExchangeClient()
            account_id = f"account{thread_id % 10 + 1}" #get one account from the 10 accounts
            symbol = ["AAPL", "MSFT", "GOOG", "AMZN", "META"][thread_id % 5] # choose one symbol from the 5 symbols

            start_time = time.time()
            for i in range(num_requests_per_thread):
                # even: buy 5 shares 
                if i % 2 == 0:
                    amount = "5"
                    limit = "100.00"
                # odd sell 5 shares
                else:
                    amount = "-5"
                    limit = "95.00"

                response = client.place_order(account_id, symbol, amount, limit)

            end_time = time.time()
            return end_time - start_time # get the execution time
        
        durations = []
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_thread = {executor.submit(worker, i): i for i in range(num_threads)} # every thread run worker(i)

            for future in future_to_thread:
                try:
                    duration = future.result()
                    durations.append(duration)
                except Exception as e:
                    print(f"Worker failed with error: {e}")

        total_requests = num_threads * num_requests_per_thread
        total_time = max(durations) if duration else 0
        requests_per_second = total_requests / total_time if total_time > 0 else 0

        return {
            'total_requests': total_requests,
            'total_time': total_time,
            'requests_per_second': requests_per_second,
            'avg_duration': statistics.mean(durations) if durations else 0,
            'max_duration': max(durations) if durations else 0,
            'min_duration': min(durations) if durations else 0
        }

    def start_server(self, cores=None):
        env = os.environ.copy()
        cmd = ['docker-compose', 'up', '--build', '-d', '--remove-orphans']

        self.server_process = subprocess.Popen(cmd, env=env)
        time.sleep(5)  # give server time to start

    def stop_server(self):
        subprocess.run(['docker-compose', 'down'])

    def run_scalability_tests(self):
        cores_to_test = [1, 2, 4]  #Assume a 4-core VM
        num_threads = 20
        requests_per_thread = 50
        num_runs = 3
        results = []

        for cores in cores_to_test:
            print(f"Testing with {cores} CPU cores...")
            per_core_results = []
            for run in range(num_runs):
                print(f"Run {run + 1} for {cores} cores...")
                time.sleep(2)
                try:
                    self.setup_data()
                    result = self.run_load_test(num_threads, requests_per_thread)
                    per_core_results.append(result)
                except Exception as e:
                    print(f"Run {run + 1} for {cores} cores failed: {e}")

            if per_core_results:
                avg_throughput = statistics.mean(r['requests_per_second'] for r in per_core_results)
                std_throughput = statistics.stdev(r['requests_per_second'] for r in per_core_results)
                final_result = per_core_results[0]
                final_result['requests_per_second'] = avg_throughput
                final_result['std_throughput'] = std_throughput
                final_result['cores'] = cores
                results.append(final_result)

                print(f"Results for {cores} cores: {avg_throughput:.2f} ± {std_throughput:.2f} req/s")

        return results
    
    def generate_plots(self, results):
        cores = [r['cores'] for r in results]
        throughput = [r['requests_per_second'] for r in results]
        errors = [r.get('std_throughput', 0) for r in results]

        plt.figure(figsize=(10, 6))
        plt.errorbar(cores, throughput, yerr=errors, fmt='o-', linewidth=2, capsize=5)
        plt.title('Exchange Server Scalability')
        plt.xlabel('Number of CPU Cores')
        plt.ylabel('Throughput (requests/second)')
        plt.grid(True)
        plt.xticks(cores)

        base_throughput = throughput[0]
        ideal = [base_throughput * c for c in cores]
        plt.plot(cores, ideal, marker='x', linestyle='--', linewidth=1, label='Ideal Scaling')

        plt.legend()
        plt.tight_layout()

        os.makedirs('writeup', exist_ok=True)
        plt.savefig('writeup/scalability_plot.png')
        plt.close()

        speedup = [t / base_throughput for t in throughput]
        ideal_speedup = [c for c in cores]

        plt.figure(figsize=(10, 6))
        plt.plot(cores, speedup, marker='o', linestyle='-', linewidth=2, label='Actual Speedup')
        plt.plot(cores, ideal_speedup, marker='x', linestyle='--', linewidth=1, label='Ideal Speedup')
        plt.title('Exchange Server Speedup')
        plt.xlabel('Number of CPU Cores')
        plt.ylabel('Speedup Factor')
        plt.grid(True)
        plt.xticks(cores)
        plt.legend()
        plt.tight_layout()

        plt.savefig('writeup/speedup_plot.png')
        plt.close()

        with open('writeup/results_table.txt', 'w') as f:
            f.write("Cores | Throughput (req/s) | Speedup | Efficiency\n")
            f.write("----- | ------------------ | ------- | ----------\n")
            for i, r in enumerate(results):
                efficiency = speedup[i] / cores[i] * 100
                f.write(f"{r['cores']} | {r['requests_per_second']:.2f} | {speedup[i]:.2f}x | {efficiency:.1f}%\n")
        
        return {
            'cores': cores,
            'throughput': throughput,
            'speedup': speedup
        }
    
if __name__ == "__main__":
    import sys
    
    # Remove the argument parsing and simply run the functional tests
    tester = ScalabilityTester(server_script='../server.py')
    results = tester.run_scalability_tests()
    plot_data = tester.generate_plots(results)
    print("Scalability testing complete. Results saved to writeup/ directory.")


        
            