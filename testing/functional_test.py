import socket
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from exchangeclient import ExchangeClient
import subprocess
import os
import signal
    
class Tester:
    def __init__(self):
        self.client = ExchangeClient() # for example, tester.client can access ExchangeClient class

    def test_account_creation(self):
        try:
            response = self.client.create_account("test1", "1000.00")
            root = ET.fromstring(response)

            created = root.find(".//created[@id='test1']")
            return created is not None
        except Exception as e:
            print(f"Error in test_account_creation: {e}")
            return False
        
    def test_symbol_creation(self):
        try:
            self.client.create_account("test2", "5000.00")
            response = self.client.create_symbol("AAPL", [("test2", "100")])
            root = ET.fromstring(response)

            created = root.find(".//created[@sym='AAPL'][@id='test2']")
            return created is not None
        except Exception as e:
            print(f"Error in test_symbol_creation: {e}")
            return False

    def test_order(self):
        try:
            self.client.create_account("test3", "10000.00")
            self.client.create_symbol("GOOG", [("test3", "100")])
            response = self.client.place_order("test3", "GOOG", "-10", "150")
            root = ET.fromstring(response)

            created = root.find(".//opened[@sym='GOOG'][@amount='-10'][@limit='150']")
            return created is not None
        except Exception as e:
            print(f"Error in test_order: {e}")
            return False

    def test_match_order(self):
        """Test order matching with unique account names"""
        try:
            # Generate unique names for this test run
            timestamp = int(time.time())
            buyer_account = f"buyer_{timestamp}"
            seller_account = f"seller_{timestamp}"
            symbol_name = f"MS_{timestamp}"
        
            # Create fresh accounts with sufficient funds
            buyer_response = self.client.create_account(buyer_account, "20000.00")  # Double the money
        
            seller_response = self.client.create_account(seller_account, "10000.00")
        
            symbol_response = self.client.create_symbol(symbol_name, [(seller_account, "100")])
        
            # Place sell order
            sell_response = self.client.place_order(seller_account, symbol_name, "-20", "50.00")
            sell_root = ET.fromstring(sell_response)
            sell_id = sell_root.find(".//opened").get("id")
        
            # Place buy order
            buy_response = self.client.place_order(buyer_account, symbol_name, "20", "55.00")
        
            # Check if buy order succeeded
            buy_root = ET.fromstring(buy_response)
            opened = buy_root.find(".//opened")
            if opened is None:
                print(f"Buy order failed: {buy_response}")
                return False
            
            buy_id = opened.get("id")
        
            # Wait a moment for order processing (optional)
            time.sleep(0.1)
        
            # Query the orders to check if they were matched
            sell_query = self.client.place_query(seller_account, sell_id)
        
            buy_query = self.client.place_query(buyer_account, buy_id)
        
            # More flexible check for executions
            sell_status = ET.fromstring(sell_query)
            buy_status = ET.fromstring(buy_query)
        
            sell_executed = sell_status.find(".//executed")
            buy_executed = buy_status.find(".//executed")
        
            if sell_executed is None:
                print("Sell order was not executed")
            if buy_executed is None:
                print("Buy order was not executed")
            
            return sell_executed is not None and buy_executed is not None
        except Exception as e:
            print(f"Error in test_order_matching: {e}")
            return False

        
    def test_order_query(self):
        """Test querying an order"""
        try:
            # Create test account and symbol
            self.client.create_account("query_test", "10000.00")
            self.client.create_symbol("IBM", [("query_test", "100")])
            
            # Place an order
            response = self.client.place_order("query_test", "IBM", "-5", "200.00")
            order_id = ET.fromstring(response).find(".//opened").get("id")
            
            # Query the order
            query_response = self.client.place_query("query_test", order_id)
            status = ET.fromstring(query_response)
            
            # Check if query returns correct status
            open_status = status.find(".//open[@shares='5']")
            return open_status is not None
        except Exception as e:
            print(f"Error in test_order_query: {e}")
            return False

    def test_order_cancel(self):
        """Test canceling an order"""
        try:
            # Create test account and symbol
            self.client.create_account("cancel_test", "10000.00")
            self.client.create_symbol("ORCL", [("cancel_test", "100")])
            
            # Place an order
            response = self.client.place_order("cancel_test", "ORCL", "-10", "75.00")
            order_id = ET.fromstring(response).find(".//opened").get("id")
            
            # Cancel the order
            cancel_response = self.client.place_cancel("cancel_test", order_id)
            cancel_status = ET.fromstring(cancel_response)
            
            # Check if order was canceled
            canceled = cancel_status.find(".//canceled[@shares='10']")
            return canceled is not None
        except Exception as e:
            print(f"Error in test_order_cancel: {e}")
            return False
    
    def test_insufficient_funds(self):
        """Test order rejection due to insufficient funds"""
        try:
            # Create test account with small balance
            self.client.create_account("poor_buyer", "100.00")
            self.client.create_symbol("TSLA", [])
            
            # Try to place buy order that exceeds balance
            response = self.client.place_order("poor_buyer", "TSLA", "10", "100.00")
            root = ET.fromstring(response)
            
            # Check if order was rejected
            error = root.find(".//error[@sym='TSLA'][@amount='10'][@limit='100.00']")
            return error is not None and "Insufficient funds" in error.text
        except Exception as e:
            print(f"Error in test_insufficient_funds: {e}")
            return False
        
    def test_insufficient_shares(self):
        """Test order rejection due to insufficient shares"""
        try:
            # Create test account with no shares
            self.client.create_account("no_shares", "10000.00")
            self.client.create_symbol("AMZN", [("no_shares", "5")])
            
            # Try to place sell order that exceeds position
            response = self.client.place_order("no_shares", "AMZN", "-10", "100.00")
            root = ET.fromstring(response)
            
            # Check if order was rejected
            error = root.find(".//error[@sym='AMZN'][@amount='-10'][@limit='100.00']")
            return error is not None and "Insufficient shares" in error.text
        except Exception as e:
            print(f"Error in test_insufficient_shares: {e}")
            return False
    
    def test_multiple_executions(self):
        """Test an order being split into multiple executions"""
        try:
            # Use unique account names for this test
            timestamp = int(time.time())
            buyer_account = f"buyer_{timestamp}"
            seller1_account = f"seller1_{timestamp}"
            seller2_account = f"seller2_{timestamp}"
            symbol_name = f"SYM_{timestamp}"
        
            # Create test accounts and symbol
            self.client.create_account(buyer_account, "20000.00")
            self.client.create_account(seller1_account, "5000.00")
            self.client.create_account(seller2_account, "5000.00")
            self.client.create_symbol(symbol_name, [
                (seller1_account, "50"),
                (seller2_account, "50")
            ])
        
            # Place two sell orders
            self.client.place_order(seller1_account, symbol_name, "-20", "150.00")
            self.client.place_order(seller2_account, symbol_name, "-30", "155.00")
        
            # Place a buy order that should match both sell orders
            buy_response = self.client.place_order(buyer_account, symbol_name, "50", "160.00")
            buy_root = ET.fromstring(buy_response)
            opened = buy_root.find(".//opened")
        
            if opened is None:
                print(f"Buy order failed: {buy_response}")
                return False
        
            buy_id = opened.get("id")
        
            # Query the buy order
            query_response = self.client.place_query(buyer_account, buy_id)
            status = ET.fromstring(query_response)
        
            # Check if there are multiple executions
            executions = status.findall(".//executed")
            return len(executions) == 2
        except Exception as e:
            print(f"Error in test_multiple_executions: {e}")
            return False
        

    def run_test(self):
        print("Running functional tests...")

        test_results = {
            "account_creation": self.test_account_creation(),
            "symbol_creation": self.test_symbol_creation(),
            "basic_order": self.test_order(),
            "order_matching": self.test_match_order(),
            "order_query": self.test_order_query(),
            "order_cancel": self.test_order_cancel(),
            "insufficient_funds": self.test_insufficient_funds(),
            "insufficient_shares": self.test_insufficient_shares(),
            "multiple_executions": self.test_multiple_executions()
        }

        success_count = sum(1 for result in test_results.values() if result)
        total_count = len(test_results)
        
        print(f"Functional tests completed: {success_count}/{total_count} passed")
        
        for test_name, result in test_results.items():
            status = "PASSED" if result else "FAILED"
            print(f"{test_name}: {status}")
            
        return success_count == total_count
    
if __name__ == "__main__":
    import sys
    
    # Remove the argument parsing and simply run the functional tests
    tester = Tester()
    success = tester.run_test()
    sys.exit(0 if success else 1)
        