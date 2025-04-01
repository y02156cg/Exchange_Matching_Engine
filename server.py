import socket
import threading
import multiprocessing
import xml.etree.ElementTree as ET # short name ET for XML parsing library ElementTree
import psycopg2
from psycopg2 import pool # connection pooling, reuse database connections instead of creating new everytime
import time
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'dbname': 'exchange',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'postgres',
    'port': '5432'
}

connection_pool = None

class ExchangeServer:
    def __init__(self, host='0.0.0.0', port=12345, num_workers=4):
        self.host = host
        self.port = port
        self.num_workers = num_workers
        self.socket = None
        self.workers = []

    def start(self):
        global connection_pool
        connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=100,
            **DB_CONFIG
        )

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.socket.listen(100)

        logger.info(f"Server started on {self.host}:{self.port}")

        try:
            while True:
                client_socket, address = self.socket.accept()
                logger.info(f"Accepted connection from {address}")
                thread = threading.Thread(target=self._handle_client, args=(client_socket, address), daemon=True)
                thread.start()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self._cleanup()


    def _handle_client(self, client_socket, address):
        try:
            # Read XML data length
            # Read XML length line
            length_line = b''
            while b'\n' not in length_line:
                chunk = client_socket.recv(1)
                if not chunk:
                    break
                length_line += chunk

            message_len = int(length_line.decode().strip())
            received_data = b''
            while len(received_data) < message_len:
                chunk = client_socket.recv(min(4096, message_len - len(received_data)))
                if not chunk:
                    break
                received_data += chunk

            xml_data = received_data.decode()

            response = self._process_xml(xml_data)

            response_bytes = response.encode()
            client_socket.sendall(f"{len(response_bytes)}\n".encode())
            client_socket.sendall(response_bytes)

        except Exception as e:
            logger.error(f"Error handling client {address}: {e}")
        finally:
            client_socket.close()

    def _process_xml(self, xml_data):
        try:
            root = ET.fromstring(xml_data)

            if root.tag == 'create':
                return self._handle_create(root)
            elif root.tag == 'transactions':
                return self._handle_transaction(root)
            else:
                logger.warning(f"Unknown XML root tag: {root.tag}")
                return "<results><error>Unknown request type</error></results>"
            
        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            return "<results><error>Invalid XML format</error></results>"
        except Exception as e:
            logger.error(f"Error processing XML: {e}")
            return "<results><error>Internal server error</error></results>"
        
    def _handle_create(self, create_node):
        conn = connection_pool.getconn()
        try:
            results = []

            for child in create_node:
                if child.tag == 'account':
                    account_id = child.get('id')
                    balance_str = child.get('balance')

                    if not account_id or not balance_str:
                        results.append(f'<error id="{account_id}">Missing required attributes</error>')
                        continue

                    try:
                        balance = Decimal(balance_str)
                    except (TypeError, InvalidOperation):
                        results.append(f'<error id="{account_id}">Invalid balance value</error>')
                        continue

                    with conn.cursor() as cur:
                        try:
                            cur.execute(
                                "INSERT INTO accounts (account_id, balance) VALUES (%s, %s) ON CONFLICT (account_id) DO NOTHING",
                                (account_id, balance)
                            )
                            conn.commit()
                            results.append(f'<created id="{account_id}"/>')
                        except Exception as e:
                            conn.rollback()
                            logger.error(f"Error inserting account {account_id}: {e}")
                            results.append(f'<error id="{account_id}">Database error</error>')

                elif child.tag == 'symbol':
                    symbol = child.get('sym')

                    with conn.cursor() as cur:
                        try:
                            cur.execute(
                                "INSERT INTO symbols (symbol) VALUES (%s) ON CONFLICT DO NOTHING",
                                (symbol,)
                            )
                            conn.commit()
                        except Exception as e:
                            conn.rollback()
                            logger.error(f"Error creating symbol {symbol}: {e}")

                    for account_node in child:
                        if account_node.tag == 'account':
                            account_id = account_node.get('id')
                            amount = account_node.text.strip() if account_node.text else "0"

                            try:
                                amount = Decimal(amount)
                            except (TypeError, InvalidOperation):
                                results.append(f'<error sym="{symbol}" id="{account_id}">Invalid amount</error>')
                                continue

                            with conn.cursor() as cur:
                                try:
                                    cur.execute("SELECT 1 FROM accounts WHERE account_id = %s", (account_id,))
                                    if cur.fetchone() is None:
                                        results.append(f'<error sym="{symbol}" id="{account_id}">Account does not exist</error>')
                                        continue

                                    cur.execute("""
                                        INSERT INTO positions (account_id, symbol, amount)
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT (account_id, symbol)
                                        DO UPDATE SET amount = positions.amount + EXCLUDED.amount
                                    """, (account_id, symbol, amount))
                                    conn.commit()
                                    results.append(f'<created sym="{symbol}" id="{account_id}"/>')
                                except Exception as e:
                                    conn.rollback()
                                    logger.error(f"Error adding position for {account_id}, {symbol}: {e}")
                                    results.append(f'<error sym="{symbol}" id="{account_id}">Database error</error>')

            return f"<results>{''.join(results)}</results>"
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Error in handle_create: {e}")
            return "<results><error>Internal server error</error></results>"
        finally:
            connection_pool.putconn(conn)
                                
    def _handle_transaction(self, transactions_node):
        account_id = transactions_node.get('id')
        conn = connection_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM accounts WHERE account_id = %s", (account_id,))
                # Account doesn't exsit, return error for all transactions
                if cur.fetchone() is None:
                    error_results = []
                    for child in transactions_node:
                        if child.tag == 'order':
                            sym = child.get('sym', '')
                            amount = child.get('amount', '')
                            limit = child.get('limit','')
                            error_results.append(f'<error sym="{sym}" amount="{amount}" limit="{limit}">Invalid account</error>')
                        elif child.tag == 'query' or child.tag == 'cancel':
                            trans_id = child.get('id', '')
                            error_results.append(f'<error id="{trans_id}">Invalid account</error>')

                    return f"<results>{''.join(error_results)}</results>"
                
            results = []
            for child in transactions_node:
                if child.tag == 'order':
                    result = self._handle_order(conn, account_id, child)
                elif child.tag == 'query':
                    result = self._handle_query(conn, account_id, child)
                elif child.tag == 'cancel':
                    result = self._handle_cancel(conn, account_id, child)
                else:
                    logger.warning(f"Unknown transaction type: {child.tag}")
                    continue

                results.append(result)

            return f"<results>{''.join(results)}</results>"
        except Exception as e:
            conn.rollback()
            logger.error(f"Error in handle_transcations: {e}")
            return "<results><error>Internal server error</error></results>"
        finally:
            connection_pool.putconn(conn)

    def _handle_order(self, conn, account_id, order_node):
        symbol = order_node.get('sym')
        amount_str = order_node.get('amount')
        limit_str = order_node.get('limit')

        try:
            amount = Decimal(amount_str)
            limit_price = Decimal(limit_str)
        except (InvalidOperation, TypeError):
            return f'<error sym="{symbol}" amount="{amount_str}" limit="{limit_str}">Invalid amount or limit value</error>'
        
        is_buy = amount > 0
        abs_amount = abs(amount)

        try:
            with conn.cursor() as cur:
                if is_buy:
                    cur.execute("SELECT balance FROM accounts WHERE account_id = %s FOR UPDATE", (account_id,))
                    row = cur.fetchone() #get the info like balance, account_id
                    if row is None:
                        return f'<error sym="{symbol}" amount="{amount_str}" limit="{limit_str}">Account not found</error>'

                    balance = Decimal(row[0])
                    limit_cost = amount * limit_price

                    if balance < limit_cost:
                        return f'<error sym="{symbol}" amount="{amount_str}" limit="{limit_str}">Insufficient funds</error>'
                    
                    cur.execute(
                        "UPDATE accounts SET balance = balance - %s WHERE account_id = %s",
                        (limit_cost, account_id)
                    )
                else:
                    cur.execute(
                        "SELECT amount FROM positions WHERE account_id = %s AND symbol = %s FOR UPDATE",
                        (account_id, symbol)
                    )
                    row = cur.fetchone()

                    if row is None or Decimal(row[0]) < abs_amount:
                        return f'<error sym="{symbol}" amount="{amount_str}" limit="{limit_str}">Insufficient shares</error>'
                    
                    cur.execute(
                        "UPDATE positions SET amount = amount - %s WHERE account_id = %s AND symbol = %s",
                        (abs_amount, account_id, symbol)
                    )

                cur.execute(
                    """
                    INSERT INTO orders (account_id, symbol, amount, limit_price, remaining_amount, status, time_created)
                    VALUES (%s, %s, %s, %s, %s, 'open', NOW())
                    RETURNING order_id, time_created
                    """,
                    (account_id, symbol, amount, limit_price, abs(amount))
                )

                order_id, order_time = cur.fetchone()

                self._match_order(conn, order_id, symbol, amount, limit_price, account_id, order_time)

                conn.commit()
                return f'<opened sym="{symbol}" amount="{amount_str}" limit="{limit_str}" id="{order_id}"/>'
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Error processing order: {e}")
            return f'<error sym="{symbol}" amount="{amount_str}" limit="{limit_str}">Database error: {e}</error>'
        

    def _match_order(self, conn, order_id, symbol, amount, limit, account_id, order_time):
        is_buy = amount > 0
        abs_amount = abs(amount)
        remaining_amount = abs_amount
        
        with conn.cursor() as cur:
            if is_buy:
                cur.execute(
                    """
                    SELECT order_id, account_id, amount, limit_price, remaining_amount, time_created FROM orders
                    WHERE symbol = %s AND status = 'open' AND amount < 0 AND limit_price <= %s
                    ORDER BY limit_price ASC, time_created ASC
                    FOR UPDATE
                    """,
                    (symbol, limit)
                )
            else:
                cur.execute(
                    """
                    SELECT order_id, account_id, amount, limit_price, remaining_amount, time_created FROM orders
                    WHERE symbol = %s AND status = 'open' AND amount > 0 AND limit_price >= %s
                    ORDER BY limit_price DESC, time_created ASC
                    FOR UPDATE
                    """,
                    (symbol, limit)
                )
            matching_orders = cur.fetchall()

            for match in matching_orders:
                if remaining_amount <= 0:
                    break

                match_id, match_account, match_amount, match_price, match_remaining, match_time = match

                execution_price = Decimal(match_price) if match_time < order_time else limit
                execution_amount = min(remaining_amount, Decimal(match_remaining))

                cur.execute(
                    """ 
                    INSERT INTO executions (order_id, shares, price, time_executed)
                    VALUES (%s, %s, %s, NOW())
                    """,
                    (order_id, execution_amount, execution_price)
                )

                cur.execute(
                    """ 
                    INSERT INTO executions (order_id, shares, price, time_executed)
                    VALUES (%s, %s, %s, NOW())
                    """,
                    (match_id, execution_amount, execution_price)
                )

                cur.execute(
                    "UPDATE orders SET remaining_amount = remaining_amount - %s WHERE order_id = %s",
                    (execution_amount, order_id)
                )

                cur.execute(
                    "UPDATE orders SET remaining_amount = remaining_amount - %s WHERE order_id = %s",
                    (execution_amount, match_id)
                )

                cur.execute(
                    "UPDATE orders SET status = CASE WHEN remaining_amount = 0 THEN 'executed' ELSE status END WHERE order_id = %s",
                    (match_id,)
                )

                if is_buy:
                    buyer = account_id
                    seller = match_account
                else:
                    buyer = match_account
                    seller = account_id

                buyer_credit = execution_amount * execution_price
                cur.execute(
                    "UPDATE accounts SET balance = balance + %s WHERE account_id = %s",
                    (buyer_credit, seller)
                )

                cur.execute(
                    """
                    INSERT INTO positions (account_id, symbol, amount)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (account_id, symbol)
                    DO UPDATE SET amount = positions.amount + EXCLUDED.amount
                    """,
                    (buyer, symbol, execution_amount)
                )

                if is_buy and execution_price < limit:
                    refund = execution_amount * (limit - execution_price)
                    cur.execute(
                        "UPDATE accounts SET balance = balance + %s WHERE account_id = %s",
                        (refund, account_id)
                    )
                remaining_amount -= execution_amount

                cur.execute(
                    "UPDATE orders SET status = CASE WHEN remaining_amount = 0 THEN 'executed' ELSE status END WHERE order_id = %s",
                    (match_id,)
                )

            # Final status update for the original order
            cur.execute(
                "UPDATE orders SET status = CASE WHEN remaining_amount = 0 THEN 'executed' ELSE status END WHERE order_id = %s",
                (order_id,)
            )

    def _handle_query(self, conn, account_id, query_node):
        trans_id = query_node.get('id')
        
        try:
            trans_id = int(trans_id)
        except ValueError:
            return f'<error id="{trans_id}">Invalid transaction ID</error>'
        
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, remaining_amount, amount, symbol, limit_price
                    FROM orders
                    WHERE order_id = %s
                    """,
                    (trans_id,)
                )

                order_row = cur.fetchone()
                if order_row is None:
                    return f'<error id="{trans_id}">Order not found</error>'
                
                status, remaining, amount, symbol, limit_price = order_row

                status_xml = f'<status id="{trans_id}">'
                if status == 'open' and remaining > 0:
                    status_xml += f'<open shares="{remaining}"/>'

                if status == 'canceled':
                    cur.execute(
                        "SELECT MAX(time_executed) FROM executions WHERE order_id = %s AND shares = 0",
                        (trans_id,)
                    )
                    cancel_time = cur.fetchone()[0]
                    unix_time = int(cancel_time.timestamp())
                    status_xml += f'<canceled shares="{remaining}" time="{unix_time}"/>'

                cur.execute(
                    """
                    SELECT shares, price, time_executed FROM executions
                    WHERE order_id = %s AND shares > 0
                    ORDER BY time_executed
                    """,
                    (trans_id,)
                )

                executions = cur.fetchall()
                for shares, price, time_executed in executions:
                    unix_time = int(time_executed.timestamp())
                    status_xml += f'<executed shares="{shares}" price="{price}" time="{unix_time}"/>'

                status_xml += '</status>'
                return status_xml
        
        except Exception as e:
            conn.rollback()
            logger.error(f"Error processing query: {e}")
            return f'<error id="{trans_id}">Database error: {e}</error>'


    def _handle_cancel(self, conn, account_id, cancel_node):
        trans_id = cancel_node.get('id')

        try:
            trans_id = int(trans_id)
        except ValueError:
            return f'<error id="{trans_id}">Invalid transaction ID</error>'
        
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status, remaining_amount, amount, limit_price, symbol, account_id
                    FROM orders
                    WHERE order_id = %s
                    FOR UPDATE
                    """,
                    (trans_id,)
                )

                order_row = cur.fetchone()
                if order_row is None:
                    return f'<error id="{trans_id}">Order not found</error>'
                
                status, remaining, amount, limit_price, symbol, order_account = order_row

                if status != 'open' or remaining <= 0:
                    return f'<error id="{trans_id}">Order cannot be canceled</error>'
                
                cur.execute(
                    """
                    UPDATE orders
                    SET status = 'cancelled'
                    WHERE order_id = %s
                    """,
                    (trans_id,)
                )

                now = datetime.now()
                cur.execute(
                    """
                    INSERT INTO executions (order_id, shares, price, time_executed)
                    VALUES (%s, 0, 0, %s)
                    """,
                    (trans_id, now)
                )

                unix_time = int(now.timestamp())

                is_buy = amount>0

                if is_buy:
                    refund_amount = remaining * limit_price
                    cur.execute(
                        "UPDATE accounts SET balance = balance + %s WHERE account_id = %s",
                        (refund_amount, order_account)
                    )
                else:
                    cur.execute(
                        """
                        UPDATE positions
                        SET amount = amount + %s
                        WHERE account_id = %s AND symbol = %s
                        """,
                        (remaining, order_account, symbol)
                    )

                cancel_xml = f'<canceled id="{trans_id}">'
                cancel_xml += f'<canceled shares="{remaining}" time="{unix_time}"/>'

                cur.execute(
                    """
                    SELECT shares, price, time_executed
                    FROM executions
                    WHERE order_id = %s AND shares > 0
                    ORDER BY time_executed
                    """,
                    (trans_id,)
                )

                executions = cur.fetchall()
                for shares, price, time_executed in executions:
                    exec_unix_time = int(time_executed.timestamp())
                    cancel_xml += f'<executed shares="{shares}" price="{price}" time="{exec_unix_time}"/>'

                cancel_xml += '</canceled>'

                conn.commit()
                return cancel_xml
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error processing cancel: {e}")
            return f'<error id="{trans_id}">Database error: {e}</error>'
        
    def _cleanup(self):
        if self.socket:
            self.socket.close()

        for worker in self.workers:
            worker.terminate()
            worker.join()

        if connection_pool:
            connection_pool.closeall()

        logger.info("Server shutdown complete")

if __name__ == "__main__":
    server = ExchangeServer()
    server.start()