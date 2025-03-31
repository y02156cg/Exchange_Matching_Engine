import socket

class ExchangeClient:
    def __init__(self, host='localhost', port=12345):
        self.host = host
        self.port = port

    def send_request(self, xml_data):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((self.host, self.port))

            data_bytes = xml_data.encode()
            s.sendall(f"{len(data_bytes)}\n".encode())
            s.sendall(data_bytes)

            # get response length
            len_line = b''
            while b'\n' not in len_line:
                chunk = s.recv(1)
                if not chunk:
                    break
                len_line += chunk

            response_len = int(len_line.decode().strip())

            # read response
            response = b''
            while len(response) < response_len:
                chunk = s.recv(min(4096, response_len - len(response)))
                if not chunk:
                    break
                response += chunk

            return response.decode()
        
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
    
    def place_query(self, account_id, order_id):
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<transactions id="{account_id}">
<query id="{order_id}"/>
</transactions>"""
        return self.send_request(xml)
    
    def place_cancel(self, account_id, order_id):
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<transactions id="{account_id}">
<cancel id="{order_id}"/>
</transactions>"""
        return self.send_request(xml)