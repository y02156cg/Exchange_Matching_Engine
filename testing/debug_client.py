#!/usr/bin/env python3
import socket
import time

def send_request_to_server(xml_data, host='localhost', port=12345):
    print(f"Connecting to {host}:{port}")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, port))
        print(f"Connected to {host}:{port}")
        
        data_bytes = xml_data.encode()
        length_message = f"{len(data_bytes)}\n".encode()
        
        print(f"Sending length: {len(data_bytes)}")
        s.sendall(length_message)
        
        print(f"Sending data: {xml_data[:50]}...")
        s.sendall(data_bytes)
        
        print("Waiting for response length...")
        len_line = b''
        while b'\n' not in len_line:
            chunk = s.recv(1)
            if not chunk:
                print("Connection closed while reading length")
                break
            len_line += chunk
        
        if len_line:
            response_len = int(len_line.decode().strip())
            print(f"Response length: {response_len}")
            
            response = b''
            while len(response) < response_len:
                chunk = s.recv(min(4096, response_len - len(response)))
                if not chunk:
                    print("Connection closed while reading response")
                    break
                response += chunk
                print(f"Received {len(response)}/{response_len} bytes")
            
            print(f"Response: {response.decode()}")
            return response.decode()
        else:
            print("No response length received")
            return None
    except Exception as e:
        print(f"Error communicating with server: {e}")
        return None
    finally:
        print("Closing connection")
        s.close()

if __name__ == "__main__":
    # Try to create an account
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<create>
<account id="debug_account" balance="1000000.00"/>
</create>"""
    
    print("Attempting to create account...")
    result = send_request_to_server(xml)
    
    if result:
        print("Successfully communicated with server")
    else:
        print("Failed to communicate with server")