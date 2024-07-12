import argparse
import socket
import threading
import time

database = {} #store tuples: (value, expiry_time)
MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"  # Hardcoded 40-character string
MASTER_REPL_OFFSET = 0

def handle_client(client_socket, addr, is_master):
    print(f"handling connection from {addr}")
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break

            response = process_command(data, is_master)
            client_socket.sendall(response.encode())
    finally:
        client_socket.close()
        print(f"Connetion from {addr} closed")

def process_command(data, is_master):
    decoded_data = data.decode('utf-8').strip()
    parts = decoded_data.split('\r\n')

    #redis commands are case-insensitive
    command = parts[2].upper() 

    if command == 'SET':
        key = parts[4]
        value = parts[6]
        return set_command(key, value, parts)
    elif command == 'GET':
        key = parts[4]
        return get_command(key)
    elif command == 'ECHO':
        message = parts[4]
        return f"${len(message)}\r\n{message}\r\n"
    elif command == 'INFO':
        section = parts[4] if len(parts) > 4 else ""
        return info_command(section, is_master)
    else:
        return '+PONG\r\n'

def set_command(key, value, parts):
    expiry = None

    if len(parts) > 8 and parts[8].upper() == 'PX':
        try:
            expiry = time.time() + (int(parts[10]) / 1000) #convert ms to s
        except (IndexError, ValueError):
            return '-ERR invalid expires time in set\r\n'
    
    database[key] = (value, expiry)
    return '+OK\r\n'

def get_command(key):
    if is_expired(key):
        if key in database:
            del database[key] 
        return '$-1\r\n' 
    
    value, _ = database.get(key, (None, None))

    if value is None:
        return '$-1\r\n'  
    else:
        return f"${len(value)}\r\n{value}\r\n"

def is_expired(key):
    if key not in database:
        return True
    _, expiry = database[key]

    if expiry is None:
        return False
    
    return time.time() > expiry

def info_command(section, is_master):
    if section.lower() == 'replication':
        info = [
            f"role:{'master' if is_master else 'slave'}",
            f"master_replid:{MASTER_REPLID}",
            f"master_repl_offset:{MASTER_REPL_OFFSET}"
        ]
        info_str = "\r\n".join(info)
        return f"${len(info_str)}\r\n{info_str}\r\n"
    else:
        return "$-1\r\n"

def connect_to_master(master_host, master_port):
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_socket.connect((master_host, master_port))
    print(f"Connected to master at {master_host}:{master_port}")

    #Send PING
    ping_command = "*1\r\n$4\r\nPING\r\n"
    master_socket.sendall(ping_command)
    print("Sent PING to master")

    return master_socket

def main():
    parser = argparse.ArgumentParser(description='Redis Lite Server')
    parser.add_argument("--port", type = int, default = 6379, help = "port to run the server on")
    parser.add_argument("--replicaof", help = "Host and port of the master server")
    args = parser.parse_args()
    
    port = args.port
    is_master = args.replicaof is None

    if not is_master:
        master_host, master_port = args.replicaof.split()
        master_socket = connect_to_master(master_host, int(master_port))

    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    print(f"Server is running on localhost:{port} as {'master' if is_master else 'slave'}")

    while True:
        print("Waiting for a connection...")
        # blocking line.
        client_socket, addr = server_socket.accept()
        print(f"Connection from {addr} has been established.")

        #create and start a new thread to handle this client
        client_thread = threading.Thread(target=handle_client, args=(client_socket, addr, is_master))
        client_thread.start()

        print(f"Active connections: {threading.active_count() - 1}")
if __name__ == "__main__":
    main()
