import argparse
import socket
import threading
import time
import base64

database = {} #store tuples: (value, expiry_time)
MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"  # Hardcoded 40-character string
MASTER_REPL_OFFSET = 0
EMPTY_RDB = base64.b64decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
replica_connections = []

def handle_client(client_socket, addr, is_master):
    print(f"handling connection from {addr}")
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            #can be tuple/str/byte string; tuple can contain str/byte
            response = process_command(data, is_master, client_socket)
            send_response(client_socket, response)
    finally:
        client_socket.close()
        print(f"Connetion from {addr} closed")

def send_response(client_socket, response):
    if isinstance(response, tuple):
        for resp in response:
            if isinstance(resp, str):
                resp = resp.encode()
            client_socket.sendall(resp)
    elif isinstance(response, str):
        client_socket.sendall(response.encode())
    else:
        client_socket.sendall(response)

def process_command(data, is_master, client_socket):
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
    elif command == "REPLCONF": 
        #Only master should receive REPLCONF command
        if is_master:
            return handle_replconf(parts, client_socket)
        return '$-1\r\n'
    elif command == "PSYNC":
        return handle_psync()
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

    #propagate command to replica
    propagate_command("\r\n".join(parts))
    
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

def connect_to_master(master_host, master_port, replica_port):
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_socket.connect((master_host, master_port))
    print(f"Connected to master at {master_host}:{master_port}")

    send_ping_to_master(master_socket)

    send_replconf_to_master(master_socket, replica_port)

    send_pysnc_to_master(master_socket)

    return master_socket

def send_ping_to_master(master_socket):
    ping_command = "*1\r\n$4\r\nPING\r\n"
    master_socket.sendall(ping_command.encode()) #or can use b"string" instead of "encode"
    print("Sent PING to master")
    response = master_socket.recv(1024)
    print("Recceived from master:", response.decode())

def send_replconf_to_master(master_socket, replica_port):
    #send replica listening port
    replconf_listening = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(replica_port))}\r\n{replica_port}\r\n"
    master_socket.sendall(replconf_listening.encode())
    response = master_socket.recv(1024)
    print("Received from master (REPLCONF listening-port):", response.decode())

    #Second REPLCONF command: REPLCONF capa psync2
    replconf_capa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
    master_socket.sendall(replconf_capa.encode())
    response = master_socket.recv(1024)
    print("Received from master (REPLCONF capa psync2):", response.decode())

def send_pysnc_to_master(master_socket):
    # PSYNC ? -1 command
    psync_command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
    master_socket.sendall(psync_command.encode())

    response = master_socket.recv(1024).decode()
    print(f"Received from master (PSYNC): {response}")

def handle_psync():
    full_resync_resopnse = f"+FULLRESYNC {MASTER_REPLID} {MASTER_REPL_OFFSET}\r\n"
    rdb_response = f"${len(EMPTY_RDB)}\r\n".encode() + EMPTY_RDB
    return (full_resync_resopnse, rdb_response)

def handle_replconf(parts, client_socket):
    if parts[4] == "listening-port":
        replica_port = int(parts[6])
        replica_ip = client_socket.getpeername()[0]
        try:
            replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            replica_socket.connect((replica_ip, replica_port))
            replica_connections.append(replica_socket)
            print(f"Connected to the replica at {replica_ip}:{replica_port}")
        except Exception as e:
            print(f"Failed to connect to replica at {replica_ip}:{replica_port}: {e}")
    return "+OK\r\n"

def propagate_command(command):
    for replica_connection in replica_connections:
        try:
            replica_connection.sendall(command.encode())
            ip, port = replica_connection.getpeername()
            print(f"Successfully propagated to {ip}:{port}")
        except Exception as e:
            try:
                ip, port = replica_connection.getpeername()
            except:
                ip, port = "unkonwn", "unknown"
            print(f"Failed to propagate to {ip}:{port}: {e}")


def main():
    parser = argparse.ArgumentParser(description='Redis Lite Server')
    parser.add_argument("--port", type = int, default = 6379, help = "port to run the server on")
    parser.add_argument("--replicaof", help = "Host and port of the master server")
    args = parser.parse_args()
    
    port = args.port
    is_master = args.replicaof is None

    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    print(f"Server is running on localhost:{port} as {'master' if is_master else 'slave'}")

    if not is_master:
        master_host, master_port = args.replicaof.split()
        master_socket = connect_to_master(master_host, int(master_port), int(port))

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
