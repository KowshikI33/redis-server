import socket
import threading
import time
import argparse
import base64

#simple in-memory database
database = {}
expiry_times  = {}
replication_sockets = []  # List to hold all replication sockets

def handle_client(client_socket, addr, is_master, replication_id, replication_offset):
    print(f"handling connection from {addr}")
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break

            print(f"Replica received data: {data}")  # Add this line for logging
            response = process_command(data, is_master, replication_id, replication_offset, client_socket)

            if response: #changed to accommodate the psync command
                client_socket.sendall(response.encode())

    finally:
        client_socket.close()
        print(f"Connetion from {addr} closed")

def process_command(data, is_master, replication_id, replication_offset, client_socket):
    decoded_data = data.decode('utf-8').strip()
    print(f"Received data to process: {decoded_data}")  # Add this line for logging
    parts = decoded_data.split('\r\n')

    #redis commands are case-insensitive
    command = parts[2].upper() 

    if command == 'SET':
        key = parts[4]
        value = parts[6]
        expiry = None
        if len(parts) > 8 and parts[8].upper() == 'PX':
            expiry = int(parts[10])
        response = set_command(key, value, expiry)
        if is_master:
            propagate_command(data)
        return response
    elif command == 'GET':
        key = parts[4]
        return get_command(key)
    elif command == 'ECHO':
        message = parts[4]
        return f"${len(message)}\r\n{message}\r\n"
    elif command == "INFO":
        section = parts[4].lower() if len(parts) > 4 else ''
        return info_command(section, is_master, replication_id, replication_offset)
    elif command == "REPLCONF":
        return "+OK\r\n"
    elif command == "PSYNC":
        full_resync_response = f"+FULLRESYNC {replication_id} {replication_offset}\r\n"
        client_socket.sendall(full_resync_response.encode())

        # # # Send the empty RDB file
        # rdb_file = get_empty_rdb_file()
        # rdb_response = f"${len(rdb_file)}\r\n".encode() + rdb_file
        # client_socket.sendall(rdb_response)
        # print("sent the empty rdb file to slave")

        # Add the client_socket to the replication_sockets list
        replication_sockets.append(client_socket)

        print(f"Added replication socket: {client_socket}")  # Print the added socket
        print(f"Current replication sockets: {replication_sockets}")  # Print the list

        return ""  # Return an empty string since response is already sent      
    else:
        return '+PONG\r\n'

def set_command(key, value, expiry=None):
    database[key] = value
    if expiry is not None:
        expiry_times[key] = time.time() + expiry / 1000.0
        threading.Thread(target = expire_key, args = (key, expiry)).start()
    return '+OK\r\n'

def get_command(key):
    # extra precaution and avoid race condition with expire_key
    # and ensure consistent result
    if key in expiry_times and time.time() > expiry_times[key]:
        del database[key]
        del expiry_times[key]
        return '$-1\r\n'  # Null bulk string for non-existent keys 

    value = database.get(key)
    if value is None:
        return '$-1\r\n'  # Null bulk string for non-existent keys
    else:
        return f"${len(value)}\r\n{value}\r\n"
    
def expire_key(key, expiry):
    time.sleep(expiry / 1000.0)
    if key in database and time.time() > expiry_times.get(key, 0):
        del database[key]
        del expiry_times[key]

def info_command(section, is_master, replication_id, replication_offset):
    # print(f"the section passed is {section}")
    if section == 'replication':
        role = "master" if is_master else "slave"
        response=f"role:{role}\r\n"
        response += f"master_replid:{replication_id}\r\n"
        response += f"master_repl_offset:{replication_offset}\r\n"
        return f"${len(response)}\r\n{response}\r\n"
    else:
        return "$-1\r\n"
    
def connect_to_master(host, port, replica_repl_port):
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #create IPV4 (first arg) TCP socket
    master_socket.connect((host, int(port))) #connect the socket to a remote address
    
    # Send PING
    send_ping_to_master(master_socket)

    # Send REPLCONF with replication port
    send_replconf_to_master(master_socket, replica_repl_port)
    
    # Send PSYNC command
    send_psync_to_master(master_socket)
    
    return master_socket

def send_ping_to_master(master_socket):
    ping_command = "*1\r\n$4\r\nPING\r\n"
    master_socket.sendall(ping_command.encode()) #encode method converts from string to byte format
    response = master_socket.recv(1024) #server just blocks and waits here for response back
    print("Recceived from master:", response.decode())

def send_replconf_to_master(master_socket, port):
    # First REPLCONF command: REPLCONF listening-port <PORT>
    replconf_listening = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(port))}\r\n{port}\r\n"
    master_socket.sendall(replconf_listening.encode())
    response = master_socket.recv(1024)
    print("Received from master (REPLCONF listening-port):", response.decode())

    # Second REPLCONF command: REPLCONF capa psync2
    replconf_capa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
    master_socket.sendall(replconf_capa.encode())
    response = master_socket.recv(1024)
    print("Received from master (REPLCONF capa psync2):", response.decode())

def send_psync_to_master(master_socket):
    # PSYNC ? -1 command
    psync_command = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
    master_socket.sendall(psync_command.encode())

    # Receive the combined FULLRESYNC response
    response = master_socket.recv(2048).decode()

    # Split the response by the line breaks to separate the FULLRESYNC response
    responses = response.split('\r\n')
    full_resync_response = responses[0]
    # rdb_length_response = responses[1]

    print("Received from master (PSYNC):", full_resync_response)
    # print(f"Received RDB file of length {len(rdb_length_response )}")

def get_empty_rdb_file():
    base64_rdb = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
    return base64.b64decode(base64_rdb) #send data in binary format


def propagate_command(data):
    print(f"Propagating command to {len(replication_sockets)} replicas")
    print(f"Data being sent: {data}")

    for i, sock in enumerate(replication_sockets):
        result = is_socket_connected(sock)
        print(result)

        try:
            print(f"Sending to replica socket {i}: {sock}")
            sock.sendall(data)
            print(f"Successfully sent to replica {i}")
        except Exception as e:
            print(f"Error sending to replica {i}: {e}")
            # Optionally, remove the failed socket from the list
            # replication_sockets.remove(sock)

    print("Finished propagating command to all replicas")

#extra function
def is_socket_connected(sock):
    try:
        # This will raise an exception if the socket is closed
        sock.getpeername()
        return True
    except:
        return False


def main():
    replication_id = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
    replication_offset = 0

    parser = argparse.ArgumentParser(description='Redis Lite Server')
    #expects optional argument --port (optional because of --). if did not use it then not optional
    parser.add_argument('--port', type=int, default=6379, help='Port to run the Redis Lite server on')
    parser.add_argument('--replicaof', help='Host and port of the master server')
    args = parser.parse_args()

    is_master = args.replicaof is None

    if not is_master:
        master_host, master_port = args.replicaof.split()
        master_socket = connect_to_master(master_host, master_port)
        send_ping_to_master(master_socket)
        print(f"connected to master at {master_host}:{master_port} and sent PING")

        # ## Send REPLCONF commands
        # send_replconf_to_master(master_socket, args.port)

        # # Send PSYNC command
        # send_psync_to_master(master_socket)

    
    server_socket = socket.create_server(("localhost", args.port), reuse_port=True)
    print(f"Server is running on localhost:{args.port} as {'master' if is_master else 'slave'}")

    while True:
        print("Waiting for a connection...")
        # blocking line.
        client_socket, addr = server_socket.accept()
        print(f"Connection from {addr} has been established.")

        #create and start a new thread to handle this client
        client_thread = threading.Thread(target=handle_client, args=(client_socket, addr, is_master, replication_id, replication_offset))
        client_thread.start()

        print(f"Active connections: {threading.active_count() - 1}")
if __name__ == "__main__":
    main()
