import argparse
import socket
import threading
import time
import base64

trigger_update = False #
database = {} #store tuples: (value, expiry_time)
MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"  # Hardcoded 40-character string
MASTER_REPL_OFFSET = 0
EMPTY_RDB = base64.b64decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
replica_sockets = []

def handle_client(client_socket, addr, is_master):
    print(f"handling connection from {addr}")
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            #always an array, and array can contain tuple(str/bytes tring)/str/byte string
            response = process_command(data, is_master, client_socket)
            send_response(client_socket, response)
    finally:
        client_socket.close()
        print(f"Connetion from {addr} closed")

def send_response(client_socket, response):
    #always an array and it could contain tuple, str or byte string
    for resp in response:
        if isinstance(resp, tuple):
            for r in resp:
                if isinstance(r, str):
                    r = r.encode()
                client_socket.sendall(r)
        elif isinstance(resp, str):
            client_socket.sendall(resp.encode())
        else:
            client_socket.sendall(resp)

def split_commands(data):
    print(f"full byte string for split_commands {data}")
    commands = []
    i = 0

    while i < len(data):
        if data[i:i+1] == b'$':
            # This is likely the RDB file or a bulk string
            length_delimiter_start = data.index(b'\r\n', i)
            length = int(data[i+1:length_delimiter_start])
            content_start = length_delimiter_start + 2
            content_end = content_start + length

            #check if there is room for \r\n at the end
            if content_end + 2 <= len(data) and data[content_end: content_end + 2] == b'\r\n':
                command_end = content_end + 2
            else:
                command_end = content_end
            
            commands.append(data[i:command_end])
            i = command_end
        elif data[i:i+1] == b'*':
            # This is a Redis command
            command_end = data.index(b'\r\n', i)
            num_parts = int(data[i+1:command_end])
            for _ in range(num_parts):
                command_end = data.index(b'\r\n', command_end + 1)
                command_end = data.index(b'\r\n', command_end + 1)
            commands.append(data[i:command_end+2])
            i = command_end + 2
        else:
            # Unexpected data
            raise ValueError(f"Unexpected data format at position {i}")
    
    print(f"all split commands are: {commands}")
    return commands 

def process_commands(data, command_processor, *args):
    try:
        commands = split_commands(data)
        responses = []

        for command in commands:
            if not command.endswith(b'\r\n'):
                print(f"Received RDB file data, length:{len(command)}")
                continue
            decoded_command = command.decode('utf-8').strip()
            parts = decoded_command.split('\r\n')
            if parts:
                response = command_processor(parts, *args)
                if response is not None:
                    responses.append(response)
        return responses if responses else '$-1\r\n'
    except UnicodeDecodeError:
        print(f"Received non-UTF-8 data (perhaps empty RDB file), unable to process. Data: {data}")
        return '$-1\r\n'
    except Exception as e:
        print(f"Error processing commands: {e}")
        return '$-1\r\n'

def process_single_command(parts, is_master, client_socket):
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
        return handle_replconf(parts, client_socket)
    elif command == "PSYNC":
        return handle_psync()
    else:
        return '+PONG\r\n'

def process_master_single_command(parts):
    command = parts[2].upper() 
    if command == 'SET':
        key = parts[4]
        value = parts[6]
        set_command(key, value, parts)

def process_command(data, is_master, client_socket):
    return process_commands(data, process_single_command, is_master, client_socket)

def process_master_command(data):
    return process_commands(data, process_master_single_command)

def set_command(key, value, parts):
    expiry = None

    if len(parts) > 8 and parts[8].upper() == 'PX':
        try:
            expiry = time.time() + (int(parts[10]) / 1000) #convert ms to s
        except (IndexError, ValueError):
            return '-ERR invalid expires time in set\r\n'
    
    database[key] = (value, expiry)

    #propagate command to replica
    propagate_command("\r\n".join(parts) + "\r\n")
    
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
        replica_ip, replica_port = client_socket.getpeername()[0], client_socket.getpeername()[1]
        replica_sockets.append(client_socket)
        print(f"Connected to the replica at {replica_ip}:{replica_port}")
    elif parts[4] == "getack": #coming from master
        return "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"

    
    return "+OK\r\n"

def propagate_command(command):
    for replica_socket in replica_sockets:
        #should remove socket which are not connected
        try:
            replica_socket.sendall(command.encode())
            ip, port = replica_socket.getpeername()
            print(f"Successfully propagated to {ip}:{port}")
        except Exception as e:
            try:
                ip, port = replica_socket.getpeername()
            except:
                ip, port = "unkonwn", "unknown"
            print(f"Failed to propagate to {ip}:{port}: {e}")

def listen_to_master(master_socket):
    while True:
        try:
            data = master_socket.recv(1024)
            if not data:
                break
            print(f"received data from master {data}")
            process_master_command(data)
        except Exception as e:
            print(f"Error receiving data from master: {e}")
            break
    print("Connection to master closed")

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

        #start a new thread to listen for commands from master
        master_listener_thread = threading.Thread(target=listen_to_master, args=(master_socket,))
        master_listener_thread.start()

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
