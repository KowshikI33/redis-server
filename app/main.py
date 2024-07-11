import socket
import threading
import time

database = {} #store tuples: (value, expiry_time)

def handle_client(client_socket, addr):
    print(f"handling connection from {addr}")
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break

            response = process_command(data)
            client_socket.sendall(response.encode())
    finally:
        client_socket.close()
        print(f"Connetion from {addr} closed")

def process_command(data):
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
    else:
        return '+PONG\r\n'

def set_command(key, value, parts):
    expiry = None

    if len(parts) > 6 and parts[7].upper() == 'PX':
        try:
            expiry = time.time() + (int(parts[9]) / 1000) #conver ms to s
        except (IndexError, ValueError):
            return '-ERR invalid expires time in set\r\n'
    
    database[key] = (value, expiry)
    return '+OK\r\n'

def get_command(key):
    if is_expired(key):
        if key in database:
            del database[key] #remove expired key
        return '$-1\r\n'  #Null bulk string for non-existent keys
    
    value, _ = database.get(key, (None, None))

    if value is None:
        return '$-1\r\n'  # Null bulk string for non-existent keys
    else:
        return f"${len(value)}\r\n{value}\r\n"

    
def is_expired(key):
    if key not in database:
        return True
    _, expiry = database[key]

    if expiry is None:
        return False
    
    return time.time() > expiry

def main():
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server is running on localhost:6379")

    while True:
        print("Waiting for a connection...")
        # blocking line.
        client_socket, addr = server_socket.accept()
        print(f"Connection from {addr} has been established.")

        #create and start a new thread to handle this client
        client_thread = threading.Thread(target=handle_client, args=(client_socket, addr))
        client_thread.start()

        print(f"Active connections: {threading.active_count() - 1}")
if __name__ == "__main__":
    main()
