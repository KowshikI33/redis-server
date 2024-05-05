import socket

def handle_client(client_socket):
    while True:
        data = client_socket.recv(1024)

        # close connection if there is no more data
        if not data:
            break

        client_socket.sendall(b'+PONG\r\n')
    
def main():
    # Create a TCP/IP socket
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server is running on localhost:6379")

    while True:
        print("Waiting for a connection...")
        # blocking line.
        client_socket, addr = server_socket.accept()
        print(f"Connection from {addr} has been established.")

        try:
            handle_client(client_socket)
        finally:
            client_socket.close() #Socket is closed on error

if __name__ == "__main__":
    main()
