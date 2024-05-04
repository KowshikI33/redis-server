# Uncomment this to pass the first stage
import socket


def main():
    # Create a TCP/IP socket
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print("Server is running on localhost:6379")

    while True:
        print("Waiting for a connection...")
        client_socket, addr = server_socket.accept()
        print(f"Connection from {addr} has been established.")

        # Send a hardcoded PONG response as soon as the client connects
        client_socket.sendall(b'+PONG\r\n')
        client_socket.close()


if __name__ == "__main__":
    main()
