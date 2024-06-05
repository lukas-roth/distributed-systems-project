from collections import deque
import socket
import struct
import threading
import queue
import logging
import logging.config
import configparser
import time

# Load logging configuration
logging.config.fileConfig('logging.conf', defaults={'logfilename': 'server.log'})
logger = logging.getLogger()

# Load configuration
config = configparser.ConfigParser()
config.read('config.cfg')

class ChatServer:
    def __init__(self):
        self.client_multicast_address = config.get('SHARED', 'ClientGroupMulticastAddress')
        self.client_multicast_port = config.getint('SHARED', 'ClientGroupMulticasPort')
        self.server_port = 5001  # Example port for clients to connect
        self.clients = []
        self.message_queue = queue.Queue()
        self.running = True
        self.last_100_messages = deque(maxlen=100)
        self.server_multicast_address = config.get('SHARED', 'ServerClientGroupMulticastAddress') #eigentlich "ServerGroupMulticastAddress"??
        self.server_multicast_port = config.getint('SHARED', 'ServerGroupMulticasPort')
        self.heartbeat = config.getint('SERVER','Heartbeat')
        


    def announce(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            ttl = struct.pack('b', 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            while self.running:
                message = f"{socket.gethostbyname(socket.gethostname())}:{self.server_port}"
                sock.sendto(message.encode('utf-8'), (self.client_multicast_address, self.client_multicast_port))
                logger.info(f"Announced server at {message}")
                time.sleep(5)  # Announce every 5 seconds

    def handle_client(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_sock:
            server_sock.bind(('', self.server_port))
            logger.info(f"Server listening on port {self.server_port}")
            while self.running:
                try:
                    data, addr = server_sock.recvfrom(1024)
                    if addr not in self.clients:
                        self.clients.append(addr)
                    if data:
                        message = data.decode('utf-8')
                        logger.info(f"Received message from {addr}: {message}")
                        self.message_queue.put((message, addr))
                except Exception as e:
                    logger.error(f"Error handling client message: {e}")


    def consumer(self):
        while self.running:
            try:
                message, addr = self.message_queue.get(timeout=1)
                logger.info(f"Processing message: {message}")
                self.last_100_messages.append(message)
                self.broadcast(self.last_100_messages, addr)
            except queue.Empty:
                continue

    def broadcast(self, messages, exclude_addr):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            messages_str = "\n".join(reversed(messages))  # Convert the deque of messages to a single string
            for client in self.clients:
                try:
                    sock.sendto(messages_str.encode('utf-8'), client)
                    logger.info(f"Sent message to client {client}: {messages_str}")
                except Exception as e:
                    logger.error(f"Error sending message to client {client}: {e}")


    def run(self):
        # Start the announcement thread
        announce_thread = threading.Thread(target=self.announce)
        announce_thread.start()

        # Start the consumer thread
        consumer_thread = threading.Thread(target=self.consumer)
        consumer_thread.start()

        # Start handling client messages
        self.handle_client()

        # Join threads when stopping
        announce_thread.join()
        consumer_thread.join()
    
    def sendHeartbeat(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            ttl = struct.pack('b', 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            while self.running:
                message = self.last_100_messages
                sock.sendto(message.encode('utf-8'), (self.server_multicast_address, self.server_multicast_port))
                logger.info(f"Leader sent heartbeat: {message}")
                time.sleep(self.heartbeat)

    def listenHeartbeat(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            # Set socket options to join the multicast group
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('', self.server_multicast_port))

            mreq = struct.pack('4sl', socket.inet_aton(self.server_multicast_address), socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

            last_heartbeat = time.time()

            while self.running:
                try:
                    sock.settimeout(self.heartbeat * 2)
                    data, addr = sock.recvfrom(1024)
                    last_heartbeat = time.time()
                    logger.info(f"Received heartbeat from {addr}: {data.decode('utf-8')}")
                except socket.timeout:
                    if time.time() - last_heartbeat > self.heartbeat * 2:
                        logger.warning("Heartbeat timeout. Initiating bully election.")
                        self.bullyElection()
                        break
                except Exception as e:
                    logger.error(f"Error receiving heartbeat: {e}")

if __name__ == "__main__":
    server = ChatServer()
    server.run()
