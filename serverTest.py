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
    def __init__(self, server_id, replicas):
        self.server_id = server_id
        self.replicas = replicas
        self.is_leader = False
        self.leader_id = None
        self.server_multicast_address = config.get('SHARED', 'ClientGroupMulticastAddress')
        self.server_multicast_port = config.getint('SHARED', 'ClientGroupMulticasPort')
        self.server_port = 5001 + server_id  # Unique port for each server instance
        self.clients = []
        self.message_queue = queue.Queue()
        self.running = True
        self.last_100_messages = deque(maxlen=100)
        self.election_ongoing = False
        self.heartbeat_interval = 2  # Heartbeat interval in seconds
        self.heartbeat_timeout = 5  # Timeout to detect leader failure in seconds
        self.heartbeat_received = True  # Flag to indicate if heartbeat was received

    def announce(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            ttl = struct.pack('b', 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            while self.running:
                message = f"{self.server_id}:{self.server_port}"
                sock.sendto(message.encode('utf-8'), (self.server_multicast_address, self.server_multicast_port))
                logger.info(f"Server {self.server_id} announced at {message}")
                time.sleep(5)  # Announce every 5 seconds

    def handle_client(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_sock:
            server_sock.bind(('', self.server_port))
            logger.info(f"Server {self.server_id} listening on port {self.server_port}")
            while self.running:
                try:
                    data, addr = server_sock.recvfrom(1024)
                    if addr not in self.clients:
                        self.clients.append(addr)
                    if data:
                        message = data.decode('utf-8')
                        if message.startswith("ELECTION:"):
                            self.handle_election_message(message, addr)
                        elif message.startswith("OK:"):
                            self.handle_ok_message()
                        elif message.startswith("COORDINATOR:"):
                            self.handle_coordinator_message(message)
                        elif message.startswith("HEARTBEAT:"):
                            self.heartbeat_received = True
                        else:
                            logger.info(f"Server {self.server_id} received message from {addr}: {message}")
                            self.message_queue.put((message, addr))
                except Exception as e:
                    logger.error(f"Server {self.server_id} error handling client message: {e}")

    def consumer(self):
        while self.running:
            try:
                message, addr = self.message_queue.get(timeout=1)
                logger.info(f"Server {self.server_id} processing message: {message}")
                self.last_100_messages.append(message)
                self.broadcast(self.last_100_messages, addr)
            except queue.Empty:
                continue

    def broadcast(self, messages, exclude_addr):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            messages_str = "\n".join(reversed(messages))  # Convert the deque of messages to a single string
            for client in self.clients:
                if client != exclude_addr:
                    try:
                        sock.sendto(messages_str.encode('utf-8'), client)
                        logger.info(f"Server {self.server_id} sent message to client {client}: {messages_str}")
                    except Exception as e:
                        logger.error(f"Server {self.server_id} error sending message to client {client}: {e}")

    def send_election_message(self, target_server_id):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            message = f"ELECTION:{self.server_id}"
            target_port = 5001 + target_server_id
            sock.sendto(message.encode('utf-8'), ('localhost', target_port))
            logger.info(f"Server {self.server_id} sent ELECTION message to Server {target_server_id}")

    def send_ok_message(self, target_server_id):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            message = f"OK:{self.server_id}"
            target_port = 5001 + target_server_id
            sock.sendto(message.encode('utf-8'), ('localhost', target_port))
            logger.info(f"Server {self.server_id} sent OK message to Server {target_server_id}")

    def send_coordinator_message(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            message = f"COORDINATOR:{self.server_id}"
            for server_id in self.replicas:
                if server_id != self.server_id:
                    target_port = 5001 + server_id
                    sock.sendto(message.encode('utf-8'), ('localhost', target_port))
                    logger.info(f"Server {self.server_id} sent COORDINATOR message to Server {server_id}")

    def send_heartbeat(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            while self.running:
                if self.is_leader:
                    message = f"HEARTBEAT:{self.server_id}"
                    for server_id in self.replicas:
                        if server_id != self.server_id:
                            target_port = 5001 + server_id
                            sock.sendto(message.encode('utf-8'), ('localhost', target_port))
                            logger.info(f"Server {self.server_id} sent HEARTBEAT to Server {server_id}")
                time.sleep(self.heartbeat_interval)

    def check_heartbeat(self):
        while self.running:
            if not self.heartbeat_received:
                logger.warning(f"Server {self.server_id} did not receive heartbeat from leader. Initiating election.")
                self.initiate_election()
            self.heartbeat_received = False
            time.sleep(self.heartbeat_timeout)

    def handle_election_message(self, message, addr):
        sender_id = int(message.split(":")[1])
        if sender_id > self.server_id:
            logger.info(f"Server {self.server_id} received ELECTION message from Server {sender_id} and is conceding")
            self.send_ok_message(sender_id)
        elif sender_id < self.server_id:
            logger.info(f"Server {self.server_id} received ELECTION message from Server {sender_id} and is initiating own election")
            self.initiate_election()

    def handle_ok_message(self):
        logger.info(f"Server {self.server_id} received OK message and is waiting for new coordinator")
        self.election_ongoing = False

    def handle_coordinator_message(self, message):
        new_leader_id = int(message.split(":")[1])
        logger.info(f"Server {self.server_id} received COORDINATOR message. New leader is Server {new_leader_id}")
        self.leader_id = new_leader_id
        self.is_leader = (self.server_id == new_leader_id)
        self.election_ongoing = False

    def initiate_election(self):
        if not self.election_ongoing:
            self.election_ongoing = True
            logger.info(f"Server {self.server_id} is initiating an election")
            for server_id in self.replicas:
                if server_id > self.server_id:
                    self.send_election_message(server_id)

            # Wait for responses
            time.sleep(5)
            if self.election_ongoing:
                self.is_leader = True
                self.leader_id = self.server_id
                self.send_coordinator_message()
                logger.info(f"Server {self.server_id} is elected as the new leader")
                self.election_ongoing = False

    def elect_leader(self):
        self.initiate_election()

    def run(self):
        # Conduct leader election
        self.elect_leader()

        # Start the heartbeat thread for leader
        heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        heartbeat_thread.start()

        # Start the heartbeat check thread for replicas
        check_heartbeat_thread = threading.Thread(target=self.check_heartbeat)
        check_heartbeat_thread.start()

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
        heartbeat_thread.join()
        check_heartbeat_thread.join()

def start_replicas(num_replicas):
    replicas = [i for i in range(1, num_replicas + 1)]
    servers = []

    for replica_id in replicas:
        server = ChatServer(replica_id, replicas)
        server_thread = threading.Thread(target=server.run)
        server_thread.start()
        servers.append(server)

if __name__ == "__main__":
    num_replicas = 4
    start_replicas(num_replicas)
