from collections import deque
import datetime
import json
import socket
import struct
import threading
import queue
import logging
import logging.config
import configparser
import time
import uuid
from multicast_listener import MulticastListener



# Load configuration
config = configparser.ConfigParser()
config.read('config.cfg')

class ChatServer:
    def __init__(self, is_replica = False):
        self.is_replica = is_replica
        self.client_multicast_address = config.get('SHARED', 'ClientGroupMulticastAddress')
        self.client_multicast_port = config.getint('SHARED', 'ClientGroupMulticasPort')
        self.server_multicast_address = config.get('SHARED', 'ServerClientGroupMulticastAddress') #eigentlich "ServerGroupMulticastAddress"??
        self.server_multicast_port = config.getint('SHARED', 'ServerGroupMulticasPort')
        self.server_port = 5001  # Example port for clients to connect

        self.clients = []
        self.message_queue = queue.Queue()
        self.running = True
        self.last_100_messages = deque(maxlen=100)
        self.heartbeat_intervall = config.getint('SERVER','HeartbeatIntverval')
        self.timeout_multiplier = config.getint('SERVER','TimeoutMultiplier')
        self.uuid = uuid.uuid4()
        self.uid = self.combine_timestamp_uuid('1970-01-01T00:00:00Z')

        # Load logging configuration
        logging.config.fileConfig('logging.conf', defaults={'logfilename': f'server-{self.uuid}.log'})
        self.logger = logging.getLogger()
        self.logger.info(f"Has uuid of {self.uuid}.")

    def announce(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            ttl = struct.pack('b', 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            while self.running:
                message = f"{socket.gethostbyname(socket.gethostname())}:{self.server_port}"
                sock.sendto(message.encode('utf-8'), (self.client_multicast_address, self.client_multicast_port))
                self.logger.info(f"Announced server at {message}")
                time.sleep(5)  # Announce every 5 seconds

    def handle_client(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_sock:
            server_sock.bind(('', self.server_port))
            self.logger.info(f"Server listening on port {self.server_port}")
            while self.running:
                try:
                    data, addr = server_sock.recvfrom(1024)
                    if addr not in self.clients:
                        self.clients.append(addr)
                    if data:
                        message = data.decode('utf-8')
                        self.logger.info(f"Received message from {addr}: {message}")
                        self.message_queue.put((message, addr))
                except Exception as e:
                    self.logger.error(f"Error handling client message: {e}")


    def consumer(self):
        while self.running:
            try:
                message, addr = self.message_queue.get(timeout=1)
                self.logger.info(f"Processing message: {message}")
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
                    self.logger.info(f"Sent message to client {client}: {messages_str}")
                except Exception as e:
                    self.logger.error(f"Error sending message to client {client}: {e}")

    def send_heartbeat(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            ttl = struct.pack('b', 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            while self.running:
                message = json.dumps({"data":list(self.last_100_messages), "time":datetime.datetime.now().isoformat()})
                sock.sendto(message.encode('utf-8'), (self.server_multicast_address, self.server_multicast_port))
                self.logger.info(f"Leader sent heartbeat: {message}")
                time.sleep(self.heartbeat_intervall)

    def listen_for_heartbeat(self, data):
        if isinstance(data, socket.timeout):
            self.logger.warning("Heartbeat timeout! Initiating bully election.")
            # self.trigger_bully_election()
        else:
            self.logger.info(f"Received heartbeat: {data}")
            self.last_100_messages = deque(json.loads(data)["data"])
            time = json.loads(data)["time"]
            self.logger.debug(f"Time heartbeat: {time}")
            self.uuid = self.combine_timestamp_uuid(time)
    
    def timestamp_to_int(self,timestamp):
        """
        Convert a timestamp to an integer representing milliseconds since the epoch.
        """
        dt = datetime.fromisoformat(timestamp)
        epoch = datetime(1970, 1, 1)
        return int((dt - epoch).total_seconds() * 1000)

    def uuid_to_int(self,server_uuid):
        """
        Convert a UUID to an integer.
        """
        return uuid.UUID(server_uuid).int

    def combine_timestamp_uuid(self,timestamp):
        """
        Combine the timestamp and UUID into a single integer.
        """
        timestamp_int = self.timestamp_to_int(timestamp)
        uuid_int = self.uuid_to_int(self.uuid)
        
        # Shift timestamp_int to the left by 128 bits and add uuid_int
        combined_int = (timestamp_int << 128) | uuid_int
        return combined_int

    def run(self):
        
        if self.is_replica: 
            self.logger.debug('Am repilca, assuming duties.')
            # Listen for heatbeats and server data
            hearbeat_timeout = self.heartbeat_intervall * self.timeout_multiplier
            heartbeat_listener = MulticastListener(self.server_multicast_address, self.server_multicast_port, self.listen_for_heartbeat, self.logger, True, hearbeat_timeout)
            heartbeat_listener.start()
        else:
            self.logger.debug('Am chat server, assuming duties.')
            # Start the hearbeat announcement thread
            heartbeat_thread = threading.Thread(target=self.send_heartbeat)
            heartbeat_thread.start()

            # Start the client announcement thread
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

if __name__ == "__main__":
    import sys
    is_replica = sys.argv[1] if len(sys.argv) > 1 else False
    server = ChatServer(is_replica)
    server.run()
