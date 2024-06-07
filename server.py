from collections import deque
from datetime import datetime, timezone
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
    def __init__(self):
        self.client_multicast_address = config.get('SHARED', 'ClientGroupMulticastAddress')
        self.client_multicast_port = config.getint('SHARED', 'ClientGroupMulticasPort')
        self.server_multicast_address = config.get('SERVER', 'ServerGroupMulticastAddress') #eigentlich "ServerGroupMulticastAddress"??
        self.server_multicast_port = config.getint('SERVER', 'ServerGroupMulticasPort')
        self.uid_multicast_address = config.get('SERVER', 'UidGroupMulticastAddress') 
        self.uid_multicast_port = config.getint('SERVER', 'UidGroupMulticasPort')
        self.server_port = 5001  # Example port for clients to connect
        self.election_port = 5002

        self.clients = []
        self.message_queue = queue.Queue()
        self.running = True
        self.last_100_messages = deque(maxlen=100)
        self.heartbeat_intervall = config.getint('SERVER','HeartbeatIntverval')
        self.timeout_multiplier = config.getint('SERVER','TimeoutMultiplier')
        self.uuid = uuid.uuid4()
        self.uid = self.combine_timestamp_uuid("1970-01-01T00:00:00+00:00")

        self.replica_uids = {}
        self.election_ongoing = False
        self.election_wait_time = config.getint('SERVER', 'ElectionWait')

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
            self.logger.info(f"Client handler listening on port {self.server_port}")
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
    
    def handle_election(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_sock:
            server_sock.bind(('', 0))
            self.election_port = server_sock.getsockname()[1]
            self.logger.info(f"Election handler listening on port {self.election_port}")
            while self.running:
                try:
                    data, addr = server_sock.recvfrom(1024)
                    if data:
                        message = data.decode('utf-8')
                        if message.startswith("ELECTION:"):
                            self.handle_election_message(message, addr)
                        elif message.startswith("OK:"):
                            self.handle_ok_message()
                        elif message.startswith("COORDINATOR:"):
                            self.handle_coordinator_message(message)
                except Exception as e:
                    self.logger.error(f"Error handling election message: {e}")

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
                message = json.dumps({"data":list(self.last_100_messages), "time":datetime.now().isoformat()})
                sock.sendto(message.encode('utf-8'), (self.server_multicast_address, self.server_multicast_port))
                self.logger.info(f"Leader sent heartbeat: {message}")
                time.sleep(self.heartbeat_intervall)
    
    def send_uid_update(self, old_uid, new_uid):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            ttl = struct.pack('b', 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            hostname = socket.gethostname()
            ip_address = socket.gethostbyname(hostname)
            message = json.dumps({"old_uid": old_uid, "new_uid": new_uid, "address": f"{ip_address}:{self.election_port}"})
            sock.sendto(message.encode('utf-8'), (self.uid_multicast_address, self.uid_multicast_port))
            self.logger.info(f"Multicasted uid change: {message}")
            
    def listen_for_uid_update(self, data): 
        data = json.loads(data)
        new_uid = str(data["new_uid"])
        old_uid = str(data["old_uid"])
        address = data["address"]
       
        if str(self.uid) != new_uid:
            self.logger.info(f"Received uid update: {data}")
            if old_uid in self.replica_uids:
                self.replica_uids[new_uid] = self.replica_uids.pop(old_uid)
            else:
                self.replica_uids[new_uid] = address
            self.logger.debug(f"Current list of replica uids: {self.replica_uids}")
        
            
    def listen_for_heartbeat(self, data):
        if isinstance(data, socket.timeout):
            self.logger.warning("Heartbeat timeout! Initiating bully election.")
            self.initiate_election()
        else:
            self.logger.info(f"Received heartbeat: {data}")
            self.last_100_messages = deque(json.loads(data)["data"])
            time = json.loads(data)["time"]
            self.logger.debug(f"Time heartbeat: {time}")
            old_uid = self.uid
            new_uid = self.combine_timestamp_uuid(time)
            self.uid = new_uid
            self.send_uid_update(old_uid,new_uid)
            
    
    def timestamp_to_int(self,timestamp):
        """
        Convert a timestamp to an integer representing milliseconds since the epoch.
        """
        dt = datetime.fromisoformat(timestamp)
        dt = dt.replace(tzinfo=timezone.utc)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        return int((dt - epoch).total_seconds() * 1000)

    def uuid_to_int(self,server_uuid):
        """
        Convert a UUID to an integer.
        """
        return uuid.UUID(str(server_uuid)).int

    def combine_timestamp_uuid(self,timestamp):
        """
        Combine the timestamp and UUID into a single integer.
        """
        timestamp_int = self.timestamp_to_int(timestamp)
        uuid_int = self.uuid_to_int(self.uuid)
        
        # Shift timestamp_int to the left by 128 bits and add uuid_int
        combined_int = (timestamp_int << 128) | uuid_int
        return combined_int
    
    def initiate_election(self):
        if not self.election_ongoing:
            self.election_ongoing = True
            self.logger.info(f"Server {self.uid} is initiating an election")
            for uid in self.replica_uids:
                if uid > self.uid:
                    self.send_election_message(uid)

            # Wait for responses
            time.sleep(self.election_wait_time)
            if self.election_ongoing:
                self.send_coordinator_message()
                self.logger.info(f"Server {self.uid} is elected as the new leader")
                self.election_ongoing = False
                self.become_chat_server()
                #todo stop current replica duties 

    def send_election_message(self, target_uid):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            message = f"ELECTION:{self.uid}"
            target_address, target_port = self.replica_uids[target_uid].split(":")
            sock.sendto(message.encode('utf-8'), (target_address, target_port))
            self.logger.info(f"Server {self.uid} sent ELECTION message to server {target_uid}")

    def send_ok_message(self, target_uid):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            message = f"OK:{self.uid}"
            target_address, target_port = self.replica_uids[target_uid].split(":")
            sock.sendto(message.encode('utf-8'), (target_address, target_port))
            self.logger.info(f"Server {self.uid} sent OK message to Server {target_uid}")

    def send_coordinator_message(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            message = f"COORDINATOR:{self.uid}"
            for uid in self.replica_uids:
                if uid != self.uid:
                    target_address, target_port = self.replica_uids[uid].split(":")
                    sock.sendto(message.encode('utf-8'), (target_address, target_port))
                    self.logger.info(f"Server {self.uid} sent COORDINATOR message to Server {uid}")    

    def handle_election_message(self, message, addr):
        sender_uid = int(message.split(":")[1])
        if sender_uid > self.uid:
            self.logger.info(f"Server {self.uid} received ELECTION message from Server {sender_uid} and is conceding")
            self.send_ok_message(sender_uid)
        elif sender_uid < self.uid:
            self.logger.info(f"Server {self.uid} received ELECTION message from Server {sender_uid} and is initiating own election")
            self.initiate_election()

    def handle_ok_message(self):
        self.logger.info(f"Server {self.uid} received OK message and is waiting for new coordinator")
        self.election_ongoing = False

    def handle_coordinator_message(self, message):
        new_leader_id = int(message.split(":")[1])
        self.logger.info(f"Server {self.uid} received COORDINATOR message. New leader is Server {new_leader_id}")
        self.election_ongoing = False


    def run(self):
        uid_listener = MulticastListener(self.uid_multicast_address, self.uid_multicast_port, self.listen_for_uid_update, self.logger)
        uid_listener.start()

        election_handler = threading.Thread(target=self.handle_election)
        election_handler.start()
    
        self.logger.debug('Am repilca, assuming duties.')
        # Listen for heatbeats and server data
        hearbeat_timeout = self.heartbeat_intervall * self.timeout_multiplier
        heartbeat_listener = MulticastListener(self.server_multicast_address, self.server_multicast_port, self.listen_for_heartbeat, self.logger, True, hearbeat_timeout)
        heartbeat_listener.start()
        
            
    def become_chat_server(self):
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
    server = ChatServer()
    server.run()
