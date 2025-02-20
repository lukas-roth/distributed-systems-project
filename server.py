from collections import deque
from datetime import datetime, timezone
from multicast_listener import MulticastListener
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

# Load configuration
config = configparser.ConfigParser()
config.read('config.cfg')

class ChatServer:
    def __init__(self):
        self.client_multicast_address = config.get('SHARED', 'ClientGroupMulticastAddress')
        self.client_multicast_port = config.getint('SHARED', 'ClientGroupMulticasPort')
        self.heartbeat_multicast_address = config.get('SERVER', 'HeartbeatGroupMulticastAddress') 
        self.heartbeat_multicast_port = config.getint('SERVER', 'HeartbeatGroupMulticastPort')
        self.uid_group_multicast_address = config.get('SERVER', 'UidGroupMulticastAddress') 
        self.uid_group_multicast_port = config.getint('SERVER', 'UidGroupMulticasPort')
        self.server_port = None  
        self.election_port = 5002

        self.clients = []
        self.message_queue = queue.Queue()
        self.last_100_messages = deque(maxlen=10)

        self.election_msg_timeout = config.getfloat('SERVER', 'ElectionMessageTimeout')
        self.heartbeat_interval = config.getfloat('SERVER','HeartbeatInterval')
        self.timeout_multiplier = config.getfloat('SERVER','TimeoutMultiplier')
        self.heartbeat_timeout = self.heartbeat_interval * self.timeout_multiplier
        
        self.uuid = uuid.uuid4()
        self.unique_server_id = self.combine_timestamp_uuid("1970-01-01T00:00:00+00:00")
        self.replica_uids = {}

        self.is_ongoing_election = False
        self.received_ok_message = False

        self.election_trigger_event = threading.Event()
        self.running = True
        self.heartbeat_listener = None

        logging.config.fileConfig('logging.conf', defaults={'logfilename': f'server-{self.uuid}.log'})
        self.logger = logging.getLogger('serverLogger')
        self.logger.info(f"Server has uuid: {self.uuid}.")
        

        

    def announce_presence(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            ttl = struct.pack('b', 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            while self.running:
                if self.server_port is not None:
                    message = f"{socket.gethostbyname(socket.gethostname())}:{self.server_port}"
                    sock.sendto(message.encode('utf-8'), (self.client_multicast_address, self.client_multicast_port))
                    self.logger.debug(f"Announced server at {message}")
                    time.sleep(5)  # Announce every 5 seconds

    def handle_client_messages(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_sock:
            server_sock.bind(('', 0))
            self.server_port = server_sock.getsockname()[1]
            self.logger.info(f"Client handler listening on port {self.server_port}")
            while self.running:
                data, addr = server_sock.recvfrom(1024*65)
                if addr not in self.clients:
                    self.clients.append(addr)
                if data:
                    message = data.decode('utf-8')
                    self.logger.debug(f"Received message from {addr}: {message}")
                    self.message_queue.put((message, addr))
  
    
    def handle_election_messages (self):
        """
        Handles all election-related messages, including "ELECTION", "OK", and "COORDINATOR".
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server_sock:
            server_sock.bind(('', 0))
            self.election_port = server_sock.getsockname()[1]
            self.logger.info(f"Election handler listening on port {self.election_port}")
            self.send_uid_announcement()
            while self.running:
                data, addr = server_sock.recvfrom(1024 * 64)
                if data:
                    self.logger.debug(f"Received Message: {data}")
                    message = data.decode('utf-8')
                    if message.startswith("ELECTION:"):
                        self.handle_election_message(message)
                    elif message.startswith("OK:"):
                        self.handle_ok_message()
                    elif message.startswith("COORDINATOR:"):
                        self.handle_coordinator_message(message)
                

    def consume_message_queue(self):
        while self.running:
            try:
                message, addr = self.message_queue.get(timeout=1)
                self.logger.debug(f"Processing message: {message}")
                self.last_100_messages.append(message)
                self.broadcast_chat_messages(self.last_100_messages, addr)
            except queue.Empty:
                continue

    def broadcast_chat_messages(self, messages, exclude_addr):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock: 
            messages_str = "\n".join(reversed(messages))  # Convert the deque of messages to a single string
            for client in self.clients:
                if client != exclude_addr:
                    sock.sendto(messages_str.encode('utf-8'), client)
                    self.logger.debug(f"Exclude {exclude_addr}")
                    self.logger.debug(f"Sent message to client {client}: {messages_str}")


    def send_heartbeat(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            ttl = struct.pack('b', 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            while self.running:
                timestamp = datetime.now().isoformat()
                message = json.dumps({"data":list(self.last_100_messages), "time":timestamp})
                old_uid = self.unique_server_id
                new_uid = self.combine_timestamp_uuid(timestamp)
                self.unique_server_id = new_uid
                self.send_uid_update(old_uid,new_uid)
                sock.sendto(message.encode('utf-8'), (self.heartbeat_multicast_address, self.heartbeat_multicast_port))
                self.logger.debug(f"Leader sent heartbeat: {message}")
                time.sleep(self.heartbeat_interval)

    
    def send_uid_update(self, old_uid, new_uid):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            ttl = struct.pack('b', 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            hostname = socket.gethostname()
            ip_address = socket.gethostbyname(hostname)
            message = json.dumps({"old_uid": old_uid, "new_uid": new_uid, "address": f"{ip_address}:{self.election_port}", "id": str(self.uuid)})
            sock.sendto(message.encode('utf-8'), (self.uid_group_multicast_address, self.uid_group_multicast_port))
            self.logger.debug(f"Multicasted uid change: {message}")

    def send_uid_announcement(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
            ttl = struct.pack('b', 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
            hostname = socket.gethostname()
            ip_address = socket.gethostbyname(hostname)
            message = json.dumps({"Announcement": True, "new_uid": self.unique_server_id, "address": f"{ip_address}:{self.election_port}", "id": str(self.uuid)})
            sock.sendto(message.encode('utf-8'), (self.uid_group_multicast_address, self.uid_group_multicast_port))
            self.logger.debug(f"Announced uid: {message}")
            
    def listen_for_uid_update(self, data): 
        data = json.loads(data)
        if "Announcement" in data:
            new_uid = str(data["new_uid"])
            address = data["address"]
            id = data["id"]
            if str(self.unique_server_id) != new_uid:
                self.logger.debug(f"Received uid announcement: {data}")
                self.replica_uids[new_uid] = {"address" :address, "id": id}
                self.logger.debug(f"Current list of replica uids: {self.replica_uids}")
                self.send_uid_update(self.unique_server_id, self.unique_server_id)
        else:
            new_uid = str(data["new_uid"])
            old_uid = str(data["old_uid"])
            address = data["address"]
            id = data["id"]
        
            if str(self.unique_server_id) != new_uid:
                self.logger.debug(f"Received uid update: {data}")
                if old_uid in self.replica_uids:
                    self.replica_uids.pop(old_uid)
                    self.replica_uids[new_uid] = {"address" :address, "id": id}
                else:
                    self.replica_uids[new_uid] = {"address" :address, "id": id}
                self.logger.debug(f"Current list of replica uids: {self.replica_uids}")
        
            
    def listen_for_heartbeat(self, data):
        if isinstance(data, socket.timeout):
            self.logger.warning("Heartbeat timeout! Initiating bully election.")
            self.heartbeat_listener.stop()
            self.election_trigger_event.set()
        else:
            self.logger.debug(f"Received heartbeat: {data}")
            self.last_100_messages = deque(json.loads(data)["data"])
            time = json.loads(data)["time"]
            self.logger.debug(f"Time heartbeat: {time}")
            old_uid = self.unique_server_id
            new_uid = self.combine_timestamp_uuid(time)
            self.unique_server_id = new_uid
            self.send_uid_update(old_uid,new_uid)

    def listen_for_initial_heartbeat(self, data):
        if isinstance(data, socket.timeout):
            self.logger.warning("No initial server heartbeat! Initiating bully election.")
            self.heartbeat_listener.stop()
            self.election_trigger_event.set()
        else:
            self.logger.debug(f"Received intial heartbeat. There's already a server")
            self.logger.info('Am repilca, assuming duties.')
            self.heartbeat_listener.stop()
            self.logger.debug(f"Is intial heartbeat intial heartbeat_listener still alive?: {self.heartbeat_listener.is_alive()}")
            self.heartbeat_listener = MulticastListener(self.heartbeat_multicast_address, self.heartbeat_multicast_port, self.listen_for_heartbeat, self.logger, True, self.heartbeat_timeout)
            self.heartbeat_listener.start()
    
    def timestamp_to_int(self,timestamp):
        dt = datetime.fromisoformat(timestamp)
        dt = dt.replace(tzinfo=timezone.utc)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        return int((dt - epoch).total_seconds() * 1000)

    def uuid_to_int(self,server_uuid):
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
        if not self.is_ongoing_election: 
            self.is_ongoing_election = True
            self.received_ok_message = False
            self.logger.info(f"Server {self.uuid} is initiating an election")
            num_sent_elections = 0
            for uid in self.replica_uids:
                self.logger.debug(f"Initiate election: {uid}, self: {self.unique_server_id}, result{int(uid) > self.unique_server_id}")
                if int(uid) > self.unique_server_id:
                    self.send_election_message(uid)
                    num_sent_elections += 1
            self.logger.debug(f"Sent {num_sent_elections}, waiting for response")
            if num_sent_elections > 1:
                wait_time = num_sent_elections * self.election_msg_timeout
                self.logger.debug(f"Calculated wait time: {wait_time}")
                time.sleep(wait_time)
            else:
                wait_time = self.timeout_multiplier * self.election_msg_timeout
                self.logger.debug(f"Calculated wait time: {wait_time}")
                time.sleep(wait_time)
            self.logger.debug(f"Has received ok?: {self.received_ok_message}")
            if not self.received_ok_message:
                self.send_coordinator_message()
                self.logger.info(f"Server {self.uuid} is elected as the new leader")
                self.is_ongoing_election = False
                self.become_chat_server()

        self.is_ongoing_election = False
        self.received_ok_message = False

    def send_election_message(self, target_uid):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            message = f"ELECTION:{self.unique_server_id}"
            target_address, target_port = self.replica_uids[target_uid]["address"].split(":")
            sock.sendto(message.encode('utf-8'), (target_address, int(target_port)))
            self.logger.info(f"Server {self.uuid} sent ELECTION message to server {self.replica_uids[target_uid]["id"]}")

    def send_ok_message(self, target_uid):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            message = f"OK:{self.unique_server_id}"
            target_address, target_port = self.replica_uids[target_uid]["address"].split(":")
            sock.sendto(message.encode('utf-8'), (target_address, int(target_port)))
            self.logger.info(f"Server {self.uuid} sent OK message to Server {self.replica_uids[target_uid]["id"]}")

    def send_coordinator_message(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            message = f"COORDINATOR:{self.unique_server_id}"
            for uid in self.replica_uids:
                if uid != str(self.unique_server_id):
                    target_address, target_port = self.replica_uids[uid]["address"].split(":")
                    sock.sendto(message.encode('utf-8'), (target_address, int(target_port)))
                    self.logger.info(f"Server {self.uuid} sent COORDINATOR message to Server {self.replica_uids[uid]["id"]}")    

    def handle_election_message(self, message):
        sender_uid = message.split(":")[1]
        if int(sender_uid) > self.unique_server_id:
            self.logger.info(f"Server {self.uuid} received ELECTION message from Server {self.replica_uids[sender_uid]["id"]} and is conceding")
            self.send_ok_message(sender_uid)
            
        elif int(sender_uid) < self.unique_server_id:
            self.logger.info(f"Server {self.uuid} received ELECTION message from Server {self.replica_uids[sender_uid]["id"]} and is initiating own election")
            self.send_ok_message(sender_uid)
            self.logger.debug(f"Is election ongoing: {self.is_ongoing_election}")
            self.election_trigger_event.set()
                
                

    def handle_ok_message(self):
        self.logger.info(f"Server {self.uuid} received OK message and is waiting for new coordinator")
        self.received_ok_message = True
        
        
    def handle_coordinator_message(self, message):
        new_leader_id = message.split(":")[1]
        self.logger.info(f"Server {self.uuid} received COORDINATOR message. New leader is Server {self.replica_uids[new_leader_id]["id"]}")
        self.logger.info(f"Staying replica.")
        if self.heartbeat_listener.is_alive(): 
            self.heartbeat_listener.stop()
        self.heartbeat_listener = MulticastListener(self.heartbeat_multicast_address, self.heartbeat_multicast_port, self.listen_for_heartbeat, self.logger, True, self.heartbeat_timeout)
        self.heartbeat_listener.start()


    def run(self):
        uid_listener = MulticastListener(self.uid_group_multicast_address, self.uid_group_multicast_port, self.listen_for_uid_update, self.logger) 
        uid_listener.start()

        election_handler = threading.Thread(target=self.handle_election_messages)
        election_handler.start()
    

        self.heartbeat_listener = MulticastListener(self.heartbeat_multicast_address, self.heartbeat_multicast_port, self.listen_for_initial_heartbeat, self.logger, True, self.heartbeat_timeout)
        self.heartbeat_listener.start()
        self.heartbeat_listener.join()

        while self.running:
            self.election_trigger_event.wait()
            self.logger.debug(f"Election event set, ongoing: {self.is_ongoing_election}")
            if not self.is_ongoing_election:
                self.logger.debug(f"Starting election from run.")
                self.initiate_election()
                self.election_trigger_event.clear()


    def become_chat_server(self):
        self.logger.info('Am chat server, assuming duties.')
        
        heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        heartbeat_thread.start()

        consumer_thread = threading.Thread(target=self.consume_message_queue)
        consumer_thread.start()

        client_handling_thread = threading.Thread(target=self.handle_client_messages)
        client_handling_thread.start()

        client_announcement_thread = threading.Thread(target=self.announce_presence)
        client_announcement_thread.start()

        # Join threads when stopping
        client_announcement_thread.join()
        client_handling_thread.join()
        consumer_thread.join()
        heartbeat_thread.join()

if __name__ == "__main__":
    server = ChatServer()
    server.run()
