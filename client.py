import random
import socket
import queue
import logging
import logging.config
import configparser
import time
import json
from multicast_listener import MulticastListener

# Load configuration
config = configparser.ConfigParser()
config.read('config.cfg')

class ChatClient:
    def __init__(self, client_id):
        self.client_group_multicast_address = config.get('SHARED', 'ClientGroupMulticastAddress')
        self.client_group_multicast_port = config.getint('SHARED', 'ClientGroupMulticasPort')
        self.wait_time_before_connect = config.getint('CLIENT', 'WaitTimeBeforeConnect')
        self.wait_time_before_message = config.getint('CLIENT', 'WaitTimeBeforeMessage')
        self.message_queue = queue.Queue()
        self.responses = self.load_responses('comments.json')
        self.username = self.get_username('usernames.json')
        self.message_count = 0
        self.current_server = None
        self.client_id = client_id

        # Load logging configuration with dynamic log file name
        log_filename = f'client_{self.client_id}.log'
        logging.config.fileConfig('logging.conf', defaults={'logfilename': log_filename})
        self.logger = logging.getLogger()
        self.logger.debug(f"Client ID:{self.client_id}")
    
    def load_responses(self, filepath):
        with open(filepath, 'r', encoding='utf-8') as file:
            return json.load(file)
    
    def get_username(self, filepath):
        with open(filepath, 'r', encoding='utf-8') as file:
            usernames = json.load(file)
            return f"{random.choice(usernames['usernames'])}{random.randint(0,255)}"
    
    def find_response(self, message):
        for event in self.responses:
            if event['event'] in message:
                return random.choice(event['comments'])
        return None
    
    def process_messages(self, messages):
        for message in messages.split('\n'):
            self.logger.info(message)
            username, content = message.split(':', 1)
            if username != self.username:
                response = self.find_response(content)
                if response:
                    return response
        return None
    
    def find_new_event(self, message):
        events = []
        if message is not None:
            for event in self.responses:
                if event['event'] not in message:
                    events.append(event['event'])
        else:
            # If message is None, include all events
            for event in self.responses:
                events.append(event['event'])
        
        return random.choice(events) if events else None


    def handle_server_message(self, message):
        self.logger.info(f"Received message on multicast address {self.client_group_multicast_address}:{self.client_group_multicast_port} : '{message}'")
        self.message_queue.put(message)
    
    def start_multicast_listener(self):
        listener = MulticastListener(self.client_group_multicast_address, self.client_group_multicast_port, self.handle_server_message, self.logger)
        listener.start()

    def run(self):
        self.start_multicast_listener()
        
        while True:
            try:
                advertised_server = self.message_queue.get(timeout=1)  # Adjust timeout as necessary
                
                if (self.current_server != advertised_server):
                    self.current_server = advertised_server 
                    server_ip, server_port = advertised_server.split(':')
                    server_port = int(server_port)
                    # Establish socket connection here
                    time.sleep(self.wait_time_before_connect)  # Wait before connecting
                    self.connect_to_server(server_ip, server_port)
            except queue.Empty:
                continue  # Continue to check for new messages
    
    def connect_to_server(self, server_ip, server_port):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            self.logger.info(f"Connected to server at {server_ip}:{server_port}")
            try:
                msg = f"{self.username}: {self.find_new_event(None)}"
                sock.sendto(msg.encode('utf-8'), (server_ip, server_port))
                self.logger.info(f"Sent response: {msg}")
                while True:
                    data, addr = sock.recvfrom(1024)
                    if data:
                        self.logger.debug(f"Received data from {addr, data}")
                        chat_message = data.decode('utf-8')
                        

                        self.message_count += 1
                        if self.message_count >= 20:
                            if random.random() < 0.55:
                                response = f"{self.username}: {self.find_new_event(chat_message)}"
                                sock.sendto(response.encode('utf-8'), (server_ip, server_port))
                                self.logger.info(f"Sent response: {response}")
                            self.message_count = 0
                        else:
                            response = self.process_messages(chat_message)
                            if response:
                                response = f"{self.username}: {response}"
                                time.sleep(self.wait_time_before_message)
                                sock.sendto(response.encode('utf-8'), (server_ip, server_port))
                                self.logger.info(f"Sent response: {response}")

            except socket.error as e:
                self.logger.error(f"Socket error: {e}")
            except Exception as e:
                self.logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    import sys
    client_id = sys.argv[1] if len(sys.argv) > 1 else "default"
    client = ChatClient(client_id)
    client.run()