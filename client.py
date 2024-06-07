from collections import deque
import random
import socket
import queue
import logging
import logging.config
import configparser
import curses
import time
import locale
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
        self.received_messages = deque(maxlen=10)

        # Load logging configuration with dynamic log file name
        log_filename = f'client_{self.client_id}.log'
        logging.config.fileConfig('logging.conf', defaults={'logfilename': log_filename})
        self.logger = logging.getLogger('clientLogger')
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
        for index, message in enumerate(messages.split('\n')):
            username, content = message.split(':', 1)
            if (index < 10) and message not in self.received_messages: 
                self.received_messages.append(message)
                self.update_chat()
            
            
                if self.message_count >= 5:
                    response = None
                    if random.random() < 0.55:
                        response = self.find_new_event(messages)
                    self.message_count = 0  
                    if response:
                        return response          
                
                if username != self.username:
                    response = self.find_response(content)
                    if response:
                        return response
        return None
    
    def draw_chat(self, stdscr):
        self.stdscr = stdscr
        # Set locale for Unicode support
        locale.setlocale(locale.LC_ALL, '')

        # Get the terminal dimensions
        height, width = stdscr.getmaxyx()

        # Clear screen
        stdscr.clear()
        
        # Draw the static part of the interface
        stdscr.addstr(0, 0, f"Chat View ({self.client_id}: {self.username})")
        stdscr.refresh()
    
    def update_chat(self):
        # Initialize color pairs
        curses.start_color()
        curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(2, curses.COLOR_MAGENTA, curses.COLOR_BLACK)

        # Define column widths
        username_width = 18
        text_start_col = username_width + 1  # One space after the username

        # Display messages
        for index, message in enumerate(self.received_messages):
            index += 1
            username, text = message.split(':')
            formatted_username = f"{username:<{username_width}}"
            
            # Display username with color
            if username == self.username:
                self.stdscr.addstr(index, 0, formatted_username, curses.color_pair(2))
            else:
                self.stdscr.addstr(index, 0, formatted_username, curses.color_pair(1))

            # Move to the starting column for text
            self.stdscr.move(index, text_start_col)
            # Clear to the end of the line
            self.stdscr.clrtoeol()
            # Display text message
            self.stdscr.addstr(index, text_start_col, text.strip())

        self.stdscr.refresh()
    
    def find_new_event(self, message):
        events = []
        if message:
            for event in self.responses:
                if event['event'] not in message:
                    events.append(event['event'])
        else:
            # If message is None, include all events
            for event in self.responses:
                events.append(event['event'])
        self.logger.debug(f"Selecting from the following events: {events}")
        return random.choice(events) if events else "Goal by Germany!"

    def handle_server_message(self, message):
        self.logger.info(f"Received message on multicast address {self.client_group_multicast_address}:{self.client_group_multicast_port} : '{message}'")
        self.message_queue.put(message)
    
    def start_multicast_listener(self):
        listener = MulticastListener(self.client_group_multicast_address, self.client_group_multicast_port, self.handle_server_message, self.logger)
        listener.start()

    def run(self):
        curses.wrapper(self._run_curses)

    def _run_curses(self, stdscr):
        self.stdscr = stdscr
        self.draw_chat(stdscr)
        self.start_multicast_listener()
        
        while True:
            try:
                advertised_server = self.message_queue.get(timeout=1)  # Adjust timeout as necessary
                
                if (self.current_server != advertised_server):
                    self.current_server = advertised_server 
                    server_ip, server_port = advertised_server.split(':')
                    server_port = int(server_port)
                    # Establish socket connection here
                    time.sleep(random.randint(1, self.wait_time_before_connect))  # Wait before connecting
                    self.connect_to_server(server_ip, server_port)
            except queue.Empty:
                # Check for user input
                self.stdscr.nodelay(True)
                key = self.stdscr.getch()
                if key == 26:  # ASCII value for Ctrl+Z
                    self.logger.info("Exiting client.")
                    break  # Exit the loop to end the program

    def connect_to_server(self, server_ip, server_port):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            self.logger.info(f"Connected to server at {server_ip}:{server_port}")
            try:
                msg = f"{self.username}: {self.find_new_event(None)}"
                sock.sendto(msg.encode('utf-8'), (server_ip, server_port))
                self.logger.info(f"Sent response: {msg}")
                while True:
                    data, addr = sock.recvfrom(1024*65)
                    if data:
                        self.logger.debug(f"Received data from {addr, data}")
                        chat_message = data.decode('utf-8')
                        self.message_count += 1
                        response = self.process_messages(chat_message)
                        if response is not None:
                            response = f"{self.username}: {response}"
                            time.sleep(random.randint(1, self.wait_time_before_message))
                            self.received_messages.append(response)
                            self.update_chat()
                            sock.sendto(response.encode('utf-8'), (server_ip, server_port))
                            self.logger.info(f"Sent response: {response}")

                    # Check for user input
                    self.stdscr.nodelay(True)
                    key = self.stdscr.getch()
                    if key == 26:  # ASCII value for Ctrl+Z
                        self.logger.info("Exiting client.")
                        exit()
                        

            except socket.error as e:
                self.logger.error(f"Socket error: {e}")
            #except Exception as e:
                #self.logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    import sys
    client_id = sys.argv[1] if len(sys.argv) > 1 else "default"
    client = ChatClient(client_id)
    client.run()
