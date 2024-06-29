from collections import deque
import random
import socket
import queue
import logging
import logging.config
import configparser
import curses
import threading
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
        self.is_connected_to_server = False
        self.trigger_response_event = threading.Event()
        self.running = True

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
            if (index < 10) and message not in self.received_messages: 
                self.received_messages.append(message)
                self.update_chat()
            
            
                
    
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
        if height > 11: 
            dash_line = '-' * width  # Create a line of dashes the width of the terminal
            stdscr.addstr(11, 0, dash_line)

        self.stdscr.addstr(12, 0, "Type:".strip())
        stdscr.refresh()
    
    def type_message_animation(self, message):
        typed_msg = ""
        username, content = message.split(':', 1)
        for index,char in enumerate(content):
            typed_msg = typed_msg + char
            self.stdscr.addstr(12, index+5, char.strip())
            self.stdscr.refresh()
            time.sleep(.05)
        time.sleep(.05)
        # Move to the starting column for text
        self.stdscr.move(12, 5)
        # Clear to the end of the line
        self.stdscr.clrtoeol()
        self.stdscr.refresh()

            

    
    def update_chat(self):
        # Initialize color pairs
        curses.start_color()
        curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(2, curses.COLOR_MAGENTA, curses.COLOR_BLACK)
        curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)

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
            elif "Info" in username or "Warning" in username:
                self.stdscr.addstr(index, 0, formatted_username, curses.color_pair(3))
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

    def handle_server_announcement_message(self, message):
        self.logger.info(f"Received message on multicast address {self.client_group_multicast_address}:{self.client_group_multicast_port} : '{message}'")
        self.message_queue.put(message)
    
    def start_multicast_listener(self):
        listener = MulticastListener(self.client_group_multicast_address, self.client_group_multicast_port, self.handle_server_announcement_message, self.logger)
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
                self.logger.debug(advertised_server)
                self.logger.debug(self.current_server)
                self.logger.debug(self.is_connected_to_server)
                self.logger.debug(self.current_server != advertised_server)
                if (self.current_server != advertised_server and not self.is_connected_to_server):
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
            

    def handle_response(self):
        
            self.logger.debug(f'Thread started!')
            try:
                while self.is_connected_to_server:
                    self.logger.debug(f'Waiting for response trigger: {self.trigger_response_event}')
                    self.trigger_response_event.wait()
                    self.logger.debug(f'Triggerd: {self.trigger_response_event}')
                    response = None
                    for message in self.received_messages:
                        username, content = message.split(':', 1)
                        self.logger.debug(f'Selecting, username: {username}, content {content}, message count {self.message_count}')
                        if self.message_count >= 5:
                            response = None
                            if random.random() < 0.75:
                                response = self.find_new_event(self.received_messages)
                            self.message_count = 0  

                            if response is not None:
                                break
                                    
                        if username != self.username:
                            response = self.find_response(content) 
                            if response is not None:
                                break
                    self.logger.debug(f'Selected response: {response}')            
                    if response is not None:
                        response = f"{self.username}: {response}"
                        time.sleep(random.randint(1, self.wait_time_before_message))
                        self.send_message(response)
                        self.logger.info(f"Sent response: {response}")
                    self.trigger_response_event.clear()
                    self.logger.debug(f'Response trigger cleared: {self.trigger_response_event}')
            except Exception as e:
                self.logger.error(e)
        
    
    def send_message(self, message):
        self.type_message_animation(message)
        self.received_messages.append(message)
        self.update_chat()
        self.server_sock.sendto(message.encode('utf-8'), (self.server_ip, self.server_port))

    def connect_to_server(self, server_ip, server_port):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            self.server_sock = sock
            self.server_ip = server_ip
            self.server_port = server_port
            self.logger.info(f"Connected to server at {server_ip}:{server_port}")
            self.is_connected_to_server = True
            sock.settimeout(10)
            try:
                if self.is_connected_to_server:
                    # When reconnecting after lost connection
                    self.received_messages.append("Info: Connection to server established")
                    self.update_chat()
                    time.sleep(1)

                msg = f"{self.username}: {self.find_new_event(None)}"
                self.send_message(msg)
                self.logger.info(f"Sent response: {msg}")

                self.logger.debug(f"Starting response thread.")
                response_handler = threading.Thread(target=self.handle_response)
                response_handler.start()

                while True:
                    try:
                        data, addr = sock.recvfrom(1024 * 65)
                        if data:
                            self.logger.debug(f"Received message from {addr, data}")
                            chat_message = data.decode('utf-8')
                            self.message_count += 1
                            self.process_messages(chat_message)
                            self.trigger_response_event.set()
                            self.logger.debug(f'Trigger response: {self.trigger_response_event}')

                        # Check for user input
                        self.stdscr.nodelay(True)
                        key = self.stdscr.getch()
                        if key == 26:  # ASCII value for Ctrl+Z
                            self.logger.info("Exiting client.")
                            exit()

                    except socket.timeout as e:
                        self.logger.error(f"Socket timeout: {e}")
                        self.is_connected_to_server = False
                        self.server_sock = None
                        self.received_messages.append("Warning!: Connection to server lost")
                        self.update_chat()
                        self.logger.debug('Done closing connection')
                        break  # Exit the while loop if a timeout occurs

            except socket.error as e:
                self.logger.error(f"Socket error: {e}")
                self.is_connected_to_server = False
                self.server_sock = None
                self.received_messages.append("Warning!: Connection to server lost")
                self.update_chat()
                self.logger.debug('Done closing connection')
            except Exception as e:
                self.logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    import sys
    client_id = sys.argv[1] if len(sys.argv) > 1 else "default"
    client = ChatClient(client_id)
    client.run()
