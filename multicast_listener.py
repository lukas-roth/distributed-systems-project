import threading
import socket
import struct
import logging

class MulticastListener(threading.Thread):
    def __init__(self, multicast_address, port, handler, logger, has_timeout=False, timeout=0):
        super().__init__()
        self.group = multicast_address
        self.port = port
        self.handler = handler
        self.logger = logger
        self.running = True
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('', self.port))
        
        mreq = struct.pack("4sl", socket.inet_aton(self.group), socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        if has_timeout:
            self.sock.settimeout(timeout)
    
    def run(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(1024)
                if data:
                    self.logger.debug(f"Received data from {addr}")
                    self.handler(data.decode('utf-8'))
            except socket.timeout as e:
                self.logger.warning("Socket timeout occurred")
                self.handler(e)
                self.running = False
            except Exception as e:
                self.logger.error(f"Error receiving message: {e}")
                self.running = False
    
    def stop(self):
        self.running = False
        self.sock.close()
