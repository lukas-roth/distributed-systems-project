import struct
import threading
import socket

class MulticastListener(threading.Thread):
        def __init__(self, mutlicast_address, port, handler, logger):
            threading.Thread.__init__(self)
            self.group = mutlicast_address
            self.port = port
            self.handler = handler
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(('', self.port))
            
            mreq = struct.pack("4sl", socket.inet_aton(self.group), socket.INADDR_ANY)
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            self.logger = logger

        def run(self):
            while True:
                data, addr = self.sock.recvfrom(1024)
                if data:
                    self.logger.debug(f"Received data from {addr}")
                    self.handler(data.decode('utf-8'))