import socket, socketserver, sys, time, threading
from collections import deque
HTTP_PORT = 80
# previous_server = 3
# lock = threading.Lock()
SERV_HOST = '10.0.0.1'
servers = {'serv1': ('192.168.0.101', None), 'serv2': ('192.168.0.102', None), 'serv3': ('192.168.0.103', None)}

servers_work = {
				'serv1': {ord('M'): 2.0, ord('V'): 1.0, ord('P'): 1.0},
				'serv2': {ord('M'): 2.0, ord('V'): 1.0, ord('P'): 1.0},
				'serv3': {ord('M'): 1.0, ord('V'): 3.0, ord('P'): 2.0},
			}


def print_time(string):
	print('%s: %s-----' % (time.strftime('%H:%M:%S', time.localtime(time.time())), string))


def createSocket(addr, port):
	for af, socktype, proto, canonname, sa in socket.getaddrinfo(addr, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
		new_sock = socket.socket(af, socktype, proto)
	return new_sock


class Servers:
	def __init__(self, servers_dict: dict, s_work: dict):
		self.servers_dict = servers_dict
		self.s_work = s_work
		
		self.servers = {s_name: (servers_dict[s_name][0], createSocket(servers_dict[s_name][0], HTTP_PORT)) for s_name in servers_dict}
		
		self.lock = threading.Lock()
		self.loads = {s_name: 0 for s_name in servers_dict}
		self.queues = {s_name: deque() for s_name in servers_dict}
	
	def get_req_server(self, file_type, file_size):
		self.lock.acquire()
		
		server_time = {s_name: self.loads[s_name] + self.request_time(file_type, file_size, s_name) for s_name in self.loads.keys()}
		best_server = min(server_time, key=server_time.get)
		self.queues[best_server].appendleft(self.request_time(file_type, file_size, best_server))
		self.loads[best_server] += self.queues[best_server][0]
		
		self.lock.release()
		
		return min(server_time, key=server_time.get)
	
	def remove_time(self, server_name):
		self.lock.acquire()
		
		time_passed = self.queues[server_name].pop()
		
		if time_passed > 0:
			for s_name, queue in self.queues.items():
				if s_name != server_name and len(queue) > 0:
					queue[-1] -= time_passed
				self.loads[s_name] -= time_passed
		
		self.lock.release()
	
	def request_time(self, file_type, file_size, server_name):
		return self.s_work[server_name][file_type]*file_size
	
	def get_server_socket(self, serv_name):
		return self.servers[serv_name][1]


	def get_server_addr(self, serv_name):
		return self.servers[serv_name][0]


servers_handler = Servers(servers, servers_work)

#class LoadBalancerRequestHandler(socketserver.BaseRequestHandler):

#	def handle(self):
#		global servers_handler
#		
#		req = client_sock.recv(2)
#		req_type, req_time = req[0], req[1]
#		serv_name = servers_handler.get_req_server(req_type, req_time)
#		print_time('recieved request %s from %s, sending to %s' % (req, self.client_address[0], servers_handler.get_server_addr(serv_name)))
#		serv_sock = servers_handler.get_server_socket(serv_name)
#		serv_sock.sendall(req)
#		data = serv_sock.recv(2)
#		servers_handler.remove_time(serv_name)
#		client_sock.sendall(data)
#		client_sock.close()


def handle_client(clientsocket, address):
	msg = clientsocket.recv(1024)
	req_type = msg[0]
	req_time = msg[1]
	serv_name = servers_handler.get_req_server(req_type, req_time)
	print_time('recieved request %s from %s, sending to %s' % (req, self.client_address[0], servers_handler.get_server_addr(serv_name)))
	serv_sock = servers_handler.get_server_socket(serv_name)
	serv_sock.sendall(req)
	data = serv_sock.recv(2)
	servers_handler.remove_time(serv_name)
	client_sock.sendall(data)
	client_sock.close()

	os._exit(0)


if __name__ == '__main__':
	print_time('LB Started')
	print_time('Connecting to servers')
	server_sock = socket.socket()
	server_sock.bind('10.0.0.1')
	server_sock.listen()

	while True:
		clientsocket, address = server_sock.accept()
		thread_id = os.fork()
		if thread_id == 0:
			handle_client(clientsocket, address)










	
