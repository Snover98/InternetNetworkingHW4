
import socket, SocketServer, Queue, sys, time, threading
HTTP_PORT = 80
# previous_server = 3
# lock = threading.Lock()
SERV_HOST = '10.0.0.1'
servers = {'serv1': ('192.168.0.101', None), 'serv2': ('192.168.0.102', None), 'serv3': ('192.168.0.103', None)}

servers_work = {
				'serv1': {'M': 2.0, 'V': 1.0, 'P': 1.0},
				'serv2': {'M': 2.0, 'V': 1.0, 'P': 1.0},
				'serv3': {'M': 1.0, 'V': 3.0, 'P': 2.0},
				}

servers_handler = Servers(servers, servers_work)


class Servers:
	def __init__(self, servers_dict: dict, s_work: dict):
		self.servers_dict = servers_dict
		self.s_work = s_work
		
		self.serv_host = {s_name: (serv_host[s_name][0], createSocket(serv_host[s_name][0], HTTP_PORT)) for s_name in servers_dict}
		
		self.lock = threading.Lock()
		self.loads = {s_name: 0 for s_name in servers_dict}
	
	def get_req_server(self, file_type, file_size):
		self.lock.acquire()
		server_time = {s_name: self.loads[s_name] + self.request_time(file_type, file_size, s_name) for s_name in self.loads.keys()}
		self.lock.release()
		
		return min(server_time, key=server_time.get)
	
	def request_time(self, file_type, file_size, server_name):
		return self.s_work[server_name][file_type]*file_size
	
	def get_server_socket(self, serv_name):
		return servers[serv_name][1]


	def get_server_addr(self, serv_name):
		return servers[serv_name][0]


def print_time(string):
	cur_time = time.strftime('%H:%M:%S', time.localtime(time.time()))
    print(f'{cur_time}: {string}-----')


def createSocket(addr, port):
    for af, socktype, proto, canonname, sa in socket.getaddrinfo(addr, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
        try:
            new_sock = socket.socket(af, socktype, proto)
        except socket.error as msg:
            print_time(msg)
            new_sock = None
            continue
        else:
            try:
                new_sock.connect(sa)
            except socket.error as msg:
                print_time(msg)
                new_sock.close()
                new_sock = None
                continue
            else:
                break

    if new_sock is None:
        print_time('could not open socket')
        sys.exit(1)
    return new_sock


class LoadBalancerRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
		global servers_handler
		
        client_sock = self.request
        req = client_sock.recv(2)
        req_type, req_time = req[0], req[1]
        serv_name = servers_handler.get_req_server(req_type, req_time)
        print_time(f'recieved request {req} from {self.client_address[0]}, sending to {servers_handler.get_server_addr(serv_name)}')
        serv_sock = servers_handler.get_server_socket(serv_name)
        serv_sock.sendall(req)
        data = serv_sock.recv(2)
        client_sock.sendall(data)
        client_sock.close()


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


if __name__ == '__main__':
    try:
        print_time('LB Started')
        print_time('Connecting to servers')

        server = ThreadedTCPServer((SERV_HOST, HTTP_PORT), LoadBalancerRequestHandler)
        server.serve_forever()
    except socket.error as msg:
        print_time(msg)