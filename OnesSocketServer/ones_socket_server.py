from socketserver import TCPServer, ThreadingMixIn, BaseRequestHandler
from datetime import datetime, date
from kafka import KafkaConsumer
from kafka import KafkaProducer
import _thread
import time

class ThreadedTCPServer(ThreadingMixIn, TCPServer):
    pass

class OnesSocketServerHandler(BaseRequestHandler):
    def handle(self):
        self.callback(self.server, self.request, self.client_address)

class OnesSocketServer():
    handler = OnesSocketServerHandler
    users = {}
    producer = KafkaProducer(bootstrap_servers=['192.168.7.55:9092'])

    def __init__(self):
        self.handler.callback = self.callback
        _thread.start_new_thread(self.start_listen_event)

    def callback(self, server, request, client_address):              
        print(f"""CONNECTED LISTENER {client_address}""")
        
        u_ref = None

        while True:
            try:
                buf = request.recv(256).decode('utf-8')
            except:
                break
            
            #print(f"""recv {buf}""")

            if not buf: 
                break

            buf = buf.strip('\n')
            if buf == 'logout': 
                break
            elif buf[0:5] == 'u_ref' and len(buf[6:]) > 1:
                u_ref = buf[6:]
               
                u_ref = u_ref.upper()

                self.users.setdefault(u_ref, dict())
                self.users[u_ref][client_address] = request

                authorize(user = u_ref, address = client_address[0], producer = self.producer)

        print(f"""DISCONNECTED LISTENER {client_address}""")
    
        if not u_ref is None:
            user = self.users.get(u_ref)

            if not user is None:
                sock = user[client_address]
                if not sock is None:
                    if sock._closed != True:
                        sock.close()
                    del user[client_address]

            #for x in user:
            #    sock = self.users[u_ref][client_address]
            #    if sock._closed:
            #        continue
            #    sock.close()
            #    del self.users[x][address]
            
            if len(user) == 0:
                del self.users[u_ref]
            
        unauthorize(user = u_ref, address = client_address[0], producer = self.producer)
        
        pass
    
    def start_listen_event(self):
        consumer = KafkaConsumer('ones_socket_send_user', 'ones_socket_send_user_send_all', 'ones_socket_add_listener', 
                                 group_id='my-group', bootstrap_servers=['192.168.7.55:9092'])
        
        for message in consumer:
            #print(message.value.decode('utf-8'))
            if message.topic=="ones_socket_send_user":
                u_ref = message.key.decode('utf-8')
                
                user = self.users.get(u_ref)
                if user is None:
                    continue
                if len(user)==0:
                    continue

                user_save = user.copy()

                for x in user_save:
                    sock = user_save[x]
                    if sock._closed:
                        continue
                    sock.sendall(message.value)
                    time.sleep(0.05)

            elif message.topic=="ones_socket_send_user_send_all":
                for x in self.users:
                    user_save = user.copy()
                    for y in user_save:
                        sock = user_save[y]
                        if sock._closed:
                            continue
                        sock.sendall(message.value)
                        time.sleep(0.05)

            elif message.topic=="ones_socket_add_listener":
                pass

            #user = self.users.get(u_ref)
            #if user is None:
            #    continue

            #for x in user:
            #    sock = user[x]
            #    if sock._closed:
            #        continue
            #    sock.sendall(message.value)


def authorize(**data):
    user = data['user']
    address = data['address']
    producer = data['producer']

    key = user.encode('utf-8')
    value = address.encode('utf-8')
                        
    try:
        producer.send(f'user_auth', key=key, value=value)
    except:
        print(f"""send to kafka failed {value}""")

    pass
   
def unauthorize(**data):
    user = data['user']
    address = data['address']
    producer = data['producer']
    
    key = user.encode('utf-8')
    value = address.encode('utf-8')
                        
    try:
        producer.send(f'user_unauth', key=key, value=value)
    except:
        print(f"""send to kafka failed {value}""")

    pass



TCP_IP = '192.168.7.220'
TCP_PORT = 11000

if __name__ == '__main__':
    ones_serv = OnesSocketServer()
    server = ThreadedTCPServer((TCP_IP, TCP_PORT), OnesSocketServerHandler)
    
    print('starting ones socket server '+str(TCP_IP)+':'+str(TCP_PORT)+' (use <Ctrl-C> to stop)')

    server.serve_forever()
    
