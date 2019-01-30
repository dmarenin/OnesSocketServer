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
    producer = KafkaProducer(bootstrap_servers=['192.168.5.131:9092'])

    def __init__(self):
        self.handler.callback = self.callback
        _thread.start_new_thread(self.start_listen_event, ())

    def callback(self, server, request, client_address):              
        print(f"""CONNECTED LISTENER {client_address}""")
        
        u_ref = None

        while True:
            try:
                buf = request.recv(256).decode('utf-8')
            except:
                break
            
            #print(f"""{client_address} recv -> {buf}""")

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
            
            if len(user) == 0:
                del self.users[u_ref]
            
        unauthorize(user = u_ref, address = client_address[0], producer = self.producer)
    
    def start_listen_event(self):
        consumer = KafkaConsumer('ones_socket_send_user', group_id='my-group', bootstrap_servers=['192.168.5.131:9092'])
        
        for message in consumer:
            print(f"""kafka event -> {message.key.decode('utf-8')} {message.value.decode('utf-8')}""")

            if message.topic == 'ones_socket_send_user':
                u_ref = message.key.decode('utf-8')
                
                u_ref = u_ref.upper()
                
                user = self.users.get(u_ref)
                if user is None:
                    print(f"""user not found -> {u_ref}""")
                    continue
                if len(user)==0:
                    print(f"""len(user) == 0 -> {u_ref}""")
                    continue

                user_save = user.copy()

                for x in user_save:
                    sock = user_save[x]
                    if sock._closed:
                        print(f"""sock closed {u_ref}""")
                        continue
                    sock.sendall(message.value)
                    
                    print(f"""sock sent -> {message.key.decode('utf-8')} {message.value.decode('utf-8')}""")
                    
                    time.sleep(0.05)



def authorize(**data):
    user = data['user']
    address = data['address']
    producer = data['producer']

    key = user.encode('utf-8')
    value = address.encode('utf-8')
                     
    #print(f"""do send authorize start {user} {address}""")                   
    
    try:
        producer.send(f'user_auth', key=key, value=value)
    except:
        print(f"""send to kafka failed {value}""")

    #print(f"""do send authorize stop {user} {address}""")        
    
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



TCP_IP = '0.0.0.0'
TCP_PORT = 11000

if __name__ == '__main__':
    ones_serv = OnesSocketServer()
    server = ThreadedTCPServer((TCP_IP, TCP_PORT), OnesSocketServerHandler)
    
    print('starting ones socket server '+str(TCP_IP)+':'+str(TCP_PORT)+' (use <Ctrl-C> to stop)')

    server.serve_forever()
    
