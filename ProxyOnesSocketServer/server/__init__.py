from flask import Flask
from flask_cors import CORS
from kafka import KafkaProducer, KafkaConsumer
import _thread


def start_listen_event():
    consumer = KafkaConsumer('ami_client_event', 'upd_external_event_trigger',group_id='my-group', bootstrap_servers=['192.168.7.55:9092'])
        
    for message in consumer:
        try:
            producer.send(f'ones_socket_send_user', key=message.key, value=message.value)
        except:
            print(f"""send to kafka failed {value}""")
   
app = Flask(__name__)

producer = KafkaProducer(bootstrap_servers=['192.168.7.55:9092'])

CORS(app, support_credentials=True)

import server.views

_thread.start_new_thread(start_listen_event)

