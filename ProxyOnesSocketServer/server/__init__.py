from flask import Flask
from flask_cors import CORS
from kafka import KafkaProducer
import _thread

app = Flask(__name__)

producer = KafkaProducer(bootstrap_servers=['192.168.5.131:9092'])

CORS(app, support_credentials=True)

import server.views

