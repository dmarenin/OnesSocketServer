from flask import request
from server import app, producer
import json


HEADERS = {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST", "Access-Control-Allow-Headers": "Content-Type"}


@app.route('/')
@app.route('/index')
def index():
    return [], 200, HEADERS

@app.route('/api_user_event',  methods=['GET', 'POST'])
def api_user_event():
    if request.method == 'POST':
        body = request.data.decode('utf-8-sig')
        data = json.loads(body)
        
        user = data.get('u_ref', '')
        jdata = data.get('jdata', '')

    else:
        user = request.args.get('u_ref', '')
        jdata = request.args.get('jdata', '')

    if len(user) == 0 or len(jdata) == 0:
        return 'bad args', 400, HEADERS
    
    key = user.encode('utf-8')
    value = jdata.encode('utf-8')

    producer.send('ones_socket_send_user', key=key, value=value)

    return 'send to kafka', 200, HEADERS

