import json
from kafka import KafkaProducer, KafkaConsumer
#import _thread
import time
import requests
from requests.auth import HTTPBasicAuth


headers = {'Content-type': 'application/json'}
           #'Accept': 'text/plain'}


ONES_USER = ''
ONES_PASSWORD = ''
ONES_HOST = '192.168.555.560'
ONES_HTTP = 'http://192.168.555.560/AA'

consumer = KafkaConsumer('ones_http_event', group_id='my-group', bootstrap_servers=['192.168.5.131:9092'])

producer = KafkaProducer(bootstrap_servers=['192.168.5.131:9092'])

for message in consumer:
    print(f"""ones_http_event->{message}""")

    method = message.key.decode('utf-8')
    
    data = message.value.decode('utf-8')

    jdata = data
    
    username = ONES_USER
    password = ONES_PASSWORD

    data = json.loads(data)

    id_request = None

    if not data.get('id_request') is None:
        id_request = data.get('id_request')
    
    if not data.get('event_setting') is None:
        event_setting = data.get('event_setting')
        
        username = event_setting.get('username')
        password = event_setting.get('password')

        jdata = event_setting.get('jdata')
        
        if not jdata.get('id_request') is None:
            id_request = jdata.get('id_request')

    auth_ones = HTTPBasicAuth(username, password)
    
    r = None

    try:
        r = requests.get(f"""{ONES_HTTP}/hs/gate/v1?method={method}&data={jdata}""", headers=headers, auth=auth_ones)
            #data=res_str, 
    except Exception as e:
        print(e)
    
    res = ''
    
    if not r is None:
        if r.status_code != 200:
            res = r.reason
            print(r.reason)
        else:
            res = r.text

    if not id_request is None:
        key = id_request.encode('utf-8')
        value = res.encode('utf-8')
        
        producer.send('ones_http_event_response', key=key, value=value)
