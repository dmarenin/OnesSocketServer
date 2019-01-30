import json
from suds.cache import NoCache
from suds.client import Client
from kafka import KafkaProducer, KafkaConsumer
import _thread
import time


DEF_USER = 'administrator'
DEF_PASSWORD = '123123123'
DEF_WS = "http://192.168.5.60/AA/ws/gate.1cws?wsdl"

upd_event_lists = []


def send(username, password, jdata, method, ws, client):
    #try:
    #    client = Client(ws, username = username, password = password, cache = NoCache(), timeout = 60)
    #except Exception as e:
    #    print(e)
    #    return
    
    eval_str = 'client.service.%s(%s)' % (method, jdata and "%r" % jdata or '')
    
    try:
        res = eval(eval_str)
    except Exception as e:
        print(e)
        return
    
    res = json.loads(res)
    
    client = None

    print(f"""eval_str->{res}""")
    
    return res

def upd_loop(ind_upd_list):
    while True:
        do_upd_loop(ind_upd_list)

def do_upd_loop(ind_upd_list):
    t_loop = 0.250
    for x in upd_event_lists[ind_upd_list]:
        try:
            client = Client(ws, username = username, password = password, cache = NoCache(), timeout = 60)
        except Exception as e:
            print(e)
            continue
        
        send(x['username'], x['password'], x['jdata'], x['method'], x['ws'], client)
        
        upd_event_lists[ind_upd_list].remove(x)

    time.sleep(t_loop)


upd_event_lists.append([])
upd_event_lists.append([])
upd_event_lists.append([])
upd_event_lists.append([])
#upd_event_lists.append([])
#upd_event_lists.append([])
#upd_event_lists.append([])
#upd_event_lists.append([])

for i, val in enumerate(upd_event_lists):
    _thread.start_new_thread(upd_loop, (i,))


#producer = KafkaProducer(bootstrap_servers=['192.168.5.131:9092'])

consumer = KafkaConsumer('ones_ws_event', group_id='my-group', bootstrap_servers=['192.168.5.131:9092'])

for message in consumer:
    print(f"""ones_ws_event->{message}""")

    method = message.key.decode('utf-8')
    
    data = message.value.decode('utf-8')

    jdata = data
    
    username = DEF_USER
    password = DEF_PASSWORD
    ws = DEF_WS
    data = json.loads(data)
    
    if not data.get('event_setting') is None:
        event_setting = data.get('event_setting')
        
        username = event_setting.get('username')
        password = event_setting.get('password')
        ws = event_setting.get('ws')
        jdata = event_setting.get('jdata')

    lens = []
    for i, val in enumerate(upd_event_lists):
        lens.append((len(upd_event_lists[i]), i))

        min_list = min(lens)

        upd_event_lists[min_list[1]].append({'username':username, 'password':password, 'ws':ws, 'jdata':jdata, 'method':method})

    #_thread.start_new_thread(send, (username, password, jdata, method, ws,))

    #res = send(username, password, jdata, method, ws)

