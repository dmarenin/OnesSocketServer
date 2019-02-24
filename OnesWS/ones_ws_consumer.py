
#from kafka import KafkaProducer, KafkaConsumer


#producer = KafkaProducer(bootstrap_servers=['192.168.5.131:9092'])

#consumer = KafkaConsumer('auto_call_event', group_id='my-group', bootstrap_servers=['192.168.5.131:9092'])
        
#for message in consumer:
#    print(f"""{message}""")
#    key = 'auto_call_set_status'.encode('utf-8')
#    try:
#        producer.send('ones_ws_event', key=message.key, value=message.value)
#    except:
#        print(f"""send to kafka failed {message.value}""")

