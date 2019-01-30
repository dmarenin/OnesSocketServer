from kafka import KafkaProducer, KafkaConsumer


producer = KafkaProducer(bootstrap_servers=['192.168.5.131:9092'])

consumer = KafkaConsumer('ami_client_event', group_id='my-group', bootstrap_servers=['192.168.5.131:9092'])
        
for message in consumer:
    print(f"""{message}""")
    try:
        producer.send('ones_socket_send_user', key=message.key, value=message.value)
    except:
        print(f"""send to kafka failed {message.value}""")

