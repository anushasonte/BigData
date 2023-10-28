from kafka import KafkaConsumer
consumer = KafkaConsumer('sample',auto_offset_reset='earliest',
                         bootstrap_servers=['localhost:9092'],consumer_timeout_ms=1000)
for message in consumer:
    print("Key=%s,Value=%s" %((message.key).decode(),(message.value).decode()))
consumer.close()