from kafka import KafkaProducer
from time import sleep
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('sample',key=b'MYID',value =b'A20518694')
producer.send('sample',key=b'MYNAME',value =b'Anusha Sonte Parameshwar')
producer.send('sample',key=b'MYEYECOLOR',value =b'Black')
sleep(10)
producer.close()