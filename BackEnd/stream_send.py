from kafka import KafkaProducer
import pandas as pd
import time

bootstrap_servers = ['localhost:9092']
topicName = 'stream_prepro'

df = pd.read_csv('/home/am/123/StreamData_preprocessed (1).csv')

producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

for _, row in df.iterrows():
    #message = str(row.values.tolist())  
    message = '*#*'.join(str(value) for value in row)
    producer.send(topicName, message.encode('utf-8'))
    print(message,'\n')
    time.sleep(5)
producer.close()
