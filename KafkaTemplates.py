import threading, logging, time
import os

from kafka import KafkaProducer, KafkaConsumer


class Producer(threading.Thread):

    def run(self):

        #setting up server variables

        topic = 'my-topic'      # 'ctm-transactions-topic'
        group_id = ''
        
        servers = os.environ['SERVER'].split(',') #Kafka broker server system environment variable $SERVER

        
        print('Starting Producer on: \n Server: {}\n Consumer Group ID: {}\n Topic: {}'.format(
            str(servers),
            str(group_id),
            str(topic)
        ))
        producer = KafkaProducer(bootstrap_servers="192.168.88.129:9092")
        print('Connected')
        while True:
            producer.send('my-topic', b"Hello Marcus!")
            print('Produced to topic')
            time.sleep(1)

        producer.close()







