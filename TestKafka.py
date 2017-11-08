#!/usr/bin/env python
import threading, logging, time
import multiprocessing
import os

from kafka import KafkaConsumer, KafkaProducer

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        
    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers=os.environ['SERVER'].split(','))

        while not self.stop_event.is_set():
            producer.send('my-topic', b"Hello World!")
            time.sleep(1)

        producer.close()

class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        
    def stop(self):
        self.stop_event.set()
        
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=os.environ['SERVER'].split(','),
                                 consumer_timeout_ms=10000)
        consumer.subscribe(['my-topic'])

        counter = 1
        while not self.stop_event.is_set():
            for message in consumer:
                print(str(counter) + " Received message " + str(message.value) + " on topic '" + str(message.topic) + "'")
                counter = counter + 1
                if self.stop_event.is_set():
                    break

        consumer.close()
        
        
def main():
    tasks = [
        Producer(),
        Consumer()
    ]

    for t in tasks:
        t.start()

    time.sleep(20)
    
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()
        
        
if __name__ == "__main__":
    main()