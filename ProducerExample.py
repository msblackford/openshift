import threading, logging, time
import os

from kafka import KafkaProducer

class Producer(threading.Thread):

    def run(self):
        print('Starting Producer')
        producer = KafkaProducer(bootstrap_servers="192.168.88.129:9092")
        print('Connected')
        while True:
            producer.send('my-topic', b"Hello Marcus!")
            print('Produced to topic')
            time.sleep(1)

        producer.close()



def main():
  produce = Producer()
  produce.run()




if __name__ == "__main__":
    main()